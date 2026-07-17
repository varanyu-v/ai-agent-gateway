"""Shared runtime for standalone agent services.

Every agent is its own HTTP service built from an AgentDefinition through
create_agent_app(). The contract is intentionally uniform so the orchestrator
can integrate any number of agents without code changes:

- `GET /.well-known/agent-card` — machine-readable card (A2A-style) describing
  identity, workflow, and capabilities; the orchestrator discovers agents here.
- `POST /runs` — plan one run and return an AgentDecision. Agents only decide;
  they never talk to Kafka or databases. The orchestrator enforces Casbin
  policy on the decision and executes the side effects, so a buggy or
  compromised agent cannot widen data access.
- `GET /health` — liveness.

The LiteLLM planner and its Langfuse generation span live here; the span is
parented to the orchestrator's `agent-run` root through the
`x-langfuse-traceparent` header so one logical Langfuse trace spans services.
"""

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Literal

import httpx
from fastapi import FastAPI, Header
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph
from langgraph.store.memory import InMemoryStore
from opentelemetry import trace as otel_trace
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind, Status, StatusCode, Tracer
from pydantic import BaseModel, Field
from typing_extensions import TypedDict

from apps.langfuse_utils import (
    build_trace_attributes,
    context_from_traceparent,
    langfuse_json,
    langfuse_payload,
    litellm_usage_attributes,
    record_span_error,
    trace_id_hex,
)
from apps.observability import (
    clean_attributes,
    setup_langfuse_observability,
    setup_observability,
)


AGENT_PROTOCOL = "ptvn.agent/v1"

LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "http://localhost:4000/v1").rstrip("/")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "")
LITELLM_MODEL = os.getenv("LITELLM_MODEL", "")
LITELLM_TIMEOUT_SECONDS = float(os.getenv("LITELLM_TIMEOUT_SECONDS", "30"))

LLM_PLANNER_SYSTEM_PROMPT = """
You route enterprise agent requests to one workflow action.
Return only a JSON object with this shape:
{"action":"sql|report|approval","reason":"short reason"}

Rules:
- Choose "approval" for destructive, write, delete, data approval, or other human approval requests.
- Choose "report" only for explicit report, dashboard, document, or export generation requests when report is allowed.
- Choose "sql" for data lookup, analytics, list, show, count, compare, summarize, or read-only database questions.
- Do not write SQL or tool payloads.
""".strip()


class AgentRunRequest(BaseModel):
    """Run payload the orchestrator sends to every agent service."""

    request_id: str
    tenant_id: str
    user_id: str
    agent_id: str
    message: str
    thread_id: str | None = None
    allowed_permissions: list[str] = Field(default_factory=list)
    policy_subjects: list[str] = Field(default_factory=list)


class AgentDecision(BaseModel):
    """What the agent wants to happen; the orchestrator enforces and executes."""

    action: Literal["tool", "approval", "deny"]
    workflow: str
    planner_action: str
    planner_source: str = "fallback"
    tool: str | None = None
    tool_input: dict[str, Any] | None = None
    required_permission: str | None = None
    audit_event: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class AgentDefinition:
    agent_id: str
    name: str
    workflow: str
    description: str
    version: str
    actions: frozenset[str]
    required_permissions: tuple[str, ...]
    tools: tuple[str, ...]
    fallback_action: Callable[[str], str]
    decide: Callable[[str, AgentRunRequest], AgentDecision]


class AgentState(TypedDict):
    request_id: str
    tenant_id: str
    user_id: str
    agent_id: str
    message: str
    thread_id: str | None
    allowed_permissions: list[str]
    policy_subjects: list[str]
    langfuse_traceparent: str | None
    decision: dict[str, Any] | None


def agent_card(definition: AgentDefinition) -> dict[str, Any]:
    return {
        "protocol": AGENT_PROTOCOL,
        "id": definition.agent_id,
        "name": definition.name,
        "description": definition.description,
        "version": definition.version,
        "workflow": definition.workflow,
        "capabilities": {"actions": sorted(definition.actions)},
        "requirements": {
            "permissions": list(definition.required_permissions),
            "tools": list(definition.tools),
        },
        "endpoints": {"run": "/runs", "health": "/health"},
    }


def planner_trace_context(request: AgentRunRequest) -> dict[str, Any]:
    return {
        "request_id": request.request_id,
        "tenant_id": request.tenant_id,
        "user_id": request.user_id,
        "agent_id": request.agent_id,
        "session_id": request.thread_id or request.request_id,
        "trace_input": langfuse_json(langfuse_payload(request.message)),
        "tempo_trace_id": trace_id_hex(otel_trace.get_current_span()),
    }


@contextmanager
def langfuse_generation_span(
    langfuse_tracer: Tracer,
    workflow: str,
    message: str,
    context: dict[str, Any] | None,
    parent_context: Context | None,
):
    """Record a planner generation under the orchestrator's Langfuse root."""
    span = langfuse_tracer.start_span(
        "agent.llm_plan",
        context=parent_context if parent_context is not None else Context(),
        kind=SpanKind.CLIENT,
        attributes=clean_attributes(
            {
                "app.workflow": workflow,
                "app.planner.model": LITELLM_MODEL or None,
                "app.user_message.length": len(message),
                "gen_ai.operation.name": "chat",
                "gen_ai.system": "openai",
                "gen_ai.request.model": LITELLM_MODEL or None,
                "langfuse.observation.type": "generation",
                "langfuse.observation.model.name": LITELLM_MODEL or None,
                "langfuse.observation.metadata.kind": "planner",
                "langfuse.observation.metadata.provider": "litellm",
                **build_trace_attributes(workflow, context),
            },
        ),
    )
    try:
        yield span
    except BaseException as exc:
        record_span_error(span, exc)
        raise
    finally:
        span.end()


async def litellm_plan_action(
    definition: AgentDefinition,
    message: str,
    context: dict[str, Any] | None,
    parent_context: Context | None,
    langfuse_tracer: Tracer,
) -> str | None:
    with langfuse_generation_span(
        langfuse_tracer,
        definition.workflow,
        message,
        context,
        parent_context,
    ) as span:
        if not LITELLM_API_KEY or not LITELLM_MODEL:
            span.set_attribute("app.planner.result", "not_configured")
            return None

        allowed_actions = definition.actions
        payload = {
            "model": LITELLM_MODEL,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": LLM_PLANNER_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"Workflow: {definition.workflow}\n"
                        f"Allowed actions: {', '.join(sorted(allowed_actions))}\n"
                        f"User message: {message}"
                    ),
                },
            ],
        }
        span.set_attribute(
            "langfuse.observation.input",
            langfuse_json(langfuse_payload(payload["messages"])),
        )
        headers = {
            "Authorization": f"Bearer {LITELLM_API_KEY}",
            "Content-Type": "application/json",
        }

        try:
            async with httpx.AsyncClient(timeout=LITELLM_TIMEOUT_SECONDS) as client:
                response = await client.post(
                    f"{LITELLM_BASE_URL}/chat/completions",
                    headers=headers,
                    json=payload,
                )
                span.set_attribute("http.response.status_code", response.status_code)
                response.raise_for_status()

            response_body = response.json()
            content = response_body["choices"][0]["message"]["content"]
            span.set_attributes(
                clean_attributes(
                    {
                        "gen_ai.response.model": response_body.get("model")
                        or LITELLM_MODEL,
                        "langfuse.observation.model.name": response_body.get("model")
                        or LITELLM_MODEL,
                        "langfuse.observation.output": langfuse_json(
                            langfuse_payload(content),
                        ),
                    },
                ),
            )
            span.set_attributes(
                litellm_usage_attributes(response_body.get("usage")),
            )
            decision = json.loads(content)
        except (
            httpx.HTTPError,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, "LiteLLM planning failed"))
            span.set_attributes(
                {
                    "app.planner.result": "fallback",
                    "langfuse.observation.level": "ERROR",
                    "langfuse.observation.status_message": str(exc),
                },
            )
            return None

        action = str(decision.get("action", "")).strip().lower()
        if action in allowed_actions:
            span.set_attributes(
                {"app.planner.result": "selected", "app.workflow.action": action},
            )
            return action

        span.set_attributes(
            {"app.planner.result": "invalid_action", "app.workflow.action": action},
        )
        return None


async def choose_plan_action(
    definition: AgentDefinition,
    message: str,
    context: dict[str, Any] | None,
    parent_context: Context | None,
    tracer: Tracer,
    langfuse_tracer: Tracer,
) -> tuple[str, str]:
    with tracer.start_as_current_span(
        "agent.choose_plan_action",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.workflow": definition.workflow,
                "app.user_message.length": len(message),
            },
        ),
    ) as span:
        action = await litellm_plan_action(
            definition,
            message,
            context,
            parent_context,
            langfuse_tracer,
        )
        if action is not None:
            span.set_attributes(
                {"app.workflow.action": action, "app.planner.source": "litellm"},
            )
            return action, "litellm"

        fallback_action = definition.fallback_action(message)
        span.set_attributes(
            {
                "app.workflow.action": fallback_action,
                "app.planner.source": "fallback",
            },
        )
        return fallback_action, "fallback"


def build_workflow(
    definition: AgentDefinition,
    tracer: Tracer,
    langfuse_tracer: Tracer,
) -> Any:
    async def plan(state: AgentState) -> dict[str, Any]:
        with tracer.start_as_current_span(
            f"agent.plan.{definition.workflow}",
            kind=SpanKind.INTERNAL,
            attributes=clean_attributes(
                {
                    "app.request_id": state["request_id"],
                    "app.agent_id": state["agent_id"],
                    "app.workflow": definition.workflow,
                    "app.allowed_permissions": state["allowed_permissions"],
                },
            ),
        ) as span:
            request = AgentRunRequest(
                request_id=state["request_id"],
                tenant_id=state["tenant_id"],
                user_id=state["user_id"],
                agent_id=state["agent_id"],
                message=state["message"],
                thread_id=state["thread_id"],
                allowed_permissions=state["allowed_permissions"],
                policy_subjects=state["policy_subjects"],
            )
            action, planner_source = await choose_plan_action(
                definition,
                request.message,
                planner_trace_context(request),
                context_from_traceparent(state["langfuse_traceparent"]),
                tracer,
                langfuse_tracer,
            )
            decision = definition.decide(action, request).model_copy(
                update={"planner_source": planner_source},
            )
            span.set_attributes(
                clean_attributes(
                    {
                        "app.workflow.action": action,
                        "app.planner.source": planner_source,
                        "app.decision.action": decision.action,
                        "app.tool": decision.tool,
                    },
                ),
            )
            return {"decision": decision.model_dump()}

    builder = StateGraph(AgentState)
    builder.add_node("plan", plan)
    builder.set_entry_point("plan")
    builder.add_edge("plan", END)
    return builder.compile(
        checkpointer=InMemorySaver(),
        store=InMemoryStore(),
    )


def create_agent_app(definition: AgentDefinition) -> FastAPI:
    app = FastAPI(title=definition.name)
    tracer = setup_observability(definition.agent_id, app)
    langfuse_tracer = setup_langfuse_observability(definition.agent_id)
    workflow = build_workflow(definition, tracer, langfuse_tracer)

    @app.get("/.well-known/agent-card")
    async def read_agent_card() -> dict[str, Any]:
        return agent_card(definition)

    @app.get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": definition.agent_id}

    @app.post("/runs")
    async def run(
        body: AgentRunRequest,
        x_langfuse_traceparent: str | None = Header(default=None),
    ) -> dict[str, Any]:
        state: AgentState = {
            "request_id": body.request_id,
            "tenant_id": body.tenant_id,
            "user_id": body.user_id,
            "agent_id": body.agent_id,
            "message": body.message,
            "thread_id": body.thread_id,
            "allowed_permissions": body.allowed_permissions,
            "policy_subjects": body.policy_subjects,
            "langfuse_traceparent": x_langfuse_traceparent,
            "decision": None,
        }
        result = await workflow.ainvoke(
            state,
            {"configurable": {"thread_id": body.thread_id or body.request_id}},
        )
        return {
            "protocol": AGENT_PROTOCOL,
            "agent_id": definition.agent_id,
            "request_id": body.request_id,
            "workflow": definition.workflow,
            "decision": result["decision"],
        }

    return app
