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

Long-running agents can return `action="async"` and then drive the run
themselves through the orchestrator's tool-broker callback API using
ToolBrokerClient: request tools, poll their results, and report the final
outcome. Every requested tool is still Casbin-checked by the orchestrator
against the policy subjects minted for the original request, so the callback
path grants no more access than the decision path.

The LiteLLM planner and its Langfuse generation span live here; the span is
parented to the orchestrator's `agent-run` root through the
`x-langfuse-traceparent` header so one logical Langfuse trace spans services.
"""

import asyncio
import json
import os
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal

import httpx
from fastapi import FastAPI, Header
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph
from langgraph.store.memory import InMemoryStore
from opentelemetry import trace as otel_trace
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind, Tracer
from pydantic import BaseModel, Field
from typing_extensions import TypedDict

from apps import litellm_client
from apps.langfuse_utils import (
    build_trace_attributes,
    context_from_traceparent,
    langfuse_json,
    langfuse_payload,
    trace_id_hex,
)
from apps.observability import (
    clean_attributes,
    setup_langfuse_observability,
    setup_observability,
)
from apps.persona import PERSONA, REPLY_LANGUAGE_RULE, Persona


AGENT_PROTOCOL = "ptvn.agent/v1"

PLANNER_ERROR_MESSAGE = "LiteLLM planning failed"

# Tool-broker callback API (async runs). Agents call back into the
# orchestrator to request tools, poll results, and report the final outcome.
ORCHESTRATOR_CALLBACK_URL = os.getenv(
    "ORCHESTRATOR_CALLBACK_URL",
    os.getenv("ORCH_URL", "http://localhost:8001"),
).rstrip("/")
AGENT_CALLBACK_TOKEN = os.getenv("AGENT_CALLBACK_TOKEN", "")
TOOL_BROKER_POLL_INTERVAL_SECONDS = float(
    os.getenv("TOOL_BROKER_POLL_INTERVAL_SECONDS", "0.5"),
)
TOOL_BROKER_TIMEOUT_SECONDS = float(os.getenv("TOOL_BROKER_TIMEOUT_SECONDS", "120"))

LLM_PLANNER_RULES = """
You plan one enterprise agent run: pick exactly one workflow action for the
user's message and, for data actions, the arguments that answer it.
Return only a JSON object with this shape:
{"action":"<allowed action>","reason":"short reason","arguments":{},"reply":""}

Rules:
- Pick "action" only from the allowed actions in the request.
- Choose "chat" for greetings, small talk, thanks, and anything outside this
  agent's domain. Leave "arguments" empty and never pick a data action for such
  messages, and write the user-facing answer in "reply":
  - Greetings and small talk: 1-3 friendly sentences that also say what this
    agent can help with.
  - Anything outside this agent's domain: first say plainly that you cannot
    reach the data this message asks for, then say what you can help with.
    Never answer such a message with only a greeting or a summary of your own
    abilities — that reads as if the request was handled. Do not claim the data
    does not exist or is unavailable in general when the truth is that it is
    outside what this agent can reach.
- Choose "approval" for destructive, write, delete, data approval, or other
  human approval requests.
- Choose "report" only for explicit report, dashboard, document, or export
  generation requests when report is allowed.
- For data actions, build "arguments" exactly as the agent guidance in the
  request describes; never invent argument fields it does not mention.
- Leave "reply" empty for every action except "chat".
""".strip()


def build_planner_system_prompt(persona: Persona) -> str:
    """Planner rules, the gateway persona, and the language rule for the
    "reply" text; the persona block is omitted entirely when nothing is
    configured. Only "reply" reaches the user, so the language rule scopes to
    it — "action", "reason", and "arguments" stay machine-readable English."""
    return "\n\n".join(
        part
        for part in (LLM_PLANNER_RULES, persona.reply_rules(), REPLY_LANGUAGE_RULE)
        if part
    )


LLM_PLANNER_SYSTEM_PROMPT = build_planner_system_prompt(PERSONA)


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

    action: Literal["tool", "approval", "deny", "async", "final"]
    workflow: str
    planner_action: str
    planner_source: str = "fallback"
    tool: str | None = None
    tool_input: dict[str, Any] | None = None
    required_permission: str | None = None
    audit_event: str | None = None
    reason: str | None = None
    # Direct user-facing answer for action="final": the run completes with this
    # text and no tool is dispatched (used for chat/small-talk turns).
    output: str | None = None


class PlannedAction(BaseModel):
    """One planner outcome: the chosen action plus any LLM-planned inputs."""

    action: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    reply: str | None = None


class ToolBrokerError(RuntimeError):
    """A tool-broker callback was rejected or a tool run did not succeed."""

    def __init__(self, detail: str, status_code: int | None = None) -> None:
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code


class ToolBrokerClient:
    """Agent-side client for the orchestrator's tool-broker callback API.

    Long-running agents use this to execute tools mid-run: every request goes
    back through the orchestrator, which enforces Casbin policy and publishes
    `tool.requested`, so agents still hold no Kafka or database access.
    """

    def __init__(
        self,
        agent_id: str,
        run_id: str,
        *,
        base_url: str | None = None,
        callback_token: str | None = None,
        poll_interval_seconds: float | None = None,
        timeout_seconds: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.agent_id = agent_id
        self.run_id = run_id
        self.base_url = (base_url or ORCHESTRATOR_CALLBACK_URL).rstrip("/")
        self.callback_token = (
            AGENT_CALLBACK_TOKEN if callback_token is None else callback_token
        )
        self.poll_interval_seconds = (
            TOOL_BROKER_POLL_INTERVAL_SECONDS
            if poll_interval_seconds is None
            else poll_interval_seconds
        )
        self.timeout_seconds = (
            TOOL_BROKER_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
        )
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "ToolBrokerClient":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.aclose()

    def _headers(self) -> dict[str, str]:
        headers = {"x-agent-id": self.agent_id}
        if self.callback_token:
            headers["x-callback-token"] = self.callback_token
        return headers

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = await self._client.request(
                method,
                path,
                headers=self._headers(),
                **kwargs,
            )
        except httpx.HTTPError as exc:
            raise ToolBrokerError(f"Tool broker is unreachable: {exc}") from exc
        if response.status_code >= 400:
            detail = f"Tool broker rejected the request ({response.status_code})"
            try:
                detail = str(response.json().get("detail") or detail)
            except ValueError:
                pass
            raise ToolBrokerError(detail, status_code=response.status_code)
        return response.json()

    async def request_tool(
        self,
        tool: str,
        tool_input: dict[str, Any] | None = None,
        required_permission: str | None = None,
    ) -> str:
        """Ask the orchestrator to execute a tool; returns the tool_call_id."""
        body = await self._request(
            "POST",
            f"/internal/runs/{self.run_id}/tool-calls",
            json={
                "tool": tool,
                "tool_input": tool_input or {},
                "required_permission": required_permission,
            },
        )
        return str(body["tool_call_id"])

    async def wait_for_tool(self, tool_call_id: str) -> dict[str, Any]:
        """Poll a tool call until it settles; returns the final record."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.timeout_seconds
        while True:
            record = await self._request(
                "GET",
                f"/internal/runs/{self.run_id}/tool-calls/{tool_call_id}",
            )
            if record.get("status") != "requested":
                return record
            if loop.time() >= deadline:
                raise ToolBrokerError(f"Tool call timed out: {tool_call_id}")
            await asyncio.sleep(self.poll_interval_seconds)

    async def run_tool(
        self,
        tool: str,
        tool_input: dict[str, Any] | None = None,
        required_permission: str | None = None,
    ) -> dict[str, Any]:
        """Request a tool and wait for it; raises ToolBrokerError on failure."""
        tool_call_id = await self.request_tool(tool, tool_input, required_permission)
        record = await self.wait_for_tool(tool_call_id)
        if record.get("status") != "completed":
            raise ToolBrokerError(
                record.get("denied_reason")
                or record.get("output")
                or f"Tool '{tool}' failed",
            )
        return record

    async def complete_run(
        self,
        status: str = "completed",
        output: str | None = None,
        result: dict[str, Any] | None = None,
    ) -> None:
        await self._request(
            "POST",
            f"/internal/runs/{self.run_id}/complete",
            json={"status": status, "output": output, "result": result},
        )


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
    decide: Callable[[PlannedAction, AgentRunRequest], AgentDecision]
    # Domain instructions appended to the planner prompt: which arguments each
    # data action takes and, for SQL-writing agents, the queryable schema.
    planner_guidance: str = ""
    # Driver for decisions with action="async": receives the run request and a
    # ToolBrokerClient, returns the final run output. The runtime schedules it
    # in the background and reports completion/failure to the orchestrator.
    run_async: Callable[[AgentRunRequest, ToolBrokerClient], Awaitable[str]] | None = (
        field(default=None)
    )


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


def langfuse_generation_span(
    langfuse_tracer: Tracer,
    workflow: str,
    message: str,
    context: dict[str, Any] | None,
    parent_context: Context | None,
):
    """Record a planner generation under the orchestrator's Langfuse root.

    The agent-run trace attributes are what distinguish this from the
    orchestrator's own generations; the rest is the shared client's span.
    """
    return litellm_client.generation_span(
        langfuse_tracer,
        "agent.llm_plan",
        workflow,
        message,
        "planner",
        parent_context,
        build_trace_attributes(workflow, context),
    )


async def litellm_plan_action(
    definition: AgentDefinition,
    message: str,
    context: dict[str, Any] | None,
    parent_context: Context | None,
    langfuse_tracer: Tracer,
) -> PlannedAction | None:
    with langfuse_generation_span(
        langfuse_tracer,
        definition.workflow,
        message,
        context,
        parent_context,
    ) as span:
        allowed_actions = definition.actions
        guidance = (
            f"Agent guidance:\n{definition.planner_guidance}\n"
            if definition.planner_guidance
            else ""
        )
        content = await litellm_client.complete(
            span,
            [
                {"role": "system", "content": LLM_PLANNER_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"Workflow: {definition.workflow}\n"
                        f"Allowed actions: {', '.join(sorted(allowed_actions))}\n"
                        f"{guidance}"
                        f"User message: {message}"
                    ),
                },
            ],
            PLANNER_ERROR_MESSAGE,
        )
        if content is None:
            return None

        # The planner prompt demands JSON; anything else is a failed generation.
        try:
            decision = json.loads(content)
        except (TypeError, ValueError) as exc:
            litellm_client.record_failure(span, exc, PLANNER_ERROR_MESSAGE)
            return None

        action = str(decision.get("action", "")).strip().lower()
        if action in allowed_actions:
            span.set_attributes(
                {"app.planner.result": "selected", "app.workflow.action": action},
            )
            arguments = decision.get("arguments")
            reply = decision.get("reply")
            return PlannedAction(
                action=action,
                arguments=arguments if isinstance(arguments, dict) else {},
                reply=str(reply).strip() or None if reply else None,
            )

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
) -> tuple[PlannedAction, str]:
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
        planned = await litellm_plan_action(
            definition,
            message,
            context,
            parent_context,
            langfuse_tracer,
        )
        if planned is not None:
            span.set_attributes(
                {"app.workflow.action": planned.action, "app.planner.source": "litellm"},
            )
            return planned, "litellm"

        fallback = PlannedAction(action=definition.fallback_action(message))
        span.set_attributes(
            {
                "app.workflow.action": fallback.action,
                "app.planner.source": "fallback",
            },
        )
        return fallback, "fallback"


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
            planned, planner_source = await choose_plan_action(
                definition,
                request.message,
                planner_trace_context(request),
                context_from_traceparent(state["langfuse_traceparent"]),
                tracer,
                langfuse_tracer,
            )
            decision = definition.decide(planned, request).model_copy(
                update={"planner_source": planner_source},
            )
            span.set_attributes(
                clean_attributes(
                    {
                        "app.workflow.action": planned.action,
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


async def drive_background_run(
    definition: AgentDefinition,
    request: AgentRunRequest,
) -> None:
    """Run an async agent workflow and always report the outcome back."""
    if definition.run_async is None:
        return
    async with ToolBrokerClient(request.agent_id, request.request_id) as broker:
        try:
            output = await definition.run_async(request, broker)
        except Exception as exc:  # noqa: BLE001 - the run must settle either way
            detail = exc.detail if isinstance(exc, ToolBrokerError) else str(exc)
            try:
                await broker.complete_run("failed", detail)
            except ToolBrokerError:
                pass
        else:
            await broker.complete_run("completed", output)


_BACKGROUND_RUNS: set[asyncio.Task[None]] = set()


def start_background_run(definition: AgentDefinition, request: AgentRunRequest) -> None:
    """Schedule the async driver; kept as a seam so tests can intercept it."""
    task = asyncio.create_task(drive_background_run(definition, request))
    _BACKGROUND_RUNS.add(task)
    task.add_done_callback(_BACKGROUND_RUNS.discard)


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
        decision = result["decision"]
        if decision.get("action") == "async" and definition.run_async is not None:
            start_background_run(definition, body)
        return {
            "protocol": AGENT_PROTOCOL,
            "agent_id": definition.agent_id,
            "request_id": body.request_id,
            "workflow": definition.workflow,
            "decision": decision,
        }

    return app
