import asyncio
import json
import os
from contextlib import asynccontextmanager, contextmanager, suppress
from dataclasses import dataclass
from typing import Any

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Header, HTTPException
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph
from langgraph.store.memory import InMemoryStore
from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.context import Context
from opentelemetry.trace import NonRecordingSpan, Span, SpanKind, Status, StatusCode
from pydantic import BaseModel
from typing_extensions import TypedDict

from apps.authz import (
    can_execute_tool,
    can_read_data_source_subjects,
    parse_policy_subjects as parse_casbin_subjects,
)
from apps.observability import (
    clean_attributes,
    inject_trace_context,
    setup_langfuse_observability,
    setup_observability,
    start_event_span,
)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "http://localhost:4000/v1").rstrip("/")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "")
LITELLM_MODEL = os.getenv("LITELLM_MODEL", "")
LITELLM_TIMEOUT_SECONDS = float(os.getenv("LITELLM_TIMEOUT_SECONDS", "30"))
_DISABLED_ENV_VALUES = {"0", "false", "no", "off"}

WORKFLOW_ACTIONS = {
    "world": {"approval", "report", "sql"},
    "procurement": {"approval", "sql"},
}
AGENT_WORKFLOW_NAMES = {
    "world-agent": "world",
    "procurement-agent": "procurement",
}

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


class AgentState(TypedDict):
    request_id: str
    tenant_id: str
    user_id: str
    agent_id: str
    message: str
    allowed_permissions: list[str]
    policy_subjects: list[str]
    needs_approval: bool
    denied_reason: str | None


class RunIn(BaseModel):
    message: str
    thread_id: str | None = None


@dataclass(frozen=True)
class LangfuseRunTrace:
    parent_context: Context
    trace_attributes: dict[str, Any]


@dataclass(frozen=True)
class LangfuseToolTrace:
    request_id: str
    span: Span


producer: AIOKafkaProducer | None = None
completed_consumer: AIOKafkaConsumer | None = None
completed_consumer_task: asyncio.Task[None] | None = None
RUNS: dict[str, dict[str, Any]] = {}
LANGFUSE_RUN_TRACES: dict[str, LangfuseRunTrace] = {}
LANGFUSE_TOOL_TRACES: dict[str, LangfuseToolTrace] = {}
tracer = setup_observability("orchestrator")
langfuse_tracer = setup_langfuse_observability("orchestrator")


def decode_event(value: bytes) -> dict[str, Any]:
    return json.loads(value.decode())


def remember_run(run_id: str, payload: dict[str, Any]) -> None:
    RUNS[run_id] = {**RUNS.get(run_id, {}), **payload}


def run_output_for_status(
    status: str,
    *,
    denied_reason: str | None = None,
    tool: str | None = None,
    result: dict[str, Any] | None = None,
) -> str:
    if status == "denied":
        return denied_reason or "Request was denied."
    if status == "requires_approval":
        return "Human approval is required before this request can continue."
    if status == "approved":
        return "Human approval was recorded. This sample does not execute destructive follow-up actions."
    if status == "completed" and tool == "sql":
        rows = result.get("rows", []) if result else []
        return f"SQL tool completed with {len(rows)} row(s)."
    if status == "completed" and tool == "report":
        report_id = result.get("report_id") if result else None
        return f"Report tool completed: {report_id}."
    if status == "failed":
        return "Tool execution failed."
    if status == "running":
        return "Agent accepted the request and is waiting for tool output."
    return "Agent request was received."


def env_enabled(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).lower() not in _DISABLED_ENV_VALUES


def langfuse_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def langfuse_payload(value: Any) -> Any:
    if env_enabled("LANGFUSE_CAPTURE_CONTENT", "true"):
        return value

    if isinstance(value, str):
        return {"content_length": len(value)}
    if isinstance(value, list):
        return [
            {
                "role": item.get("role"),
                "content_length": len(str(item.get("content", ""))),
            }
            for item in value
            if isinstance(item, dict)
        ]
    return {"content_type": type(value).__name__}


def langfuse_planner_context(state: AgentState) -> dict[str, str]:
    return {
        "request_id": state["request_id"],
        "tenant_id": state["tenant_id"],
        "user_id": state["user_id"],
        "agent_id": state["agent_id"],
    }


def langfuse_trace_attributes(
    workflow: str,
    context: dict[str, Any] | None,
) -> dict[str, Any]:
    context = context or {}
    request_id = context.get("request_id")
    run_trace = LANGFUSE_RUN_TRACES.get(str(request_id)) if request_id else None
    if run_trace is not None:
        return run_trace.trace_attributes

    return {
        "langfuse.trace.name": "agent-run",
        "langfuse.user.id": context.get("user_id"),
        "langfuse.session.id": context.get("session_id") or request_id,
        "langfuse.trace.tags": [
            "ai-agent-gateway",
            "agent",
            "llm-planner",
            f"workflow:{workflow}",
        ],
        "langfuse.trace.input": context.get("trace_input"),
        "langfuse.trace.metadata.agent_id": context.get("agent_id"),
        "langfuse.trace.metadata.request_id": request_id,
        "langfuse.trace.metadata.tenant_id": context.get("tenant_id"),
        "langfuse.trace.metadata.workflow": workflow,
        "langfuse.trace.metadata.tempo_trace_id": context.get("tempo_trace_id"),
    }


def litellm_usage_attributes(usage: dict[str, Any] | None) -> dict[str, Any]:
    if not usage:
        return {}

    prompt_tokens = usage.get("prompt_tokens")
    completion_tokens = usage.get("completion_tokens")
    total_tokens = usage.get("total_tokens")
    usage_details = {
        "input": prompt_tokens,
        "output": completion_tokens,
        "total": total_tokens,
    }
    return clean_attributes(
        {
            "gen_ai.usage.input_tokens": prompt_tokens,
            "gen_ai.usage.output_tokens": completion_tokens,
            "gen_ai.usage.total_tokens": total_tokens,
            "langfuse.observation.usage_details": langfuse_json(
                {
                    key: value
                    for key, value in usage_details.items()
                    if value is not None
                },
            ),
        },
    )


def _trace_id(span: Span) -> str | None:
    span_context = span.get_span_context()
    if not span_context.is_valid:
        return None
    return f"{span_context.trace_id:032x}"


def _langfuse_child_context(span: Span) -> Context | None:
    span_context = span.get_span_context()
    if not span_context.is_valid:
        return None
    return otel_trace.set_span_in_context(
        NonRecordingSpan(span_context),
        Context(),
    )


def _record_span_error(span: Span, exc: BaseException | str) -> None:
    message = str(exc)
    if isinstance(exc, BaseException):
        span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, message))
    span.set_attributes(
        {
            "langfuse.observation.level": "ERROR",
            "langfuse.observation.status_message": message,
        },
    )


@contextmanager
def langfuse_agent_trace(
    *,
    request_id: str,
    session_id: str,
    tenant_id: str,
    user_id: str,
    agent_id: str,
    workflow: str,
    message: str,
):
    """Create the logical Langfuse root while preserving Tempo trace correlation."""
    tempo_parent_context = otel_context.get_current()
    tempo_span = otel_trace.get_current_span(tempo_parent_context)
    tempo_trace_id = _trace_id(tempo_span)
    trace_input = langfuse_json(langfuse_payload(message))
    trace_attributes = clean_attributes(
        langfuse_trace_attributes(
            workflow,
            {
                "request_id": request_id,
                "session_id": session_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "agent_id": agent_id,
                "tempo_trace_id": tempo_trace_id,
                "trace_input": trace_input,
            },
        ),
    )
    span = langfuse_tracer.start_span(
        "agent-run",
        context=tempo_parent_context if tempo_trace_id else Context(),
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                **trace_attributes,
                "app.request_id": request_id,
                "app.agent_id": agent_id,
                "app.workflow": workflow,
                "langfuse.observation.type": "span",
                "langfuse.observation.input": trace_input,
                "langfuse.observation.metadata.kind": "agent",
            },
        ),
    )
    child_context = _langfuse_child_context(span)
    if child_context is not None:
        LANGFUSE_RUN_TRACES[request_id] = LangfuseRunTrace(
            parent_context=child_context,
            trace_attributes=trace_attributes,
        )

    try:
        yield span
    except BaseException as exc:
        _record_span_error(span, exc)
        discard_langfuse_run_trace(request_id, "Agent run failed")
        raise
    finally:
        span.end()


@contextmanager
def langfuse_generation_span(
    workflow: str,
    message: str,
    context: dict[str, Any] | None,
):
    """Record a planner generation under the logical Langfuse agent trace."""
    context = context or {}
    request_id = context.get("request_id")
    run_trace = LANGFUSE_RUN_TRACES.get(str(request_id)) if request_id else None
    span = langfuse_tracer.start_span(
        "orchestrator.llm_plan",
        context=run_trace.parent_context if run_trace else Context(),
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
                **langfuse_trace_attributes(workflow, context),
            },
        ),
    )
    try:
        yield span
    except BaseException as exc:
        _record_span_error(span, exc)
        raise
    finally:
        span.end()


def _create_langfuse_tool_span(
    payload: dict[str, Any],
    run_trace: LangfuseRunTrace,
) -> Span:
    request_id = str(payload.get("request_id") or "")
    tool_call_id = str(payload.get("tool_call_id") or "")
    tool = str(payload.get("tool") or "unknown")
    return langfuse_tracer.start_span(
        f"tool.{tool}",
        context=run_trace.parent_context,
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                **run_trace.trace_attributes,
                "app.request_id": request_id,
                "app.workflow": payload.get("workflow"),
                "app.tool": tool,
                "app.tool_call_id": tool_call_id,
                "langfuse.observation.type": "span",
                "langfuse.observation.input": langfuse_json(
                    langfuse_payload(payload.get("input")),
                ),
                "langfuse.observation.metadata.kind": "tool",
                "langfuse.observation.metadata.tool": tool,
                "langfuse.observation.metadata.tool_call_id": tool_call_id,
            },
        ),
    )


def start_langfuse_tool_trace(payload: dict[str, Any]) -> None:
    request_id = str(payload.get("request_id") or "")
    tool_call_id = str(payload.get("tool_call_id") or "")
    run_trace = LANGFUSE_RUN_TRACES.get(request_id)
    if not request_id or not tool_call_id or run_trace is None:
        return

    span = _create_langfuse_tool_span(payload, run_trace)
    LANGFUSE_TOOL_TRACES[tool_call_id] = LangfuseToolTrace(
        request_id=request_id,
        span=span,
    )


def fail_langfuse_tool_trace(tool_call_id: str, exc: BaseException | str) -> None:
    tool_trace = LANGFUSE_TOOL_TRACES.pop(tool_call_id, None)
    if tool_trace is None:
        return
    _record_span_error(tool_trace.span, exc)
    tool_trace.span.end()


def finish_langfuse_tool_trace(event: dict[str, Any], output: str) -> None:
    request_id = str(event.get("request_id") or "")
    tool_call_id = str(event.get("tool_call_id") or "")
    run_trace = LANGFUSE_RUN_TRACES.get(request_id)
    tool_trace = LANGFUSE_TOOL_TRACES.pop(tool_call_id, None)
    span = tool_trace.span if tool_trace is not None else None

    if span is None and run_trace is not None:
        span = _create_langfuse_tool_span(event, run_trace)

    if span is None:
        return

    status = str(event.get("status") or "completed")
    result = event.get("result")
    final_output = {
        "status": status,
        "tool": event.get("tool"),
        "message": output,
        "result": langfuse_payload(result),
    }
    span.set_attributes(
        clean_attributes(
            {
                "app.run_status": status,
                "langfuse.observation.output": langfuse_json(
                    langfuse_payload(result),
                ),
                "langfuse.trace.output": langfuse_json(final_output),
            },
        ),
    )
    if status == "failed":
        _record_span_error(span, "Tool execution failed")
    span.end()
    LANGFUSE_RUN_TRACES.pop(request_id, None)


def discard_langfuse_run_trace(request_id: str, reason: str | None = None) -> None:
    for tool_call_id, tool_trace in list(LANGFUSE_TOOL_TRACES.items()):
        if tool_trace.request_id != request_id:
            continue
        LANGFUSE_TOOL_TRACES.pop(tool_call_id, None)
        if reason:
            _record_span_error(tool_trace.span, reason)
        tool_trace.span.end()
    LANGFUSE_RUN_TRACES.pop(request_id, None)


def close_pending_langfuse_traces() -> None:
    for request_id in list(LANGFUSE_RUN_TRACES):
        discard_langfuse_run_trace(request_id, "Orchestrator shutting down")
    for tool_call_id in list(LANGFUSE_TOOL_TRACES):
        fail_langfuse_tool_trace(tool_call_id, "Orchestrator shutting down")


async def consume_tool_completed() -> None:
    if completed_consumer is None:
        return

    async for msg in completed_consumer:
        event = decode_event(msg.value)
        request_id = event.get("request_id")
        if not request_id:
            continue

        with start_event_span(
            tracer,
            "orchestrator.tool_completed",
            event,
            attributes={
                "app.request_id": request_id,
                "app.agent_id": event.get("agent_id"),
                "app.workflow": event.get("workflow"),
                "app.tool": event.get("tool"),
                "app.tool_call_id": event.get("tool_call_id"),
                "app.run_status": event.get("status", "completed"),
                "messaging.system": "kafka",
                "messaging.destination.name": "tool.completed",
            },
        ) as span:
            if event.get("status") == "failed":
                span.set_status(Status(StatusCode.ERROR, "Tool execution failed"))

            output = run_output_for_status(
                event.get("status", "completed"),
                tool=event.get("tool"),
                result=event.get("result"),
                denied_reason=event.get("denied_reason"),
            )
            finish_langfuse_tool_trace(event, output)
            remember_run(
                request_id,
                {
                    "run_id": request_id,
                    "request_id": request_id,
                    "status": event.get("status", "completed"),
                    "agent_id": event.get("agent_id"),
                    "tenant_id": event.get("tenant_id"),
                    "user_id": event.get("user_id"),
                    "workflow": event.get("workflow"),
                    "tool": event.get("tool"),
                    "tool_call_id": event.get("tool_call_id"),
                    "input": event.get("input"),
                    "result": event.get("result"),
                    "denied_reason": event.get("denied_reason"),
                    "output": output,
                },
            )


async def publish(topic: str, payload: dict[str, Any]) -> None:
    if producer is None:
        raise RuntimeError("Kafka producer has not been started")

    tool_call_id = str(payload.get("tool_call_id") or "")
    if topic == "tool.requested":
        start_langfuse_tool_trace(payload)

    traced_payload = inject_trace_context(payload)
    try:
        with tracer.start_as_current_span(
            "kafka.publish",
            kind=SpanKind.PRODUCER,
            attributes=clean_attributes(
                {
                    "app.request_id": payload.get("request_id"),
                    "app.agent_id": payload.get("agent_id"),
                    "app.workflow": payload.get("workflow"),
                    "app.tool": payload.get("tool"),
                    "app.tool_call_id": payload.get("tool_call_id"),
                    "app.event": payload.get("event"),
                    "messaging.system": "kafka",
                    "messaging.destination.name": topic,
                    "messaging.operation": "publish",
                },
            ),
        ):
            await producer.send_and_wait(
                topic,
                key=payload["request_id"].encode(),
                value=json.dumps(traced_payload).encode(),
            )
    except BaseException as exc:
        if tool_call_id:
            fail_langfuse_tool_trace(tool_call_id, exc)
        raise


def parse_allowed_permissions(header_value: str) -> list[str]:
    return [
        permission.strip()
        for permission in header_value.split(",")
        if permission.strip()
    ]


def can_use_permission(state: AgentState, permission: str) -> bool:
    return can_read_data_source_subjects(
        state["policy_subjects"],
        state["tenant_id"],
        permission,
    )


def can_use_tool(state: AgentState, tool: str) -> bool:
    return can_execute_tool(
        state["policy_subjects"],
        state["tenant_id"],
        tool,
    )


def fallback_plan_action(workflow: str, message: str) -> str:
    text = message.lower()
    if "delete" in text or "remove" in text:
        return "approval"

    if workflow == "world":
        if "report" in text:
            return "report"
        return "sql"

    return "sql"


async def litellm_plan_action(
    workflow: str,
    message: str,
    context: dict[str, Any] | None = None,
) -> str | None:
    with langfuse_generation_span(workflow, message, context) as span:
        if not LITELLM_API_KEY or not LITELLM_MODEL:
            span.set_attribute("app.planner.result", "not_configured")
            return None

        allowed_actions = WORKFLOW_ACTIONS[workflow]
        payload = {
            "model": LITELLM_MODEL,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": LLM_PLANNER_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"Workflow: {workflow}\n"
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
    workflow: str,
    message: str,
    context: dict[str, Any] | None = None,
) -> str:
    with tracer.start_as_current_span(
        "orchestrator.choose_plan_action",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.workflow": workflow,
                "app.user_message.length": len(message),
            },
        ),
    ) as span:
        action = await litellm_plan_action(workflow, message, context)
        if action is not None:
            span.set_attributes(
                {"app.workflow.action": action, "app.planner.source": "litellm"},
            )
            return action

        fallback_action = fallback_plan_action(workflow, message)
        span.set_attributes(
            {
                "app.workflow.action": fallback_action,
                "app.planner.source": "fallback",
            },
        )
        return fallback_action


async def deny_permission_access(
    state: AgentState,
    workflow: str,
    permission: str,
) -> dict[str, Any]:
    denied_reason = f"User cannot use data source permission: {permission}"
    with tracer.start_as_current_span(
        "orchestrator.permission_denied",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.request_id": state["request_id"],
                "app.agent_id": state["agent_id"],
                "app.workflow": workflow,
                "app.permission": permission,
                "app.denied_reason": denied_reason,
            },
        ),
    ) as span:
        span.set_status(Status(StatusCode.ERROR, denied_reason))
        await publish(
            "audit.events",
            {
                **state,
                "workflow": workflow,
                "event": "permission_access_denied",
                "permission": permission,
                "reason": denied_reason,
            },
        )
    return {"denied_reason": denied_reason}


async def deny_tool_access(
    state: AgentState,
    workflow: str,
    tool: str,
) -> dict[str, Any]:
    denied_reason = f"User cannot execute tool: {tool}"
    with tracer.start_as_current_span(
        "orchestrator.tool_denied",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.request_id": state["request_id"],
                "app.agent_id": state["agent_id"],
                "app.workflow": workflow,
                "app.tool": tool,
                "app.denied_reason": denied_reason,
            },
        ),
    ) as span:
        span.set_status(Status(StatusCode.ERROR, denied_reason))
        await publish(
            "audit.events",
            {
                **state,
                "workflow": workflow,
                "event": "tool_access_denied",
                "tool": tool,
                "reason": denied_reason,
            },
        )
    return {"denied_reason": denied_reason}


async def world_plan(state: AgentState) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "agent.plan.world",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.request_id": state["request_id"],
                "app.agent_id": state["agent_id"],
                "app.workflow": "world",
                "app.allowed_permissions": state["allowed_permissions"],
            },
        ),
    ) as span:
        action = await choose_plan_action(
            "world",
            state["message"],
            langfuse_planner_context(state),
        )
        span.set_attribute("app.workflow.action", action)

        if action == "approval":
            await publish(
                "audit.events",
                {**state, "workflow": "world", "event": "human_approval_required"},
            )
            span.set_attribute("app.run_status", "requires_approval")
            return {"needs_approval": True}

        if action == "report":
            if not can_use_permission(state, "world-db"):
                return await deny_permission_access(state, "world", "world-db")
            if not can_use_tool(state, "report"):
                return await deny_tool_access(state, "world", "report")

            tool_call_id = f"{state['request_id']}:report:1"
            span.set_attributes(
                {"app.tool": "report", "app.tool_call_id": tool_call_id},
            )
            await publish(
                "tool.requested",
                {
                    **state,
                    "tool": "report",
                    "tool_call_id": tool_call_id,
                    "workflow": "world",
                    "input": {
                        "report_type": "world_market_summary",
                        "database": "world",
                    },
                },
            )
            span.set_attribute("app.run_status", "running")
            return {"needs_approval": False, "denied_reason": None}

        if not can_use_permission(state, "world-db"):
            return await deny_permission_access(state, "world", "world-db")
        if not can_use_tool(state, "sql"):
            return await deny_tool_access(state, "world", "sql")

        tool_call_id = f"{state['request_id']}:sql:1"
        span.set_attributes({"app.tool": "sql", "app.tool_call_id": tool_call_id})
        await publish(
            "tool.requested",
            {
                **state,
                "tool": "sql",
                "tool_call_id": tool_call_id,
                "workflow": "world",
                "input": {
                    "database": "world",
                    "sql": (
                        "select city.name as city, country.name as country, "
                        "country.continent, city.district, city.population "
                        "from city "
                        "join country on country.code = city.country_code "
                        "order by city.population desc "
                        "limit 10"
                    )
                },
            },
        )
        span.set_attribute("app.run_status", "running")
        return {"needs_approval": False, "denied_reason": None}


async def procurement_plan(state: AgentState) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "agent.plan.procurement",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.request_id": state["request_id"],
                "app.agent_id": state["agent_id"],
                "app.workflow": "procurement",
                "app.allowed_permissions": state["allowed_permissions"],
            },
        ),
    ) as span:
        action = await choose_plan_action(
            "procurement",
            state["message"],
            langfuse_planner_context(state),
        )
        span.set_attribute("app.workflow.action", action)

        if action == "approval":
            await publish(
                "audit.events",
                {**state, "workflow": "procurement", "event": "procurement_approval_required"},
            )
            span.set_attribute("app.run_status", "requires_approval")
            return {"needs_approval": True}

        if not can_use_permission(state, "procurement-db"):
            return await deny_permission_access(state, "procurement", "procurement-db")
        if not can_use_tool(state, "sql"):
            return await deny_tool_access(state, "procurement", "sql")

        tool_call_id = f"{state['request_id']}:sql:1"
        span.set_attributes({"app.tool": "sql", "app.tool_call_id": tool_call_id})
        await publish(
            "tool.requested",
            {
                **state,
                "tool": "sql",
                "tool_call_id": tool_call_id,
                "workflow": "procurement",
                "input": {
                    "database": "procurement",
                    "sql": (
                        "select supplier_name, category, country, total_spend, "
                        "order_count, risk_level, last_order_date "
                        "from supplier_summary "
                        "order by total_spend desc "
                        "limit 10"
                    )
                },
            },
        )
        span.set_attribute("app.run_status", "running")
        return {"needs_approval": False, "denied_reason": None}


def route_after_plan(state: AgentState) -> str:
    return END


def build_workflow(name: str, plan_node) -> Any:
    builder = StateGraph(AgentState)
    builder.add_node(name, plan_node)
    builder.set_entry_point(name)
    builder.add_conditional_edges(name, route_after_plan)
    return builder.compile(
        checkpointer=InMemorySaver(),
        store=InMemoryStore(),
    )


WORKFLOWS = {
    "world-agent": build_workflow("world_plan", world_plan),
    "procurement-agent": build_workflow("procurement_plan", procurement_plan),
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global completed_consumer, completed_consumer_task, producer

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    completed_consumer = AIOKafkaConsumer(
        "tool.completed",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="agent-orchestrator-run-results",
    )
    await producer.start()
    await completed_consumer.start()
    completed_consumer_task = asyncio.create_task(consume_tool_completed())
    try:
        yield
    finally:
        if completed_consumer_task is not None:
            completed_consumer_task.cancel()
            with suppress(asyncio.CancelledError):
                await completed_consumer_task
        if completed_consumer is not None:
            await completed_consumer.stop()
        await producer.stop()
        close_pending_langfuse_traces()
        completed_consumer_task = None
        completed_consumer = None
        producer = None


app = FastAPI(title="Agent Orchestrator API", lifespan=lifespan)
setup_observability("orchestrator", app)


@app.post("/internal/agents/{agent_id}/runs")
async def run(
    agent_id: str,
    body: RunIn,
    x_request_id: str = Header(),
    x_tenant_id: str = Header(),
    x_user_id: str = Header(),
    x_allowed_permissions: str = Header(default=""),
    x_policy_subjects: str = Header(default=""),
) -> dict[str, str | None]:
    allowed_permissions = parse_allowed_permissions(x_allowed_permissions)
    state: AgentState = {
        "request_id": x_request_id,
        "tenant_id": x_tenant_id,
        "user_id": x_user_id,
        "agent_id": agent_id,
        "message": body.message,
        "allowed_permissions": allowed_permissions,
        "policy_subjects": parse_casbin_subjects(x_policy_subjects),
        "needs_approval": False,
        "denied_reason": None,
    }

    with tracer.start_as_current_span(
        "orchestrator.agent_run",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.request_id": x_request_id,
                "app.agent_id": agent_id,
                "app.tenant_id": x_tenant_id,
                "app.user_id": x_user_id,
                "app.allowed_permissions": state["allowed_permissions"],
                "app.policy_subject_count": len(state["policy_subjects"]),
                "app.user_message.length": len(body.message),
            },
        ),
    ) as span:
        remember_run(
            x_request_id,
            {
                "run_id": x_request_id,
                "request_id": x_request_id,
                "status": "requested",
                "agent_id": agent_id,
                "tenant_id": x_tenant_id,
                "user_id": x_user_id,
                "message": body.message,
                "allowed_permissions": state["allowed_permissions"],
                "result": None,
                "denied_reason": None,
                "output": run_output_for_status("requested"),
            },
        )

        workflow = WORKFLOWS.get(agent_id)
        if workflow is None:
            span.set_status(Status(StatusCode.ERROR, "Unknown agent_id"))
            raise HTTPException(
                status_code=404,
                detail=f"Unknown agent_id. Available agents: {sorted(WORKFLOWS)}",
            )

        workflow_name = AGENT_WORKFLOW_NAMES.get(agent_id, agent_id)
        with langfuse_agent_trace(
            request_id=x_request_id,
            session_id=body.thread_id or x_request_id,
            tenant_id=x_tenant_id,
            user_id=x_user_id,
            agent_id=agent_id,
            workflow=workflow_name,
            message=body.message,
        ) as langfuse_span:
            await publish("agent.requested", state)
            result = await workflow.ainvoke(
                state,
                {"configurable": {"thread_id": body.thread_id or x_request_id}},
            )

            if result.get("denied_reason"):
                status = "denied"
            elif result["needs_approval"]:
                status = "requires_approval"
            else:
                status = "running"

            denied_reason = result.get("denied_reason")
            output = run_output_for_status(
                status,
                denied_reason=denied_reason,
            )
            if status == "denied":
                span.set_status(Status(StatusCode.ERROR, denied_reason))
                _record_span_error(langfuse_span, denied_reason or "Agent run denied")
            span.set_attributes(
                clean_attributes(
                    {
                        "app.run_status": status,
                        "app.denied_reason": denied_reason,
                    },
                ),
            )

            langfuse_output = {
                "status": status,
                "agent_id": agent_id,
                "message": output,
                "denied_reason": denied_reason,
            }
            langfuse_span.set_attributes(
                clean_attributes(
                    {
                        "app.run_status": status,
                        "app.denied_reason": denied_reason,
                        "langfuse.observation.output": langfuse_json(langfuse_output),
                        "langfuse.observation.metadata.status": status,
                        "langfuse.trace.output": langfuse_json(langfuse_output),
                    },
                ),
            )

            remember_run(
                x_request_id,
                {
                    "status": status,
                    "agent_id": agent_id,
                    "denied_reason": denied_reason,
                    "output": output,
                },
            )

            if status != "running":
                discard_langfuse_run_trace(x_request_id)

            return {
                "run_id": x_request_id,
                "status": status,
                "agent_id": agent_id,
                "denied_reason": denied_reason,
            }


@app.get("/internal/runs/{run_id}")
async def get_run(
    run_id: str,
    x_tenant_id: str = Header(),
    x_user_id: str = Header(),
) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "orchestrator.run_status_lookup",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.run_id": run_id,
                "app.tenant_id": x_tenant_id,
                "app.user_id": x_user_id,
            },
        ),
    ) as span:
        run_result = RUNS.get(run_id)
        if run_result is None:
            span.set_status(Status(StatusCode.ERROR, "Run not found"))
            raise HTTPException(status_code=404, detail="Run not found")

        if run_result.get("tenant_id") != x_tenant_id or run_result.get("user_id") != x_user_id:
            span.set_status(Status(StatusCode.ERROR, "Run not found"))
            raise HTTPException(status_code=404, detail="Run not found")

        span.set_attributes(
            clean_attributes(
                {
                    "app.agent_id": run_result.get("agent_id"),
                    "app.run_status": run_result.get("status"),
                    "app.tool": run_result.get("tool"),
                    "app.tool_call_id": run_result.get("tool_call_id"),
                },
            ),
        )
        return run_result


@app.post("/internal/runs/{run_id}/approve")
async def approve_run(
    run_id: str,
    x_tenant_id: str = Header(),
    x_user_id: str = Header(),
) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "orchestrator.approval_record",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.run_id": run_id,
                "app.tenant_id": x_tenant_id,
                "app.user_id": x_user_id,
            },
        ),
    ) as span:
        run_result = RUNS.get(run_id)
        if run_result is None:
            span.set_status(Status(StatusCode.ERROR, "Run not found"))
            raise HTTPException(status_code=404, detail="Run not found")

        if run_result.get("tenant_id") != x_tenant_id or run_result.get("user_id") != x_user_id:
            span.set_status(Status(StatusCode.ERROR, "Run not found"))
            raise HTTPException(status_code=404, detail="Run not found")

        if run_result.get("status") != "requires_approval":
            span.set_status(Status(StatusCode.ERROR, "Run does not require approval"))
            raise HTTPException(status_code=409, detail="Run does not require approval")

        approval_result = {
            "approved": True,
            "approved_by": x_user_id,
            "message": run_output_for_status("approved"),
        }
        remember_run(
            run_id,
            {
                "status": "approved",
                "result": approval_result,
                "approved_by": x_user_id,
                "output": approval_result["message"],
            },
        )

        await publish(
            "audit.events",
            {
                "request_id": run_id,
                "tenant_id": x_tenant_id,
                "user_id": x_user_id,
                "agent_id": run_result.get("agent_id"),
                "message": run_result.get("message"),
                "workflow": run_result.get("workflow"),
                "event": "human_approval_approved",
            },
        )
        span.set_attributes(
            clean_attributes(
                {
                    "app.agent_id": run_result.get("agent_id"),
                    "app.run_status": "approved",
                    "app.workflow": run_result.get("workflow"),
                },
            ),
        )
        return RUNS[run_id]
