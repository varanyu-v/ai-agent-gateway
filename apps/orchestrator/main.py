"""Agent orchestrator: routes runs to external agent services and executes
their decisions.

Agents are separate HTTP services discovered through the AgentRegistry
(AGENT_SERVICES env var + agent cards). The orchestrator stays the single
trusted execution point: it enforces Casbin source/tool policy on every agent
decision, publishes Kafka events, tracks run state and approvals, and owns the
logical Langfuse `agent-run` trace that agent services parent their planner
generations to.

The orchestrator also exposes a virtual supervisor agent (see
apps/orchestrator/router.py): runs addressed to ROUTER_AGENT_ID are classified
and either answered directly (general questions) or delegated to the matching
registered agent, subject to the caller's Casbin agent-invoke policy.
"""

import asyncio
import json
import os
from contextlib import asynccontextmanager, contextmanager, suppress
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Header, HTTPException
from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.context import Context
from opentelemetry.trace import Span, SpanKind, Status, StatusCode
from pydantic import BaseModel
from typing_extensions import TypedDict

from apps.authz import (
    can_execute_mcp_server,
    can_invoke_agent_subjects,
    can_read_data_source_subjects,
    parse_policy_subjects as parse_casbin_subjects,
)
from apps.langfuse_utils import (
    LANGFUSE_PARENT_HEADER,
    build_trace_attributes,
    langfuse_json,
    langfuse_payload,
    record_span_error,
    span_child_context,
    trace_id_hex,
    traceparent_value,
)
from apps.observability import (
    clean_attributes,
    inject_trace_context,
    setup_langfuse_observability,
    setup_observability,
    start_event_span,
)
from apps.orchestrator.mcp_registry import (
    DEFAULT_MCP_SERVICES,
    McpRegistry,
)
from apps.orchestrator.agent_registry import (
    DEFAULT_AGENT_SERVICES,
    AgentRegistry,
    AgentServiceError,
    RegisteredAgent,
)
from apps.orchestrator.router import (
    GENERAL_ROUTE,
    ROUTER_AGENT_ID,
    ROUTER_WORKFLOW,
    RouteDecision,
    answer_general,
    classify_route,
    fallback_route,
    is_router_agent,
)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
AGENT_SERVICES = os.getenv("AGENT_SERVICES", DEFAULT_AGENT_SERVICES)
AGENT_CONNECT_TIMEOUT_SECONDS = float(os.getenv("AGENT_CONNECT_TIMEOUT_SECONDS", "5"))
AGENT_READ_TIMEOUT_SECONDS = float(os.getenv("AGENT_READ_TIMEOUT_SECONDS", "60"))
MCP_SERVICES = os.getenv("MCP_SERVICES", DEFAULT_MCP_SERVICES)
MCP_CONNECT_TIMEOUT_SECONDS = float(os.getenv("MCP_CONNECT_TIMEOUT_SECONDS", "5"))
MCP_READ_TIMEOUT_SECONDS = float(os.getenv("MCP_READ_TIMEOUT_SECONDS", "30"))
# Shared secret agents must present on tool-broker callbacks. Empty disables
# the check (network trust only), matching the header-trust model elsewhere.
AGENT_CALLBACK_TOKEN = os.getenv("AGENT_CALLBACK_TOKEN", "")

# MCP is the only tool transport: every executable decision names tool="mcp"
# and addresses a registered MCP server, which carries its own Casbin object.
MCP_TOOL = "mcp"


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


class ToolCallIn(BaseModel):
    """A tool execution an agent requests mid-run through the tool broker."""

    tool: str
    tool_input: dict[str, Any] | None = None
    required_permission: str | None = None


class RunCompleteIn(BaseModel):
    """Final outcome an agent reports for a callback-mode run."""

    status: str = "completed"
    output: str | None = None
    result: dict[str, Any] | None = None


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
agent_registry = AgentRegistry(
    AGENT_SERVICES,
    connect_timeout_seconds=AGENT_CONNECT_TIMEOUT_SECONDS,
    read_timeout_seconds=AGENT_READ_TIMEOUT_SECONDS,
)
mcp_registry = McpRegistry(
    MCP_SERVICES,
    connect_timeout_seconds=MCP_CONNECT_TIMEOUT_SECONDS,
    read_timeout_seconds=MCP_READ_TIMEOUT_SECONDS,
)
tracer = setup_observability("orchestrator")
langfuse_tracer = setup_langfuse_observability("orchestrator")


def decode_event(value: bytes) -> dict[str, Any]:
    return json.loads(value.decode())


def remember_run(run_id: str, payload: dict[str, Any]) -> None:
    RUNS[run_id] = {**RUNS.get(run_id, {}), **payload}


def completed_tool_output(
    tool: str | None,
    result: dict[str, Any] | None,
) -> str | None:
    if tool == MCP_TOOL:
        name = result.get("tool") if result else None
        output = (result or {}).get("output") or {}
        row_count = output.get("row_count")
        report_id = output.get("report_id")
        if isinstance(row_count, int):
            suffix = f" with {row_count} row(s)"
        elif report_id:
            suffix = f": report {report_id}"
        else:
            suffix = ""
        return f"MCP tool '{name}' completed{suffix}."
    return None


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
    if status == "completed":
        tool_output = completed_tool_output(tool, result)
        if tool_output is not None:
            return tool_output
    if status == "failed":
        return "Tool execution failed."
    if status == "running":
        return "Agent accepted the request and is waiting for tool output."
    return "Agent request was received."


def settle_router_run(
    span: Span,
    langfuse_span: Span,
    state: AgentState,
    *,
    status: str,
    output: str,
    denied_reason: str | None = None,
    result: dict[str, Any] | None = None,
) -> dict[str, str | None]:
    """Record the final state of a run the router settled without an agent."""
    request_id = state["request_id"]
    if status == "denied":
        span.set_status(Status(StatusCode.ERROR, denied_reason))
        record_span_error(langfuse_span, denied_reason or "Run denied")
    span.set_attributes(
        clean_attributes(
            {"app.run_status": status, "app.denied_reason": denied_reason},
        ),
    )
    langfuse_output = {
        "status": status,
        "agent_id": state["agent_id"],
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
        request_id,
        {
            "status": status,
            "agent_id": state["agent_id"],
            "denied_reason": denied_reason,
            "result": result,
            "output": output,
        },
    )
    discard_langfuse_run_trace(request_id)
    return {
        "run_id": request_id,
        "status": status,
        "agent_id": state["agent_id"],
        "denied_reason": denied_reason,
        "output": output,
    }


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
    tempo_trace_id = trace_id_hex(tempo_span)
    trace_input = langfuse_json(langfuse_payload(message))
    trace_attributes = clean_attributes(
        build_trace_attributes(
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
    child_context = span_child_context(span)
    if child_context is not None:
        LANGFUSE_RUN_TRACES[request_id] = LangfuseRunTrace(
            parent_context=child_context,
            trace_attributes=trace_attributes,
        )

    try:
        yield span
    except BaseException as exc:
        record_span_error(span, exc)
        discard_langfuse_run_trace(request_id, "Agent run failed")
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
    record_span_error(tool_trace.span, exc)
    tool_trace.span.end()


def finish_langfuse_tool_trace(
    event: dict[str, Any],
    output: str,
    *,
    end_run_trace: bool = True,
) -> None:
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
        record_span_error(span, "Tool execution failed")
    span.end()
    if end_run_trace:
        LANGFUSE_RUN_TRACES.pop(request_id, None)


def discard_langfuse_run_trace(request_id: str, reason: str | None = None) -> None:
    for tool_call_id, tool_trace in list(LANGFUSE_TOOL_TRACES.items()):
        if tool_trace.request_id != request_id:
            continue
        LANGFUSE_TOOL_TRACES.pop(tool_call_id, None)
        if reason:
            record_span_error(tool_trace.span, reason)
        tool_trace.span.end()
    LANGFUSE_RUN_TRACES.pop(request_id, None)


def close_pending_langfuse_traces() -> None:
    for request_id in list(LANGFUSE_RUN_TRACES):
        discard_langfuse_run_trace(request_id, "Orchestrator shutting down")
    for tool_call_id in list(LANGFUSE_TOOL_TRACES):
        fail_langfuse_tool_trace(tool_call_id, "Orchestrator shutting down")


def record_tool_call(run_id: str, tool_call_id: str, payload: dict[str, Any]) -> None:
    run_record = RUNS.setdefault(run_id, {})
    tool_calls = run_record.setdefault("tool_calls", {})
    tool_calls[tool_call_id] = {**tool_calls.get(tool_call_id, {}), **payload}


def handle_tool_completed_event(event: dict[str, Any]) -> None:
    request_id = event.get("request_id")
    if not request_id:
        return

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

        if RUNS.get(request_id, {}).get("mode") == "callback":
            # Callback-mode runs stay "running" until the agent reports the
            # final outcome; a tool completion only settles that one call.
            finish_langfuse_tool_trace(event, output, end_run_trace=False)
            record_tool_call(
                request_id,
                str(event.get("tool_call_id") or ""),
                {
                    "tool_call_id": event.get("tool_call_id"),
                    "tool": event.get("tool"),
                    "status": event.get("status", "completed"),
                    "input": event.get("input"),
                    "result": event.get("result"),
                    "denied_reason": event.get("denied_reason"),
                    "output": output,
                },
            )
            return

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


async def consume_tool_completed() -> None:
    if completed_consumer is None:
        return

    async for msg in completed_consumer:
        handle_tool_completed_event(decode_event(msg.value))


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


def can_use_mcp_server(state: AgentState, server_id: str) -> bool:
    return can_execute_mcp_server(
        state["policy_subjects"],
        state["tenant_id"],
        server_id,
    )


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
    return {"needs_approval": False, "denied_reason": denied_reason}


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
    return {"needs_approval": False, "denied_reason": denied_reason}


async def invoke_agent_service(
    agent: RegisteredAgent,
    state: AgentState,
    thread_id: str | None,
    langfuse_span: Span,
) -> dict[str, Any]:
    """Call the agent service's /runs endpoint and return its decision."""
    headers = {
        "x-request-id": state["request_id"],
        "x-tenant-id": state["tenant_id"],
        "x-user-id": state["user_id"],
    }
    langfuse_parent = traceparent_value(langfuse_span)
    if langfuse_parent:
        headers[LANGFUSE_PARENT_HEADER] = langfuse_parent

    with tracer.start_as_current_span(
        "orchestrator.agent_invoke",
        kind=SpanKind.CLIENT,
        attributes=clean_attributes(
            {
                "app.request_id": state["request_id"],
                "app.agent_id": agent.agent_id,
                "app.workflow": agent.workflow,
                "server.address": agent.base_url,
            },
        ),
    ) as span:
        try:
            response = await agent_registry.invoke_run(
                agent,
                {
                    "request_id": state["request_id"],
                    "tenant_id": state["tenant_id"],
                    "user_id": state["user_id"],
                    "agent_id": state["agent_id"],
                    "message": state["message"],
                    "thread_id": thread_id,
                    "allowed_permissions": state["allowed_permissions"],
                    "policy_subjects": state["policy_subjects"],
                },
                headers=headers,
            )
        except AgentServiceError as error:
            span.set_status(Status(StatusCode.ERROR, error.detail))
            raise HTTPException(status_code=error.status_code, detail=error.detail)

        decision = response["decision"]
        span.set_attributes(
            clean_attributes(
                {
                    "app.decision.action": decision.get("action"),
                    "app.tool": decision.get("tool"),
                    "app.planner.source": decision.get("planner_source"),
                },
            ),
        )
        return decision


async def apply_agent_decision(
    state: AgentState,
    workflow: str,
    decision: dict[str, Any],
) -> dict[str, Any]:
    """Enforce Casbin policy on the agent's decision, then execute it."""
    action = decision.get("action")

    if action == "approval":
        await publish(
            "audit.events",
            {
                **state,
                "workflow": workflow,
                "event": decision.get("audit_event") or "human_approval_required",
            },
        )
        return {"needs_approval": True, "denied_reason": None}

    if action == "deny":
        return {
            "needs_approval": False,
            "denied_reason": decision.get("reason") or "Agent denied the request",
        }

    if action == "final":
        # The agent answered the message itself (chat/small-talk turns): the
        # run completes now with the agent's text and no tool is dispatched.
        await publish(
            "audit.events",
            {
                **state,
                "workflow": workflow,
                "event": decision.get("audit_event") or "agent_final_answered",
            },
        )
        output = str(decision.get("output") or "").strip()
        return {
            "needs_approval": False,
            "denied_reason": None,
            "final_output": output or "The agent has nothing further to add.",
        }

    if action == "async":
        # The agent will drive this run itself through the tool-broker
        # callback API; every tool it requests is policy-checked there. Keep
        # the policy subjects from the original trusted request so callbacks
        # are enforced against what the gateway minted, not what the agent
        # sends later.
        remember_run(
            state["request_id"],
            {
                "mode": "callback",
                "workflow": workflow,
                "policy_subjects": state["policy_subjects"],
                "tool_calls": {},
                "tool_call_seq": 0,
            },
        )
        await publish(
            "audit.events",
            {
                **state,
                "workflow": workflow,
                "event": decision.get("audit_event") or "agent_callback_run_accepted",
            },
        )
        return {"needs_approval": False, "denied_reason": None}

    if action == "tool":
        tool = str(decision.get("tool") or "")
        tool_input = decision.get("tool_input") or {}
        permission = decision.get("required_permission")
        if permission and not can_use_permission(state, str(permission)):
            return await deny_permission_access(state, workflow, str(permission))
        if tool != MCP_TOOL:
            return await deny_tool_access(state, workflow, tool or "unknown")
        server_id = str(tool_input.get("server") or "")
        if not server_id or not can_use_mcp_server(state, server_id):
            return await deny_tool_access(
                state,
                workflow,
                f"mcp:{server_id or 'unknown'}",
            )

        tool_call_id = f"{state['request_id']}:{tool}:1"
        await publish(
            "tool.requested",
            {
                **state,
                "tool": tool,
                "tool_call_id": tool_call_id,
                "workflow": workflow,
                "input": tool_input,
            },
        )
        return {"needs_approval": False, "denied_reason": None}

    return {
        "needs_approval": False,
        "denied_reason": f"Agent returned an unsupported action: {action}",
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    global completed_consumer, completed_consumer_task, producer

    await agent_registry.start()
    await mcp_registry.start()
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
        await agent_registry.aclose()
        await mcp_registry.aclose()
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

        agent = agent_registry.get(agent_id)
        if agent is None and not is_router_agent(agent_id):
            span.set_status(Status(StatusCode.ERROR, "Unknown agent_id"))
            raise HTTPException(
                status_code=404,
                detail=(
                    "Unknown agent_id. Available agents: "
                    f"{[ROUTER_AGENT_ID, *agent_registry.agent_ids]}"
                ),
            )

        workflow_name = agent.workflow if agent is not None else ROUTER_WORKFLOW
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

            if agent is None:
                # Supervisor path: classify the message, then either answer it
                # directly (general topics) or hand it to the matching
                # registered agent through the normal policy-enforced flow.
                # Only agents this caller may invoke reach the planner, so no
                # description of an unreachable agent enters the prompt.
                registered = [
                    candidate
                    for candidate_id in agent_registry.agent_ids
                    if (candidate := agent_registry.get(candidate_id)) is not None
                ]
                candidates = [
                    candidate
                    for candidate in registered
                    if can_invoke_agent_subjects(
                        state["policy_subjects"],
                        x_tenant_id,
                        candidate.agent_id,
                    )
                ]
                reachable_ids = {candidate.agent_id for candidate in candidates}
                unreachable = [
                    candidate
                    for candidate in registered
                    if candidate.agent_id not in reachable_ids
                ]
                route = await classify_route(
                    body.message,
                    candidates,
                    span_child_context(langfuse_span),
                )

                if route.target == GENERAL_ROUTE and unreachable:
                    # Withholding unreachable agents from the planner would
                    # also turn a probe for forbidden data into an ordinary
                    # general answer, losing the explicit denial and its audit
                    # record. Match those agents deterministically instead, so
                    # the refusal stays visible to the user and to audit.
                    blocked = fallback_route(body.message, unreachable)
                    if blocked != GENERAL_ROUTE:
                        route = RouteDecision(target=blocked, source="policy_filter")
                route_attributes = clean_attributes(
                    {
                        "app.route.target": route.target,
                        "app.route.source": route.source,
                    },
                )
                span.set_attributes(route_attributes)
                langfuse_span.set_attributes(route_attributes)
                await publish(
                    "audit.events",
                    {
                        **state,
                        "workflow": ROUTER_WORKFLOW,
                        "event": "assistant_route_selected",
                        "route": route.target,
                        "route_source": route.source,
                    },
                )

                if route.target == GENERAL_ROUTE:
                    answer = await answer_general(
                        body.message,
                        span_child_context(langfuse_span),
                        candidates,
                    )
                    await publish(
                        "audit.events",
                        {
                            **state,
                            "workflow": ROUTER_WORKFLOW,
                            "event": "assistant_general_answered",
                            "answer_source": answer.source,
                        },
                    )
                    return settle_router_run(
                        span,
                        langfuse_span,
                        state,
                        status="completed",
                        output=answer.text,
                        result={
                            "route": GENERAL_ROUTE,
                            "answer_source": answer.source,
                        },
                    )

                agent = agent_registry.get(route.target)
                if agent is None or not can_invoke_agent_subjects(
                    state["policy_subjects"],
                    x_tenant_id,
                    route.target,
                ):
                    denied_reason = f"User cannot access agent: {route.target}"
                    await publish(
                        "audit.events",
                        {
                            **state,
                            "workflow": ROUTER_WORKFLOW,
                            "event": "agent_access_denied",
                            "route": route.target,
                            "reason": denied_reason,
                        },
                    )
                    return settle_router_run(
                        span,
                        langfuse_span,
                        state,
                        status="denied",
                        output=run_output_for_status(
                            "denied",
                            denied_reason=denied_reason,
                        ),
                        denied_reason=denied_reason,
                    )

                # The run now belongs to the routed agent: tool-broker
                # callbacks authenticate against the run's agent_id.
                workflow_name = agent.workflow
                state["agent_id"] = agent.agent_id
                remember_run(
                    x_request_id,
                    {"agent_id": agent.agent_id, "routed_from": ROUTER_AGENT_ID},
                )

            try:
                decision = await invoke_agent_service(
                    agent,
                    state,
                    body.thread_id,
                    langfuse_span,
                )
            except HTTPException as error:
                remember_run(
                    x_request_id,
                    {
                        "status": "failed",
                        "agent_id": state["agent_id"],
                        "denied_reason": None,
                        "output": error.detail,
                    },
                )
                raise

            result = await apply_agent_decision(state, workflow_name, decision)

            final_output = result.get("final_output")
            if result.get("denied_reason"):
                status = "denied"
            elif result["needs_approval"]:
                status = "requires_approval"
            elif final_output is not None:
                status = "completed"
            else:
                status = "running"

            denied_reason = result.get("denied_reason")
            output = (
                final_output
                if final_output is not None
                else run_output_for_status(
                    status,
                    denied_reason=denied_reason,
                )
            )
            if status == "denied":
                span.set_status(Status(StatusCode.ERROR, denied_reason))
                record_span_error(langfuse_span, denied_reason or "Agent run denied")
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
                "agent_id": state["agent_id"],
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
                    "agent_id": state["agent_id"],
                    "denied_reason": denied_reason,
                    "output": output,
                },
            )

            if status != "running":
                discard_langfuse_run_trace(x_request_id)

            return {
                "run_id": x_request_id,
                "status": status,
                "agent_id": state["agent_id"],
                "denied_reason": denied_reason,
            }


@app.get("/internal/health", include_in_schema=False)
async def internal_health() -> dict[str, str]:
    return {"status": "ok", "service": "orchestrator"}


@app.get("/internal/agents", include_in_schema=False)
async def list_agents() -> dict[str, Any]:
    return {
        "agents": [
            {
                "agent_id": agent.agent_id,
                "name": agent.name,
                "workflow": agent.workflow,
                "base_url": agent.base_url,
                "card": agent.card,
            }
            for agent_id in agent_registry.agent_ids
            if (agent := agent_registry.get(agent_id)) is not None
        ],
    }


@app.get("/internal/mcp", include_in_schema=False)
async def list_mcp_servers() -> dict[str, Any]:
    return {
        "servers": [
            {
                "server_id": server.server_id,
                "name": server.name,
                "base_url": server.base_url,
                "protocol_version": server.protocol_version,
                "tools": server.tools,
                "card": server.card,
            }
            for server_id in mcp_registry.server_ids
            if (server := mcp_registry.get(server_id)) is not None
        ],
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


def require_callback_run(
    run_id: str,
    agent_id: str,
    callback_token: str,
) -> dict[str, Any]:
    """Authorize a tool-broker callback and return the owning run record.

    Policy context always comes from the stored run (minted by the gateway at
    request time), never from the callback itself, so an agent cannot widen
    its access by sending different subjects later.
    """
    if AGENT_CALLBACK_TOKEN and callback_token != AGENT_CALLBACK_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid callback token")

    run_record = RUNS.get(run_id)
    if run_record is None or run_record.get("agent_id") != agent_id:
        raise HTTPException(status_code=404, detail="Run not found")

    if run_record.get("mode") != "callback":
        raise HTTPException(
            status_code=409,
            detail="Run does not accept tool-broker callbacks",
        )
    return run_record


def callback_state_from_run(run_record: dict[str, Any]) -> AgentState:
    return {
        "request_id": run_record["run_id"],
        "tenant_id": run_record.get("tenant_id") or "",
        "user_id": run_record.get("user_id") or "",
        "agent_id": run_record.get("agent_id") or "",
        "message": run_record.get("message") or "",
        "allowed_permissions": run_record.get("allowed_permissions") or [],
        "policy_subjects": run_record.get("policy_subjects") or [],
        "needs_approval": False,
        "denied_reason": None,
    }


@app.post("/internal/runs/{run_id}/tool-calls")
async def request_tool_call(
    run_id: str,
    body: ToolCallIn,
    x_agent_id: str = Header(),
    x_callback_token: str = Header(default=""),
) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "orchestrator.tool_call_requested",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.run_id": run_id,
                "app.agent_id": x_agent_id,
                "app.tool": body.tool,
            },
        ),
    ) as span:
        run_record = require_callback_run(run_id, x_agent_id, x_callback_token)
        if run_record.get("status") != "running":
            span.set_status(Status(StatusCode.ERROR, "Run is not active"))
            raise HTTPException(status_code=409, detail="Run is not active")

        state = callback_state_from_run(run_record)
        workflow = str(run_record.get("workflow") or "")
        tool = body.tool.strip()
        tool_input = body.tool_input or {}

        if body.required_permission and not can_use_permission(
            state,
            body.required_permission,
        ):
            denial = await deny_permission_access(
                state,
                workflow,
                body.required_permission,
            )
            span.set_status(Status(StatusCode.ERROR, denial["denied_reason"]))
            raise HTTPException(status_code=403, detail=denial["denied_reason"])
        if tool != MCP_TOOL:
            denial = await deny_tool_access(state, workflow, tool or "unknown")
            span.set_status(Status(StatusCode.ERROR, denial["denied_reason"]))
            raise HTTPException(status_code=403, detail=denial["denied_reason"])
        server_id = str(tool_input.get("server") or "")
        if not server_id or not can_use_mcp_server(state, server_id):
            denial = await deny_tool_access(
                state,
                workflow,
                f"mcp:{server_id or 'unknown'}",
            )
            span.set_status(Status(StatusCode.ERROR, denial["denied_reason"]))
            raise HTTPException(status_code=403, detail=denial["denied_reason"])

        sequence = int(run_record.get("tool_call_seq") or 0) + 1
        run_record["tool_call_seq"] = sequence
        tool_call_id = f"{run_id}:{tool}:{sequence}"
        record_tool_call(
            run_id,
            tool_call_id,
            {
                "tool_call_id": tool_call_id,
                "tool": tool,
                "status": "requested",
                "input": tool_input,
                "result": None,
                "denied_reason": None,
                "output": None,
            },
        )
        await publish(
            "tool.requested",
            {
                **state,
                "tool": tool,
                "tool_call_id": tool_call_id,
                "workflow": workflow,
                "input": tool_input,
            },
        )
        span.set_attribute("app.tool_call_id", tool_call_id)
        return {"run_id": run_id, "tool_call_id": tool_call_id, "status": "requested"}


@app.get("/internal/runs/{run_id}/tool-calls/{tool_call_id}")
async def get_tool_call(
    run_id: str,
    tool_call_id: str,
    x_agent_id: str = Header(),
    x_callback_token: str = Header(default=""),
) -> dict[str, Any]:
    run_record = require_callback_run(run_id, x_agent_id, x_callback_token)
    tool_call = run_record.get("tool_calls", {}).get(tool_call_id)
    if tool_call is None:
        raise HTTPException(status_code=404, detail="Tool call not found")
    return tool_call


@app.post("/internal/runs/{run_id}/complete")
async def complete_run(
    run_id: str,
    body: RunCompleteIn,
    x_agent_id: str = Header(),
    x_callback_token: str = Header(default=""),
) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "orchestrator.callback_run_completed",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.run_id": run_id,
                "app.agent_id": x_agent_id,
                "app.run_status": body.status,
            },
        ),
    ) as span:
        run_record = require_callback_run(run_id, x_agent_id, x_callback_token)
        if run_record.get("status") != "running":
            span.set_status(Status(StatusCode.ERROR, "Run is not active"))
            raise HTTPException(status_code=409, detail="Run is not active")

        status = body.status if body.status in {"completed", "failed"} else "failed"
        output = body.output or run_output_for_status(status)
        remember_run(
            run_id,
            {
                "status": status,
                "result": body.result,
                "output": output,
            },
        )
        await publish(
            "audit.events",
            {
                "request_id": run_id,
                "tenant_id": run_record.get("tenant_id"),
                "user_id": run_record.get("user_id"),
                "agent_id": run_record.get("agent_id"),
                "workflow": run_record.get("workflow"),
                "event": f"agent_callback_run_{status}",
            },
        )
        if status == "failed":
            span.set_status(Status(StatusCode.ERROR, output))
            discard_langfuse_run_trace(run_id, output)
        else:
            discard_langfuse_run_trace(run_id)
        return RUNS[run_id]


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
