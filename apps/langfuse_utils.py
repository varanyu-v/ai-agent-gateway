"""Langfuse trace helpers shared by the orchestrator and the agent services.

The orchestrator owns the logical `agent-run` root span; agent services emit
planner generations under it. Because they are separate processes, the root
span context crosses the HTTP boundary in the `x-langfuse-traceparent` header
(W3C traceparent format) so every observation lands in one Langfuse trace.
"""

import json
import os
from typing import Any

from opentelemetry import trace as otel_trace
from opentelemetry.context import Context
from opentelemetry.trace import NonRecordingSpan, Span, Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

from apps.observability import clean_attributes


LANGFUSE_PARENT_HEADER = "x-langfuse-traceparent"
_DISABLED_ENV_VALUES = {"0", "false", "no", "off"}


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


def build_trace_attributes(
    workflow: str,
    context: dict[str, Any] | None,
) -> dict[str, Any]:
    """Langfuse trace-level attributes shared by the run root and children."""
    context = context or {}
    request_id = context.get("request_id")
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


def trace_id_hex(span: Span) -> str | None:
    span_context = span.get_span_context()
    if not span_context.is_valid:
        return None
    return f"{span_context.trace_id:032x}"


def span_child_context(span: Span) -> Context | None:
    span_context = span.get_span_context()
    if not span_context.is_valid:
        return None
    return otel_trace.set_span_in_context(
        NonRecordingSpan(span_context),
        Context(),
    )


def traceparent_value(span: Span) -> str | None:
    span_context = span.get_span_context()
    if not span_context.is_valid:
        return None
    return (
        f"00-{span_context.trace_id:032x}"
        f"-{span_context.span_id:016x}"
        f"-{int(span_context.trace_flags):02x}"
    )


def context_from_traceparent(header_value: str | None) -> Context | None:
    if not header_value:
        return None
    context = TraceContextTextMapPropagator().extract(
        {"traceparent": header_value},
        context=Context(),
    )
    span_context = otel_trace.get_current_span(context).get_span_context()
    if not span_context.is_valid:
        return None
    return context


def record_span_error(span: Span, exc: BaseException | str) -> None:
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
