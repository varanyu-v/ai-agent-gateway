"""Shared LiteLLM chat-completion client for every LLM caller in the gateway.

Both callers — the orchestrator's supervisor router (apps/orchestrator/
router.py) and the agent-service planner (apps/agents/runtime.py) — talk to the
same LiteLLM proxy with the same request shape, record the same Langfuse
generation span, and share one error contract: never raise, return None so the
caller degrades to its deterministic fallback. That plumbing lives here once;
each caller keeps only what is genuinely its own — prompts, span naming, and
response parsing.

Configuration is read once at import, like every other env-driven setting in
this codebase; restart the services to apply changes. When LITELLM_API_KEY or
LITELLM_MODEL is unset the client is "not configured": complete() records that
on the span and returns None without issuing a request, so an unconfigured
deployment still answers from fallbacks instead of failing runs.
"""

import os
from contextlib import contextmanager
from typing import Any

import httpx
from opentelemetry.context import Context
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer

from apps.langfuse_utils import (
    langfuse_json,
    langfuse_payload,
    litellm_usage_attributes,
    record_span_error,
)
from apps.observability import clean_attributes


BASE_URL = os.getenv("LITELLM_BASE_URL", "http://localhost:4000/v1").rstrip("/")
API_KEY = os.getenv("LITELLM_API_KEY", "")
MODEL = os.getenv("LITELLM_MODEL", "")
TIMEOUT_SECONDS = float(os.getenv("LITELLM_TIMEOUT_SECONDS", "30"))

DEFAULT_ERROR_MESSAGE = "LiteLLM call failed"


def is_configured() -> bool:
    """True when a model and key are set, i.e. calls will actually be issued."""
    return bool(API_KEY and MODEL)


@contextmanager
def generation_span(
    tracer: Tracer,
    name: str,
    workflow: str,
    message: str,
    kind: str,
    parent_context: Context | None,
    extra_attributes: dict[str, Any] | None = None,
):
    """Record one LLM call as a Langfuse generation under the caller's root span.

    `kind` becomes the observation's metadata.kind, so callers with several
    prompts (router vs. answer, planner) stay distinguishable in Langfuse.
    """
    span = tracer.start_span(
        name,
        context=parent_context if parent_context is not None else Context(),
        kind=SpanKind.CLIENT,
        attributes=clean_attributes(
            {
                "app.workflow": workflow,
                "app.planner.model": MODEL or None,
                "app.user_message.length": len(message),
                "gen_ai.operation.name": "chat",
                "gen_ai.system": "openai",
                "gen_ai.request.model": MODEL or None,
                "langfuse.observation.type": "generation",
                "langfuse.observation.model.name": MODEL or None,
                "langfuse.observation.metadata.kind": kind,
                "langfuse.observation.metadata.provider": "litellm",
                **(extra_attributes or {}),
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


def record_failure(span: Span, exc: BaseException, message: str) -> None:
    """Mark the generation failed. Callers then return their own fallback.

    Also used for failures the caller detects after the call returns, such as
    a response that is not the JSON the prompt asked for.
    """
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, message))
    span.set_attributes(
        {
            "app.planner.result": "fallback",
            "langfuse.observation.level": "ERROR",
            "langfuse.observation.status_message": str(exc),
        },
    )


async def complete(
    span: Span,
    messages: list[dict[str, str]],
    error_message: str = DEFAULT_ERROR_MESSAGE,
) -> str | None:
    """POST one chat completion and record it on `span`.

    Returns the reply content, or None when the client is not configured or the
    call fails — never raises, so a LiteLLM outage degrades a run to its
    fallback instead of breaking it.
    """
    if not is_configured():
        span.set_attribute("app.planner.result", "not_configured")
        return None

    span.set_attribute(
        "langfuse.observation.input",
        langfuse_json(langfuse_payload(messages)),
    )
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            response = await client.post(
                f"{BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": MODEL,
                    "temperature": 0,
                    "messages": messages,
                },
            )
            span.set_attribute("http.response.status_code", response.status_code)
            response.raise_for_status()
        response_body = response.json()
        content = str(response_body["choices"][0]["message"]["content"])
    except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
        record_failure(span, exc, error_message)
        return None

    span.set_attributes(
        clean_attributes(
            {
                "gen_ai.response.model": response_body.get("model") or MODEL,
                "langfuse.observation.model.name": response_body.get("model") or MODEL,
                "langfuse.observation.output": langfuse_json(langfuse_payload(content)),
            },
        ),
    )
    span.set_attributes(litellm_usage_attributes(response_body.get("usage")))
    return content
