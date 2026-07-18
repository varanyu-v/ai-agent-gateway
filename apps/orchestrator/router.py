"""Supervisor router exposed by the orchestrator as a virtual agent.

The router (agent id ROUTER_AGENT_ID, default "assistant") lets users send any
question to one place. It classifies each message with the LiteLLM planner —
falling back to deterministic keyword matching when the LLM is unavailable —
and either answers general questions directly or hands domain questions
(procurement, world, ...) to the matching registered agent service through the
orchestrator's normal policy-enforced run path.

Classification candidates are built from the live agent registry, so adding a
new agent service to AGENT_SERVICES extends the router without code changes.
Both LLM calls are recorded as Langfuse generations under the run's
`agent-run` root span.
"""

import json
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterable

import httpx
from opentelemetry.context import Context
from opentelemetry.trace import Span, SpanKind, Status, StatusCode

from apps.langfuse_utils import (
    langfuse_json,
    langfuse_payload,
    litellm_usage_attributes,
    record_span_error,
)
from apps.observability import clean_attributes, setup_langfuse_observability
from apps.orchestrator.agent_registry import RegisteredAgent


ROUTER_AGENT_ID = os.getenv("ROUTER_AGENT_ID", "assistant")
ROUTER_WORKFLOW = "assistant"
GENERAL_ROUTE = "general"

LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "http://localhost:4000/v1").rstrip("/")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "")
LITELLM_MODEL = os.getenv("LITELLM_MODEL", "")
LITELLM_TIMEOUT_SECONDS = float(os.getenv("LITELLM_TIMEOUT_SECONDS", "30"))

# Deterministic fallback vocabulary per agent workflow, used when the LLM
# router is not configured or fails. Single words match whole words only;
# phrases match as substrings.
DOMAIN_KEYWORDS: dict[str, tuple[str, ...]] = {
    "world": (
        "world",
        "city",
        "cities",
        "country",
        "countries",
        "continent",
        "population",
        "market brief",
    ),
    "procurement": (
        "procurement",
        "supplier",
        "suppliers",
        "vendor",
        "vendors",
        "purchase",
        "purchasing",
        "sourcing",
        "spend",
        "rfq",
        "purchase order",
    ),
}

ROUTE_SYSTEM_PROMPT_HEADER = """
You route user messages for an enterprise assistant.
Return only a JSON object with this shape:
{"route":"<route-id>","reason":"short reason"}
""".strip()

ROUTE_SYSTEM_PROMPT_RULES = """
Rules:
- Pick a specialist route only when the message is about that domain's data or actions.
- Pick "general" for greetings, small talk, and every other topic.
- When unsure, pick "general".
""".strip()

GENERAL_ANSWER_SYSTEM_PROMPT = """
You are the enterprise gateway assistant. Answer the user's question directly,
concisely, and helpfully. You have no access to company databases or tools in
this mode; if the question needs procurement or world data, say that the
matching specialist agent will handle it when asked directly.
""".strip()

FALLBACK_GENERAL_ANSWER = (
    "I can take general questions here and route procurement or world data "
    "questions to their specialist agents. The language model is not "
    "configured, so I cannot compose an answer to this question right now."
)


@dataclass(frozen=True)
class RouteDecision:
    target: str  # GENERAL_ROUTE or a registered agent_id
    source: str  # "litellm" | "fallback"
    reason: str | None = None


@dataclass(frozen=True)
class GeneralAnswer:
    text: str
    source: str  # "litellm" | "fallback"


langfuse_tracer = setup_langfuse_observability("orchestrator")


def is_router_agent(agent_id: str) -> bool:
    return agent_id == ROUTER_AGENT_ID


def _keyword_matches(text: str, words: set[str], keyword: str) -> bool:
    if " " in keyword:
        return keyword in text
    return keyword in words


def _agent_domains(agent: RegisteredAgent) -> set[str]:
    # Before card discovery the registry defaults workflow to the agent id,
    # so derive the domain name from both spellings.
    return {agent.workflow, agent.agent_id, agent.agent_id.removesuffix("-agent")}


def fallback_route(message: str, agents: Iterable[RegisteredAgent]) -> str:
    text = message.lower()
    words = set(re.findall(r"[a-z0-9]+", text))
    for agent in agents:
        keywords = _agent_domains(agent)
        for domain in tuple(keywords):
            keywords.update(DOMAIN_KEYWORDS.get(domain, ()))
        if any(_keyword_matches(text, words, keyword) for keyword in keywords):
            return agent.agent_id
    return GENERAL_ROUTE


def route_system_prompt(agents: Iterable[RegisteredAgent]) -> str:
    routes = ["- general: greetings, small talk, and any other topic."]
    routes.extend(
        f"- {agent.agent_id}: {agent.card.get('description') or agent.name}"
        for agent in agents
    )
    return "\n".join(
        [ROUTE_SYSTEM_PROMPT_HEADER, "", "Routes:", *routes, "", ROUTE_SYSTEM_PROMPT_RULES],
    )


@contextmanager
def _generation_span(name: str, kind: str, message: str, parent_context: Context | None):
    span = langfuse_tracer.start_span(
        name,
        context=parent_context if parent_context is not None else Context(),
        kind=SpanKind.CLIENT,
        attributes=clean_attributes(
            {
                "app.workflow": ROUTER_WORKFLOW,
                "app.planner.model": LITELLM_MODEL or None,
                "app.user_message.length": len(message),
                "gen_ai.operation.name": "chat",
                "gen_ai.system": "openai",
                "gen_ai.request.model": LITELLM_MODEL or None,
                "langfuse.observation.type": "generation",
                "langfuse.observation.model.name": LITELLM_MODEL or None,
                "langfuse.observation.metadata.kind": kind,
                "langfuse.observation.metadata.provider": "litellm",
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


async def _litellm_completion(span: Span, messages: list[dict[str, str]]) -> str | None:
    if not LITELLM_API_KEY or not LITELLM_MODEL:
        span.set_attribute("app.planner.result", "not_configured")
        return None

    span.set_attribute(
        "langfuse.observation.input",
        langfuse_json(langfuse_payload(messages)),
    )
    try:
        async with httpx.AsyncClient(timeout=LITELLM_TIMEOUT_SECONDS) as client:
            response = await client.post(
                f"{LITELLM_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {LITELLM_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": LITELLM_MODEL,
                    "temperature": 0,
                    "messages": messages,
                },
            )
            span.set_attribute("http.response.status_code", response.status_code)
            response.raise_for_status()
        response_body = response.json()
        content = str(response_body["choices"][0]["message"]["content"])
    except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, "LiteLLM call failed"))
        span.set_attributes(
            {
                "app.planner.result": "fallback",
                "langfuse.observation.level": "ERROR",
                "langfuse.observation.status_message": str(exc),
            },
        )
        return None

    span.set_attributes(
        clean_attributes(
            {
                "gen_ai.response.model": response_body.get("model") or LITELLM_MODEL,
                "langfuse.observation.model.name": response_body.get("model")
                or LITELLM_MODEL,
                "langfuse.observation.output": langfuse_json(langfuse_payload(content)),
            },
        ),
    )
    span.set_attributes(litellm_usage_attributes(response_body.get("usage")))
    return content


async def classify_route(
    message: str,
    agents: list[RegisteredAgent],
    langfuse_parent: Context | None = None,
) -> RouteDecision:
    """Pick a registered agent or "general" for the message."""
    allowed_routes = {agent.agent_id for agent in agents}
    with _generation_span("assistant.route", "router", message, langfuse_parent) as span:
        content = await _litellm_completion(
            span,
            [
                {"role": "system", "content": route_system_prompt(agents)},
                {"role": "user", "content": message},
            ],
        )
        if content is not None:
            try:
                parsed = json.loads(content)
            except ValueError:
                parsed = None
            if isinstance(parsed, dict):
                target = str(parsed.get("route") or "").strip()
                if target == GENERAL_ROUTE or target in allowed_routes:
                    span.set_attributes(
                        {"app.planner.result": "selected", "app.route.target": target},
                    )
                    reason = parsed.get("reason")
                    return RouteDecision(
                        target=target,
                        source="litellm",
                        reason=str(reason) if reason else None,
                    )
                span.set_attributes(
                    {"app.planner.result": "invalid_route", "app.route.target": target},
                )

        target = fallback_route(message, agents)
        span.set_attribute("app.route.target", target)
        return RouteDecision(target=target, source="fallback")


async def answer_general(
    message: str,
    langfuse_parent: Context | None = None,
) -> GeneralAnswer:
    """Answer a general (non-domain) question with the LLM, or a static fallback."""
    with _generation_span("assistant.answer", "answer", message, langfuse_parent) as span:
        content = await _litellm_completion(
            span,
            [
                {"role": "system", "content": GENERAL_ANSWER_SYSTEM_PROMPT},
                {"role": "user", "content": message},
            ],
        )
        if content and content.strip():
            span.set_attribute("app.planner.result", "answered")
            return GeneralAnswer(text=content.strip(), source="litellm")
        span.set_attribute("app.planner.result", "fallback")
        return GeneralAnswer(text=FALLBACK_GENERAL_ANSWER, source="fallback")
