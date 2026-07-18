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
`agent-run` root span. User-visible text (general answers and the static
fallback) speaks in the configurable gateway persona (apps/persona.py).
"""

import json
import os
import re
from dataclasses import dataclass
from typing import Iterable

from opentelemetry.context import Context

from apps import litellm_client
from apps.observability import setup_langfuse_observability
from apps.orchestrator.agent_registry import RegisteredAgent
from apps.persona import PERSONA, Persona


ROUTER_AGENT_ID = os.getenv("ROUTER_AGENT_ID", "assistant")
ROUTER_WORKFLOW = "assistant"
GENERAL_ROUTE = "general"

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

# Capability rules stay hardcoded (not part of the configurable persona) so
# branding can never widen or misstate what this mode may claim to access.
GENERAL_ANSWER_RULES = """
Answer the user's question directly, concisely, and helpfully. You have no
access to company databases or tools in this mode; if the question needs
procurement or world data, say that the matching specialist agent will handle
it when asked directly.
""".strip()


def build_general_answer_system_prompt(persona: Persona) -> str:
    return "\n\n".join((persona.preamble(), GENERAL_ANSWER_RULES))


def build_fallback_general_answer(persona: Persona) -> str:
    return (
        f"{persona.introduction()}. I can take general questions here and "
        "route procurement or world data questions to their specialist "
        "agents. The language model is not configured, so I cannot compose "
        "an answer to this question right now."
    )


GENERAL_ANSWER_SYSTEM_PROMPT = build_general_answer_system_prompt(PERSONA)
FALLBACK_GENERAL_ANSWER = build_fallback_general_answer(PERSONA)


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


def _generation_span(name: str, kind: str, message: str, parent_context: Context | None):
    return litellm_client.generation_span(
        langfuse_tracer,
        name,
        ROUTER_WORKFLOW,
        message,
        kind,
        parent_context,
    )


async def classify_route(
    message: str,
    agents: list[RegisteredAgent],
    langfuse_parent: Context | None = None,
) -> RouteDecision:
    """Pick a registered agent or "general" for the message."""
    allowed_routes = {agent.agent_id for agent in agents}
    with _generation_span("assistant.route", "router", message, langfuse_parent) as span:
        content = await litellm_client.complete(
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
        content = await litellm_client.complete(
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
