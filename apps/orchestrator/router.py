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
from typing import Any, Iterable

from opentelemetry.context import Context

from apps import litellm_client
from apps.observability import setup_langfuse_observability
from apps.orchestrator.agent_registry import RegisteredAgent
from apps.persona import PERSONA, REPLY_LANGUAGE_RULE, Persona


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
access to company databases or tools in this mode. When the question needs
data a specialist agent covers, say that specialist will handle it when asked
directly.
""".strip()

# The capability list answers "what can I do here?" without widening the rule
# above: it names specialists the caller may reach, not abilities this mode
# has. Only agents the caller can already invoke are ever listed, so the model
# cannot disclose the existence of an agent the user has no access to.
CAPABILITY_RULES = """
Specialists this user may ask for directly (you cannot run them yourself in
this mode, and you must not mention any specialist, database, or tool that is
absent from this list):
""".strip()

NO_CAPABILITY_RULES = """
This user may not reach any specialist agent. Answer general questions only,
and do not name or describe any specialist, database, or tool.
""".strip()

# Tools the caller's specialists can run for them, listed so the assistant can
# answer "what can you do / list the tools" accurately. Like the agent list, it
# names abilities reachable through specialists — not powers this general mode
# has — and only tools the caller may already use are ever passed in, so the
# model cannot disclose a tool the user has no access to.
TOOL_CAPABILITY_RULES = """
Tools the specialists above can run for this user, so you may list them when the
user asks what you can do or which tools exist (you cannot run them yourself in
this mode, and you must not mention any tool absent from this list):
""".strip()

# Agent cards and MCP tool metadata are fetched over HTTP from each service, so
# their descriptions are untrusted input to every prompt they land in.
DESCRIPTION_MAX_LENGTH = 200


def _agent_summary(agent: RegisteredAgent) -> str:
    """Collapse an agent card description to one capped, single-line summary.

    A card carrying newlines or its own "Rules:" section must not be able to
    restructure the prompt it is interpolated into.
    """
    raw = str(agent.card.get("description") or agent.name or agent.agent_id)
    summary = " ".join(raw.split())
    if len(summary) > DESCRIPTION_MAX_LENGTH:
        summary = f"{summary[:DESCRIPTION_MAX_LENGTH].rstrip()}..."
    return summary


def capability_rules(agents: Iterable[RegisteredAgent]) -> str:
    lines = [f"- {agent.agent_id}: {_agent_summary(agent)}" for agent in agents]
    if not lines:
        return NO_CAPABILITY_RULES
    return "\n".join([CAPABILITY_RULES, *lines])


def _tool_summary(tool: dict[str, Any]) -> str:
    """One capped, single-line summary of a tool from untrusted MCP metadata,
    so a description carrying newlines or its own "Rules:" section cannot
    restructure the prompt it lands in — same treatment as an agent card."""
    raw = str(tool.get("description") or tool.get("name") or "")
    summary = " ".join(raw.split())
    if len(summary) > DESCRIPTION_MAX_LENGTH:
        summary = f"{summary[:DESCRIPTION_MAX_LENGTH].rstrip()}..."
    return summary


def tool_capability_rules(tools: Iterable[dict[str, Any]]) -> str:
    """Prompt block naming the caller's permitted tools, or "" when there are
    none (callers omit the block entirely rather than emit an empty list)."""
    lines = []
    for tool in tools:
        name = str(tool.get("name") or "").strip()
        if not name:
            continue
        lines.append(f"- {name}: {_tool_summary(tool)}")
    if not lines:
        return ""
    return "\n".join([TOOL_CAPABILITY_RULES, *lines])


def build_general_answer_system_prompt(
    persona: Persona,
    agents: Iterable[RegisteredAgent] = (),
    tools: Iterable[dict[str, Any]] = (),
) -> str:
    agents = list(agents)
    tool_block = tool_capability_rules(tools)
    blocks = [persona.preamble(), GENERAL_ANSWER_RULES]
    if agents:
        blocks.append(capability_rules(agents))
    elif not tool_block:
        # No specialists and no tools: keep the explicit "name nothing" guard.
        blocks.append(NO_CAPABILITY_RULES)
    # else: no reachable agents but the caller has tools — the tool block is the
    # only capability list and carries its own "mention nothing else" guard.
    if tool_block:
        blocks.append(tool_block)
    # Last block: the capability lists above are always English, and the rule
    # has to outrank them when the user writes in another language.
    blocks.append(REPLY_LANGUAGE_RULE)
    return "\n\n".join(blocks)


def build_fallback_general_answer(persona: Persona) -> str:
    # Names no specialist: this static text is shared by every caller, so it
    # cannot know which agents the reader is allowed to reach.
    return (
        f"{persona.introduction()}. I can take general questions here and "
        "route domain questions to the specialist agents you have access to. "
        "The language model is not configured, so I cannot compose "
        "an answer to this question right now."
    )


def build_access_denied_answer(persona: Persona, agent_id: str) -> str:
    """User-facing refusal when a message needs an agent the caller cannot use.

    The reader is told the real reason — a permission boundary — instead of the
    deflecting "here is what I can do instead" greeting they used to get from
    whichever reachable agent absorbed the question. Only the voice comes from
    the persona; like every other capability statement the wording is hardcoded,
    so branding cannot soften the refusal into an implied "not supported yet".

    The technical `denied_reason` recorded for audit stays separate from this.
    """
    domain = agent_id.removesuffix("-agent").replace("-", " ")
    return (
        f"{persona.introduction()}. Answering this needs the {domain} "
        f"specialist, and your account is not permitted to use it, so I cannot "
        "run it or its tools for you — this is an access limit, not missing "
        "data. Ask your administrator to grant access if you need it."
    )


def _build_denied_answer(persona: Persona, needed: str) -> str:
    """Shared refusal wording for a run stopped by an orchestrator policy check.

    Reaches the reader after the agent was already allowed to plan, so it says
    what the plan needed rather than naming the specialist.
    """
    return (
        f"{persona.introduction()}. Answering this needs {needed}, and your "
        "account is not permitted to use it, so I cannot run that step for "
        "you — this is an access limit, not missing data. Ask your "
        "administrator to grant access if you need it."
    )


def build_source_denied_answer(persona: Persona, permission: str) -> str:
    """User-facing refusal when the caller cannot read a required data source.

    The caller could invoke the agent, so the agent planned a real lookup and
    only the source check stopped it. Without this the chat UI fell back to the
    audit string ("User cannot use data source permission: procurement-db"),
    which reads like an internal error rather than a permission boundary.
    """
    source = permission.removesuffix("-db").replace("-", " ")
    return _build_denied_answer(persona, f"the {source} data source")


def build_tool_denied_answer(persona: Persona, tool: str) -> str:
    """User-facing refusal when the caller cannot execute a required tool."""
    server = tool.removeprefix("mcp:").removesuffix("-mcp").replace("-", " ")
    return _build_denied_answer(persona, f"the {server} tools")


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
    routes.extend(f"- {agent.agent_id}: {_agent_summary(agent)}" for agent in agents)
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
    agents: Iterable[RegisteredAgent] = (),
    tools: Iterable[dict[str, Any]] = (),
) -> GeneralAnswer:
    """Answer a general (non-domain) question with the LLM, or a static fallback.

    `agents` and `tools` must already be filtered to what the caller may reach —
    both are interpolated into the prompt, and the model treats everything in
    its context as fair game to repeat.
    """
    system_prompt = build_general_answer_system_prompt(PERSONA, agents, tools)
    with _generation_span("assistant.answer", "answer", message, langfuse_parent) as span:
        content = await litellm_client.complete(
            span,
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": message},
            ],
        )
        if content and content.strip():
            span.set_attribute("app.planner.result", "answered")
            return GeneralAnswer(text=content.strip(), source="litellm")
        span.set_attribute("app.planner.result", "fallback")
        return GeneralAnswer(text=FALLBACK_GENERAL_ANSWER, source="fallback")
