"""Configurable persona for every user-facing answer in the gateway.

Operators brand the assistant — its name, gender, tone, and extra style —
through ASSISTANT_* environment variables, without code changes. Every
prompt-bearing service (orchestrator router, agent services) and the gateway
chat UI reads the same variables, shared through the compose `x-app-env`
anchor, so the whole gateway speaks as one persona.

The persona carries voice only. Capability rules — what an agent may claim it
can do or access — stay hardcoded next to each prompt, so branding can never
widen or misstate an agent's abilities.

Like every other env-driven setting in this codebase, the persona is read once
at import; restart the services to apply changes. When no ASSISTANT_* variable
is set, every composed prompt and reply stays in the neutral default voice.
"""

import os
from dataclasses import dataclass


DEFAULT_ROLE = "the enterprise gateway assistant"


@dataclass(frozen=True)
class Persona:
    """Identity and voice applied to LLM prompts and static fallback replies."""

    name: str = ""  # introduction name; empty keeps the neutral role-only voice
    role: str = DEFAULT_ROLE
    gender: str = ""  # picks polite/self-referential forms in gendered languages
    tone: str = ""
    style: str = ""  # free-form extra voice instructions
    preamble_override: str = ""  # replaces the generated persona block wholesale
    welcome: str = ""  # chat UI welcome bubble; empty keeps the UI default

    @classmethod
    def from_env(cls) -> "Persona":
        return cls(
            name=os.getenv("ASSISTANT_NAME", "").strip(),
            role=os.getenv("ASSISTANT_ROLE", "").strip() or DEFAULT_ROLE,
            gender=os.getenv("ASSISTANT_GENDER", "").strip(),
            tone=os.getenv("ASSISTANT_TONE", "").strip(),
            style=os.getenv("ASSISTANT_STYLE", "").strip(),
            preamble_override=os.getenv("ASSISTANT_PERSONA_PROMPT", "").strip(),
            welcome=os.getenv("ASSISTANT_WELCOME_MESSAGE", "").strip(),
        )

    def _voice_lines(self) -> list[str]:
        lines = []
        if self.tone:
            lines.append(f"Tone: {self.tone}.")
        if self.gender:
            lines.append(
                f"Your persona is {self.gender}; use the matching polite and "
                "self-referential forms in gendered languages (for example "
                "Thai sentence particles)."
            )
        if self.style:
            lines.append(self.style)
        return lines

    def preamble(self) -> str:
        """Persona block for prompts where the model answers the user directly."""
        if self.preamble_override:
            return self.preamble_override
        if self.name:
            lines = [
                f"You are {self.name}, {self.role}.",
                f"Introduce and refer to yourself as {self.name}.",
            ]
        else:
            lines = [f"You are {self.role}."]
        lines.extend(self._voice_lines())
        return "\n".join(lines)

    def reply_rules(self) -> str:
        """Persona block for planner prompts whose only user-visible text is the
        "reply" field. Empty when nothing is configured, so unbranded planner
        prompts stay byte-identical to the pre-persona version."""
        if self.preamble_override:
            body = self.preamble_override
        else:
            lines = []
            if self.name:
                lines.append(
                    f"Speak as {self.name}, {self.role}; introduce yourself by "
                    "that name when greeting."
                )
            lines.extend(self._voice_lines())
            if not lines:
                return ""
            body = "\n".join(f"- {line}" for line in lines)
        return f'Persona for the user-facing "reply" text:\n{body}'

    def introduction(self, role: str | None = None) -> str:
        """Self-introduction for static fallback replies: "I'm <name>, <role>"."""
        described = role or self.role
        if self.name:
            return f"I'm {self.name}, {described}"
        return f"I'm {described}"


PERSONA = Persona.from_env()
