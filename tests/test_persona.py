"""Persona composition: branding reshapes prompts, defaults stay neutral."""

import unittest

from apps.agents import runtime
from apps.orchestrator import router
from apps.orchestrator.agent_registry import RegisteredAgent
from apps.persona import DEFAULT_ROLE, REPLY_LANGUAGE_RULE, Persona


def _agent(agent_id: str, description: str) -> RegisteredAgent:
    return RegisteredAgent(
        agent_id=agent_id,
        base_url=f"http://{agent_id}:8000",
        workflow=agent_id.removesuffix("-agent"),
        name=agent_id,
        card={"description": description},
    )


BRANDED = Persona(
    name="Taokae Procurement Agent",
    role="the procurement assistant",
    gender="female",
    tone="warm and concise",
)


class PersonaTests(unittest.TestCase):
    def test_default_preamble_keeps_neutral_identity(self) -> None:
        self.assertEqual(Persona().preamble(), f"You are {DEFAULT_ROLE}.")

    def test_named_preamble_carries_identity_and_voice(self) -> None:
        preamble = BRANDED.preamble()
        self.assertIn(
            "You are Taokae Procurement Agent, the procurement assistant.",
            preamble,
        )
        self.assertIn(
            "Introduce and refer to yourself as Taokae Procurement Agent.",
            preamble,
        )
        self.assertIn("Tone: warm and concise.", preamble)
        self.assertIn("female", preamble)

    def test_override_replaces_generated_preamble(self) -> None:
        persona = Persona(name="Ignored", preamble_override="You are Somchai.")
        self.assertEqual(persona.preamble(), "You are Somchai.")

    def test_default_reply_rules_are_empty(self) -> None:
        self.assertEqual(Persona().reply_rules(), "")

    def test_branded_reply_rules_scope_persona_to_reply_text(self) -> None:
        rules = BRANDED.reply_rules()
        self.assertIn('Persona for the user-facing "reply" text:', rules)
        self.assertIn("Taokae Procurement Agent", rules)
        self.assertIn("Tone: warm and concise.", rules)

    def test_introduction_with_and_without_name(self) -> None:
        self.assertEqual(Persona().introduction(), f"I'm {DEFAULT_ROLE}")
        self.assertEqual(
            BRANDED.introduction("the world analyst agent"),
            "I'm Taokae Procurement Agent, the world analyst agent",
        )


class PromptCompositionTests(unittest.TestCase):
    def test_general_answer_prompt_keeps_capability_rules(self) -> None:
        prompt = router.build_general_answer_system_prompt(BRANDED)
        self.assertIn("Taokae Procurement Agent", prompt)
        self.assertIn("access to company databases", prompt)

    def test_fallback_general_answer_introduces_persona(self) -> None:
        text = router.build_fallback_general_answer(BRANDED)
        self.assertTrue(text.startswith("I'm Taokae Procurement Agent"))
        self.assertIn("language model is not configured", text)

    def test_access_denied_answer_names_the_permission_boundary(self) -> None:
        text = router.build_access_denied_answer(BRANDED, "procurement-agent")
        self.assertTrue(text.startswith("I'm Taokae Procurement Agent"))
        # The reader must hear why the answer is missing, in these terms: a
        # permission limit on a specialist that exists, not absent data.
        self.assertIn("procurement specialist", text)
        self.assertIn("not permitted", text)
        self.assertIn("not missing data", text)

    def test_access_denied_answer_stays_truthful_unbranded(self) -> None:
        text = router.build_access_denied_answer(Persona(), "world-agent")
        self.assertTrue(text.startswith(f"I'm {DEFAULT_ROLE}"))
        self.assertIn("world specialist", text)

    def test_unbranded_planner_prompt_adds_no_persona(self) -> None:
        prompt = runtime.build_planner_system_prompt(Persona())
        self.assertEqual(
            prompt,
            f"{runtime.LLM_PLANNER_RULES}\n\n{REPLY_LANGUAGE_RULE}",
        )

    def test_branded_planner_prompt_appends_reply_persona(self) -> None:
        prompt = runtime.build_planner_system_prompt(BRANDED)
        self.assertTrue(prompt.startswith(runtime.LLM_PLANNER_RULES))
        self.assertIn("Taokae Procurement Agent", prompt)


class ReplyLanguageTests(unittest.TestCase):
    """The user's language wins over every English block around it, in both
    prompts that produce text the user reads."""

    def test_general_answer_prompt_ends_with_the_language_rule(self) -> None:
        # Last block, so it outranks the English capability lists above it.
        prompt = router.build_general_answer_system_prompt(
            BRANDED,
            agents=[_agent("procurement-agent", "Procurement data specialist")],
            tools=[{"name": "supplier_spend_summary", "description": "Spend by supplier"}],
        )
        self.assertTrue(prompt.endswith(REPLY_LANGUAGE_RULE))

    def test_language_rule_survives_an_empty_capability_list(self) -> None:
        self.assertIn(REPLY_LANGUAGE_RULE, router.build_general_answer_system_prompt(Persona()))

    def test_planner_prompt_carries_the_language_rule_branded_or_not(self) -> None:
        for persona in (Persona(), BRANDED):
            with self.subTest(persona=persona.name or "unbranded"):
                self.assertIn(
                    REPLY_LANGUAGE_RULE,
                    runtime.build_planner_system_prompt(persona),
                )

    def test_rule_names_switching_and_ignores_surrounding_english(self) -> None:
        # The behaviour the rule exists for: a user who switches to Thai after
        # asking in English gets Thai back, despite an all-English prompt.
        # Wrapped prose, so compare on a single normalized line.
        rule = " ".join(REPLY_LANGUAGE_RULE.split())
        self.assertIn("language of the user's latest message", rule)
        self.assertIn("switch language at any time", rule)
        self.assertIn("rather than earlier ones", rule)

    def test_language_rule_is_not_brandable(self) -> None:
        # An operator persona must not be able to override or drop the rule.
        hijacked = Persona(preamble_override="Always answer in English.")
        self.assertIn(
            REPLY_LANGUAGE_RULE,
            router.build_general_answer_system_prompt(hijacked),
        )
        self.assertIn(
            REPLY_LANGUAGE_RULE,
            runtime.build_planner_system_prompt(hijacked),
        )


if __name__ == "__main__":
    unittest.main()
