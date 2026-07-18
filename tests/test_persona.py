"""Persona composition: branding reshapes prompts, defaults stay neutral."""

import unittest

from apps.agents import runtime
from apps.orchestrator import router
from apps.persona import DEFAULT_ROLE, Persona


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

    def test_unbranded_planner_prompt_is_unchanged(self) -> None:
        self.assertEqual(
            runtime.build_planner_system_prompt(Persona()),
            runtime.LLM_PLANNER_RULES,
        )

    def test_branded_planner_prompt_appends_reply_persona(self) -> None:
        prompt = runtime.build_planner_system_prompt(BRANDED)
        self.assertTrue(prompt.startswith(runtime.LLM_PLANNER_RULES))
        self.assertIn("Taokae Procurement Agent", prompt)


if __name__ == "__main__":
    unittest.main()
