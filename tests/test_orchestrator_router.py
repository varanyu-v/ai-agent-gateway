"""Supervisor router: the orchestrator answers general questions itself and
routes procurement/world questions to the matching agent service.

Routing decisions are policy-checked: a delegated run only proceeds when the
caller's policy subjects can invoke the routed agent.
"""

import unittest
from unittest.mock import patch

import httpx

from apps.orchestrator import main as orchestrator
from apps.orchestrator import router
from apps.orchestrator.registry import DEFAULT_AGENT_SERVICES, AgentRegistry, RegisteredAgent


RUN_ID = "run-router-1"

RUN_HEADERS = {
    "x-request-id": RUN_ID,
    "x-tenant-id": "demo-tenant",
    "x-user-id": "demo-user",
    "x-allowed-permissions": "world-db",
    "x-policy-subjects": "role:world-analyst",
}


def registry_agents() -> list[RegisteredAgent]:
    return [
        RegisteredAgent(
            agent_id="world-agent",
            base_url="http://world-agent:8004",
            workflow="world",
            name="World Analyst Agent",
            card={"description": "Answers world-database questions."},
        ),
        RegisteredAgent(
            agent_id="procurement-agent",
            base_url="http://procurement-agent:8005",
            workflow="procurement",
            name="Procurement Analyst Agent",
            card={"description": "Answers procurement-database questions."},
        ),
    ]


def orchestrator_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=orchestrator.app),
        base_url="http://orchestrator-under-test",
    )


class RouterTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        orchestrator.RUNS.clear()
        self.published: list[tuple[str, dict]] = []

        async def fake_publish(topic: str, payload: dict) -> None:
            self.published.append((topic, payload))

        for patcher in (
            patch.object(orchestrator, "publish", fake_publish),
            patch.object(orchestrator, "agent_registry", AgentRegistry(DEFAULT_AGENT_SERVICES)),
            patch.object(router, "LITELLM_API_KEY", ""),
            patch.object(router, "LITELLM_MODEL", ""),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.addCleanup(orchestrator.RUNS.clear)

    def published_events(self, topic: str) -> list[dict]:
        return [payload for name, payload in self.published if name == topic]


class FallbackRouteTests(unittest.TestCase):
    def test_procurement_keywords_route_to_procurement_agent(self) -> None:
        route = router.fallback_route(
            "show supplier spend by vendor",
            registry_agents(),
        )
        self.assertEqual(route, "procurement-agent")

    def test_world_keywords_route_to_world_agent(self) -> None:
        route = router.fallback_route(
            "list the largest cities by population",
            registry_agents(),
        )
        self.assertEqual(route, "world-agent")

    def test_general_message_routes_to_general(self) -> None:
        route = router.fallback_route(
            "hello, what can you do for me?",
            registry_agents(),
        )
        self.assertEqual(route, router.GENERAL_ROUTE)

    def test_keywords_match_whole_words_only(self) -> None:
        # "capacity" must not match the "city" keyword.
        route = router.fallback_route(
            "explain capacity planning",
            registry_agents(),
        )
        self.assertEqual(route, router.GENERAL_ROUTE)


class ClassifyRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_llm_route_is_used_when_valid(self) -> None:
        async def fake_completion(span, messages) -> str:
            return '{"route":"procurement-agent","reason":"supplier data"}'

        with patch.object(router, "_litellm_completion", fake_completion):
            decision = await router.classify_route("supplier question", registry_agents())

        self.assertEqual(decision.target, "procurement-agent")
        self.assertEqual(decision.source, "litellm")
        self.assertEqual(decision.reason, "supplier data")

    async def test_invalid_llm_route_falls_back_to_keywords(self) -> None:
        async def fake_completion(span, messages) -> str:
            return '{"route":"nonexistent-agent"}'

        with patch.object(router, "_litellm_completion", fake_completion):
            decision = await router.classify_route(
                "list countries by population",
                registry_agents(),
            )

        self.assertEqual(decision.target, "world-agent")
        self.assertEqual(decision.source, "fallback")

    async def test_unparseable_llm_output_falls_back(self) -> None:
        async def fake_completion(span, messages) -> str:
            return "not json"

        with patch.object(router, "_litellm_completion", fake_completion):
            decision = await router.classify_route("hello there", registry_agents())

        self.assertEqual(decision.target, router.GENERAL_ROUTE)
        self.assertEqual(decision.source, "fallback")


class AssistantRunTests(RouterTestCase):
    async def test_general_question_is_answered_directly(self) -> None:
        async with orchestrator_client() as client:
            response = await client.post(
                f"/internal/agents/{router.ROUTER_AGENT_ID}/runs",
                headers=RUN_HEADERS,
                json={"message": "hello, what can you help me with today?"},
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "completed")
        self.assertEqual(body["agent_id"], router.ROUTER_AGENT_ID)
        self.assertEqual(body["output"], router.FALLBACK_GENERAL_ANSWER)

        run_record = orchestrator.RUNS[RUN_ID]
        self.assertEqual(run_record["status"], "completed")
        self.assertEqual(run_record["result"]["route"], router.GENERAL_ROUTE)

        events = [event["event"] for event in self.published_events("audit.events")]
        self.assertEqual(events, ["assistant_route_selected", "assistant_general_answered"])

    async def test_world_question_is_delegated_to_world_agent(self) -> None:
        invoked: list[str] = []

        async def fake_invoke(agent, state, thread_id, langfuse_span) -> dict:
            invoked.append(agent.agent_id)
            return {
                "action": "tool",
                "tool": "sql",
                "required_permission": "world-db",
                "tool_input": {"database": "world", "sql": "select 1"},
            }

        with patch.object(orchestrator, "invoke_agent_service", fake_invoke):
            async with orchestrator_client() as client:
                response = await client.post(
                    f"/internal/agents/{router.ROUTER_AGENT_ID}/runs",
                    headers=RUN_HEADERS,
                    json={"message": "show the top cities by population"},
                )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "running")
        self.assertEqual(body["agent_id"], "world-agent")
        self.assertEqual(invoked, ["world-agent"])

        run_record = orchestrator.RUNS[RUN_ID]
        self.assertEqual(run_record["agent_id"], "world-agent")
        self.assertEqual(run_record["routed_from"], router.ROUTER_AGENT_ID)

        route_events = [
            event
            for event in self.published_events("audit.events")
            if event["event"] == "assistant_route_selected"
        ]
        self.assertEqual(route_events[0]["route"], "world-agent")
        tool_requests = self.published_events("tool.requested")
        self.assertEqual(tool_requests[0]["agent_id"], "world-agent")
        # Workflow comes from the registry entry (agent id until the card is
        # discovered), exactly as on the direct-agent path.
        routed = orchestrator.agent_registry.get("world-agent")
        self.assertEqual(tool_requests[0]["workflow"], routed.workflow)

    async def test_routed_agent_requires_invoke_permission(self) -> None:
        async with orchestrator_client() as client:
            response = await client.post(
                f"/internal/agents/{router.ROUTER_AGENT_ID}/runs",
                headers=RUN_HEADERS,
                json={"message": "show supplier spend for this quarter"},
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "denied")
        self.assertIn("procurement-agent", body["denied_reason"])
        self.assertEqual(orchestrator.RUNS[RUN_ID]["status"], "denied")

        events = [event["event"] for event in self.published_events("audit.events")]
        self.assertIn("agent_access_denied", events)
        self.assertEqual(self.published_events("tool.requested"), [])

    async def test_unknown_agent_still_returns_404(self) -> None:
        async with orchestrator_client() as client:
            response = await client.post(
                "/internal/agents/no-such-agent/runs",
                headers=RUN_HEADERS,
                json={"message": "hello"},
            )

        self.assertEqual(response.status_code, 404)
        self.assertIn(router.ROUTER_AGENT_ID, response.json()["detail"])


if __name__ == "__main__":
    unittest.main()
