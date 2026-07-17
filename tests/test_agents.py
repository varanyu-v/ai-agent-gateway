import json
import unittest
from unittest.mock import MagicMock, patch

import httpx

from apps.agents import runtime
from apps.agents.procurement.main import app as procurement_app
from apps.agents.world import main as world_main
from apps.agents.world.main import app as world_app
from apps.orchestrator.registry import (
    AgentRegistry,
    AgentServiceError,
    parse_agent_services,
)


RUN_PAYLOAD = {
    "request_id": "req-1",
    "tenant_id": "demo-tenant",
    "user_id": "demo-user",
    "agent_id": "world-agent",
    "message": "show the largest cities by population",
    "thread_id": None,
    "allowed_permissions": ["world-db"],
    "policy_subjects": ["role:world-analyst"],
}


def agent_client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://agent-under-test",
    )


class AgentContractTests(unittest.IsolatedAsyncioTestCase):
    """Every agent service exposes the same discovery and run contract."""

    async def test_world_agent_card_describes_identity_and_capabilities(self) -> None:
        async with agent_client(world_app) as client:
            response = await client.get("/.well-known/agent-card")

        self.assertEqual(response.status_code, 200)
        card = response.json()
        self.assertEqual(card["protocol"], runtime.AGENT_PROTOCOL)
        self.assertEqual(card["id"], "world-agent")
        self.assertEqual(card["workflow"], "world")
        self.assertEqual(
            card["capabilities"]["actions"],
            ["approval", "brief", "report", "sql"],
        )
        self.assertEqual(card["requirements"]["permissions"], ["world-db"])
        self.assertEqual(card["endpoints"]["run"], "/runs")

    async def test_procurement_agent_card_has_no_report_action(self) -> None:
        async with agent_client(procurement_app) as client:
            response = await client.get("/.well-known/agent-card")

        card = response.json()
        self.assertEqual(card["id"], "procurement-agent")
        self.assertEqual(card["workflow"], "procurement")
        self.assertEqual(card["capabilities"]["actions"], ["approval", "sql"])

    async def test_health_reports_agent_service_name(self) -> None:
        async with agent_client(procurement_app) as client:
            response = await client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["service"], "procurement-agent")


class AgentDecisionTests(unittest.IsolatedAsyncioTestCase):
    """With no LiteLLM configured, the deterministic fallback planner routes."""

    def setUp(self) -> None:
        patcher = patch.object(runtime, "LITELLM_API_KEY", "")
        patcher.start()
        self.addCleanup(patcher.stop)

    async def run_agent(self, app, message: str) -> dict:
        async with agent_client(app) as client:
            response = await client.post(
                "/runs",
                json={**RUN_PAYLOAD, "message": message},
            )
        self.assertEqual(response.status_code, 200)
        return response.json()

    async def test_world_agent_routes_lookup_to_sql_tool(self) -> None:
        body = await self.run_agent(world_app, "show the largest cities")
        decision = body["decision"]
        self.assertEqual(body["workflow"], "world")
        self.assertEqual(decision["action"], "tool")
        self.assertEqual(decision["tool"], "sql")
        self.assertEqual(decision["required_permission"], "world-db")
        self.assertEqual(decision["tool_input"]["database"], "world")
        self.assertEqual(decision["planner_source"], "fallback")

    async def test_world_agent_routes_report_requests_to_report_tool(self) -> None:
        body = await self.run_agent(world_app, "generate a world market entry report")
        decision = body["decision"]
        self.assertEqual(decision["action"], "tool")
        self.assertEqual(decision["tool"], "report")
        self.assertEqual(
            decision["tool_input"]["report_type"],
            "world_market_summary",
        )

    async def test_world_agent_routes_destructive_requests_to_approval(self) -> None:
        body = await self.run_agent(world_app, "delete old city records")
        decision = body["decision"]
        self.assertEqual(decision["action"], "approval")
        self.assertEqual(decision["audit_event"], "human_approval_required")
        self.assertIsNone(decision["tool"])

    async def test_procurement_agent_routes_lookup_to_sql_tool(self) -> None:
        body = await self.run_agent(procurement_app, "rank suppliers by spend")
        decision = body["decision"]
        self.assertEqual(body["workflow"], "procurement")
        self.assertEqual(decision["action"], "tool")
        self.assertEqual(decision["tool"], "sql")
        self.assertEqual(decision["required_permission"], "procurement-db")
        self.assertEqual(decision["tool_input"]["database"], "procurement")

    async def test_procurement_agent_routes_destructive_requests_to_approval(self) -> None:
        body = await self.run_agent(
            procurement_app,
            "remove blocked supplier records",
        )
        decision = body["decision"]
        self.assertEqual(decision["action"], "approval")
        self.assertEqual(decision["audit_event"], "procurement_approval_required")

    async def test_world_agent_routes_brief_to_async_callback_run(self) -> None:
        with patch.object(runtime, "start_background_run") as start_background:
            body = await self.run_agent(world_app, "prepare a world market brief")
        decision = body["decision"]
        self.assertEqual(decision["action"], "async")
        self.assertEqual(decision["audit_event"], "world_market_brief_started")
        self.assertIsNone(decision["tool"])
        start_background.assert_called_once()
        definition, request = start_background.call_args.args
        self.assertIs(definition, world_main.DEFINITION)
        self.assertEqual(request.request_id, "req-1")


class OrchestratorBrokerStub:
    """Simulates the orchestrator's tool-broker API behind httpx.MockTransport.

    Each tool call answers one poll with "requested" before settling with the
    configured outcome, so client polling is actually exercised.
    """

    def __init__(self, results: dict[str, dict]) -> None:
        self.results = results
        self.seq = 0
        self.pending_polls: dict[str, int] = {}
        self.completed_with: dict | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.headers.get("x-agent-id") != "world-agent":
            return httpx.Response(404, json={"detail": "Run not found"})
        path = request.url.path
        if request.method == "POST" and path.endswith("/complete"):
            self.completed_with = json.loads(request.content.decode())
            return httpx.Response(200, json={"status": "ok"})
        if request.method == "POST" and path.endswith("/tool-calls"):
            body = json.loads(request.content.decode())
            tool = body["tool"]
            outcome = self.results.get(tool)
            if outcome is not None and outcome.get("status_code"):
                return httpx.Response(
                    outcome["status_code"],
                    json={"detail": outcome["detail"]},
                )
            self.seq += 1
            tool_call_id = f"req-1:{tool}:{self.seq}"
            self.pending_polls[tool_call_id] = 1
            return httpx.Response(
                200,
                json={
                    "run_id": "req-1",
                    "tool_call_id": tool_call_id,
                    "status": "requested",
                },
            )
        if request.method == "GET" and "/tool-calls/" in path:
            tool_call_id = path.rsplit("/", 1)[-1]
            tool = tool_call_id.split(":")[1]
            if self.pending_polls.get(tool_call_id, 0) > 0:
                self.pending_polls[tool_call_id] -= 1
                return httpx.Response(
                    200,
                    json={"tool_call_id": tool_call_id, "status": "requested"},
                )
            return httpx.Response(
                200,
                json={"tool_call_id": tool_call_id, "tool": tool, **self.results[tool]},
            )
        return httpx.Response(404, json={"detail": "not found"})


class ToolBrokerClientTests(unittest.IsolatedAsyncioTestCase):
    def broker_client(self, stub: OrchestratorBrokerStub) -> runtime.ToolBrokerClient:
        return runtime.ToolBrokerClient(
            "world-agent",
            "req-1",
            base_url="http://orchestrator-under-test",
            callback_token="",
            poll_interval_seconds=0,
            timeout_seconds=5,
            transport=httpx.MockTransport(stub.handler),
        )

    async def test_market_brief_drives_sql_then_report_and_completes(self) -> None:
        stub = OrchestratorBrokerStub(
            {
                "sql": {"status": "completed", "result": {"rows": [{"city": "a"}] * 3}},
                "report": {"status": "completed", "result": {"report_id": "report-9"}},
            },
        )
        request = runtime.AgentRunRequest(
            **{**RUN_PAYLOAD, "message": "prepare a world market brief"},
        )
        client = self.broker_client(stub)
        with patch.object(runtime, "ToolBrokerClient", MagicMock(return_value=client)):
            await runtime.drive_background_run(world_main.DEFINITION, request)

        self.assertIsNotNone(stub.completed_with)
        self.assertEqual(stub.completed_with["status"], "completed")
        self.assertIn("3 city row(s)", stub.completed_with["output"])
        self.assertIn("report-9", stub.completed_with["output"])

    async def test_failed_tool_reports_run_failure(self) -> None:
        stub = OrchestratorBrokerStub(
            {"sql": {"status": "failed", "output": "Tool execution failed."}},
        )
        request = runtime.AgentRunRequest(
            **{**RUN_PAYLOAD, "message": "prepare a world market brief"},
        )
        client = self.broker_client(stub)
        with patch.object(runtime, "ToolBrokerClient", MagicMock(return_value=client)):
            await runtime.drive_background_run(world_main.DEFINITION, request)

        self.assertEqual(stub.completed_with["status"], "failed")
        self.assertEqual(stub.completed_with["output"], "Tool execution failed.")

    async def test_denied_tool_call_raises_broker_error_with_detail(self) -> None:
        stub = OrchestratorBrokerStub(
            {
                "sql": {
                    "status_code": 403,
                    "detail": "User cannot use data source permission: world-db",
                },
            },
        )
        async with self.broker_client(stub) as client:
            with self.assertRaises(runtime.ToolBrokerError) as raised:
                await client.run_tool("sql", {"database": "world"}, "world-db")
        self.assertEqual(raised.exception.status_code, 403)
        self.assertIn("world-db", raised.exception.detail)


class AgentRegistryTests(unittest.IsolatedAsyncioTestCase):
    def test_parse_agent_services(self) -> None:
        services = parse_agent_services(
            "world-agent=http://world-agent:8004/, procurement-agent=http://procurement-agent:8005",
        )
        self.assertEqual(
            services,
            {
                "world-agent": "http://world-agent:8004",
                "procurement-agent": "http://procurement-agent:8005",
            },
        )

    def test_parse_agent_services_skips_malformed_entries(self) -> None:
        self.assertEqual(parse_agent_services("bad-entry,,=http://x"), {})

    async def test_registry_discovers_cards_and_invokes_runs(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/.well-known/agent-card":
                return httpx.Response(
                    200,
                    json={"id": "world-agent", "name": "World Analyst Agent", "workflow": "world"},
                )
            if request.url.path == "/runs":
                body = json.loads(request.content.decode())
                return httpx.Response(
                    200,
                    json={
                        "agent_id": "world-agent",
                        "request_id": body["request_id"],
                        "workflow": "world",
                        "decision": {"action": "tool", "tool": "sql"},
                    },
                )
            return httpx.Response(404)

        registry = AgentRegistry("world-agent=http://world-agent:8004")
        await registry.start(transport=httpx.MockTransport(handler))
        try:
            agent = registry.get("world-agent")
            self.assertIsNotNone(agent)
            self.assertEqual(agent.workflow, "world")
            self.assertEqual(agent.name, "World Analyst Agent")

            response = await registry.invoke_run(agent, RUN_PAYLOAD)
            self.assertEqual(response["decision"]["tool"], "sql")

            self.assertIsNone(registry.get("unknown-agent"))
        finally:
            await registry.aclose()

    async def test_registry_normalizes_agent_failures(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/.well-known/agent-card":
                return httpx.Response(200, json={"workflow": "world"})
            return httpx.Response(500)

        registry = AgentRegistry("world-agent=http://world-agent:8004")
        await registry.start(transport=httpx.MockTransport(handler))
        try:
            agent = registry.get("world-agent")
            with self.assertRaises(AgentServiceError) as raised:
                await registry.invoke_run(agent, RUN_PAYLOAD)
            self.assertEqual(raised.exception.status_code, 502)
        finally:
            await registry.aclose()


if __name__ == "__main__":
    unittest.main()
