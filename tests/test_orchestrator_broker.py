"""Tool-broker callback API: long-running agents request tools mid-run.

Every callback is enforced against the policy subjects stored when the run was
created, so the async path grants exactly the same access as the decision path.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import httpx

from apps.agents import runtime as agent_runtime
from apps.orchestrator import main as orchestrator
from apps.orchestrator import router
from apps.persona import PERSONA


RUN_ID = "run-broker-1"

RUNNING_OUTPUT = "Agent accepted the request and is waiting for tool output."


def seed_callback_run(**overrides) -> dict:
    record = {
        "run_id": RUN_ID,
        "request_id": RUN_ID,
        "status": "running",
        "agent_id": "world-agent",
        "tenant_id": "demo-tenant",
        "user_id": "demo-user",
        "message": "prepare a world market brief",
        "allowed_permissions": ["world-db"],
        "policy_subjects": ["role:world-analyst"],
        "mode": "callback",
        "workflow": "world",
        "tool_calls": {},
        "tool_call_seq": 0,
        "result": None,
        "denied_reason": None,
        "output": RUNNING_OUTPUT,
    }
    record.update(overrides)
    orchestrator.RUNS[RUN_ID] = record
    return record


def orchestrator_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=orchestrator.app),
        base_url="http://orchestrator-under-test",
    )


AGENT_HEADERS = {"x-agent-id": "world-agent"}


class ToolBrokerTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        orchestrator.RUNS.clear()
        self.published: list[tuple[str, dict]] = []

        async def fake_publish(topic: str, payload: dict) -> None:
            self.published.append((topic, payload))

        patcher = patch.object(orchestrator, "publish", fake_publish)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(orchestrator.RUNS.clear)

    def published_topics(self) -> list[str]:
        return [topic for topic, _ in self.published]


class AsyncDecisionTests(ToolBrokerTestCase):
    async def test_async_decision_marks_run_callback_mode(self) -> None:
        state: orchestrator.AgentState = {
            "request_id": RUN_ID,
            "tenant_id": "demo-tenant",
            "user_id": "demo-user",
            "agent_id": "world-agent",
            "message": "prepare a world market brief",
            "allowed_permissions": ["world-db"],
            "policy_subjects": ["role:world-analyst"],
            "needs_approval": False,
            "denied_reason": None,
        }

        result = await orchestrator.apply_agent_decision(
            state,
            "world",
            {"action": "async", "audit_event": "world_market_brief_started"},
        )

        self.assertEqual(result, {"needs_approval": False, "denied_reason": None})
        run_record = orchestrator.RUNS[RUN_ID]
        self.assertEqual(run_record["mode"], "callback")
        self.assertEqual(run_record["workflow"], "world")
        self.assertEqual(run_record["policy_subjects"], ["role:world-analyst"])
        self.assertEqual(run_record["tool_calls"], {})
        self.assertEqual(self.published_topics(), ["audit.events"])
        self.assertEqual(self.published[0][1]["event"], "world_market_brief_started")


class ToolCallEndpointTests(ToolBrokerTestCase):
    async def test_agent_requests_tool_then_completion_flows_back(self) -> None:
        seed_callback_run()
        tool_input = {
            "server": "world-mcp",
            "name": "list_top_cities",
            "arguments": {"limit": 1},
        }
        async with orchestrator_client() as client:
            response = await client.post(
                f"/internal/runs/{RUN_ID}/tool-calls",
                headers=AGENT_HEADERS,
                json={
                    "tool": "mcp",
                    "tool_input": tool_input,
                    "required_permission": "world-db",
                },
            )
            self.assertEqual(response.status_code, 200)
            tool_call_id = response.json()["tool_call_id"]
            self.assertEqual(tool_call_id, f"{RUN_ID}:mcp:1")

            topic, payload = self.published[-1]
            self.assertEqual(topic, "tool.requested")
            self.assertEqual(payload["tool_call_id"], tool_call_id)
            self.assertEqual(payload["input"], tool_input)
            self.assertEqual(payload["tenant_id"], "demo-tenant")

            pending = await client.get(
                f"/internal/runs/{RUN_ID}/tool-calls/{tool_call_id}",
                headers=AGENT_HEADERS,
            )
            self.assertEqual(pending.json()["status"], "requested")

            tool_result = {
                "server": "world-mcp",
                "tool": "list_top_cities",
                "output": {"rows": [{"value": 1}], "row_count": 1},
            }
            orchestrator.handle_tool_completed_event(
                {
                    "request_id": RUN_ID,
                    "tool": "mcp",
                    "tool_call_id": tool_call_id,
                    "status": "completed",
                    "result": tool_result,
                },
            )

            settled = await client.get(
                f"/internal/runs/{RUN_ID}/tool-calls/{tool_call_id}",
                headers=AGENT_HEADERS,
            )
            self.assertEqual(settled.json()["status"], "completed")
            self.assertEqual(settled.json()["result"], tool_result)
            # A settled tool call must not flip the whole run out of running.
            self.assertEqual(orchestrator.RUNS[RUN_ID]["status"], "running")

            done = await client.post(
                f"/internal/runs/{RUN_ID}/complete",
                headers=AGENT_HEADERS,
                json={"status": "completed", "output": "Brief is ready."},
            )
            self.assertEqual(done.status_code, 200)

            status = await client.get(
                f"/internal/runs/{RUN_ID}",
                headers={"x-tenant-id": "demo-tenant", "x-user-id": "demo-user"},
            )
        body = status.json()
        self.assertEqual(body["status"], "completed")
        self.assertEqual(body["output"], "Brief is ready.")
        self.assertIn(f"{RUN_ID}:mcp:1", body["tool_calls"])
        self.assertIn("audit.events", self.published_topics())
        self.assertEqual(self.published[-1][1]["event"], "agent_callback_run_completed")

    async def test_tool_call_sequence_increments(self) -> None:
        seed_callback_run()
        async with orchestrator_client() as client:
            first = await client.post(
                f"/internal/runs/{RUN_ID}/tool-calls",
                headers=AGENT_HEADERS,
                json={
                    "tool": "mcp",
                    "tool_input": {"server": "world-mcp", "name": "list_top_cities"},
                },
            )
            second = await client.post(
                f"/internal/runs/{RUN_ID}/tool-calls",
                headers=AGENT_HEADERS,
                json={
                    "tool": "mcp",
                    "tool_input": {
                        "server": "report-mcp",
                        "name": "generate_report",
                        "arguments": {"report_type": "brief"},
                    },
                },
            )
        self.assertEqual(first.json()["tool_call_id"], f"{RUN_ID}:mcp:1")
        self.assertEqual(second.json()["tool_call_id"], f"{RUN_ID}:mcp:2")
        self.assertEqual(len(orchestrator.RUNS[RUN_ID]["tool_calls"]), 2)

    async def test_tool_call_denied_without_permission(self) -> None:
        seed_callback_run(policy_subjects=["role:procurement-analyst"])
        async with orchestrator_client() as client:
            response = await client.post(
                f"/internal/runs/{RUN_ID}/tool-calls",
                headers=AGENT_HEADERS,
                json={
                    "tool": "mcp",
                    "tool_input": {"server": "world-mcp", "name": "list_top_cities"},
                    "required_permission": "world-db",
                },
            )
        self.assertEqual(response.status_code, 403)
        self.assertIn("world-db", response.json()["detail"])
        self.assertEqual(self.published_topics(), ["audit.events"])
        self.assertEqual(self.published[0][1]["event"], "permission_access_denied")
        # The run stays running: the agent decides how to proceed or fail.
        self.assertEqual(orchestrator.RUNS[RUN_ID]["status"], "running")

    async def test_tool_call_denied_for_disallowed_mcp_server(self) -> None:
        seed_callback_run(policy_subjects=["role:procurement-analyst"])
        async with orchestrator_client() as client:
            response = await client.post(
                f"/internal/runs/{RUN_ID}/tool-calls",
                headers=AGENT_HEADERS,
                json={
                    "tool": "mcp",
                    "tool_input": {"server": "report-mcp", "name": "generate_report"},
                },
            )
        self.assertEqual(response.status_code, 403)
        self.assertIn("mcp:report-mcp", response.json()["detail"])
        self.assertEqual(self.published[0][1]["event"], "tool_access_denied")

    async def test_tool_call_denied_for_non_mcp_tool(self) -> None:
        seed_callback_run()
        async with orchestrator_client() as client:
            response = await client.post(
                f"/internal/runs/{RUN_ID}/tool-calls",
                headers=AGENT_HEADERS,
                json={"tool": "sql", "tool_input": {"database": "world"}},
            )
        self.assertEqual(response.status_code, 403)
        self.assertIn("sql", response.json()["detail"])
        self.assertEqual(self.published[0][1]["event"], "tool_access_denied")

    async def test_callback_requires_matching_agent(self) -> None:
        seed_callback_run()
        async with orchestrator_client() as client:
            response = await client.post(
                f"/internal/runs/{RUN_ID}/tool-calls",
                headers={"x-agent-id": "procurement-agent"},
                json={"tool": "mcp"},
            )
        self.assertEqual(response.status_code, 404)
        self.assertEqual(self.published, [])

    async def test_callback_rejected_for_decision_mode_run(self) -> None:
        seed_callback_run(mode=None)
        async with orchestrator_client() as client:
            response = await client.post(
                f"/internal/runs/{RUN_ID}/tool-calls",
                headers=AGENT_HEADERS,
                json={"tool": "mcp"},
            )
        self.assertEqual(response.status_code, 409)

    async def test_callback_token_enforced_when_configured(self) -> None:
        seed_callback_run()
        tool_input = {"server": "world-mcp", "name": "list_top_cities"}
        with patch.object(orchestrator, "AGENT_CALLBACK_TOKEN", "secret"):
            async with orchestrator_client() as client:
                missing = await client.post(
                    f"/internal/runs/{RUN_ID}/tool-calls",
                    headers=AGENT_HEADERS,
                    json={"tool": "mcp", "tool_input": tool_input},
                )
                allowed = await client.post(
                    f"/internal/runs/{RUN_ID}/tool-calls",
                    headers={**AGENT_HEADERS, "x-callback-token": "secret"},
                    json={"tool": "mcp", "tool_input": tool_input},
                )
        self.assertEqual(missing.status_code, 401)
        self.assertEqual(allowed.status_code, 200)


class CompleteEndpointTests(ToolBrokerTestCase):
    async def test_failed_completion_records_failure(self) -> None:
        seed_callback_run()
        async with orchestrator_client() as client:
            response = await client.post(
                f"/internal/runs/{RUN_ID}/complete",
                headers=AGENT_HEADERS,
                json={"status": "failed", "output": "SQL step failed."},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(orchestrator.RUNS[RUN_ID]["status"], "failed")
        self.assertEqual(orchestrator.RUNS[RUN_ID]["output"], "SQL step failed.")
        self.assertEqual(self.published[-1][1]["event"], "agent_callback_run_failed")

    async def test_complete_rejected_when_run_already_settled(self) -> None:
        seed_callback_run(status="completed")
        async with orchestrator_client() as client:
            response = await client.post(
                f"/internal/runs/{RUN_ID}/complete",
                headers=AGENT_HEADERS,
                json={"status": "completed"},
            )
        self.assertEqual(response.status_code, 409)


class AsyncRunDenialTests(ToolBrokerTestCase):
    """How a refused tool call ends an async run, driven through the real agent
    runtime so the 403 → `complete_run` handoff is exercised, not stubbed."""

    def drive(self, run_async) -> None:
        """Run the agent runtime's background driver against the orchestrator."""
        real_client = agent_runtime.ToolBrokerClient

        def broker_factory(agent_id: str, run_id: str) -> agent_runtime.ToolBrokerClient:
            return real_client(
                agent_id,
                run_id,
                base_url="http://orchestrator-under-test",
                callback_token="",
                poll_interval_seconds=0,
                timeout_seconds=5,
                transport=httpx.ASGITransport(app=orchestrator.app),
            )

        definition = SimpleNamespace(run_async=run_async)
        request = agent_runtime.AgentRunRequest(
            request_id=RUN_ID,
            tenant_id="demo-tenant",
            user_id="demo-user",
            agent_id="world-agent",
            message="prepare a world market brief",
        )
        return patch.object(agent_runtime, "ToolBrokerClient", broker_factory), (
            definition,
            request,
        )

    async def test_fatal_denial_settles_the_run_as_denied_not_failed(self) -> None:
        """A permission boundary must not reach the user as a failed run: the
        chat answers a failure with "try rephrasing", which cannot help someone
        who simply lacks access."""
        seed_callback_run(policy_subjects=["role:procurement-analyst"])

        async def run_async(request, broker) -> str:
            await broker.run_tool(
                "mcp",
                {"server": "world-mcp", "name": "list_top_cities"},
                "world-db",
            )
            return "unreachable: the tool call is refused"

        patcher, (definition, request) = self.drive(run_async)
        with patcher:
            await agent_runtime.drive_background_run(definition, request)

        run = orchestrator.RUNS[RUN_ID]
        self.assertEqual(run["status"], "denied")
        # Audit keeps the technical string; the caller reads the explanation.
        self.assertEqual(
            run["denied_reason"],
            "User cannot use data source permission: world-db",
        )
        self.assertEqual(
            run["output"],
            router.build_source_denied_answer(PERSONA, "world-db"),
        )

        events = [payload["event"] for _, payload in self.published]
        self.assertIn("permission_access_denied", events)
        self.assertIn("agent_callback_run_denied", events)

    async def test_unrelated_failure_after_a_denial_is_still_a_failure(self) -> None:
        """The agent recovered from the refusal and died of something else, so
        reporting the run as `denied` would blame the wrong thing and send the
        user to an administrator who cannot help."""
        seed_callback_run(policy_subjects=["role:procurement-analyst"])

        async def run_async(request, broker) -> str:
            with self.assertRaises(agent_runtime.ToolBrokerError):
                await broker.run_tool(
                    "mcp",
                    {"server": "world-mcp", "name": "list_top_cities"},
                    "world-db",
                )
            raise RuntimeError("report renderer crashed")

        patcher, (definition, request) = self.drive(run_async)
        with patcher:
            await agent_runtime.drive_background_run(definition, request)

        run = orchestrator.RUNS[RUN_ID]
        self.assertEqual(run["status"], "failed")
        self.assertIsNone(run["denied_reason"])
        self.assertEqual(run["output"], "report renderer crashed")


if __name__ == "__main__":
    unittest.main()
