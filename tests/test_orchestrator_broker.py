"""Tool-broker callback API: long-running agents request tools mid-run.

Every callback is enforced against the policy subjects stored when the run was
created, so the async path grants exactly the same access as the decision path.
"""

import unittest
from unittest.mock import patch

import httpx

from apps.orchestrator import main as orchestrator


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


if __name__ == "__main__":
    unittest.main()
