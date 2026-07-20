import unittest
import uuid

import httpx

from apps.gateway import main as gateway_main
from apps.gateway.auth import current_user
from apps.gateway.proxy import BreakerState, CircuitBreaker
from apps.gateway.traffic import (
    IdempotencyCache,
    IdempotencyConflict,
    TokenBucketRateLimiter,
    payload_fingerprint,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TokenBucketRateLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def test_burst_is_allowed_then_throttled(self) -> None:
        clock = FakeClock()
        limiter = TokenBucketRateLimiter(rate_per_second=1.0, burst=2, clock=clock)

        self.assertTrue((await limiter.acquire("t:u")).allowed)
        self.assertTrue((await limiter.acquire("t:u")).allowed)

        decision = await limiter.acquire("t:u")
        self.assertFalse(decision.allowed)
        self.assertAlmostEqual(decision.retry_after_seconds, 1.0, places=3)

    async def test_tokens_refill_over_time(self) -> None:
        clock = FakeClock()
        limiter = TokenBucketRateLimiter(rate_per_second=1.0, burst=1, clock=clock)

        self.assertTrue((await limiter.acquire("t:u")).allowed)
        self.assertFalse((await limiter.acquire("t:u")).allowed)

        clock.advance(1.0)
        self.assertTrue((await limiter.acquire("t:u")).allowed)

    async def test_keys_are_isolated(self) -> None:
        clock = FakeClock()
        limiter = TokenBucketRateLimiter(rate_per_second=1.0, burst=1, clock=clock)

        self.assertTrue((await limiter.acquire("tenant:alice")).allowed)
        self.assertFalse((await limiter.acquire("tenant:alice")).allowed)
        self.assertTrue((await limiter.acquire("tenant:bob")).allowed)


class IdempotencyCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_response_is_replayed(self) -> None:
        clock = FakeClock()
        cache = IdempotencyCache(ttl_seconds=60, clock=clock)
        fingerprint = payload_fingerprint({"message": "hello"})

        self.assertIsNone(await cache.reserve("key", fingerprint))
        await cache.complete("key", {"run_id": "run-1"})

        self.assertEqual(await cache.reserve("key", fingerprint), {"run_id": "run-1"})

    async def test_in_flight_duplicate_conflicts(self) -> None:
        cache = IdempotencyCache(ttl_seconds=60, clock=FakeClock())
        fingerprint = payload_fingerprint({"message": "hello"})

        self.assertIsNone(await cache.reserve("key", fingerprint))
        with self.assertRaises(IdempotencyConflict):
            await cache.reserve("key", fingerprint)

    async def test_payload_mismatch_conflicts(self) -> None:
        cache = IdempotencyCache(ttl_seconds=60, clock=FakeClock())

        self.assertIsNone(await cache.reserve("key", payload_fingerprint({"message": "a"})))
        await cache.complete("key", {"run_id": "run-1"})
        with self.assertRaises(IdempotencyConflict):
            await cache.reserve("key", payload_fingerprint({"message": "b"}))

    async def test_release_allows_retry_after_failure(self) -> None:
        cache = IdempotencyCache(ttl_seconds=60, clock=FakeClock())
        fingerprint = payload_fingerprint({"message": "hello"})

        self.assertIsNone(await cache.reserve("key", fingerprint))
        await cache.release("key")
        self.assertIsNone(await cache.reserve("key", fingerprint))

    async def test_completed_records_expire_after_ttl(self) -> None:
        clock = FakeClock()
        cache = IdempotencyCache(ttl_seconds=60, clock=clock)
        fingerprint = payload_fingerprint({"message": "hello"})

        self.assertIsNone(await cache.reserve("key", fingerprint))
        await cache.complete("key", {"run_id": "run-1"})

        clock.advance(61)
        self.assertIsNone(await cache.reserve("key", fingerprint))


class CircuitBreakerTests(unittest.TestCase):
    def test_opens_after_consecutive_failures(self) -> None:
        clock = FakeClock()
        breaker = CircuitBreaker(failure_threshold=2, reset_seconds=30, clock=clock)

        breaker.record_failure()
        self.assertTrue(breaker.allow_request())
        breaker.record_failure()
        self.assertFalse(breaker.allow_request())
        self.assertGreater(breaker.seconds_until_retry, 0)

    def test_half_open_probe_then_close_on_success(self) -> None:
        clock = FakeClock()
        breaker = CircuitBreaker(failure_threshold=1, reset_seconds=30, clock=clock)

        breaker.record_failure()
        self.assertFalse(breaker.allow_request())

        clock.advance(30)
        self.assertEqual(breaker.state, BreakerState.HALF_OPEN)
        self.assertTrue(breaker.allow_request())

        breaker.record_success()
        self.assertEqual(breaker.state, BreakerState.CLOSED)

    def test_half_open_failure_reopens(self) -> None:
        clock = FakeClock()
        breaker = CircuitBreaker(failure_threshold=1, reset_seconds=30, clock=clock)

        breaker.record_failure()
        clock.advance(30)
        self.assertEqual(breaker.state, BreakerState.HALF_OPEN)

        breaker.record_failure()
        self.assertFalse(breaker.allow_request())


WORLD_ANALYST = {
    "user_id": "world-analyst",
    "tenant_id": "demo-tenant",
    "roles": ["role:world-analyst"],
}


def orchestrator_stub(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path.startswith("/internal/agents/") and path.endswith("/runs"):
        agent_id = path.split("/")[3]
        return httpx.Response(
            200,
            json={
                "run_id": str(uuid.uuid4()),
                "status": "running",
                "agent_id": agent_id,
                "denied_reason": None,
            },
        )
    if path == "/internal/runs/run-404":
        return httpx.Response(404, json={"detail": "Run not found"})
    if path.startswith("/internal/runs/") and path.endswith("/approve"):
        return httpx.Response(409, json={"detail": "Run is not waiting for approval"})
    if path.startswith("/internal/runs/"):
        return httpx.Response(
            200,
            json={
                "run_id": path.rsplit("/", 1)[-1],
                "status": "completed",
                "agent_id": "world-agent",
            },
        )
    if path == "/internal/health":
        return httpx.Response(200, json={"status": "ok"})
    if path == "/internal/agents":
        return httpx.Response(
            200,
            json={
                "agents": [
                    {"agent_id": "world-agent", "name": "World Agent"},
                    {"agent_id": "procurement-agent", "name": "Procurement Agent"},
                    # Running but absent from the policy file.
                    {"agent_id": "shadow-agent", "name": "Shadow Agent"},
                ],
            },
        )
    if path == "/internal/mcp":
        return httpx.Response(
            200,
            json={
                "servers": [
                    {
                        "server_id": "world-mcp",
                        "tools": [{"name": "query_world", "description": "World SQL"}],
                    },
                    {
                        "server_id": "procurement-mcp",
                        "tools": [{"name": "query_spend", "description": "Spend SQL"}],
                    },
                ],
            },
        )
    return httpx.Response(500, json={"detail": "unexpected path"})


class GatewayApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        gateway_main.app.dependency_overrides[current_user] = lambda: WORLD_ANALYST
        gateway_main.rate_limiter = TokenBucketRateLimiter(
            rate_per_second=1000.0,
            burst=1000,
        )
        gateway_main.idempotency_cache = IdempotencyCache(ttl_seconds=60)
        gateway_main.orchestrator.breaker = CircuitBreaker(
            failure_threshold=5,
            reset_seconds=30,
        )
        await gateway_main.orchestrator.aclose()
        await gateway_main.orchestrator.start(
            transport=httpx.MockTransport(orchestrator_stub),
        )
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=gateway_main.app),
            base_url="http://gateway.test",
        )

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        await gateway_main.orchestrator.aclose()
        gateway_main.app.dependency_overrides.clear()

    async def test_ui_config_does_not_expose_the_policy_catalog(self) -> None:
        response = await self.client.get("/ui/config")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        for leaked_key in ("agents", "tools", "toolPolicies"):
            self.assertNotIn(leaked_key, body)

    async def test_catalog_flags_access_without_naming_granting_roles(self) -> None:
        response = await self.client.get("/catalog")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["registryAvailable"])

        agents = {agent["id"]: agent for agent in body["agents"]}
        self.assertTrue(agents["world-agent"]["allowed"])
        self.assertFalse(agents["procurement-agent"]["allowed"])
        self.assertEqual(agents["world-agent"]["name"], "World Agent")
        # A non-admin never learns which subject would grant the denied entry.
        self.assertNotIn("roles", agents["procurement-agent"])

    async def test_catalog_reports_registry_and_policy_drift(self) -> None:
        response = await self.client.get("/catalog")
        agents = {agent["id"]: agent for agent in response.json()["agents"]}

        # Running, but no policy row: listed, ungoverned, and denied by default.
        self.assertTrue(agents["shadow-agent"]["registered"])
        self.assertFalse(agents["shadow-agent"]["governed"])
        self.assertFalse(agents["shadow-agent"]["allowed"])

        # In policy, but not registered with the orchestrator.
        self.assertTrue(agents["assistant"]["governed"])
        self.assertFalse(agents["assistant"]["registered"])

    async def test_catalog_withholds_tool_detail_for_denied_servers(self) -> None:
        response = await self.client.get("/catalog")
        servers = {server["id"]: server for server in response.json()["mcpServers"]}

        self.assertTrue(servers["world-mcp"]["allowed"])
        self.assertEqual(
            [tool["name"] for tool in servers["world-mcp"]["tools"]],
            ["query_world"],
        )
        self.assertFalse(servers["procurement-mcp"]["allowed"])
        self.assertEqual(servers["procurement-mcp"]["tools"], [])

    async def test_catalog_requires_authentication(self) -> None:
        gateway_main.app.dependency_overrides.clear()

        response = await self.client.get("/catalog")

        self.assertEqual(response.status_code, 401)

    async def test_healthz_reports_ok(self) -> None:
        response = await self.client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")

    async def test_run_agent_forwards_and_returns_request_id(self) -> None:
        response = await self.client.post(
            "/agents/world-agent/runs",
            json={"message": "show the largest cities"},
            headers={"x-request-id": "test-req-12345"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["request_id"], "test-req-12345")
        self.assertEqual(payload["status"], "running")
        self.assertEqual(response.headers["x-request-id"], "test-req-12345")

    async def test_run_agent_denies_unauthorized_agent(self) -> None:
        response = await self.client.post(
            "/agents/procurement-agent/runs",
            json={"message": "rank suppliers"},
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["detail"], "User cannot access this agent")

    async def test_run_agent_rejects_empty_message(self) -> None:
        response = await self.client.post(
            "/agents/world-agent/runs",
            json={"message": "   "},
        )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["detail"], "message must not be empty")

    async def test_run_agent_rejects_oversized_message(self) -> None:
        too_long = "x" * (gateway_main.settings.max_message_chars + 1)

        response = await self.client.post(
            "/agents/world-agent/runs",
            json={"message": too_long},
        )

        self.assertEqual(response.status_code, 422)
        self.assertIn("maximum length", response.json()["detail"])

    async def test_run_agent_is_rate_limited(self) -> None:
        gateway_main.rate_limiter = TokenBucketRateLimiter(
            rate_per_second=1e-9,
            burst=1,
        )

        first = await self.client.post(
            "/agents/world-agent/runs",
            json={"message": "show the largest cities"},
        )
        second = await self.client.post(
            "/agents/world-agent/runs",
            json={"message": "show the largest cities"},
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)
        self.assertIn("retry-after", second.headers)

    async def test_idempotency_key_replays_first_response(self) -> None:
        body = {"message": "show the largest cities"}
        headers = {"Idempotency-Key": "idem-key-1"}

        first = await self.client.post(
            "/agents/world-agent/runs", json=body, headers=headers,
        )
        second = await self.client.post(
            "/agents/world-agent/runs", json=body, headers=headers,
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["run_id"], second.json()["run_id"])
        self.assertEqual(second.headers.get("x-idempotent-replay"), "true")
        self.assertNotIn("x-idempotent-replay", first.headers)

    async def test_idempotency_key_with_different_payload_conflicts(self) -> None:
        headers = {"Idempotency-Key": "idem-key-2"}

        first = await self.client.post(
            "/agents/world-agent/runs",
            json={"message": "show the largest cities"},
            headers=headers,
        )
        second = await self.client.post(
            "/agents/world-agent/runs",
            json={"message": "a different message"},
            headers=headers,
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 409)

    async def test_unreachable_orchestrator_maps_to_502(self) -> None:
        def unreachable(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        await gateway_main.orchestrator.aclose()
        await gateway_main.orchestrator.start(
            transport=httpx.MockTransport(unreachable),
        )

        response = await self.client.post(
            "/agents/world-agent/runs",
            json={"message": "show the largest cities"},
        )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["detail"], "Orchestrator is unreachable")

    async def test_open_circuit_fails_fast_with_503(self) -> None:
        def unreachable(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        gateway_main.orchestrator.breaker = CircuitBreaker(
            failure_threshold=1,
            reset_seconds=30,
        )
        await gateway_main.orchestrator.aclose()
        await gateway_main.orchestrator.start(
            transport=httpx.MockTransport(unreachable),
        )

        first = await self.client.post(
            "/agents/world-agent/runs",
            json={"message": "show the largest cities"},
        )
        second = await self.client.post(
            "/agents/world-agent/runs",
            json={"message": "show the largest cities"},
        )

        self.assertEqual(first.status_code, 502)
        self.assertEqual(second.status_code, 503)
        self.assertIn("retry-after", second.headers)

    async def test_run_status_not_found_maps_to_404(self) -> None:
        response = await self.client.get("/runs/run-404")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"], "Run not found")

    async def test_run_status_returns_upstream_payload(self) -> None:
        response = await self.client.get("/runs/run-123")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["run_id"], "run-123")
        self.assertEqual(response.headers.get("cache-control"), "no-store")

    async def test_approve_conflict_passes_through_detail(self) -> None:
        response = await self.client.post("/runs/run-123/approve")

        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.json()["detail"],
            "Run is not waiting for approval",
        )

    async def test_invalid_inbound_request_id_is_replaced(self) -> None:
        response = await self.client.post(
            "/agents/world-agent/runs",
            json={"message": "show the largest cities"},
            headers={"x-request-id": "bad id!"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertNotEqual(response.headers["x-request-id"], "bad id!")
        self.assertEqual(
            response.json()["request_id"],
            response.headers["x-request-id"],
        )


if __name__ == "__main__":
    unittest.main()
