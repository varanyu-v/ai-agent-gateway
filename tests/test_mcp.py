import json
import unittest

import httpx

from apps.mcp import runtime
from apps.mcp.procurement.main import DEFINITION as procurement_definition
from apps.mcp.world.main import DEFINITION as world_definition
from apps.mcp.world.main import app as world_app
from apps.orchestrator.mcp_registry import (
    McpRegistry,
    McpServiceError,
    parse_mcp_services,
)
from apps.workers.mcp_worker import execute_mcp_tool


def rpc(method: str, params: dict | None = None, request_id: int = 1) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params or {},
    }


def mcp_client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://mcp-under-test",
    )


IDENTITY_HEADERS = {"x-tenant-id": "demo-tenant", "x-user-id": "demo-user"}


class McpContractTests(unittest.IsolatedAsyncioTestCase):
    """Every MCP service exposes the same discovery and JSON-RPC contract."""

    async def test_world_mcp_card_describes_identity_and_tools(self) -> None:
        async with mcp_client(world_app) as client:
            response = await client.get("/.well-known/mcp-card")

        self.assertEqual(response.status_code, 200)
        card = response.json()
        self.assertEqual(card["protocol"], runtime.MCP_PROTOCOL)
        self.assertEqual(card["id"], "world-mcp")
        self.assertEqual(
            card["capabilities"]["tools"],
            ["list_top_cities", "country_overview"],
        )
        self.assertEqual(card["requirements"]["permissions"], ["world-db"])
        self.assertEqual(card["endpoints"]["mcp"], "/mcp")

    async def test_initialize_reports_protocol_and_server_info(self) -> None:
        async with mcp_client(world_app) as client:
            response = await client.post(
                "/mcp",
                json=rpc("initialize", {"protocolVersion": "2025-06-18"}),
            )

        body = response.json()
        self.assertEqual(body["id"], 1)
        result = body["result"]
        self.assertEqual(result["protocolVersion"], runtime.MCP_PROTOCOL_VERSION)
        self.assertEqual(result["serverInfo"]["name"], "World MCP Service")
        self.assertIn("tools", result["capabilities"])

    async def test_tools_list_returns_schemas_and_permissions(self) -> None:
        procurement_app = runtime.create_mcp_app(procurement_definition)
        async with mcp_client(procurement_app) as client:
            response = await client.post("/mcp", json=rpc("tools/list"))

        tools = response.json()["result"]["tools"]
        self.assertEqual(
            [tool["name"] for tool in tools],
            ["list_recent_purchase_orders", "supplier_spend_summary"],
        )
        for tool in tools:
            self.assertEqual(tool["inputSchema"]["type"], "object")
            self.assertEqual(
                tool["_meta"]["ptvn/required_permission"],
                "procurement-db",
            )

    async def test_notification_is_acknowledged_without_response(self) -> None:
        async with mcp_client(world_app) as client:
            response = await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
            )

        self.assertEqual(response.status_code, 202)

    async def test_unknown_method_and_unknown_tool_are_rpc_errors(self) -> None:
        async with mcp_client(world_app) as client:
            unknown_method = await client.post("/mcp", json=rpc("resources/list"))
            unknown_tool = await client.post(
                "/mcp",
                json=rpc("tools/call", {"name": "no-such-tool", "arguments": {}}),
            )

        self.assertEqual(unknown_method.json()["error"]["code"], -32601)
        self.assertEqual(unknown_tool.json()["error"]["code"], -32602)


class FakeDataPlane:
    """Captures /query requests and plays back a canned data-plane response."""

    def __init__(self, rows: list[dict] | None = None, status_code: int = 200) -> None:
        self.rows = rows if rows is not None else []
        self.status_code = status_code
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path != "/query":
            return httpx.Response(404)
        if self.status_code >= 400:
            return httpx.Response(
                self.status_code,
                json={"detail": "Tables are not allowed"},
            )
        return httpx.Response(200, json={"rows": self.rows})

    @property
    def last_sql(self) -> str:
        return json.loads(self.requests[-1].content.decode())["sql"]


class McpToolExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_top_cities_delegates_to_world_plane(self) -> None:
        plane = FakeDataPlane(rows=[{"city": "Bangkok", "population": 6320174}])
        app = runtime.create_mcp_app(
            world_definition,
            outbound_transport=httpx.MockTransport(plane.handler),
        )
        async with mcp_client(app) as client:
            response = await client.post(
                "/mcp",
                json=rpc(
                    "tools/call",
                    {
                        "name": "list_top_cities",
                        "arguments": {"limit": 2, "continent": "Asia"},
                    },
                ),
                headers=IDENTITY_HEADERS,
            )

        result = response.json()["result"]
        self.assertFalse(result["isError"])
        self.assertEqual(result["structuredContent"]["row_count"], 1)
        self.assertEqual(
            result["structuredContent"]["rows"][0]["city"],
            "Bangkok",
        )

        self.assertIn("where country.continent = 'Asia'", plane.last_sql)
        self.assertIn("limit 2", plane.last_sql)
        self.assertEqual(plane.requests[-1].headers["x-tenant-id"], "demo-tenant")
        self.assertEqual(plane.requests[-1].headers["x-user-id"], "demo-user")

    async def test_supplier_spend_summary_filters_by_risk_level(self) -> None:
        plane = FakeDataPlane(rows=[])
        app = runtime.create_mcp_app(
            procurement_definition,
            outbound_transport=httpx.MockTransport(plane.handler),
        )
        async with mcp_client(app) as client:
            response = await client.post(
                "/mcp",
                json=rpc(
                    "tools/call",
                    {
                        "name": "supplier_spend_summary",
                        "arguments": {"risk_level": "high"},
                    },
                ),
                headers=IDENTITY_HEADERS,
            )

        self.assertFalse(response.json()["result"]["isError"])
        self.assertIn("where risk_level = 'high'", plane.last_sql)
        self.assertIn("from supplier_summary", plane.last_sql)

    async def test_missing_identity_headers_is_a_tool_error(self) -> None:
        plane = FakeDataPlane()
        app = runtime.create_mcp_app(
            world_definition,
            outbound_transport=httpx.MockTransport(plane.handler),
        )
        async with mcp_client(app) as client:
            response = await client.post(
                "/mcp",
                json=rpc("tools/call", {"name": "list_top_cities", "arguments": {}}),
            )

        result = response.json()["result"]
        self.assertTrue(result["isError"])
        self.assertIn("x-tenant-id", result["content"][0]["text"])
        self.assertEqual(plane.requests, [])

    async def test_invalid_arguments_are_tool_errors(self) -> None:
        plane = FakeDataPlane()
        app = runtime.create_mcp_app(
            world_definition,
            outbound_transport=httpx.MockTransport(plane.handler),
        )
        async with mcp_client(app) as client:
            bad_limit = await client.post(
                "/mcp",
                json=rpc(
                    "tools/call",
                    {"name": "list_top_cities", "arguments": {"limit": 0}},
                ),
                headers=IDENTITY_HEADERS,
            )
            bad_continent = await client.post(
                "/mcp",
                json=rpc(
                    "tools/call",
                    {
                        "name": "list_top_cities",
                        "arguments": {"continent": "Atlantis"},
                    },
                ),
                headers=IDENTITY_HEADERS,
            )

        self.assertTrue(bad_limit.json()["result"]["isError"])
        self.assertIn("limit", bad_limit.json()["result"]["content"][0]["text"])
        self.assertTrue(bad_continent.json()["result"]["isError"])
        self.assertIn("Atlantis", bad_continent.json()["result"]["content"][0]["text"])
        self.assertEqual(plane.requests, [])

    async def test_plane_refusal_surfaces_as_tool_error(self) -> None:
        plane = FakeDataPlane(status_code=403)
        app = runtime.create_mcp_app(
            world_definition,
            outbound_transport=httpx.MockTransport(plane.handler),
        )
        async with mcp_client(app) as client:
            response = await client.post(
                "/mcp",
                json=rpc(
                    "tools/call",
                    {"name": "list_top_cities", "arguments": {}},
                ),
                headers=IDENTITY_HEADERS,
            )

        result = response.json()["result"]
        self.assertTrue(result["isError"])
        self.assertIn("Tables are not allowed", result["content"][0]["text"])


class McpRegistryTests(unittest.IsolatedAsyncioTestCase):
    def test_parse_mcp_services(self) -> None:
        services = parse_mcp_services(
            "world-mcp=http://world-mcp:8010/, procurement-mcp=http://procurement-mcp:8011",
        )
        self.assertEqual(
            services,
            {
                "world-mcp": "http://world-mcp:8010",
                "procurement-mcp": "http://procurement-mcp:8011",
            },
        )

    async def test_registry_discovers_cards_and_calls_tools(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/.well-known/mcp-card":
                return httpx.Response(
                    200,
                    json={"id": "world-mcp", "name": "World MCP Service"},
                )
            if request.url.path == "/mcp":
                body = json.loads(request.content.decode())
                if body["method"] == "initialize":
                    return httpx.Response(
                        200,
                        json={
                            "jsonrpc": "2.0",
                            "id": body["id"],
                            "result": {"protocolVersion": "2025-06-18"},
                        },
                    )
                if body["method"] == "tools/list":
                    return httpx.Response(
                        200,
                        json={
                            "jsonrpc": "2.0",
                            "id": body["id"],
                            "result": {
                                "tools": [
                                    {
                                        "name": "list_top_cities",
                                        "inputSchema": {"type": "object"},
                                    },
                                ],
                            },
                        },
                    )
                if body["method"] == "tools/call":
                    return httpx.Response(
                        200,
                        json={
                            "jsonrpc": "2.0",
                            "id": body["id"],
                            "result": {
                                "content": [{"type": "text", "text": "{}"}],
                                "structuredContent": {"rows": []},
                                "isError": False,
                            },
                        },
                    )
            return httpx.Response(404)

        registry = McpRegistry("world-mcp=http://world-mcp:8010")
        await registry.start(transport=httpx.MockTransport(handler))
        try:
            server = registry.get("world-mcp")
            self.assertIsNotNone(server)
            self.assertEqual(server.name, "World MCP Service")
            self.assertEqual(server.protocol_version, "2025-06-18")
            self.assertEqual(
                [tool["name"] for tool in server.tools],
                ["list_top_cities"],
            )
            self.assertEqual(
                registry.list_tools(),
                [
                    {
                        "server_id": "world-mcp",
                        "name": "list_top_cities",
                        "inputSchema": {"type": "object"},
                    },
                ],
            )

            result = await registry.call_tool(
                server,
                "list_top_cities",
                {"limit": 1},
                headers=IDENTITY_HEADERS,
            )
            self.assertEqual(result["structuredContent"], {"rows": []})

            self.assertIsNone(registry.get("unknown-mcp"))
        finally:
            await registry.aclose()

    async def test_registry_normalizes_server_failures(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/.well-known/mcp-card":
                return httpx.Response(200, json={"name": "World MCP Service"})
            return httpx.Response(500)

        registry = McpRegistry("world-mcp=http://world-mcp:8010")
        await registry.start(transport=httpx.MockTransport(handler))
        try:
            server = registry.get("world-mcp")
            with self.assertRaises(McpServiceError) as raised:
                await registry.call_tool(server, "list_top_cities", {})
            self.assertEqual(raised.exception.status_code, 502)
        finally:
            await registry.aclose()

    async def test_registry_maps_invalid_params_to_client_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/.well-known/mcp-card":
                return httpx.Response(200, json={"name": "World MCP Service"})
            body = json.loads(request.content.decode())
            if body["method"] in {"initialize", "tools/list"}:
                result = (
                    {"tools": []}
                    if body["method"] == "tools/list"
                    else {"protocolVersion": "2025-06-18"}
                )
                return httpx.Response(
                    200,
                    json={"jsonrpc": "2.0", "id": body["id"], "result": result},
                )
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": body["id"],
                    "error": {"code": -32602, "message": "Unknown tool: nope"},
                },
            )

        registry = McpRegistry("world-mcp=http://world-mcp:8010")
        await registry.start(transport=httpx.MockTransport(handler))
        try:
            server = registry.get("world-mcp")
            with self.assertRaises(McpServiceError) as raised:
                await registry.call_tool(server, "nope", {})
            self.assertEqual(raised.exception.status_code, 400)
            self.assertIn("Unknown tool", raised.exception.detail)
        finally:
            await registry.aclose()


TOOL_EVENT = {
    "request_id": "req-1",
    "tenant_id": "demo-tenant",
    "user_id": "demo-user",
    "agent_id": "world-agent",
    "workflow": "world",
    "tool": "mcp",
    "tool_call_id": "req-1:mcp:1",
    "input": {
        "server": "world-mcp",
        "name": "list_top_cities",
        "arguments": {"limit": 2},
    },
}


class FakeMcpServer:
    """MockTransport handler speaking the discovery + JSON-RPC contract."""

    def __init__(self, call_result: dict | None = None, rpc_status: int = 200) -> None:
        self.call_result = call_result or {
            "content": [{"type": "text", "text": "{}"}],
            "structuredContent": {"rows": [], "row_count": 0},
            "isError": False,
        }
        self.rpc_status = rpc_status
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/.well-known/mcp-card":
            return httpx.Response(200, json={"name": "World MCP Service"})
        if request.url.path != "/mcp" or self.rpc_status >= 400:
            return httpx.Response(self.rpc_status if self.rpc_status >= 400 else 404)
        body = json.loads(request.content.decode())
        if body["method"] == "initialize":
            result = {"protocolVersion": "2025-06-18"}
        elif body["method"] == "tools/list":
            result = {"tools": [{"name": "list_top_cities"}]}
        else:
            result = self.call_result
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": body["id"], "result": result},
        )


class McpWorkerTests(unittest.IsolatedAsyncioTestCase):
    """The MCP worker routes tool.requested events to the named MCP server."""

    async def started_registry(self, server: FakeMcpServer) -> McpRegistry:
        registry = McpRegistry("world-mcp=http://world-mcp:8010")
        await registry.start(transport=httpx.MockTransport(server.handler))
        self.addAsyncCleanup(registry.aclose)
        return registry

    async def test_executes_tool_and_returns_structured_output(self) -> None:
        server = FakeMcpServer(
            call_result={
                "content": [{"type": "text", "text": "{}"}],
                "structuredContent": {"rows": [{"city": "Bangkok"}], "row_count": 1},
                "isError": False,
            },
        )
        registry = await self.started_registry(server)

        status, result = await execute_mcp_tool(registry, TOOL_EVENT)

        self.assertEqual(status, "completed")
        self.assertEqual(
            result,
            {
                "server": "world-mcp",
                "tool": "list_top_cities",
                "output": {"rows": [{"city": "Bangkok"}], "row_count": 1},
            },
        )
        tool_call = server.requests[-1]
        self.assertEqual(tool_call.headers["x-tenant-id"], "demo-tenant")
        self.assertEqual(tool_call.headers["x-user-id"], "demo-user")
        self.assertEqual(
            json.loads(tool_call.content.decode())["params"]["arguments"],
            {"limit": 2},
        )

    async def test_unknown_server_fails_the_tool_call(self) -> None:
        registry = await self.started_registry(FakeMcpServer())

        status, result = await execute_mcp_tool(
            registry,
            {**TOOL_EVENT, "input": {"server": "ghost-mcp", "name": "x"}},
        )

        self.assertEqual(status, "failed")
        self.assertIn("No MCP server registered", result["error"])

    async def test_is_error_result_fails_the_tool_call(self) -> None:
        server = FakeMcpServer(
            call_result={
                "content": [{"type": "text", "text": "limit must be an integer"}],
                "isError": True,
            },
        )
        registry = await self.started_registry(server)

        status, result = await execute_mcp_tool(registry, TOOL_EVENT)

        self.assertEqual(status, "failed")
        self.assertEqual(result["error"], "limit must be an integer")

    async def test_server_error_is_normalized_to_failure(self) -> None:
        server = FakeMcpServer(rpc_status=500)
        registry = McpRegistry("world-mcp=http://world-mcp:8010")
        await registry.start(transport=httpx.MockTransport(server.handler))
        self.addAsyncCleanup(registry.aclose)

        status, result = await execute_mcp_tool(registry, TOOL_EVENT)

        self.assertEqual(status, "failed")
        self.assertIn("internal error", result["error"])


if __name__ == "__main__":
    unittest.main()
