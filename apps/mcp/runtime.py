"""Shared runtime for standalone MCP tool services.

Every MCP server is its own HTTP service built from an McpServerDefinition
through create_mcp_app(). The contract is intentionally uniform — the MCP
analogue of the agent-service contract in `apps/agents/runtime.py` — so the
orchestrator can integrate any number of developer-built MCP servers without
code changes:

- `GET /.well-known/mcp-card` — machine-readable card describing identity,
  protocol version, tools, and required permissions; the orchestrator's
  McpRegistry discovers servers here.
- `POST /mcp` — the MCP endpoint: JSON-RPC 2.0 requests in, single JSON
  responses out (Streamable HTTP, JSON mode). Supports `initialize`, `ping`,
  `tools/list`, and `tools/call`; notifications are acknowledged with `202`.
- `GET /health` — liveness.

MCP servers are credential-free by convention, like agents: the shipped
examples delegate every read to the owning data plane, which holds the
database credentials and enforces the final SQL guard. Tenant and user
identity arrive as trusted `x-tenant-id` / `x-user-id` headers and are
forwarded to the data plane, so RLS still applies to tool reads.
"""

import json
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import httpx
from fastapi import FastAPI, Header, Request, Response
from fastapi.responses import JSONResponse
from opentelemetry.trace import SpanKind, Status, StatusCode

from apps.observability import clean_attributes, setup_observability


MCP_PROTOCOL = "ptvn.mcp/v1"
# MCP specification revision this runtime implements.
MCP_PROTOCOL_VERSION = "2025-06-18"

JSONRPC_PARSE_ERROR = -32700
JSONRPC_INVALID_REQUEST = -32600
JSONRPC_METHOD_NOT_FOUND = -32601
JSONRPC_INVALID_PARAMS = -32602


class McpToolError(RuntimeError):
    """A tool run failed; reported to the caller as an isError tool result."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


@dataclass
class McpToolContext:
    """Per-call context handed to every tool handler."""

    tenant_id: str | None
    user_id: str | None
    http: httpx.AsyncClient


@dataclass(frozen=True)
class McpTool:
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: Callable[[dict[str, Any], McpToolContext], Awaitable[dict[str, Any]]]
    # Advertised on the card so the platform can align Casbin `datasource:*`
    # policy with what the tool ultimately reads.
    required_permission: str | None = None


@dataclass(frozen=True)
class McpServerDefinition:
    server_id: str
    name: str
    description: str
    version: str
    tools: tuple[McpTool, ...]

    def tool(self, name: str) -> McpTool | None:
        for tool in self.tools:
            if tool.name == name:
                return tool
        return None


def mcp_card(definition: McpServerDefinition) -> dict[str, Any]:
    return {
        "protocol": MCP_PROTOCOL,
        "protocol_version": MCP_PROTOCOL_VERSION,
        "id": definition.server_id,
        "name": definition.name,
        "description": definition.description,
        "version": definition.version,
        "capabilities": {"tools": [tool.name for tool in definition.tools]},
        "requirements": {
            "permissions": sorted(
                {
                    tool.required_permission
                    for tool in definition.tools
                    if tool.required_permission
                },
            ),
        },
        "endpoints": {"mcp": "/mcp", "health": "/health"},
    }


def tool_descriptor(tool: McpTool) -> dict[str, Any]:
    """One entry of the MCP `tools/list` result."""
    descriptor: dict[str, Any] = {
        "name": tool.name,
        "description": tool.description,
        "inputSchema": tool.input_schema,
    }
    if tool.required_permission:
        descriptor["_meta"] = {"ptvn/required_permission": tool.required_permission}
    return descriptor


def parse_limit_argument(
    arguments: dict[str, Any],
    default: int = 10,
    maximum: int = 50,
) -> int:
    """Validate the common `limit` tool argument into a bounded int."""
    value = arguments.get("limit", default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise McpToolError("limit must be an integer")
    if not 1 <= value <= maximum:
        raise McpToolError(f"limit must be between 1 and {maximum}")
    return value


async def query_data_plane(
    context: McpToolContext,
    base_url: str,
    database: str,
    sql: str,
) -> list[dict[str, Any]]:
    """Run one read-only query through the data plane that owns the database.

    The MCP server holds no credentials; the plane applies the final SQL guard
    (single SELECT, table allowlist, row cap, tenant-scoped RLS session).
    Failures are raised as McpToolError so they surface as tool errors.
    """
    if not context.tenant_id or not context.user_id:
        raise McpToolError(
            "x-tenant-id and x-user-id headers are required for data reads",
        )

    try:
        response = await context.http.post(
            f"{base_url}/query",
            json={"database": database, "sql": sql},
            headers={
                "x-tenant-id": context.tenant_id,
                "x-user-id": context.user_id,
            },
        )
    except httpx.TimeoutException as exc:
        raise McpToolError(f"Data plane timed out: {database}") from exc
    except httpx.TransportError as exc:
        raise McpToolError(f"Data plane is unreachable: {database}") from exc

    if response.status_code >= 400:
        detail = f"Data plane refused the query ({response.status_code})"
        with suppress(ValueError):
            detail = str(response.json().get("detail") or detail)
        raise McpToolError(detail)

    try:
        rows = response.json().get("rows")
    except ValueError as exc:
        raise McpToolError(f"Data plane returned an invalid response: {database}") from exc
    return rows if isinstance(rows, list) else []


def _rpc_result(request_id: Any, result: dict[str, Any]) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": request_id, "result": result})


def _rpc_error(request_id: Any, code: int, message: str) -> JSONResponse:
    return JSONResponse(
        {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}},
    )


async def execute_tool(
    definition: McpServerDefinition,
    tracer: Any,
    params: dict[str, Any],
    context: McpToolContext,
) -> dict[str, Any] | None:
    """Execute one tool; returns the MCP result, or None for an unknown name."""
    tool = definition.tool(str(params.get("name") or ""))
    if tool is None:
        return None
    arguments = params.get("arguments") or {}

    with tracer.start_as_current_span(
        f"mcp.tool.{tool.name}",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.mcp.server": definition.server_id,
                "app.mcp.tool": tool.name,
                "app.tenant_id": context.tenant_id,
                "app.user_id": context.user_id,
            },
        ),
    ) as span:
        try:
            if not isinstance(arguments, dict):
                raise McpToolError("arguments must be an object")
            output = await tool.handler(arguments, context)
        except Exception as exc:  # noqa: BLE001 - tool failures are results
            detail = exc.detail if isinstance(exc, McpToolError) else str(exc)
            span.set_status(Status(StatusCode.ERROR, detail))
            return {
                "content": [{"type": "text", "text": detail}],
                "isError": True,
            }
        return {
            "content": [
                {"type": "text", "text": json.dumps(output, default=str)},
            ],
            "structuredContent": output,
            "isError": False,
        }


async def dispatch_rpc(
    definition: McpServerDefinition,
    tracer: Any,
    payload: Any,
    context: McpToolContext,
) -> Response:
    """Handle one parsed JSON-RPC message and build the HTTP response."""
    # This spec revision has no batching: exactly one request per POST.
    if not isinstance(payload, dict) or payload.get("jsonrpc") != "2.0":
        return _rpc_error(None, JSONRPC_INVALID_REQUEST, "Expected a JSON-RPC 2.0 request object")
    method = payload.get("method")
    if not isinstance(method, str):
        return _rpc_error(payload.get("id"), JSONRPC_INVALID_REQUEST, "method must be a string")
    if "id" not in payload:
        # Notification (e.g. notifications/initialized): acknowledge only.
        return Response(status_code=202)

    request_id = payload["id"]
    params = payload.get("params")
    params = params if isinstance(params, dict) else {}

    if method == "initialize":
        return _rpc_result(
            request_id,
            {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {
                    "name": definition.name,
                    "version": definition.version,
                },
                "instructions": definition.description,
            },
        )
    if method == "ping":
        return _rpc_result(request_id, {})
    if method == "tools/list":
        return _rpc_result(
            request_id,
            {"tools": [tool_descriptor(tool) for tool in definition.tools]},
        )
    if method == "tools/call":
        result = await execute_tool(definition, tracer, params, context)
        if result is None:
            return _rpc_error(
                request_id,
                JSONRPC_INVALID_PARAMS,
                f"Unknown tool: {params.get('name')}",
            )
        return _rpc_result(request_id, result)

    return _rpc_error(request_id, JSONRPC_METHOD_NOT_FOUND, f"Unknown method: {method}")


def create_mcp_app(
    definition: McpServerDefinition,
    outbound_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    """Build the FastAPI app for one MCP server.

    `outbound_transport` overrides the transport of the shared client that tool
    handlers use for upstream calls (data planes), so tests can fake upstreams.
    """
    state: dict[str, httpx.AsyncClient | None] = {"http": None}

    def outbound_http() -> httpx.AsyncClient:
        # Created lazily so the app also works under test transports that do
        # not run the lifespan; the lifespan only closes it on shutdown.
        client = state["http"]
        if client is None:
            client = httpx.AsyncClient(timeout=30, transport=outbound_transport)
            state["http"] = client
        return client

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            yield
        finally:
            client = state["http"]
            if client is not None:
                await client.aclose()
            state["http"] = None

    app = FastAPI(title=definition.name, lifespan=lifespan)
    tracer = setup_observability(definition.server_id, app)

    @app.get("/.well-known/mcp-card")
    async def read_mcp_card() -> dict[str, Any]:
        return mcp_card(definition)

    @app.get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": definition.server_id}

    @app.post("/mcp")
    async def mcp_endpoint(
        request: Request,
        x_tenant_id: str | None = Header(default=None),
        x_user_id: str | None = Header(default=None),
    ) -> Response:
        try:
            payload = await request.json()
        except ValueError:
            return _rpc_error(None, JSONRPC_PARSE_ERROR, "Request body is not valid JSON")

        context = McpToolContext(
            tenant_id=x_tenant_id,
            user_id=x_user_id,
            http=outbound_http(),
        )
        return await dispatch_rpc(definition, tracer, payload, context)

    return app
