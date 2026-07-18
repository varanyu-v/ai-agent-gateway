"""Registry of external MCP tool services the platform can discover and call.

MCP servers are declared in the MCP_SERVICES environment variable as
`server-id=base-url` pairs separated by commas — the same shape as
AGENT_SERVICES. On startup (and lazily on first use) the registry fetches
each server's `/.well-known/mcp-card` and asks its MCP endpoint for the tool
list (`initialize` + `tools/list`), so adding a developer-built MCP server to
the platform is configuration only: run its service and list it here.

Any server that implements the standard contract can join, regardless of
stack: the discovery card, a JSON-RPC 2.0 `POST /mcp` endpoint supporting
`initialize` / `tools/list` / `tools/call`, and `GET /health`. The two shipped
examples are built from `create_mcp_app()` in `apps/mcp/runtime.py`.
"""

from contextlib import suppress
from dataclasses import dataclass, field
from itertools import count
from typing import Any

import httpx

from apps.orchestrator.registry import parse_agent_services


DEFAULT_MCP_SERVICES = (
    "world-mcp=http://localhost:8010,procurement-mcp=http://localhost:8011"
)

# MCP specification revision this client speaks.
MCP_PROTOCOL_VERSION = "2025-06-18"

# Same `id=base-url` spec shape as AGENT_SERVICES and DATA_PLANES.
parse_mcp_services = parse_agent_services


class McpServiceError(Exception):
    """Normalized MCP-service failure carrying the client-facing status."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass
class RegisteredMcpServer:
    server_id: str
    base_url: str
    name: str
    card: dict[str, Any] = field(default_factory=dict)
    tools: list[dict[str, Any]] = field(default_factory=list)
    protocol_version: str | None = None


class McpRegistry:
    def __init__(
        self,
        spec: str,
        connect_timeout_seconds: float = 5.0,
        read_timeout_seconds: float = 30.0,
    ) -> None:
        self._servers = {
            server_id: RegisteredMcpServer(
                server_id=server_id,
                base_url=base_url,
                name=server_id,
            )
            for server_id, base_url in parse_mcp_services(spec).items()
        }
        self._connect_timeout = connect_timeout_seconds
        self._read_timeout = read_timeout_seconds
        self._client: httpx.AsyncClient | None = None
        self._request_ids = count(1)

    @property
    def server_ids(self) -> list[str]:
        return sorted(self._servers)

    def get(self, server_id: str) -> RegisteredMcpServer | None:
        return self._servers.get(server_id)

    async def start(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=self._connect_timeout,
                read=self._read_timeout,
                write=self._read_timeout,
                pool=self._connect_timeout,
            ),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            transport=transport,
        )
        for server in self._servers.values():
            with suppress(httpx.HTTPError, ValueError, McpServiceError):
                await self.discover(server)

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def discover(self, server: RegisteredMcpServer) -> None:
        """Refresh the server's card and tool list; raises on failure."""
        if self._client is None:
            raise RuntimeError("MCP registry has not been started")

        response = await self._client.get(
            f"{server.base_url}/.well-known/mcp-card",
        )
        response.raise_for_status()
        card = response.json()
        if not isinstance(card, dict):
            raise ValueError("MCP card must be a JSON object")
        server.card = card
        server.name = str(card.get("name") or server.server_id)

        initialized = await self._rpc(
            server,
            "initialize",
            {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "ai-agent-gateway", "version": "1.0.0"},
            },
        )
        server.protocol_version = str(
            initialized.get("protocolVersion") or MCP_PROTOCOL_VERSION,
        )

        listed = await self._rpc(server, "tools/list", {})
        tools = listed.get("tools")
        if not isinstance(tools, list):
            raise ValueError("MCP server returned no tool list")
        server.tools = [tool for tool in tools if isinstance(tool, dict)]

    def list_tools(self) -> list[dict[str, Any]]:
        """Flattened discovered tools across all registered servers."""
        return [
            {"server_id": server_id, **tool}
            for server_id in self.server_ids
            for tool in self._servers[server_id].tools
        ]

    async def call_tool(
        self,
        server: RegisteredMcpServer,
        tool_name: str,
        arguments: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Invoke one tool; returns the MCP result (content/structuredContent/
        isError). Tool-level failures come back as `isError: true` results —
        the caller decides how to treat them — while transport and protocol
        failures raise McpServiceError, mirroring AgentRegistry.invoke_run."""
        if self._client is None:
            raise McpServiceError(503, "MCP registry has not been started")

        if not server.card:
            with suppress(httpx.HTTPError, ValueError, McpServiceError):
                await self.discover(server)

        result = await self._rpc(
            server,
            "tools/call",
            {"name": tool_name, "arguments": arguments or {}},
            headers=headers,
        )
        if not isinstance(result.get("content"), list):
            raise McpServiceError(
                502,
                f"MCP service '{server.server_id}' returned no content",
            )
        return result

    async def _rpc(
        self,
        server: RegisteredMcpServer,
        method: str,
        params: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """One JSON-RPC exchange with upstream failures normalized the same
        way AgentRegistry maps agent-service errors."""
        if self._client is None:
            raise McpServiceError(503, "MCP registry has not been started")

        try:
            response = await self._client.post(
                f"{server.base_url}/mcp",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": next(self._request_ids),
                    "method": method,
                    "params": params,
                },
            )
        except httpx.TimeoutException as exc:
            raise McpServiceError(
                504,
                f"MCP service '{server.server_id}' timed out",
            ) from exc
        except httpx.TransportError as exc:
            raise McpServiceError(
                502,
                f"MCP service '{server.server_id}' is unreachable",
            ) from exc

        if response.status_code >= 500:
            raise McpServiceError(
                502,
                f"MCP service '{server.server_id}' returned an internal error",
            )
        if response.status_code >= 400:
            detail = f"MCP service '{server.server_id}' rejected the request"
            with suppress(ValueError):
                detail = str(response.json().get("detail") or detail)
            raise McpServiceError(response.status_code, detail)

        try:
            body = response.json()
        except ValueError as exc:
            raise McpServiceError(
                502,
                f"MCP service '{server.server_id}' returned an invalid response",
            ) from exc

        error = body.get("error") if isinstance(body, dict) else None
        if isinstance(error, dict):
            message = str(
                error.get("message")
                or f"MCP service '{server.server_id}' rejected the request",
            )
            # Invalid params (e.g. unknown tool) is a caller mistake; every
            # other JSON-RPC error is an upstream fault.
            status = 400 if error.get("code") == -32602 else 502
            raise McpServiceError(status, message)

        result = body.get("result") if isinstance(body, dict) else None
        if not isinstance(result, dict):
            raise McpServiceError(
                502,
                f"MCP service '{server.server_id}' returned no result",
            )
        return result
