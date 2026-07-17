"""Registry of external agent services the orchestrator can route runs to.

Agents are declared in the AGENT_SERVICES environment variable as
`agent-id=base-url` pairs separated by commas. On startup (and lazily on
first use) the registry fetches each agent's `/.well-known/agent-card` to
learn its workflow name and capabilities, so adding a new agent to the
platform is configuration only: run its service and list it here.
"""

from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

import httpx


DEFAULT_AGENT_SERVICES = (
    "world-agent=http://localhost:8004,procurement-agent=http://localhost:8005"
)


class AgentServiceError(Exception):
    """Normalized agent-service failure carrying the client-facing status."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass
class RegisteredAgent:
    agent_id: str
    base_url: str
    workflow: str
    name: str
    card: dict[str, Any] = field(default_factory=dict)


def parse_agent_services(spec: str) -> dict[str, str]:
    services: dict[str, str] = {}
    for entry in spec.split(","):
        agent_id, _, base_url = entry.strip().partition("=")
        agent_id = agent_id.strip()
        base_url = base_url.strip().rstrip("/")
        if agent_id and base_url:
            services[agent_id] = base_url
    return services


class AgentRegistry:
    def __init__(
        self,
        spec: str,
        connect_timeout_seconds: float = 5.0,
        read_timeout_seconds: float = 60.0,
    ) -> None:
        self._agents = {
            agent_id: RegisteredAgent(
                agent_id=agent_id,
                base_url=base_url,
                workflow=agent_id,
                name=agent_id,
            )
            for agent_id, base_url in parse_agent_services(spec).items()
        }
        self._connect_timeout = connect_timeout_seconds
        self._read_timeout = read_timeout_seconds
        self._client: httpx.AsyncClient | None = None

    @property
    def agent_ids(self) -> list[str]:
        return sorted(self._agents)

    def get(self, agent_id: str) -> RegisteredAgent | None:
        return self._agents.get(agent_id)

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
        for agent in self._agents.values():
            with suppress(httpx.HTTPError, ValueError):
                await self.discover(agent)

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def discover(self, agent: RegisteredAgent) -> None:
        """Refresh the agent's card; raises httpx.HTTPError/ValueError on failure."""
        if self._client is None:
            raise RuntimeError("Agent registry has not been started")

        response = await self._client.get(
            f"{agent.base_url}/.well-known/agent-card",
        )
        response.raise_for_status()
        card = response.json()
        if not isinstance(card, dict):
            raise ValueError("Agent card must be a JSON object")
        agent.card = card
        agent.workflow = str(card.get("workflow") or agent.agent_id)
        agent.name = str(card.get("name") or agent.agent_id)

    async def invoke_run(
        self,
        agent: RegisteredAgent,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if self._client is None:
            raise AgentServiceError(503, "Agent registry has not been started")

        if not agent.card:
            with suppress(httpx.HTTPError, ValueError):
                await self.discover(agent)

        try:
            response = await self._client.post(
                f"{agent.base_url}/runs",
                headers=headers,
                json=payload,
            )
        except httpx.TimeoutException as exc:
            raise AgentServiceError(
                504,
                f"Agent service '{agent.agent_id}' timed out",
            ) from exc
        except httpx.TransportError as exc:
            raise AgentServiceError(
                502,
                f"Agent service '{agent.agent_id}' is unreachable",
            ) from exc

        if response.status_code >= 500:
            raise AgentServiceError(
                502,
                f"Agent service '{agent.agent_id}' returned an internal error",
            )
        if response.status_code >= 400:
            detail = f"Agent service '{agent.agent_id}' rejected the request"
            with suppress(ValueError):
                detail = str(response.json().get("detail") or detail)
            raise AgentServiceError(response.status_code, detail)

        try:
            body = response.json()
        except ValueError as exc:
            raise AgentServiceError(
                502,
                f"Agent service '{agent.agent_id}' returned an invalid response",
            ) from exc

        if not isinstance(body, dict) or not isinstance(body.get("decision"), dict):
            raise AgentServiceError(
                502,
                f"Agent service '{agent.agent_id}' returned no decision",
            )
        return body
