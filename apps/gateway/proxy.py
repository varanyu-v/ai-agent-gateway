import time
from enum import Enum
from typing import Any, Callable

import httpx


Clock = Callable[[], float]


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class UpstreamError(Exception):
    """Normalized upstream failure carrying the client-facing status and detail."""

    def __init__(
        self,
        status_code: int,
        detail: str,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.retry_after_seconds = retry_after_seconds


class CircuitBreaker:
    """Consecutive-failure breaker: fail fast while the upstream is down.

    OPEN rejects requests until reset_seconds elapse, then HALF_OPEN lets a
    probe through; a success closes the circuit, a failure reopens it.
    """

    def __init__(
        self,
        failure_threshold: int,
        reset_seconds: float,
        clock: Clock = time.monotonic,
    ) -> None:
        self._failure_threshold = max(1, failure_threshold)
        self._reset_seconds = reset_seconds
        self._clock = clock
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0

    @property
    def state(self) -> BreakerState:
        if (
            self._state is BreakerState.OPEN
            and self._clock() - self._opened_at >= self._reset_seconds
        ):
            self._state = BreakerState.HALF_OPEN
        return self._state

    def allow_request(self) -> bool:
        return self.state is not BreakerState.OPEN

    @property
    def seconds_until_retry(self) -> float:
        if self.state is not BreakerState.OPEN:
            return 0.0
        return max(0.0, self._reset_seconds - (self._clock() - self._opened_at))

    def record_success(self) -> None:
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0

    def record_failure(self) -> None:
        was_half_open = self.state is BreakerState.HALF_OPEN
        self._consecutive_failures += 1
        if was_half_open or self._consecutive_failures >= self._failure_threshold:
            self._state = BreakerState.OPEN
            self._opened_at = self._clock()


class OrchestratorClient:
    """Pooled HTTP client for the orchestrator with breaker-guarded requests.

    Transport failures and upstream 5xx responses are normalized into
    UpstreamError so route handlers never surface raw stack traces:
    circuit open -> 503, timeout -> 504, unreachable -> 502, upstream 5xx -> 502.
    """

    def __init__(
        self,
        base_url: str,
        connect_timeout_seconds: float,
        read_timeout_seconds: float,
        breaker: CircuitBreaker,
    ) -> None:
        self._base_url = base_url
        self._connect_timeout = connect_timeout_seconds
        self._read_timeout = read_timeout_seconds
        self.breaker = breaker
        self._client: httpx.AsyncClient | None = None

    async def start(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=httpx.Timeout(
                connect=self._connect_timeout,
                read=self._read_timeout,
                write=self._read_timeout,
                pool=self._connect_timeout,
            ),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            transport=transport,
        )

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> httpx.Response:
        if self._client is None:
            raise UpstreamError(503, "Gateway upstream client is not started")

        if not self.breaker.allow_request():
            raise UpstreamError(
                503,
                "Orchestrator is temporarily unavailable; retry later",
                retry_after_seconds=self.breaker.seconds_until_retry,
            )

        try:
            response = await self._client.request(
                method,
                path,
                headers=headers,
                json=json_body,
            )
        except httpx.TimeoutException as exc:
            self.breaker.record_failure()
            raise UpstreamError(504, "Orchestrator timed out") from exc
        except httpx.TransportError as exc:
            self.breaker.record_failure()
            raise UpstreamError(502, "Orchestrator is unreachable") from exc

        if response.status_code >= 500:
            self.breaker.record_failure()
            raise UpstreamError(502, "Orchestrator returned an internal error")

        self.breaker.record_success()
        return response
