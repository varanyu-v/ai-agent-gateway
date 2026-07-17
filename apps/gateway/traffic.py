import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Callable


Clock = Callable[[], float]


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: float


class TokenBucketRateLimiter:
    """Per-key token bucket held in process memory.

    Suitable for a single gateway replica; multi-replica deployments should
    back this interface with a shared store such as Redis.
    """

    def __init__(
        self,
        rate_per_second: float,
        burst: int,
        clock: Clock = time.monotonic,
        max_keys: int = 10_000,
    ) -> None:
        self._rate = max(rate_per_second, 1e-9)
        self._burst = float(max(1, burst))
        self._clock = clock
        self._max_keys = max_keys
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, key: str) -> RateLimitDecision:
        async with self._lock:
            now = self._clock()
            tokens, updated_at = self._buckets.get(key, (self._burst, now))
            tokens = min(self._burst, tokens + (now - updated_at) * self._rate)

            if tokens >= 1.0:
                self._buckets[key] = (tokens - 1.0, now)
                return RateLimitDecision(allowed=True, retry_after_seconds=0.0)

            self._buckets[key] = (tokens, now)
            self._prune(now)
            return RateLimitDecision(
                allowed=False,
                retry_after_seconds=(1.0 - tokens) / self._rate,
            )

    def _prune(self, now: float) -> None:
        if len(self._buckets) <= self._max_keys:
            return
        refill_window = self._burst / self._rate
        self._buckets = {
            key: (tokens, updated_at)
            for key, (tokens, updated_at) in self._buckets.items()
            if now - updated_at < refill_window
        }


class IdempotencyConflict(Exception):
    pass


@dataclass
class _IdempotencyRecord:
    fingerprint: str
    response: dict[str, Any] | None
    stored_at: float

    @property
    def pending(self) -> bool:
        return self.response is None


def payload_fingerprint(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class IdempotencyCache:
    """Replay cache for run creation keyed by client Idempotency-Key.

    In-memory with TTL eviction; multi-replica deployments should back this
    interface with a shared store such as Redis.
    """

    def __init__(self, ttl_seconds: float, clock: Clock = time.monotonic) -> None:
        self._ttl = ttl_seconds
        self._clock = clock
        self._records: dict[str, _IdempotencyRecord] = {}
        self._lock = asyncio.Lock()

    async def reserve(self, key: str, fingerprint: str) -> dict[str, Any] | None:
        """Reserve the key for a new request.

        Returns a cached response to replay, or None when the caller owns the
        reservation and should dispatch the request. Raises IdempotencyConflict
        when the key is reused with a different payload or is still in flight.
        """
        async with self._lock:
            self._evict(self._clock())
            record = self._records.get(key)
            if record is None:
                self._records[key] = _IdempotencyRecord(
                    fingerprint=fingerprint,
                    response=None,
                    stored_at=self._clock(),
                )
                return None
            if record.fingerprint != fingerprint:
                raise IdempotencyConflict(
                    "Idempotency-Key was already used with a different request payload",
                )
            if record.pending:
                raise IdempotencyConflict(
                    "A request with this Idempotency-Key is still in flight",
                )
            return record.response

    async def complete(self, key: str, response: dict[str, Any]) -> None:
        async with self._lock:
            record = self._records.get(key)
            if record is not None:
                record.response = response
                record.stored_at = self._clock()

    async def release(self, key: str) -> None:
        """Drop a pending reservation after a failed dispatch so the client can retry."""
        async with self._lock:
            record = self._records.get(key)
            if record is not None and record.pending:
                del self._records[key]

    def _evict(self, now: float) -> None:
        expired = [
            key
            for key, record in self._records.items()
            if not record.pending and now - record.stored_at >= self._ttl
        ]
        for key in expired:
            del self._records[key]
