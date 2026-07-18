import math
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import FileResponse
from opentelemetry import metrics
from opentelemetry.trace import SpanKind, Status, StatusCode
from pydantic import BaseModel

from apps.authz import (
    agent_access_rules,
    allowed_agents,
    allowed_data_sources,
    allowed_mcp_servers,
    can_invoke_agent,
    data_source_access_rules,
    mcp_server_access_rules,
    policy_subjects,
)
from apps.gateway.auth import current_user
from apps.gateway.config import settings
from apps.gateway.proxy import CircuitBreaker, OrchestratorClient, UpstreamError
from apps.gateway.traffic import (
    IdempotencyCache,
    IdempotencyConflict,
    TokenBucketRateLimiter,
    payload_fingerprint,
)
from apps.observability import clean_attributes, setup_observability


UI_FILE = Path(__file__).resolve().parents[1] / "frontend" / "index.html"
UI_PATHS = {"/", "/ui"}
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$")
SERVICE_STARTED_AT = time.monotonic()

orchestrator = OrchestratorClient(
    base_url=settings.orchestrator_url,
    connect_timeout_seconds=settings.upstream_connect_timeout_seconds,
    read_timeout_seconds=settings.upstream_read_timeout_seconds,
    breaker=CircuitBreaker(
        failure_threshold=settings.breaker_failure_threshold,
        reset_seconds=settings.breaker_reset_seconds,
    ),
)
rate_limiter = TokenBucketRateLimiter(
    rate_per_second=settings.run_rate_per_minute / 60.0,
    burst=settings.run_rate_burst,
)
idempotency_cache = IdempotencyCache(ttl_seconds=settings.idempotency_ttl_seconds)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await orchestrator.start()
    yield
    await orchestrator.aclose()


app = FastAPI(title="AI Agent Gateway", lifespan=lifespan)
tracer = setup_observability("gateway", app)
meter = metrics.get_meter("gateway")
run_requests = meter.create_counter(
    "gateway.agent_runs",
    description="Agent run requests received by the gateway, by agent and outcome",
)
upstream_failures = meter.create_counter(
    "gateway.upstream_failures",
    description="Upstream orchestrator failures observed by the gateway",
)


class AgentRunRequest(BaseModel):
    message: str
    thread_id: str | None = None


@app.middleware("http")
async def request_context(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    incoming = request.headers.get("x-request-id", "")
    request_id = incoming if REQUEST_ID_PATTERN.fullmatch(incoming) else str(uuid.uuid4())
    request.state.request_id = request_id

    response = await call_next(request)

    response.headers["x-request-id"] = request_id
    response.headers.setdefault("x-content-type-options", "nosniff")
    if request.url.path not in UI_PATHS:
        response.headers.setdefault("cache-control", "no-store")
    return response


def can_access_agent(user: dict[str, Any], agent_id: str) -> bool:
    return can_invoke_agent(user, agent_id)


def allowed_permissions(user: dict[str, Any]) -> list[str]:
    return allowed_data_sources(user)


def trusted_context_headers(request_id: str, user: dict[str, Any]) -> dict[str, str]:
    return {
        "x-request-id": request_id,
        "x-tenant-id": user["tenant_id"],
        "x-user-id": user["user_id"],
    }


def retry_after_headers(retry_after_seconds: float | None) -> dict[str, str] | None:
    if not retry_after_seconds:
        return None
    return {"retry-after": str(max(1, math.ceil(retry_after_seconds)))}


def upstream_http_error(error: UpstreamError) -> HTTPException:
    upstream_failures.add(
        1,
        {"app.upstream": "orchestrator", "app.status_code": error.status_code},
    )
    return HTTPException(
        status_code=error.status_code,
        detail=error.detail,
        headers=retry_after_headers(error.retry_after_seconds),
    )


def upstream_detail(response: Any, fallback: str) -> str:
    try:
        detail = response.json().get("detail")
    except ValueError:
        detail = None
    return str(detail) if detail else fallback


@app.get("/", include_in_schema=False)
async def test_console() -> FileResponse:
    return FileResponse(UI_FILE)


@app.get("/ui", include_in_schema=False)
async def test_console_alias() -> FileResponse:
    return FileResponse(UI_FILE)


@app.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "gateway",
        "uptime_seconds": round(time.monotonic() - SERVICE_STARTED_AT, 3),
    }


@app.get("/readyz", include_in_schema=False)
async def readyz() -> dict[str, Any]:
    try:
        response = await orchestrator.request("GET", "/internal/health")
    except UpstreamError as error:
        raise HTTPException(
            status_code=503,
            detail=f"Orchestrator is not ready: {error.detail}",
            headers=retry_after_headers(error.retry_after_seconds),
        )
    if response.status_code >= 400:
        raise HTTPException(status_code=503, detail="Orchestrator is not ready")
    return {"status": "ready", "orchestrator": "ok"}


@app.get("/ui/config", include_in_schema=False)
async def ui_config() -> dict[str, Any]:
    return {
        "keycloakIssuer": settings.issuer,
        "keycloakTokenUrl": f"{settings.issuer}/protocol/openid-connect/token",
        "keycloakClientId": settings.keycloak_client_id,
        "audience": settings.audience,
        "policyMode": "casbin",
        "agents": agent_access_rules(),
        "tools": data_source_access_rules(),
        "toolPolicies": mcp_server_access_rules(),
        "monitorTools": [
            {"id": "grafana", "label": "Grafana", "url": settings.grafana_url},
            {"id": "prometheus", "label": "Prometheus", "url": settings.prometheus_url},
            {"id": "loki", "label": "Loki", "url": settings.loki_ui_url},
            {"id": "tempo", "label": "Tempo", "url": settings.tempo_ui_url},
        ],
    }


@app.get("/ui/permissions", include_in_schema=False)
async def ui_permissions(
    user: dict[str, Any] = Depends(current_user),
) -> dict[str, Any]:
    data_sources = allowed_data_sources(user)
    return {
        "userId": user["user_id"],
        "tenantId": user["tenant_id"],
        "roles": user["roles"],
        "policySubjects": policy_subjects(user),
        "allowedAgents": allowed_agents(user),
        "allowedDataSources": data_sources,
        "allowedPermissions": data_sources,
        "allowedTools": allowed_mcp_servers(user),
    }


@app.post("/agents/{agent_id}/runs")
async def run_agent(
    agent_id: str,
    body: AgentRunRequest,
    request: Request,
    response: Response,
    user: dict[str, Any] = Depends(current_user),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    request_id = request.state.request_id
    permissions = allowed_permissions(user)
    subjects = policy_subjects(user)
    with tracer.start_as_current_span(
        "gateway.agent_call",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.request_id": request_id,
                "app.agent_id": agent_id,
                "app.tenant_id": user["tenant_id"],
                "app.user_id": user["user_id"],
                "app.allowed_permissions": permissions,
                "app.policy_subject_count": len(subjects),
                "app.user_message.length": len(body.message),
            },
        ),
    ) as span:
        if not body.message.strip():
            span.set_status(Status(StatusCode.ERROR, "Empty message"))
            run_requests.add(1, {"app.agent_id": agent_id, "app.outcome": "invalid"})
            raise HTTPException(status_code=422, detail="message must not be empty")
        if len(body.message) > settings.max_message_chars:
            span.set_status(Status(StatusCode.ERROR, "Message too long"))
            run_requests.add(1, {"app.agent_id": agent_id, "app.outcome": "invalid"})
            raise HTTPException(
                status_code=422,
                detail=(
                    "message exceeds the maximum length of "
                    f"{settings.max_message_chars} characters"
                ),
            )
        if body.thread_id and len(body.thread_id) > settings.max_thread_id_chars:
            span.set_status(Status(StatusCode.ERROR, "thread_id too long"))
            run_requests.add(1, {"app.agent_id": agent_id, "app.outcome": "invalid"})
            raise HTTPException(
                status_code=422,
                detail=(
                    "thread_id exceeds the maximum length of "
                    f"{settings.max_thread_id_chars} characters"
                ),
            )

        if settings.rate_limit_enabled:
            decision = await rate_limiter.acquire(
                f"{user['tenant_id']}:{user['user_id']}",
            )
            if not decision.allowed:
                span.set_attribute("app.outcome", "throttled")
                span.set_status(Status(StatusCode.ERROR, "Run rate limit exceeded"))
                run_requests.add(1, {"app.agent_id": agent_id, "app.outcome": "throttled"})
                raise HTTPException(
                    status_code=429,
                    detail="Run rate limit exceeded; retry later",
                    headers=retry_after_headers(decision.retry_after_seconds),
                )

        if not can_access_agent(user, agent_id):
            span.set_attribute("app.authorization.outcome", "denied")
            span.set_status(Status(StatusCode.ERROR, "User cannot access this agent"))
            run_requests.add(1, {"app.agent_id": agent_id, "app.outcome": "denied"})
            raise HTTPException(status_code=403, detail="User cannot access this agent")

        span.set_attribute("app.authorization.outcome", "allowed")

        idempotency_scope: str | None = None
        if idempotency_key:
            idempotency_scope = (
                f"{user['tenant_id']}:{user['user_id']}:{agent_id}:{idempotency_key}"
            )
            fingerprint = payload_fingerprint(
                {
                    "agent_id": agent_id,
                    "message": body.message,
                    "thread_id": body.thread_id,
                },
            )
            try:
                cached = await idempotency_cache.reserve(idempotency_scope, fingerprint)
            except IdempotencyConflict as conflict:
                span.set_status(Status(StatusCode.ERROR, str(conflict)))
                raise HTTPException(status_code=409, detail=str(conflict))
            if cached is not None:
                span.set_attribute("app.idempotent_replay", True)
                run_requests.add(1, {"app.agent_id": agent_id, "app.outcome": "replayed"})
                response.headers["x-idempotent-replay"] = "true"
                return cached

        headers = {
            **trusted_context_headers(request_id, user),
            "x-allowed-permissions": ",".join(permissions),
            "x-policy-subjects": ",".join(subjects),
        }

        try:
            upstream = await orchestrator.request(
                "POST",
                f"/internal/agents/{agent_id}/runs",
                headers=headers,
                json_body=body.model_dump(),
            )
        except UpstreamError as error:
            if idempotency_scope:
                await idempotency_cache.release(idempotency_scope)
            span.set_status(Status(StatusCode.ERROR, error.detail))
            run_requests.add(1, {"app.agent_id": agent_id, "app.outcome": "upstream_error"})
            raise upstream_http_error(error)

        span.set_attribute("http.response.status_code", upstream.status_code)
        if upstream.status_code >= 400:
            if idempotency_scope:
                await idempotency_cache.release(idempotency_scope)
            detail = upstream_detail(upstream, "Orchestrator rejected the request")
            span.set_status(Status(StatusCode.ERROR, detail))
            run_requests.add(1, {"app.agent_id": agent_id, "app.outcome": "rejected"})
            raise HTTPException(status_code=upstream.status_code, detail=detail)

        payload = upstream.json()
        result = {"request_id": request_id, **payload}
        if idempotency_scope:
            await idempotency_cache.complete(idempotency_scope, result)

        span.set_attributes(
            clean_attributes(
                {
                    "app.run_id": payload.get("run_id"),
                    "app.run_status": payload.get("status"),
                    "app.denied_reason": payload.get("denied_reason"),
                },
            ),
        )
        run_requests.add(
            1,
            {"app.agent_id": agent_id, "app.outcome": payload.get("status", "unknown")},
        )

    return result


@app.get("/runs/{run_id}")
async def get_run_status(
    run_id: str,
    request: Request,
    user: dict[str, Any] = Depends(current_user),
) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "gateway.run_status_response",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.run_id": run_id,
                "app.tenant_id": user["tenant_id"],
                "app.user_id": user["user_id"],
            },
        ),
    ) as span:
        try:
            upstream = await orchestrator.request(
                "GET",
                f"/internal/runs/{run_id}",
                headers=trusted_context_headers(request.state.request_id, user),
            )
        except UpstreamError as error:
            span.set_status(Status(StatusCode.ERROR, error.detail))
            raise upstream_http_error(error)

        span.set_attribute("http.response.status_code", upstream.status_code)
        if upstream.status_code == 404:
            span.set_status(Status(StatusCode.ERROR, "Run not found"))
            raise HTTPException(status_code=404, detail="Run not found")
        if upstream.status_code >= 400:
            detail = upstream_detail(upstream, "Orchestrator rejected the request")
            span.set_status(Status(StatusCode.ERROR, detail))
            raise HTTPException(status_code=upstream.status_code, detail=detail)

        payload = upstream.json()
        span.set_attributes(
            clean_attributes(
                {
                    "app.agent_id": payload.get("agent_id"),
                    "app.run_status": payload.get("status"),
                    "app.tool": payload.get("tool"),
                    "app.tool_call_id": payload.get("tool_call_id"),
                },
            ),
        )
        return payload


@app.post("/runs/{run_id}/approve")
async def approve_run(
    run_id: str,
    request: Request,
    user: dict[str, Any] = Depends(current_user),
) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "gateway.approval_response",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.run_id": run_id,
                "app.tenant_id": user["tenant_id"],
                "app.user_id": user["user_id"],
            },
        ),
    ) as span:
        try:
            upstream = await orchestrator.request(
                "POST",
                f"/internal/runs/{run_id}/approve",
                headers=trusted_context_headers(request.state.request_id, user),
            )
        except UpstreamError as error:
            span.set_status(Status(StatusCode.ERROR, error.detail))
            raise upstream_http_error(error)

        span.set_attribute("http.response.status_code", upstream.status_code)
        if upstream.status_code == 404:
            span.set_status(Status(StatusCode.ERROR, "Run not found"))
            raise HTTPException(status_code=404, detail="Run not found")
        if upstream.status_code >= 400:
            detail = upstream_detail(upstream, "Orchestrator rejected the approval")
            span.set_status(Status(StatusCode.ERROR, detail))
            raise HTTPException(status_code=upstream.status_code, detail=detail)

        payload = upstream.json()
        span.set_attributes(
            clean_attributes(
                {
                    "app.agent_id": payload.get("agent_id"),
                    "app.run_status": payload.get("status"),
                },
            ),
        )
        return payload
