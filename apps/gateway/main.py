import os
import uuid
from pathlib import Path
from typing import Any

import httpx
import jwt
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient
from opentelemetry.trace import SpanKind, Status, StatusCode
from pydantic import BaseModel

from apps.authz import (
    agent_access_rules,
    allowed_agents,
    allowed_data_sources,
    allowed_tools,
    can_invoke_agent,
    data_source_access_rules,
    policy_subjects,
    tool_access_rules,
)
from apps.observability import clean_attributes, setup_observability


ISSUER = os.getenv("KEYCLOAK_ISSUER", "http://localhost:8080/realms/ptvn")
AUDIENCE = os.getenv("KEYCLOAK_AUDIENCE", "agent-gateway")
KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "agent-frontend")
JWKS_URL = os.getenv(
    "KEYCLOAK_JWKS_URL",
    f"{ISSUER}/protocol/openid-connect/certs",
)
ORCH_URL = os.getenv("ORCH_URL", "http://localhost:8001")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
DEFAULT_LOKI_EXPLORE_PATH = (
    "/explore?orgId=1&left=%7B%22datasource%22:%22loki%22,"
    "%22queries%22:%5B%7B%22expr%22:%22%7Bservice%3D%5C%22gateway%5C%22%7D%22%7D%5D,"
    "%22range%22:%7B%22from%22:%22now-1h%22,%22to%22:%22now%22%7D%7D"
)
LOKI_UI_URL = os.getenv(
    "LOKI_UI_URL",
    f"{GRAFANA_URL.rstrip('/')}{DEFAULT_LOKI_EXPLORE_PATH}",
)
DEFAULT_TEMPO_EXPLORE_PATH = (
    "/explore?orgId=1&left=%7B%22datasource%22:%22tempo%22,"
    "%22queries%22:%5B%5D,"
    "%22range%22:%7B%22from%22:%22now-1h%22,%22to%22:%22now%22%7D%7D"
)
TEMPO_UI_URL = os.getenv(
    "TEMPO_UI_URL",
    f"{GRAFANA_URL.rstrip('/')}{DEFAULT_TEMPO_EXPLORE_PATH}",
)
UI_FILE = Path(__file__).resolve().parents[1] / "frontend" / "index.html"

jwks = PyJWKClient(JWKS_URL)
security = HTTPBearer()
app = FastAPI(title="AI Agent Gateway")
tracer = setup_observability("gateway", app)


class AgentRunRequest(BaseModel):
    message: str
    thread_id: str | None = None


@app.get("/", include_in_schema=False)
async def test_console() -> FileResponse:
    return FileResponse(UI_FILE)


@app.get("/ui", include_in_schema=False)
async def test_console_alias() -> FileResponse:
    return FileResponse(UI_FILE)


@app.get("/ui/config", include_in_schema=False)
async def ui_config() -> dict[str, Any]:
    return {
        "keycloakIssuer": ISSUER,
        "keycloakTokenUrl": f"{ISSUER}/protocol/openid-connect/token",
        "keycloakClientId": KEYCLOAK_CLIENT_ID,
        "audience": AUDIENCE,
        "policyMode": "casbin",
        "agents": agent_access_rules(),
        "tools": data_source_access_rules(),
        "toolPolicies": tool_access_rules(),
        "monitorTools": [
            {"id": "grafana", "label": "Grafana", "url": GRAFANA_URL},
            {"id": "prometheus", "label": "Prometheus", "url": PROMETHEUS_URL},
            {"id": "loki", "label": "Loki", "url": LOKI_UI_URL},
            {"id": "tempo", "label": "Tempo", "url": TEMPO_UI_URL},
        ],
    }


def current_user(
    creds: HTTPAuthorizationCredentials = Depends(security),
) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "gateway.jwt_validation",
        kind=SpanKind.INTERNAL,
        attributes={"app.auth.issuer": ISSUER, "app.auth.audience": AUDIENCE},
    ) as span:
        token = creds.credentials
        signing_key = jwks.get_signing_key_from_jwt(token).key

        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=AUDIENCE,
            issuer=ISSUER,
        )

        tenant_id = claims.get("tenant_id")
        if not tenant_id:
            span.set_status(Status(StatusCode.ERROR, "Token is missing tenant_id"))
            raise HTTPException(status_code=401, detail="Token is missing tenant_id")

        user_id = claims.get("sub") or claims.get("preferred_username") or claims.get("email")
        if not user_id:
            span.set_status(Status(StatusCode.ERROR, "Token is missing user identity"))
            raise HTTPException(status_code=401, detail="Token is missing user identity")

        roles = claims.get("realm_access", {}).get("roles", [])
        span.set_attributes(
            clean_attributes(
                {
                    "app.tenant_id": tenant_id,
                    "app.user_id": user_id,
                    "app.role_count": len(roles),
                },
            ),
        )

        return {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "roles": roles,
        }


def can_access_agent(user: dict[str, Any], agent_id: str) -> bool:
    return can_invoke_agent(user, agent_id)


def allowed_permissions(user: dict[str, Any]) -> list[str]:
    return allowed_data_sources(user)


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
        "allowedTools": allowed_tools(user),
    }


@app.post("/agents/{agent_id}/runs")
async def run_agent(
    agent_id: str,
    body: AgentRunRequest,
    user: dict[str, Any] = Depends(current_user),
) -> dict[str, Any]:
    request_id = str(uuid.uuid4())
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
        if not can_access_agent(user, agent_id):
            span.set_attribute("app.authorization.outcome", "denied")
            span.set_status(Status(StatusCode.ERROR, "User cannot access this agent"))
            raise HTTPException(status_code=403, detail="User cannot access this agent")

        span.set_attribute("app.authorization.outcome", "allowed")
        headers = {
            "x-request-id": request_id,
            "x-tenant-id": user["tenant_id"],
            "x-user-id": user["user_id"],
            "x-allowed-permissions": ",".join(permissions),
            "x-policy-subjects": ",".join(subjects),
        }

        async with httpx.AsyncClient(base_url=ORCH_URL, timeout=60) as client:
            response = await client.post(
                f"/internal/agents/{agent_id}/runs",
                json=body.model_dump(),
                headers=headers,
            )
            span.set_attribute("http.response.status_code", response.status_code)
            response.raise_for_status()

        payload = response.json()
        span.set_attributes(
            clean_attributes(
                {
                    "app.run_id": payload.get("run_id"),
                    "app.run_status": payload.get("status"),
                    "app.denied_reason": payload.get("denied_reason"),
                },
            ),
        )

    return {"request_id": request_id, **payload}


@app.get("/runs/{run_id}")
async def get_run_status(
    run_id: str,
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
        headers = {
            "x-tenant-id": user["tenant_id"],
            "x-user-id": user["user_id"],
        }

        async with httpx.AsyncClient(base_url=ORCH_URL, timeout=60) as client:
            response = await client.get(f"/internal/runs/{run_id}", headers=headers)

        span.set_attribute("http.response.status_code", response.status_code)
        if response.status_code == 404:
            span.set_status(Status(StatusCode.ERROR, "Run not found"))
            raise HTTPException(status_code=404, detail="Run not found")

        response.raise_for_status()
        payload = response.json()
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
        headers = {
            "x-tenant-id": user["tenant_id"],
            "x-user-id": user["user_id"],
        }

        async with httpx.AsyncClient(base_url=ORCH_URL, timeout=60) as client:
            response = await client.post(f"/internal/runs/{run_id}/approve", headers=headers)

        span.set_attribute("http.response.status_code", response.status_code)
        if response.status_code == 404:
            span.set_status(Status(StatusCode.ERROR, "Run not found"))
            raise HTTPException(status_code=404, detail="Run not found")
        if response.status_code == 409:
            detail = response.json().get("detail")
            span.set_status(Status(StatusCode.ERROR, str(detail)))
            raise HTTPException(status_code=409, detail=detail)

        response.raise_for_status()
        payload = response.json()
        span.set_attributes(
            clean_attributes(
                {
                    "app.agent_id": payload.get("agent_id"),
                    "app.run_status": payload.get("status"),
                },
            ),
        )
        return payload
