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
from pydantic import BaseModel


ISSUER = os.getenv("KEYCLOAK_ISSUER", "http://localhost:8080/realms/ptvn")
AUDIENCE = os.getenv("KEYCLOAK_AUDIENCE", "agent-gateway")
KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "agent-frontend")
JWKS_URL = os.getenv(
    "KEYCLOAK_JWKS_URL",
    f"{ISSUER}/protocol/openid-connect/certs",
)
ORCH_URL = os.getenv("ORCH_URL", "http://localhost:8001")
UI_FILE = Path(__file__).resolve().parents[1] / "frontend" / "index.html"
AGENT_ROLES = {
    "world-agent": "agent:world-agent:invoke",
    "procurement-agent": "agent:procurement-agent:invoke",
}
PERMISSION_ROLES = {
    "world-db": "permission:world-db:read",
    "procurement-db": "permission:procurement-db:read",
}

jwks = PyJWKClient(JWKS_URL)
security = HTTPBearer()
app = FastAPI(title="AI Agent Gateway")


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
        "agents": [
            {"id": agent_id, "role": role}
            for agent_id, role in sorted(AGENT_ROLES.items())
        ],
        "tools": [
            {"id": permission_name, "role": role}
            for permission_name, role in sorted(PERMISSION_ROLES.items())
        ],
    }


def current_user(
    creds: HTTPAuthorizationCredentials = Depends(security),
) -> dict[str, Any]:
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
        raise HTTPException(status_code=401, detail="Token is missing tenant_id")

    user_id = claims.get("sub") or claims.get("preferred_username") or claims.get("email")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token is missing user identity")

    return {
        "user_id": user_id,
        "tenant_id": tenant_id,
        "roles": claims.get("realm_access", {}).get("roles", []),
    }


def can_access_agent(user: dict[str, Any], agent_id: str) -> bool:
    roles = set(user["roles"])
    return f"agent:{agent_id}:invoke" in roles


def allowed_permissions(user: dict[str, Any]) -> list[str]:
    roles = set(user["roles"])
    return sorted(
        permission_name
        for permission_name, role_name in PERMISSION_ROLES.items()
        if role_name in roles
    )


@app.post("/agents/{agent_id}/runs")
async def run_agent(
    agent_id: str,
    body: AgentRunRequest,
    user: dict[str, Any] = Depends(current_user),
) -> dict[str, Any]:
    if not can_access_agent(user, agent_id):
        raise HTTPException(status_code=403, detail="User cannot access this agent")

    request_id = str(uuid.uuid4())
    headers = {
        "x-request-id": request_id,
        "x-tenant-id": user["tenant_id"],
        "x-user-id": user["user_id"],
        "x-allowed-permissions": ",".join(allowed_permissions(user)),
    }

    async with httpx.AsyncClient(base_url=ORCH_URL, timeout=60) as client:
        response = await client.post(
            f"/internal/agents/{agent_id}/runs",
            json=body.model_dump(),
            headers=headers,
        )
        response.raise_for_status()

    return {"request_id": request_id, **response.json()}


@app.get("/runs/{run_id}")
async def get_run_status(
    run_id: str,
    user: dict[str, Any] = Depends(current_user),
) -> dict[str, Any]:
    headers = {
        "x-tenant-id": user["tenant_id"],
        "x-user-id": user["user_id"],
    }

    async with httpx.AsyncClient(base_url=ORCH_URL, timeout=60) as client:
        response = await client.get(f"/internal/runs/{run_id}", headers=headers)

    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Run not found")

    response.raise_for_status()
    return response.json()


@app.post("/runs/{run_id}/approve")
async def approve_run(
    run_id: str,
    user: dict[str, Any] = Depends(current_user),
) -> dict[str, Any]:
    headers = {
        "x-tenant-id": user["tenant_id"],
        "x-user-id": user["user_id"],
    }

    async with httpx.AsyncClient(base_url=ORCH_URL, timeout=60) as client:
        response = await client.post(f"/internal/runs/{run_id}/approve", headers=headers)

    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Run not found")
    if response.status_code == 409:
        raise HTTPException(status_code=409, detail=response.json().get("detail"))

    response.raise_for_status()
    return response.json()
