from typing import Any

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from apps.gateway.config import settings
from apps.observability import clean_attributes


UNAUTHORIZED_HEADERS = {"WWW-Authenticate": "Bearer"}

security = HTTPBearer()
jwks = PyJWKClient(settings.jwks_url)
tracer = trace.get_tracer("gateway.auth")


def _unauthorized(span: trace.Span, detail: str) -> HTTPException:
    span.set_status(Status(StatusCode.ERROR, detail))
    return HTTPException(status_code=401, detail=detail, headers=UNAUTHORIZED_HEADERS)


def current_user(
    creds: HTTPAuthorizationCredentials = Depends(security),
) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "gateway.jwt_validation",
        kind=SpanKind.INTERNAL,
        attributes={
            "app.auth.issuer": settings.issuer,
            "app.auth.audience": settings.audience,
        },
    ) as span:
        token = creds.credentials

        try:
            signing_key = jwks.get_signing_key_from_jwt(token).key
        except jwt.exceptions.PyJWKClientConnectionError as exc:
            span.set_status(Status(StatusCode.ERROR, "Identity provider is unreachable"))
            raise HTTPException(
                status_code=503,
                detail="Identity provider is unreachable",
            ) from exc
        except jwt.exceptions.PyJWKClientError as exc:
            raise _unauthorized(span, "Token signing key is not recognized") from exc
        except jwt.exceptions.InvalidTokenError as exc:
            raise _unauthorized(span, "Token is malformed") from exc

        try:
            claims = jwt.decode(
                token,
                signing_key,
                algorithms=["RS256"],
                audience=settings.audience,
                issuer=settings.issuer,
                leeway=settings.jwt_leeway_seconds,
            )
        except jwt.exceptions.InvalidTokenError as exc:
            raise _unauthorized(span, "Token is invalid or expired") from exc

        tenant_id = claims.get("tenant_id")
        if not tenant_id:
            raise _unauthorized(span, "Token is missing tenant_id")

        user_id = claims.get("sub") or claims.get("preferred_username") or claims.get("email")
        if not user_id:
            raise _unauthorized(span, "Token is missing user identity")

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
