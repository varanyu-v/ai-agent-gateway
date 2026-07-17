import os
from dataclasses import dataclass


DEFAULT_LOKI_EXPLORE_PATH = (
    "/explore?orgId=1&left=%7B%22datasource%22:%22loki%22,"
    "%22queries%22:%5B%7B%22expr%22:%22%7Bservice%3D%5C%22gateway%5C%22%7D%22%7D%5D,"
    "%22range%22:%7B%22from%22:%22now-1h%22,%22to%22:%22now%22%7D%7D"
)
DEFAULT_TEMPO_EXPLORE_PATH = (
    "/explore?orgId=1&left=%7B%22datasource%22:%22tempo%22,"
    "%22queries%22:%5B%5D,"
    "%22range%22:%7B%22from%22:%22now-1h%22,%22to%22:%22now%22%7D%7D"
)


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if not value:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class GatewaySettings:
    issuer: str
    audience: str
    keycloak_client_id: str
    jwks_url: str
    orchestrator_url: str
    grafana_url: str
    prometheus_url: str
    loki_ui_url: str
    tempo_ui_url: str
    jwt_leeway_seconds: float
    rate_limit_enabled: bool
    run_rate_per_minute: float
    run_rate_burst: int
    max_message_chars: int
    max_thread_id_chars: int
    idempotency_ttl_seconds: float
    upstream_connect_timeout_seconds: float
    upstream_read_timeout_seconds: float
    breaker_failure_threshold: int
    breaker_reset_seconds: float

    @classmethod
    def from_env(cls) -> "GatewaySettings":
        issuer = _env_str("KEYCLOAK_ISSUER", "http://localhost:8080/realms/ptvn")
        grafana_url = _env_str("GRAFANA_URL", "http://localhost:3000")
        return cls(
            issuer=issuer,
            audience=_env_str("KEYCLOAK_AUDIENCE", "agent-gateway"),
            keycloak_client_id=_env_str("KEYCLOAK_CLIENT_ID", "agent-frontend"),
            jwks_url=_env_str(
                "KEYCLOAK_JWKS_URL",
                f"{issuer}/protocol/openid-connect/certs",
            ),
            orchestrator_url=_env_str("ORCH_URL", "http://localhost:8001"),
            grafana_url=grafana_url,
            prometheus_url=_env_str("PROMETHEUS_URL", "http://localhost:9090"),
            loki_ui_url=_env_str(
                "LOKI_UI_URL",
                f"{grafana_url.rstrip('/')}{DEFAULT_LOKI_EXPLORE_PATH}",
            ),
            tempo_ui_url=_env_str(
                "TEMPO_UI_URL",
                f"{grafana_url.rstrip('/')}{DEFAULT_TEMPO_EXPLORE_PATH}",
            ),
            jwt_leeway_seconds=_env_float("GATEWAY_JWT_LEEWAY_SECONDS", 30.0),
            rate_limit_enabled=_env_bool("GATEWAY_RATE_LIMIT_ENABLED", True),
            run_rate_per_minute=_env_float("GATEWAY_RUN_RATE_PER_MINUTE", 30.0),
            run_rate_burst=_env_int("GATEWAY_RUN_RATE_BURST", 10),
            max_message_chars=_env_int("GATEWAY_MAX_MESSAGE_CHARS", 4000),
            max_thread_id_chars=_env_int("GATEWAY_MAX_THREAD_ID_CHARS", 128),
            idempotency_ttl_seconds=_env_float("GATEWAY_IDEMPOTENCY_TTL_SECONDS", 600.0),
            upstream_connect_timeout_seconds=_env_float(
                "GATEWAY_UPSTREAM_CONNECT_TIMEOUT_SECONDS", 5.0,
            ),
            upstream_read_timeout_seconds=_env_float(
                "GATEWAY_UPSTREAM_READ_TIMEOUT_SECONDS", 60.0,
            ),
            breaker_failure_threshold=_env_int("GATEWAY_BREAKER_FAILURE_THRESHOLD", 5),
            breaker_reset_seconds=_env_float("GATEWAY_BREAKER_RESET_SECONDS", 30.0),
        )


settings = GatewaySettings.from_env()
