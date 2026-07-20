"""Shared runtime for per-agent database access layers.

Each agent owns a dedicated data plane: a small FastAPI service that holds the
credentials for *only* its own database and enforces the final read-only,
table-allowlisted, tenant-scoped SQL guard. The two shipped planes
(`apps/data_access/world`, `apps/data_access/procurement`) are just an
``AgentDataPlane`` definition plus ``create_data_access_app``; a plane for a
new agent is added the same way.

The orchestrator remains the policy control plane: it enforces Casbin on the
agent's decision before any tool is dispatched. This layer is the last-mile
data guard, and crucially it is credential-isolated — the world plane cannot
reach the procurement database and vice versa.
"""

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Iterable

import asyncpg
import sqlglot
from fastapi import FastAPI, Header, HTTPException
from opentelemetry.trace import SpanKind, Status, StatusCode
from pydantic import BaseModel
from sqlglot import exp

from apps.observability import clean_attributes, setup_observability


DATA_PLANE_PROTOCOL = "ptvn.dataplane/v1"

FORBIDDEN_STATEMENTS = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Alter,
    exp.Create,
)


@dataclass(frozen=True)
class AgentDataPlane:
    """Immutable description of one agent's database access layer."""

    database: str
    service_name: str
    allowed_tables: frozenset[str]
    url_env: tuple[str, ...]
    max_rows: int = 500
    title: str | None = None


class QueryIn(BaseModel):
    database: str = ""
    sql: str


def resolve_database_url(url_env: Iterable[str]) -> str | None:
    """First non-empty value among the candidate env var names."""

    for name in url_env:
        value = os.getenv(name)
        if value:
            return value
    return None


def parse_data_planes(spec: str) -> dict[str, str]:
    """Parse ``database=base-url,...`` into a routing map for the SQL worker.

    Mirrors ``parse_agent_services`` in the orchestrator registry so the two
    registries read the same way. Malformed entries are skipped rather than
    raising, so one bad pair cannot break routing for the rest.
    """

    planes: dict[str, str] = {}
    for chunk in spec.split(","):
        entry = chunk.strip()
        if not entry or "=" not in entry:
            continue
        database, _, base_url = entry.partition("=")
        database = database.strip()
        base_url = base_url.strip().rstrip("/")
        if not database or not base_url:
            continue
        planes[database] = base_url
    return planes


def validate_query(
    tracer: Any,
    sql: str,
    allowed_tables: frozenset[str],
    database: str,
) -> None:
    """Reject anything that is not a single read-only SELECT over the
    plane's allowlisted tables."""

    with tracer.start_as_current_span(
        "data_access.validate_sql",
        kind=SpanKind.INTERNAL,
        attributes=clean_attributes(
            {
                "app.database": database,
                "app.sql.length": len(sql),
            },
        ),
    ) as span:
        try:
            trees = sqlglot.parse(sql)
        except sqlglot.errors.ParseError as exc:
            span.set_status(Status(StatusCode.ERROR, "SQL parse failed"))
            raise HTTPException(status_code=400, detail="SQL parse failed") from exc

        span.set_attribute("app.sql.statement_count", len(trees))
        if len(trees) != 1 or trees[0] is None:
            span.set_status(Status(StatusCode.ERROR, "Exactly one SQL statement is allowed"))
            raise HTTPException(status_code=400, detail="Exactly one SQL statement is allowed")

        tree = trees[0]
        if any(tree.find(kind) for kind in FORBIDDEN_STATEMENTS) or not tree.find(exp.Select):
            span.set_status(Status(StatusCode.ERROR, "Only read-only SELECT queries are allowed"))
            raise HTTPException(
                status_code=400,
                detail="Only read-only SELECT queries are allowed",
            )

        tables = {table.name for table in tree.find_all(exp.Table)}
        span.set_attribute("app.sql.tables", sorted(tables))
        disallowed_tables = tables - allowed_tables
        if disallowed_tables:
            span.set_attributes(
                clean_attributes({"app.sql.disallowed_tables": disallowed_tables}),
            )
            span.set_status(Status(StatusCode.ERROR, "Tables are not allowed"))
            raise HTTPException(
                status_code=403,
                detail=f"Tables are not allowed: {sorted(disallowed_tables)}",
            )


def create_data_access_app(definition: AgentDataPlane) -> FastAPI:
    """Build the FastAPI app for one agent's data plane.

    The plane owns a single connection pool to a single database. Requests for
    any other database are refused, so a routing mistake cannot leak across
    domains even before the SQL guard runs.
    """

    state: dict[str, asyncpg.Pool | None] = {"pool": None}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        url = resolve_database_url(definition.url_env)
        if not url:
            raise RuntimeError(
                f"{definition.service_name} requires one of these env vars: "
                f"{', '.join(definition.url_env)}",
            )
        state["pool"] = await asyncpg.create_pool(url)
        try:
            yield
        finally:
            pool = state["pool"]
            if pool is not None:
                await pool.close()
            state["pool"] = None

    app = FastAPI(
        title=definition.title or f"{definition.database} data access",
        lifespan=lifespan,
    )
    tracer = setup_observability(definition.service_name, app)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {
            "status": "ok",
            "protocol": DATA_PLANE_PROTOCOL,
            "service": definition.service_name,
            "database": definition.database,
        }

    @app.post("/query")
    async def query(
        body: QueryIn,
        x_tenant_id: str = Header(),
        x_user_id: str = Header(),
    ) -> dict[str, list[dict[str, Any]]]:
        with tracer.start_as_current_span(
            "data_access.query",
            kind=SpanKind.INTERNAL,
            attributes=clean_attributes(
                {
                    "app.database": body.database,
                    "app.tenant_id": x_tenant_id,
                    "app.user_id": x_user_id,
                    "app.sql.length": len(body.sql),
                },
            ),
        ) as span:
            # A plane serves exactly one database. Refuse foreign requests up
            # front so a misrouted call never touches this plane's pool.
            if body.database and body.database != definition.database:
                span.set_status(Status(StatusCode.ERROR, "Database not served by this plane"))
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"This data plane only serves '{definition.database}', "
                        f"not '{body.database}'"
                    ),
                )

            validate_query(tracer, body.sql, definition.allowed_tables, definition.database)

            pool = state["pool"]
            if pool is None:
                span.set_status(Status(StatusCode.ERROR, "Database is not configured"))
                raise HTTPException(
                    status_code=503,
                    detail=f"Database is not configured: {definition.database}",
                )

            wrapped_sql = f"select * from ({body.sql}) q limit {definition.max_rows}"
            with tracer.start_as_current_span(
                "data_access.execute_sql",
                kind=SpanKind.CLIENT,
                attributes=clean_attributes(
                    {
                        "app.database": definition.database,
                        "app.max_rows": definition.max_rows,
                        "db.system.name": "postgresql",
                        "db.operation.name": "SELECT",
                    },
                ),
            ) as execute_span:
                async with pool.acquire() as conn:
                    async with conn.transaction(readonly=True):
                        await conn.execute(
                            "select set_config('app.tenant_id', $1, true)",
                            x_tenant_id,
                        )
                        await conn.execute(
                            "select set_config('app.user_id', $1, true)",
                            x_user_id,
                        )
                        rows = await conn.fetch(wrapped_sql)
                execute_span.set_attribute("app.rows", len(rows))

            span.set_attribute("app.rows", len(rows))
            return {"rows": [dict(row) for row in rows]}

    return app
