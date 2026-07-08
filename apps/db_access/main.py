import os
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
import sqlglot
from fastapi import FastAPI, Header, HTTPException
from opentelemetry.trace import SpanKind, Status, StatusCode
from pydantic import BaseModel
from sqlglot import exp

from apps.observability import clean_attributes, setup_observability


WORLD_DB_TABLES = {"city", "country", "country_language", "country_flag"}
PROCUREMENT_DB_TABLES = {"suppliers", "purchase_orders", "supplier_summary"}

DATABASE_CONFIGS: dict[str, dict[str, Any]] = {
    "world": {
        "url": os.getenv("WORLD_DATABASE_URL") or os.getenv("DATABASE_URL"),
        "allowed_tables": WORLD_DB_TABLES,
        "max_rows": 500,
    },
    "procurement": {
        "url": os.getenv("PROCUREMENT_DATABASE_URL"),
        "allowed_tables": PROCUREMENT_DB_TABLES,
        "max_rows": 500,
    },
}

pools: dict[str, asyncpg.Pool] = {}


class QueryIn(BaseModel):
    database: str = "world"
    sql: str


def validate_query(sql: str, allowed_tables: set[str], database: str) -> None:
    with tracer.start_as_current_span(
        "db_access.validate_sql",
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
        forbidden = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Alter, exp.Create)
        if any(tree.find(kind) for kind in forbidden) or not tree.find(exp.Select):
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pools

    configured = {
        name: config
        for name, config in DATABASE_CONFIGS.items()
        if config.get("url")
    }
    if not configured:
        raise RuntimeError(
            "At least one database URL must be set: "
            "DATABASE_URL, WORLD_DATABASE_URL, or PROCUREMENT_DATABASE_URL"
        )

    pools = {
        name: await asyncpg.create_pool(config["url"])
        for name, config in configured.items()
    }
    try:
        yield
    finally:
        for database_pool in pools.values():
            await database_pool.close()
        pools = {}


app = FastAPI(title="Database Access Layer", lifespan=lifespan)
tracer = setup_observability("db-access", app)


@app.post("/query")
async def query(
    body: QueryIn,
    x_tenant_id: str = Header(),
    x_user_id: str = Header(),
) -> dict[str, list[dict[str, Any]]]:
    with tracer.start_as_current_span(
        "db_access.query",
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
        config = DATABASE_CONFIGS.get(body.database)
        if config is None:
            span.set_status(Status(StatusCode.ERROR, "Unknown database"))
            raise HTTPException(
                status_code=404,
                detail=f"Unknown database. Available databases: {sorted(DATABASE_CONFIGS)}",
            )

        pool = pools.get(body.database)
        if pool is None:
            span.set_status(Status(StatusCode.ERROR, "Database is not configured"))
            raise HTTPException(
                status_code=503,
                detail=f"Database is not configured: {body.database}",
            )

        validate_query(body.sql, config["allowed_tables"], body.database)

        wrapped_sql = f"select * from ({body.sql}) q limit {config['max_rows']}"
        with tracer.start_as_current_span(
            "db_access.execute_sql",
            kind=SpanKind.CLIENT,
            attributes=clean_attributes(
                {
                    "app.database": body.database,
                    "app.max_rows": config["max_rows"],
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
