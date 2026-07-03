import os
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
import sqlglot
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from sqlglot import exp


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


def validate_query(sql: str, allowed_tables: set[str]) -> None:
    try:
        trees = sqlglot.parse(sql)
    except sqlglot.errors.ParseError as exc:
        raise HTTPException(status_code=400, detail="SQL parse failed") from exc

    if len(trees) != 1 or trees[0] is None:
        raise HTTPException(status_code=400, detail="Exactly one SQL statement is allowed")

    tree = trees[0]
    forbidden = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Alter, exp.Create)
    if any(tree.find(kind) for kind in forbidden) or not tree.find(exp.Select):
        raise HTTPException(
            status_code=400,
            detail="Only read-only SELECT queries are allowed",
        )

    tables = {table.name for table in tree.find_all(exp.Table)}
    disallowed_tables = tables - allowed_tables
    if disallowed_tables:
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


@app.post("/query")
async def query(
    body: QueryIn,
    x_tenant_id: str = Header(),
    x_user_id: str = Header(),
) -> dict[str, list[dict[str, Any]]]:
    config = DATABASE_CONFIGS.get(body.database)
    if config is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown database. Available databases: {sorted(DATABASE_CONFIGS)}",
        )

    pool = pools.get(body.database)
    if pool is None:
        raise HTTPException(
            status_code=503,
            detail=f"Database is not configured: {body.database}",
        )

    validate_query(body.sql, config["allowed_tables"])

    wrapped_sql = f"select * from ({body.sql}) q limit {config['max_rows']}"
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute("select set_config('app.tenant_id', $1, true)", x_tenant_id)
            await conn.execute("select set_config('app.user_id', $1, true)", x_user_id)
            rows = await conn.fetch(wrapped_sql)

    return {"rows": [dict(row) for row in rows]}
