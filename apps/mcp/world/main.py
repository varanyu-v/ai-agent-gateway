"""World MCP service.

Example MCP server exposing read-only tools over the world database. It holds
no database credentials: every tool builds a guarded SELECT and delegates to
the world data plane (`world-db-access`), which enforces the final read-only,
table-allowlisted, tenant-scoped SQL guard. Tool inputs are validated against
closed vocabularies before any SQL is built.
"""

import os
from typing import Any

from apps.mcp.runtime import (
    McpServerDefinition,
    McpTool,
    McpToolContext,
    McpToolError,
    create_mcp_app,
    parse_limit_argument,
    parse_sql_argument,
    query_data_plane,
)


WORLD_DATA_PLANE_URL = os.getenv(
    "WORLD_DATA_PLANE_URL",
    "http://localhost:8006",
).rstrip("/")

CONTINENTS = (
    "Africa",
    "Antarctica",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
)


async def list_top_cities(
    arguments: dict[str, Any],
    context: McpToolContext,
) -> dict[str, Any]:
    limit = parse_limit_argument(arguments)
    continent = arguments.get("continent")
    where = ""
    if continent is not None:
        if continent not in CONTINENTS:
            raise McpToolError(
                f"Unknown continent: {continent}. Expected one of: {list(CONTINENTS)}",
            )
        where = f"where country.continent = '{continent}' "

    sql = (
        "select city.name as city, country.name as country, "
        "country.continent, city.district, city.population "
        "from city "
        "join country on country.code = city.country_code "
        f"{where}"
        f"order by city.population desc limit {limit}"
    )
    rows = await query_data_plane(context, WORLD_DATA_PLANE_URL, "world", sql)
    return {"rows": rows, "row_count": len(rows)}


async def country_overview(
    arguments: dict[str, Any],
    context: McpToolContext,
) -> dict[str, Any]:
    code = str(arguments.get("country_code") or "").strip().upper()
    if not (2 <= len(code) <= 3 and code.isalpha()):
        raise McpToolError("country_code must be a 2- or 3-letter ISO country code")

    sql = (
        "select country.code, country.name, country.continent, country.region, "
        "country.population, country_language.language, "
        "country_language.is_official, country_language.percentage "
        "from country "
        "left join country_language on country_language.country_code = country.code "
        f"where country.code = '{code}' "
        "order by country_language.percentage desc"
    )
    rows = await query_data_plane(context, WORLD_DATA_PLANE_URL, "world", sql)
    if not rows:
        raise McpToolError(f"Unknown country code: {code}")

    first = rows[0]
    return {
        "country": {
            "code": first.get("code"),
            "name": first.get("name"),
            "continent": first.get("continent"),
            "region": first.get("region"),
            "population": first.get("population"),
        },
        "languages": [
            {
                "language": row.get("language"),
                "is_official": row.get("is_official"),
                "percentage": row.get("percentage"),
            }
            for row in rows
            if row.get("language")
        ],
    }


async def run_sql(
    arguments: dict[str, Any],
    context: McpToolContext,
) -> dict[str, Any]:
    """Run one planner-written SELECT under the world data plane's guard."""
    sql = parse_sql_argument(arguments)
    rows = await query_data_plane(context, WORLD_DATA_PLANE_URL, "world", sql)
    return {"rows": rows, "row_count": len(rows), "sql": sql}


DEFINITION = McpServerDefinition(
    server_id="world-mcp",
    name="World MCP Service",
    description=(
        "Read-only tools over the world database: largest cities by "
        "population and per-country overviews with languages. All reads go "
        "through the credential-isolated world data plane."
    ),
    version="1.0.0",
    tools=(
        McpTool(
            name="list_top_cities",
            description=(
                "List the world's largest cities by population, optionally "
                "filtered to one continent."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 10,
                        "description": "How many cities to return.",
                    },
                    "continent": {
                        "type": "string",
                        "enum": list(CONTINENTS),
                        "description": "Restrict results to one continent.",
                    },
                },
                "additionalProperties": False,
            },
            handler=list_top_cities,
            required_permission="world-db",
        ),
        McpTool(
            name="country_overview",
            description=(
                "Look up one country by ISO code: identity, population, and "
                "the languages spoken there."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "country_code": {
                        "type": "string",
                        "minLength": 2,
                        "maxLength": 3,
                        "description": "ISO country code, e.g. THA or FR.",
                    },
                },
                "required": ["country_code"],
                "additionalProperties": False,
            },
            handler=country_overview,
            required_permission="world-db",
        ),
        McpTool(
            name="run_sql",
            description=(
                "Run one read-only SELECT over the world database (tables: "
                "city, country, country_language). The data plane enforces "
                "the final SELECT-only, table-allowlisted, row-capped guard."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "A single read-only SELECT statement.",
                    },
                },
                "required": ["sql"],
                "additionalProperties": False,
            },
            handler=run_sql,
            required_permission="world-db",
        ),
    ),
)

app = create_mcp_app(DEFINITION)
