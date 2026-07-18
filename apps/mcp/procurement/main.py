"""Procurement MCP service.

Example MCP server exposing read-only tools over the procurement database. It
holds no database credentials: every tool builds a guarded SELECT and
delegates to the procurement data plane (`procurement-db-access`), which
enforces the final read-only, table-allowlisted, tenant-scoped SQL guard.
Tool inputs are validated against closed vocabularies before any SQL is built.
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


PROCUREMENT_DATA_PLANE_URL = os.getenv(
    "PROCUREMENT_DATA_PLANE_URL",
    "http://localhost:8007",
).rstrip("/")

PO_STATUSES = ("approved", "review", "blocked")
RISK_LEVELS = ("low", "medium", "high")


async def list_recent_purchase_orders(
    arguments: dict[str, Any],
    context: McpToolContext,
) -> dict[str, Any]:
    limit = parse_limit_argument(arguments)
    status = arguments.get("status")
    where = ""
    if status is not None:
        if status not in PO_STATUSES:
            raise McpToolError(
                f"Unknown status: {status}. Expected one of: {list(PO_STATUSES)}",
            )
        where = f"where po.status = '{status}' "

    sql = (
        "select po.po_number, s.supplier_name, po.business_unit, "
        "po.order_date, po.status, po.total_amount "
        "from purchase_orders po "
        "join suppliers s on s.supplier_id = po.supplier_id "
        f"{where}"
        f"order by po.order_date desc limit {limit}"
    )
    rows = await query_data_plane(
        context,
        PROCUREMENT_DATA_PLANE_URL,
        "procurement",
        sql,
    )
    return {"rows": rows, "row_count": len(rows)}


async def supplier_spend_summary(
    arguments: dict[str, Any],
    context: McpToolContext,
) -> dict[str, Any]:
    limit = parse_limit_argument(arguments)
    risk_level = arguments.get("risk_level")
    where = ""
    if risk_level is not None:
        if risk_level not in RISK_LEVELS:
            raise McpToolError(
                f"Unknown risk_level: {risk_level}. "
                f"Expected one of: {list(RISK_LEVELS)}",
            )
        where = f"where risk_level = '{risk_level}' "

    sql = (
        "select supplier_name, category, country, total_spend, order_count, "
        "risk_level, last_order_date "
        "from supplier_summary "
        f"{where}"
        f"order by total_spend desc limit {limit}"
    )
    rows = await query_data_plane(
        context,
        PROCUREMENT_DATA_PLANE_URL,
        "procurement",
        sql,
    )
    return {"rows": rows, "row_count": len(rows)}


async def run_sql(
    arguments: dict[str, Any],
    context: McpToolContext,
) -> dict[str, Any]:
    """Run one planner-written SELECT under the procurement plane's guard."""
    sql = parse_sql_argument(arguments)
    rows = await query_data_plane(
        context,
        PROCUREMENT_DATA_PLANE_URL,
        "procurement",
        sql,
    )
    return {"rows": rows, "row_count": len(rows), "sql": sql}


DEFINITION = McpServerDefinition(
    server_id="procurement-mcp",
    name="Procurement MCP Service",
    description=(
        "Read-only tools over the procurement database: recent purchase "
        "orders and supplier spend summaries. All reads go through the "
        "credential-isolated procurement data plane."
    ),
    version="1.0.0",
    tools=(
        McpTool(
            name="list_recent_purchase_orders",
            description=(
                "List the most recent purchase orders with their supplier, "
                "business unit, status, and amount; optionally filtered by "
                "status."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 10,
                        "description": "How many purchase orders to return.",
                    },
                    "status": {
                        "type": "string",
                        "enum": list(PO_STATUSES),
                        "description": "Restrict results to one order status.",
                    },
                },
                "additionalProperties": False,
            },
            handler=list_recent_purchase_orders,
            required_permission="procurement-db",
        ),
        McpTool(
            name="supplier_spend_summary",
            description=(
                "Summarize total spend, order count, and risk level per "
                "supplier, ranked by spend; optionally filtered by risk level."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 10,
                        "description": "How many suppliers to return.",
                    },
                    "risk_level": {
                        "type": "string",
                        "enum": list(RISK_LEVELS),
                        "description": "Restrict results to one risk level.",
                    },
                },
                "additionalProperties": False,
            },
            handler=supplier_spend_summary,
            required_permission="procurement-db",
        ),
        McpTool(
            name="run_sql",
            description=(
                "Run one read-only SELECT over the procurement database "
                "(tables: suppliers, purchase_orders, supplier_summary). The "
                "data plane enforces the final SELECT-only, table-allowlisted, "
                "row-capped guard."
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
            required_permission="procurement-db",
        ),
    ),
)

app = create_mcp_app(DEFINITION)
