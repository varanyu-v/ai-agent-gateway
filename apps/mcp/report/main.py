"""Report MCP service.

Example MCP server exposing report generation as a job-submission tool. It
reads no databases: `generate_report` validates its input, mints a report id
scoped to the calling request, and returns a queued job reference the caller
can poll for the finished document. Access is gated by the `mcp:report-mcp`
Casbin object plus whatever datasource permission the requesting agent names,
so report generation stays a separately grantable capability.
"""

import re
import uuid
from typing import Any

from apps.mcp.runtime import (
    McpServerDefinition,
    McpTool,
    McpToolContext,
    McpToolError,
    create_mcp_app,
)


REPORT_TYPE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


async def generate_report(
    arguments: dict[str, Any],
    context: McpToolContext,
) -> dict[str, Any]:
    report_type = str(arguments.get("report_type") or "").strip()
    if not REPORT_TYPE_PATTERN.fullmatch(report_type):
        raise McpToolError(
            "report_type must be a lowercase slug (letters, digits, '-' or '_')",
        )

    source_rows = arguments.get("source_rows")
    if source_rows is not None and (
        isinstance(source_rows, bool)
        or not isinstance(source_rows, int)
        or source_rows < 0
    ):
        raise McpToolError("source_rows must be a non-negative integer")

    reference = context.request_id or uuid.uuid4().hex
    report_id = f"{reference}-{report_type}"
    return {
        "report_id": report_id,
        "report_type": report_type,
        "status": "queued",
        "download_url": f"https://reports.example.com/{report_id}.pdf",
    }


DEFINITION = McpServerDefinition(
    server_id="report-mcp",
    name="Report MCP Service",
    description=(
        "Report generation as a queued job: generate_report validates the "
        "request, mints a report id, and returns the reference the caller "
        "polls for the finished document. It reads no databases directly."
    ),
    version="1.0.0",
    tools=(
        McpTool(
            name="generate_report",
            description=(
                "Queue one report for generation and return its report_id "
                "and download location."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "report_type": {
                        "type": "string",
                        "pattern": "^[a-z0-9][a-z0-9_-]{0,63}$",
                        "description": (
                            "Which report to build, e.g. world_market_summary."
                        ),
                    },
                    "database": {
                        "type": "string",
                        "description": "Source database the report is scoped to.",
                    },
                    "source_rows": {
                        "type": "integer",
                        "minimum": 0,
                        "description": (
                            "How many source rows fed this report request."
                        ),
                    },
                },
                "required": ["report_type"],
                "additionalProperties": False,
            },
            handler=generate_report,
        ),
    ),
)

app = create_mcp_app(DEFINITION)
