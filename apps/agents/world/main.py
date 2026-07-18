"""World analyst agent service.

Plans world-database runs: read-only data lookups, market reports, a human
approval gate for destructive requests, or a direct chat reply for greetings
and off-topic messages. Side effects are executed by the orchestrator after
policy enforcement.

Every executable decision names the `mcp` tool: the MCP worker routes the
call to the named MCP server (`world-mcp` for reads, `report-mcp` for report
jobs), each behind its own `mcp:{server}` Casbin object plus the datasource
permission.

The LLM planner may answer a data question with a purpose-built SELECT in
`arguments.sql`; it is executed through world-mcp's `run_sql` tool, so the
world data plane still applies the final read-only, table-allowlisted,
tenant-scoped SQL guard. Without planned SQL the agent falls back to the
canned top-cities lookup.

"Market brief" requests demonstrate the async path: the agent accepts the run
with action="async", then drives a multi-step workflow (a city lookup, then a
report built from it) through the orchestrator's tool-broker callback API.
"""

from apps.agents.runtime import (
    AgentDecision,
    AgentDefinition,
    AgentRunRequest,
    PlannedAction,
    ToolBrokerClient,
    create_agent_app,
)
from apps.persona import PERSONA


DATA_KEYWORDS = (
    "world",
    "city",
    "cities",
    "population",
    "continent",
    "language",
    "largest",
    "biggest",
    "top",
    "list",
    "show",
    "rank",
    "count",
    "compare",
    "data",
    "sql",
)

CHAT_FALLBACK_REPLY = (
    f"Hello! {PERSONA.introduction('the world analyst agent')}. Ask me about "
    "countries, cities, populations, and languages — for example: 'show the "
    "largest cities in Asia' or 'give me a country overview for THA'."
)

PLANNER_GUIDANCE = """
Data actions for this agent:
- "sql": read-only lookup over the world database. Put one PostgreSQL SELECT
  statement in arguments.sql, using only these tables:
    city(id, name, country_code, district, population)
    country(code, name, continent, region, population)
    country_language(country_code, language, is_official, percentage)
  Join city.country_code = country.code. Always end with LIMIT (50 or less).
- "country": one country profile with languages. Set arguments.country_code
  to the ISO code (e.g. THA, FR).
- "report": world market report generation. No arguments.
- "brief": multi-step world market brief. No arguments.
""".strip()


def fallback_action(message: str) -> str:
    text = message.lower()
    if "delete" in text or "remove" in text:
        return "approval"
    if "brief" in text:
        return "brief"
    if "report" in text:
        return "report"
    if "country" in text:
        return "country"
    if any(keyword in text for keyword in DATA_KEYWORDS):
        return "sql"
    return "chat"


def extract_country_code(message: str) -> str:
    """First 2-3 letter uppercase token in the message, or the demo default."""
    for token in message.split():
        stripped = token.strip(".,!?()'\"")
        if stripped.isalpha() and stripped.isupper() and 2 <= len(stripped) <= 3:
            return stripped
    return "THA"


def decide(planned: PlannedAction, request: AgentRunRequest) -> AgentDecision:
    action = planned.action

    if action == "chat":
        return AgentDecision(
            action="final",
            workflow="world",
            planner_action=action,
            audit_event="agent_chat_answered",
            output=planned.reply or CHAT_FALLBACK_REPLY,
        )

    if action == "approval":
        return AgentDecision(
            action="approval",
            workflow="world",
            planner_action=action,
            audit_event="human_approval_required",
        )

    if action == "brief":
        return AgentDecision(
            action="async",
            workflow="world",
            planner_action=action,
            audit_event="world_market_brief_started",
        )

    if action == "report":
        return AgentDecision(
            action="tool",
            workflow="world",
            planner_action=action,
            tool="mcp",
            required_permission="world-db",
            tool_input={
                "server": "report-mcp",
                "name": "generate_report",
                "arguments": {
                    "report_type": "world_market_summary",
                    "database": "world",
                },
            },
        )

    if action == "country":
        code = planned.arguments.get("country_code")
        return AgentDecision(
            action="tool",
            workflow="world",
            planner_action=action,
            tool="mcp",
            required_permission="world-db",
            tool_input={
                "server": "world-mcp",
                "name": "country_overview",
                "arguments": {
                    "country_code": code
                    if isinstance(code, str) and code.strip()
                    else extract_country_code(request.message),
                },
            },
        )

    planned_sql = planned.arguments.get("sql")
    if isinstance(planned_sql, str) and planned_sql.strip():
        return AgentDecision(
            action="tool",
            workflow="world",
            planner_action="sql",
            tool="mcp",
            required_permission="world-db",
            tool_input={
                "server": "world-mcp",
                "name": "run_sql",
                "arguments": {"sql": planned_sql.strip()},
            },
        )

    return AgentDecision(
        action="tool",
        workflow="world",
        planner_action="sql",
        tool="mcp",
        required_permission="world-db",
        tool_input={
            "server": "world-mcp",
            "name": "list_top_cities",
            "arguments": {"limit": 10},
        },
    )


async def run_market_brief(request: AgentRunRequest, broker: ToolBrokerClient) -> str:
    """Multi-step async run: a city lookup, then a report built from its rows."""
    cities_call = await broker.run_tool(
        "mcp",
        {
            "server": "world-mcp",
            "name": "list_top_cities",
            "arguments": {"limit": 10},
        },
        required_permission="world-db",
    )
    cities_output = (cities_call.get("result") or {}).get("output") or {}
    rows = cities_output.get("rows") or []

    report_call = await broker.run_tool(
        "mcp",
        {
            "server": "report-mcp",
            "name": "generate_report",
            "arguments": {
                "report_type": "world_market_brief",
                "database": "world",
                "source_rows": len(rows),
            },
        },
        required_permission="world-db",
    )
    report_output = (report_call.get("result") or {}).get("output") or {}
    report_id = report_output.get("report_id")
    return (
        f"World market brief is ready: {len(rows)} city row(s) analyzed, "
        f"report {report_id}."
    )


DEFINITION = AgentDefinition(
    agent_id="world-agent",
    name="World Analyst Agent",
    description=(
        "Answers world-database questions with read-only MCP lookups, "
        "generates world market reports and multi-step market briefs, and "
        "gates destructive requests behind human approval."
    ),
    version="1.0.0",
    workflow="world",
    actions=frozenset({"chat", "sql", "report", "brief", "country", "approval"}),
    required_permissions=("world-db",),
    tools=("mcp",),
    fallback_action=fallback_action,
    decide=decide,
    run_async=run_market_brief,
    planner_guidance=PLANNER_GUIDANCE,
)

app = create_agent_app(DEFINITION)
