"""World analyst agent service.

Plans world-database runs: read-only SQL lookups, market reports, or a human
approval gate for destructive requests. Side effects are executed by the
orchestrator after policy enforcement.

"Market brief" requests demonstrate the async path: the agent accepts the run
with action="async", then drives a multi-step workflow (SQL lookup, then a
report built from it) through the orchestrator's tool-broker callback API.
"""

from apps.agents.runtime import (
    AgentDecision,
    AgentDefinition,
    AgentRunRequest,
    ToolBrokerClient,
    create_agent_app,
)


WORLD_TOP_CITIES_SQL = (
    "select city.name as city, country.name as country, "
    "country.continent, city.district, city.population "
    "from city "
    "join country on country.code = city.country_code "
    "order by city.population desc "
    "limit 10"
)


def fallback_action(message: str) -> str:
    text = message.lower()
    if "delete" in text or "remove" in text:
        return "approval"
    if "brief" in text:
        return "brief"
    if "report" in text:
        return "report"
    return "sql"


def decide(action: str, request: AgentRunRequest) -> AgentDecision:
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
            tool="report",
            required_permission="world-db",
            tool_input={
                "report_type": "world_market_summary",
                "database": "world",
            },
        )

    return AgentDecision(
        action="tool",
        workflow="world",
        planner_action="sql",
        tool="sql",
        required_permission="world-db",
        tool_input={"database": "world", "sql": WORLD_TOP_CITIES_SQL},
    )


async def run_market_brief(request: AgentRunRequest, broker: ToolBrokerClient) -> str:
    """Multi-step async run: SQL lookup, then a report built from its rows."""
    sql_call = await broker.run_tool(
        "sql",
        {"database": "world", "sql": WORLD_TOP_CITIES_SQL},
        required_permission="world-db",
    )
    rows = (sql_call.get("result") or {}).get("rows") or []

    report_call = await broker.run_tool(
        "report",
        {
            "report_type": "world_market_brief",
            "database": "world",
            "source_rows": len(rows),
        },
        required_permission="world-db",
    )
    report_id = (report_call.get("result") or {}).get("report_id")
    return (
        f"World market brief is ready: {len(rows)} city row(s) analyzed, "
        f"report {report_id}."
    )


DEFINITION = AgentDefinition(
    agent_id="world-agent",
    name="World Analyst Agent",
    description=(
        "Answers world-database questions with read-only SQL, generates "
        "world market reports and multi-step market briefs, and gates "
        "destructive requests behind human approval."
    ),
    version="1.0.0",
    workflow="world",
    actions=frozenset({"sql", "report", "brief", "approval"}),
    required_permissions=("world-db",),
    tools=("sql", "report"),
    fallback_action=fallback_action,
    decide=decide,
    run_async=run_market_brief,
)

app = create_agent_app(DEFINITION)
