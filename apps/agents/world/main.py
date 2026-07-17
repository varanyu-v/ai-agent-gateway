"""World analyst agent service.

Plans world-database runs: read-only SQL lookups, market reports, or a human
approval gate for destructive requests. Side effects are executed by the
orchestrator after policy enforcement.
"""

from apps.agents.runtime import AgentDecision, AgentDefinition, AgentRunRequest, create_agent_app


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


DEFINITION = AgentDefinition(
    agent_id="world-agent",
    name="World Analyst Agent",
    description=(
        "Answers world-database questions with read-only SQL, generates "
        "world market reports, and gates destructive requests behind "
        "human approval."
    ),
    version="1.0.0",
    workflow="world",
    actions=frozenset({"sql", "report", "approval"}),
    required_permissions=("world-db",),
    tools=("sql", "report"),
    fallback_action=fallback_action,
    decide=decide,
)

app = create_agent_app(DEFINITION)
