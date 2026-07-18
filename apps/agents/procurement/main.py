"""Procurement analyst agent service.

Plans procurement-database runs: read-only SQL lookups or a human approval
gate for destructive requests. Side effects are executed by the orchestrator
after policy enforcement.

"Risk" requests demonstrate the MCP path: the decision names the `mcp` tool
and the MCP worker routes it to the procurement MCP server's
`supplier_spend_summary` tool, still behind the same Casbin checks.
"""

from apps.agents.runtime import AgentDecision, AgentDefinition, AgentRunRequest, create_agent_app


PROCUREMENT_TOP_SPEND_SQL = (
    "select supplier_name, category, country, total_spend, "
    "order_count, risk_level, last_order_date "
    "from supplier_summary "
    "order by total_spend desc "
    "limit 10"
)


RISK_LEVELS = ("high", "medium", "low")


def fallback_action(message: str) -> str:
    text = message.lower()
    if "delete" in text or "remove" in text:
        return "approval"
    if "risk" in text:
        return "risk"
    return "sql"


def extract_risk_level(message: str) -> str:
    text = message.lower()
    for level in RISK_LEVELS:
        if level in text:
            return level
    return "high"


def decide(action: str, request: AgentRunRequest) -> AgentDecision:
    if action == "approval":
        return AgentDecision(
            action="approval",
            workflow="procurement",
            planner_action=action,
            audit_event="procurement_approval_required",
        )

    if action == "risk":
        return AgentDecision(
            action="tool",
            workflow="procurement",
            planner_action=action,
            tool="mcp",
            required_permission="procurement-db",
            tool_input={
                "server": "procurement-mcp",
                "name": "supplier_spend_summary",
                "arguments": {"risk_level": extract_risk_level(request.message)},
            },
        )

    return AgentDecision(
        action="tool",
        workflow="procurement",
        planner_action="sql",
        tool="sql",
        required_permission="procurement-db",
        tool_input={"database": "procurement", "sql": PROCUREMENT_TOP_SPEND_SQL},
    )


DEFINITION = AgentDefinition(
    agent_id="procurement-agent",
    name="Procurement Analyst Agent",
    description=(
        "Answers procurement-database questions with read-only SQL and "
        "gates destructive requests behind human approval."
    ),
    version="1.0.0",
    workflow="procurement",
    actions=frozenset({"sql", "risk", "approval"}),
    required_permissions=("procurement-db",),
    tools=("sql", "mcp"),
    fallback_action=fallback_action,
    decide=decide,
)

app = create_agent_app(DEFINITION)
