"""Procurement analyst agent service.

Plans procurement-database runs: read-only SQL lookups or a human approval
gate for destructive requests. Side effects are executed by the orchestrator
after policy enforcement.
"""

from apps.agents.runtime import AgentDecision, AgentDefinition, AgentRunRequest, create_agent_app


PROCUREMENT_TOP_SPEND_SQL = (
    "select supplier_name, category, country, total_spend, "
    "order_count, risk_level, last_order_date "
    "from supplier_summary "
    "order by total_spend desc "
    "limit 10"
)


def fallback_action(message: str) -> str:
    text = message.lower()
    if "delete" in text or "remove" in text:
        return "approval"
    return "sql"


def decide(action: str, request: AgentRunRequest) -> AgentDecision:
    if action == "approval":
        return AgentDecision(
            action="approval",
            workflow="procurement",
            planner_action=action,
            audit_event="procurement_approval_required",
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
    actions=frozenset({"sql", "approval"}),
    required_permissions=("procurement-db",),
    tools=("sql",),
    fallback_action=fallback_action,
    decide=decide,
)

app = create_agent_app(DEFINITION)
