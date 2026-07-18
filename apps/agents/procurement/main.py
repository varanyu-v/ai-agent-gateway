"""Procurement analyst agent service.

Plans procurement-database runs: read-only data lookups or a human approval
gate for destructive requests. Side effects are executed by the orchestrator
after policy enforcement.

Every executable decision names the `mcp` tool: the MCP worker routes the
call to the procurement MCP server (`supplier_spend_summary` for both spend
rankings and risk reviews), behind the `mcp:procurement-mcp` Casbin object
plus the `procurement-db` datasource permission.
"""

from apps.agents.runtime import AgentDecision, AgentDefinition, AgentRunRequest, create_agent_app


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
        tool="mcp",
        required_permission="procurement-db",
        tool_input={
            "server": "procurement-mcp",
            "name": "supplier_spend_summary",
            "arguments": {"limit": 10},
        },
    )


DEFINITION = AgentDefinition(
    agent_id="procurement-agent",
    name="Procurement Analyst Agent",
    description=(
        "Answers procurement-database questions with read-only MCP lookups "
        "and gates destructive requests behind human approval."
    ),
    version="1.0.0",
    workflow="procurement",
    actions=frozenset({"sql", "risk", "approval"}),
    required_permissions=("procurement-db",),
    tools=("mcp",),
    fallback_action=fallback_action,
    decide=decide,
)

app = create_agent_app(DEFINITION)
