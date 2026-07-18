"""Procurement analyst agent service.

Plans procurement-database runs: read-only data lookups, a human approval
gate for destructive requests, or a direct chat reply for greetings and
off-topic messages. Side effects are executed by the orchestrator after
policy enforcement.

Every executable decision names the `mcp` tool: the MCP worker routes the
call to the procurement MCP server, behind the `mcp:procurement-mcp` Casbin
object plus the `procurement-db` datasource permission. The LLM planner may
answer a data question with a purpose-built SELECT in `arguments.sql`,
executed through procurement-mcp's `run_sql` tool under the procurement data
plane's final SQL guard.
"""

from apps.agents.runtime import (
    AgentDecision,
    AgentDefinition,
    AgentRunRequest,
    PlannedAction,
    create_agent_app,
)


RISK_LEVELS = ("high", "medium", "low")

DATA_KEYWORDS = (
    "procurement",
    "supplier",
    "suppliers",
    "vendor",
    "vendors",
    "purchase",
    "spend",
    "order",
    "orders",
    "sourcing",
    "rank",
    "list",
    "show",
    "count",
    "compare",
    "data",
    "sql",
)

CHAT_FALLBACK_REPLY = (
    "Hello! I'm the procurement analyst agent. Ask me about suppliers, "
    "purchase orders, spend, and risk — for example: 'rank suppliers by "
    "total spend' or 'show recent blocked purchase orders'."
)

PLANNER_GUIDANCE = """
Data actions for this agent:
- "sql": read-only lookup over the procurement database. Put one PostgreSQL
  SELECT statement in arguments.sql, using only these tables:
    suppliers(supplier_id, supplier_name, category, country, risk_level)
    purchase_orders(po_number, supplier_id, business_unit, order_date,
                    status, total_amount)
    supplier_summary(supplier_name, category, country, total_spend,
                     order_count, risk_level, last_order_date)
  Join purchase_orders.supplier_id = suppliers.supplier_id. Always end with
  LIMIT (50 or less).
- "risk": supplier spend summary for one risk level. Set
  arguments.risk_level to "high", "medium", or "low".
""".strip()


def fallback_action(message: str) -> str:
    text = message.lower()
    if "delete" in text or "remove" in text:
        return "approval"
    if "risk" in text:
        return "risk"
    if any(keyword in text for keyword in DATA_KEYWORDS):
        return "sql"
    return "chat"


def extract_risk_level(message: str) -> str:
    text = message.lower()
    for level in RISK_LEVELS:
        if level in text:
            return level
    return "high"


def decide(planned: PlannedAction, request: AgentRunRequest) -> AgentDecision:
    action = planned.action

    if action == "chat":
        return AgentDecision(
            action="final",
            workflow="procurement",
            planner_action=action,
            audit_event="agent_chat_answered",
            output=planned.reply or CHAT_FALLBACK_REPLY,
        )

    if action == "approval":
        return AgentDecision(
            action="approval",
            workflow="procurement",
            planner_action=action,
            audit_event="procurement_approval_required",
        )

    if action == "risk":
        level = planned.arguments.get("risk_level")
        return AgentDecision(
            action="tool",
            workflow="procurement",
            planner_action=action,
            tool="mcp",
            required_permission="procurement-db",
            tool_input={
                "server": "procurement-mcp",
                "name": "supplier_spend_summary",
                "arguments": {
                    "risk_level": level
                    if isinstance(level, str) and level in RISK_LEVELS
                    else extract_risk_level(request.message),
                },
            },
        )

    planned_sql = planned.arguments.get("sql")
    if isinstance(planned_sql, str) and planned_sql.strip():
        return AgentDecision(
            action="tool",
            workflow="procurement",
            planner_action="sql",
            tool="mcp",
            required_permission="procurement-db",
            tool_input={
                "server": "procurement-mcp",
                "name": "run_sql",
                "arguments": {"sql": planned_sql.strip()},
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
    actions=frozenset({"chat", "sql", "risk", "approval"}),
    required_permissions=("procurement-db",),
    tools=("mcp",),
    fallback_action=fallback_action,
    decide=decide,
    planner_guidance=PLANNER_GUIDANCE,
)

app = create_agent_app(DEFINITION)
