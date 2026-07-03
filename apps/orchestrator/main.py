import asyncio
import json
import os
from contextlib import asynccontextmanager, suppress
from typing import Any

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Header, HTTPException
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph
from langgraph.store.memory import InMemoryStore
from pydantic import BaseModel
from typing_extensions import TypedDict


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "http://localhost:4000/v1").rstrip("/")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "")
LITELLM_MODEL = os.getenv("LITELLM_MODEL", "")
LITELLM_TIMEOUT_SECONDS = float(os.getenv("LITELLM_TIMEOUT_SECONDS", "30"))

WORKFLOW_ACTIONS = {
    "world": {"approval", "report", "sql"},
    "procurement": {"approval", "sql"},
}

LLM_PLANNER_SYSTEM_PROMPT = """
You route enterprise agent requests to one workflow action.
Return only a JSON object with this shape:
{"action":"sql|report|approval","reason":"short reason"}

Rules:
- Choose "approval" for destructive, write, delete, data approval, or other human approval requests.
- Choose "report" only for explicit report, dashboard, document, or export generation requests when report is allowed.
- Choose "sql" for data lookup, analytics, list, show, count, compare, summarize, or read-only database questions.
- Do not write SQL or tool payloads.
""".strip()


class AgentState(TypedDict):
    request_id: str
    tenant_id: str
    user_id: str
    agent_id: str
    message: str
    allowed_permissions: list[str]
    needs_approval: bool
    denied_reason: str | None


class RunIn(BaseModel):
    message: str
    thread_id: str | None = None


producer: AIOKafkaProducer | None = None
completed_consumer: AIOKafkaConsumer | None = None
completed_consumer_task: asyncio.Task[None] | None = None
RUNS: dict[str, dict[str, Any]] = {}


def decode_event(value: bytes) -> dict[str, Any]:
    return json.loads(value.decode())


def remember_run(run_id: str, payload: dict[str, Any]) -> None:
    RUNS[run_id] = {**RUNS.get(run_id, {}), **payload}


def run_output_for_status(
    status: str,
    *,
    denied_reason: str | None = None,
    tool: str | None = None,
    result: dict[str, Any] | None = None,
) -> str:
    if status == "denied":
        return denied_reason or "Request was denied."
    if status == "requires_approval":
        return "Human approval is required before this request can continue."
    if status == "approved":
        return "Human approval was recorded. This sample does not execute destructive follow-up actions."
    if status == "completed" and tool == "sql":
        rows = result.get("rows", []) if result else []
        return f"SQL tool completed with {len(rows)} row(s)."
    if status == "completed" and tool == "report":
        report_id = result.get("report_id") if result else None
        return f"Report tool completed: {report_id}."
    if status == "failed":
        return "Tool execution failed."
    if status == "running":
        return "Agent accepted the request and is waiting for tool output."
    return "Agent request was received."


async def consume_tool_completed() -> None:
    if completed_consumer is None:
        return

    async for msg in completed_consumer:
        event = decode_event(msg.value)
        request_id = event.get("request_id")
        if not request_id:
            continue

        remember_run(
            request_id,
            {
                "run_id": request_id,
                "request_id": request_id,
                "status": event.get("status", "completed"),
                "agent_id": event.get("agent_id"),
                "tenant_id": event.get("tenant_id"),
                "user_id": event.get("user_id"),
                "workflow": event.get("workflow"),
                "tool": event.get("tool"),
                "tool_call_id": event.get("tool_call_id"),
                "input": event.get("input"),
                "result": event.get("result"),
                "denied_reason": event.get("denied_reason"),
                "output": run_output_for_status(
                    event.get("status", "completed"),
                    tool=event.get("tool"),
                    result=event.get("result"),
                    denied_reason=event.get("denied_reason"),
                ),
            },
        )


async def publish(topic: str, payload: dict[str, Any]) -> None:
    if producer is None:
        raise RuntimeError("Kafka producer has not been started")

    await producer.send_and_wait(
        topic,
        key=payload["request_id"].encode(),
        value=json.dumps(payload).encode(),
    )


def parse_allowed_permissions(header_value: str) -> list[str]:
    return [
        permission.strip()
        for permission in header_value.split(",")
        if permission.strip()
    ]


def can_use_permission(state: AgentState, permission: str) -> bool:
    return permission in state["allowed_permissions"]


def fallback_plan_action(workflow: str, message: str) -> str:
    text = message.lower()
    if "delete" in text or "remove" in text:
        return "approval"

    if workflow == "world":
        if "report" in text:
            return "report"
        return "sql"

    return "sql"


async def litellm_plan_action(workflow: str, message: str) -> str | None:
    if not LITELLM_API_KEY or not LITELLM_MODEL:
        return None

    allowed_actions = WORKFLOW_ACTIONS[workflow]
    payload = {
        "model": LITELLM_MODEL,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": LLM_PLANNER_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Workflow: {workflow}\n"
                    f"Allowed actions: {', '.join(sorted(allowed_actions))}\n"
                    f"User message: {message}"
                ),
            },
        ],
    }
    headers = {
        "Authorization": f"Bearer {LITELLM_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=LITELLM_TIMEOUT_SECONDS) as client:
            response = await client.post(
                f"{LITELLM_BASE_URL}/chat/completions",
                headers=headers,
                json=payload,
            )
            response.raise_for_status()

        content = response.json()["choices"][0]["message"]["content"]
        decision = json.loads(content)
    except (httpx.HTTPError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None

    action = str(decision.get("action", "")).strip().lower()
    if action in allowed_actions:
        return action

    return None


async def choose_plan_action(workflow: str, message: str) -> str:
    action = await litellm_plan_action(workflow, message)
    if action is not None:
        return action

    return fallback_plan_action(workflow, message)


async def deny_permission_access(
    state: AgentState,
    workflow: str,
    permission: str,
) -> dict[str, Any]:
    denied_reason = f"User cannot use data source permission: {permission}"
    await publish(
        "audit.events",
        {
            **state,
            "workflow": workflow,
            "event": "permission_access_denied",
            "permission": permission,
            "reason": denied_reason,
        },
    )
    return {"denied_reason": denied_reason}


async def world_plan(state: AgentState) -> dict[str, Any]:
    action = await choose_plan_action("world", state["message"])

    if action == "approval":
        await publish(
            "audit.events",
            {**state, "workflow": "world", "event": "human_approval_required"},
        )
        return {"needs_approval": True}

    if action == "report":
        if not can_use_permission(state, "world-db"):
            return await deny_permission_access(state, "world", "world-db")

        await publish(
            "tool.requested",
            {
                **state,
                "tool": "report",
                "tool_call_id": f"{state['request_id']}:report:1",
                "workflow": "world",
                "input": {
                    "report_type": "world_market_summary",
                    "database": "world",
                },
            },
        )
        return {"needs_approval": False, "denied_reason": None}

    if not can_use_permission(state, "world-db"):
        return await deny_permission_access(state, "world", "world-db")

    await publish(
        "tool.requested",
        {
            **state,
            "tool": "sql",
            "tool_call_id": f"{state['request_id']}:sql:1",
            "workflow": "world",
            "input": {
                "database": "world",
                "sql": (
                    "select city.name as city, country.name as country, "
                    "country.continent, city.district, city.population "
                    "from city "
                    "join country on country.code = city.country_code "
                    "order by city.population desc "
                    "limit 10"
                )
            },
        },
    )
    return {"needs_approval": False, "denied_reason": None}


async def procurement_plan(state: AgentState) -> dict[str, Any]:
    action = await choose_plan_action("procurement", state["message"])

    if action == "approval":
        await publish(
            "audit.events",
            {**state, "workflow": "procurement", "event": "procurement_approval_required"},
        )
        return {"needs_approval": True}

    if not can_use_permission(state, "procurement-db"):
        return await deny_permission_access(state, "procurement", "procurement-db")

    await publish(
        "tool.requested",
        {
            **state,
            "tool": "sql",
            "tool_call_id": f"{state['request_id']}:sql:1",
            "workflow": "procurement",
            "input": {
                "database": "procurement",
                "sql": (
                    "select supplier_name, category, country, total_spend, "
                    "order_count, risk_level, last_order_date "
                    "from supplier_summary "
                    "order by total_spend desc "
                    "limit 10"
                )
            },
        },
    )
    return {"needs_approval": False, "denied_reason": None}


def route_after_plan(state: AgentState) -> str:
    return END


def build_workflow(name: str, plan_node) -> Any:
    builder = StateGraph(AgentState)
    builder.add_node(name, plan_node)
    builder.set_entry_point(name)
    builder.add_conditional_edges(name, route_after_plan)
    return builder.compile(
        checkpointer=InMemorySaver(),
        store=InMemoryStore(),
    )


WORKFLOWS = {
    "world-agent": build_workflow("world_plan", world_plan),
    "procurement-agent": build_workflow("procurement_plan", procurement_plan),
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global completed_consumer, completed_consumer_task, producer

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    completed_consumer = AIOKafkaConsumer(
        "tool.completed",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="agent-orchestrator-run-results",
    )
    await producer.start()
    await completed_consumer.start()
    completed_consumer_task = asyncio.create_task(consume_tool_completed())
    try:
        yield
    finally:
        if completed_consumer_task is not None:
            completed_consumer_task.cancel()
            with suppress(asyncio.CancelledError):
                await completed_consumer_task
        if completed_consumer is not None:
            await completed_consumer.stop()
        await producer.stop()
        completed_consumer_task = None
        completed_consumer = None
        producer = None


app = FastAPI(title="Agent Orchestrator API", lifespan=lifespan)


@app.post("/internal/agents/{agent_id}/runs")
async def run(
    agent_id: str,
    body: RunIn,
    x_request_id: str = Header(),
    x_tenant_id: str = Header(),
    x_user_id: str = Header(),
    x_allowed_permissions: str = Header(default=""),
) -> dict[str, str | None]:
    state: AgentState = {
        "request_id": x_request_id,
        "tenant_id": x_tenant_id,
        "user_id": x_user_id,
        "agent_id": agent_id,
        "message": body.message,
        "allowed_permissions": parse_allowed_permissions(x_allowed_permissions),
        "needs_approval": False,
        "denied_reason": None,
    }
    remember_run(
        x_request_id,
        {
            "run_id": x_request_id,
            "request_id": x_request_id,
            "status": "requested",
            "agent_id": agent_id,
            "tenant_id": x_tenant_id,
            "user_id": x_user_id,
            "message": body.message,
            "allowed_permissions": state["allowed_permissions"],
            "result": None,
            "denied_reason": None,
            "output": run_output_for_status("requested"),
        },
    )

    workflow = WORKFLOWS.get(agent_id)
    if workflow is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown agent_id. Available agents: {sorted(WORKFLOWS)}",
        )

    await publish("agent.requested", state)
    result = await workflow.ainvoke(
        state,
        {"configurable": {"thread_id": body.thread_id or x_request_id}},
    )

    if result.get("denied_reason"):
        status = "denied"
    elif result["needs_approval"]:
        status = "requires_approval"
    else:
        status = "running"

    remember_run(
        x_request_id,
        {
            "status": status,
            "agent_id": agent_id,
            "denied_reason": result.get("denied_reason"),
            "output": run_output_for_status(
                status,
                denied_reason=result.get("denied_reason"),
            ),
        },
    )

    return {
        "run_id": x_request_id,
        "status": status,
        "agent_id": agent_id,
        "denied_reason": result.get("denied_reason"),
    }


@app.get("/internal/runs/{run_id}")
async def get_run(
    run_id: str,
    x_tenant_id: str = Header(),
    x_user_id: str = Header(),
) -> dict[str, Any]:
    run_result = RUNS.get(run_id)
    if run_result is None:
        raise HTTPException(status_code=404, detail="Run not found")

    if run_result.get("tenant_id") != x_tenant_id or run_result.get("user_id") != x_user_id:
        raise HTTPException(status_code=404, detail="Run not found")

    return run_result


@app.post("/internal/runs/{run_id}/approve")
async def approve_run(
    run_id: str,
    x_tenant_id: str = Header(),
    x_user_id: str = Header(),
) -> dict[str, Any]:
    run_result = RUNS.get(run_id)
    if run_result is None:
        raise HTTPException(status_code=404, detail="Run not found")

    if run_result.get("tenant_id") != x_tenant_id or run_result.get("user_id") != x_user_id:
        raise HTTPException(status_code=404, detail="Run not found")

    if run_result.get("status") != "requires_approval":
        raise HTTPException(status_code=409, detail="Run does not require approval")

    approval_result = {
        "approved": True,
        "approved_by": x_user_id,
        "message": run_output_for_status("approved"),
    }
    remember_run(
        run_id,
        {
            "status": "approved",
            "result": approval_result,
            "approved_by": x_user_id,
            "output": approval_result["message"],
        },
    )

    await publish(
        "audit.events",
        {
            "request_id": run_id,
            "tenant_id": x_tenant_id,
            "user_id": x_user_id,
            "agent_id": run_result.get("agent_id"),
            "message": run_result.get("message"),
            "workflow": run_result.get("workflow"),
            "event": "human_approval_approved",
        },
    )
    return RUNS[run_id]
