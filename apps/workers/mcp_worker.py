"""MCP tool worker — the platform's only tool worker.

Consumes `tool.requested` events (tool is always "mcp") and routes each call
to the MCP server named in the event, using the same `MCP_SERVICES` registry
spec the orchestrator reads. The event's `input` addresses the target:

    {"server": "world-mcp", "name": "list_top_cities", "arguments": {...}}

The orchestrator has already enforced Casbin before publishing the event
(the server's `mcp:{server}` execute object plus the decision's required
datasource permission), so this worker only routes. It owns no credentials:
MCP servers delegate reads to their data planes, and the caller's
request/tenant/user identity is forwarded so RLS still applies. Results are
published to `tool.completed`.
"""

import asyncio
import json
import os
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from opentelemetry.trace import SpanKind, Status, StatusCode

from apps.observability import (
    clean_attributes,
    inject_trace_context,
    setup_observability,
    start_event_span,
)
from apps.orchestrator.mcp_registry import (
    DEFAULT_MCP_SERVICES,
    McpRegistry,
    McpServiceError,
)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MCP_SERVICES = os.getenv("MCP_SERVICES", DEFAULT_MCP_SERVICES)
tracer = setup_observability("mcp-worker")


def decode_event(value: bytes) -> dict[str, Any]:
    return json.loads(value.decode())


def tool_error_text(result: dict[str, Any]) -> str:
    """First text block of an isError MCP result."""
    for block in result.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text":
            return str(block.get("text") or "MCP tool failed")
    return "MCP tool failed"


async def execute_mcp_tool(
    registry: McpRegistry,
    event: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Resolve and invoke the MCP tool the event names; returns (status, result)."""
    tool_input = event.get("input") or {}
    server_id = str(tool_input.get("server") or "")
    tool_name = str(tool_input.get("name") or "")
    arguments = tool_input.get("arguments")

    server = registry.get(server_id)
    if server is None:
        # No registered server owns this call. Fail the run explicitly rather
        # than guessing, mirroring the SQL worker's unknown-database handling.
        return "failed", {"error": f"No MCP server registered for '{server_id}'"}
    if not tool_name:
        return "failed", {"error": "MCP tool call is missing a tool name"}

    headers = {
        "x-request-id": event["request_id"],
        "x-tenant-id": event["tenant_id"],
        "x-user-id": event["user_id"],
    }
    try:
        result = await registry.call_tool(
            server,
            tool_name,
            arguments if isinstance(arguments, dict) else {},
            headers=headers,
        )
    except McpServiceError as exc:
        return "failed", {
            "error": exc.detail,
            "server": server_id,
            "tool": tool_name,
        }

    if result.get("isError"):
        return "failed", {
            "error": tool_error_text(result),
            "server": server_id,
            "tool": tool_name,
        }

    output = result.get("structuredContent")
    if not isinstance(output, dict):
        output = {"content": result.get("content")}
    return "completed", {"server": server_id, "tool": tool_name, "output": output}


async def publish_completed(
    producer: AIOKafkaProducer,
    event: dict[str, Any],
    status: str,
    result: dict[str, Any],
) -> None:
    completed = {
        **event,
        "status": status,
        "result": result,
    }
    traced_completed = inject_trace_context(completed)

    with tracer.start_as_current_span(
        "kafka.publish",
        kind=SpanKind.PRODUCER,
        attributes=clean_attributes(
            {
                "app.request_id": event.get("request_id"),
                "app.agent_id": event.get("agent_id"),
                "app.workflow": event.get("workflow"),
                "app.tool": event.get("tool"),
                "app.tool_call_id": event.get("tool_call_id"),
                "app.run_status": status,
                "messaging.system": "kafka",
                "messaging.destination.name": "tool.completed",
                "messaging.operation": "publish",
            },
        ),
    ):
        await producer.send_and_wait(
            "tool.completed",
            key=event["request_id"].encode(),
            value=json.dumps(traced_completed).encode(),
        )


async def handle_mcp_tool(
    producer: AIOKafkaProducer,
    registry: McpRegistry,
    event: dict[str, Any],
) -> None:
    tool_input = event.get("input", {})
    with start_event_span(
        tracer,
        "worker.mcp_tool",
        event,
        attributes=clean_attributes(
            {
                "app.request_id": event.get("request_id"),
                "app.agent_id": event.get("agent_id"),
                "app.workflow": event.get("workflow"),
                "app.tool": event.get("tool"),
                "app.tool_call_id": event.get("tool_call_id"),
                "app.mcp.server": tool_input.get("server"),
                "app.mcp.tool": tool_input.get("name"),
                "messaging.system": "kafka",
                "messaging.destination.name": "tool.requested",
            },
        ),
    ) as span:
        status, result = await execute_mcp_tool(registry, event)
        span.set_attribute("app.run_status", status)
        if status == "failed":
            span.set_status(Status(StatusCode.ERROR, result.get("error") or "MCP tool failed"))
        await publish_completed(producer, event, status, result)


async def main() -> None:
    consumer = AIOKafkaConsumer(
        "tool.requested",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="mcp-tool-service",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    registry = McpRegistry(MCP_SERVICES)

    await registry.start()
    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            event = decode_event(msg.value)
            if event.get("tool") != "mcp":
                continue

            await handle_mcp_tool(producer, registry, event)
    finally:
        await consumer.stop()
        await producer.stop()
        await registry.aclose()


if __name__ == "__main__":
    asyncio.run(main())
