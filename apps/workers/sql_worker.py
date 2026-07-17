import asyncio
import json
import os
from typing import Any

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from opentelemetry.trace import SpanKind, Status, StatusCode

from apps.data_access.runtime import parse_data_planes
from apps.observability import (
    clean_attributes,
    inject_trace_context,
    setup_observability,
    start_event_span,
)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
# Route each SQL tool to the data plane that owns its database. Each plane
# holds credentials for only its own database, so the worker never needs any
# database credentials itself.
DATA_PLANES = parse_data_planes(os.getenv("DATA_PLANES", ""))
tracer = setup_observability("sql-worker")


def decode_event(value: bytes) -> dict[str, Any]:
    return json.loads(value.decode())


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


async def handle_sql_tool(
    producer: AIOKafkaProducer,
    event: dict[str, Any],
) -> None:
    with start_event_span(
        tracer,
        "worker.sql_tool",
        event,
        attributes=clean_attributes(
            {
                "app.request_id": event.get("request_id"),
                "app.agent_id": event.get("agent_id"),
                "app.workflow": event.get("workflow"),
                "app.tool": event.get("tool"),
                "app.tool_call_id": event.get("tool_call_id"),
                "app.database": event.get("input", {}).get("database"),
                "messaging.system": "kafka",
                "messaging.destination.name": "tool.requested",
            },
        ),
    ) as span:
        database = event.get("input", {}).get("database")
        base_url = DATA_PLANES.get(database)
        if base_url is None:
            # No data plane owns this database. Fail the run explicitly rather
            # than routing the query somewhere it does not belong.
            status = "failed"
            result = {"error": f"No data plane registered for database '{database}'"}
            span.set_status(Status(StatusCode.ERROR, "No data plane for database"))
            span.set_attributes(
                clean_attributes(
                    {"app.run_status": status, "app.database": database},
                ),
            )
            await publish_completed(producer, event, status, result)
            return

        headers = {
            "x-request-id": event["request_id"],
            "x-tenant-id": event["tenant_id"],
            "x-user-id": event["user_id"],
        }

        async with httpx.AsyncClient(base_url=base_url, timeout=60) as client:
            response = await client.post("/query", json=event["input"], headers=headers)

        status = "completed" if response.status_code == 200 else "failed"
        result = response.json()
        span.set_attributes(
            clean_attributes(
                {
                    "http.response.status_code": response.status_code,
                    "app.run_status": status,
                    "app.rows": len(result.get("rows", [])) if isinstance(result, dict) else None,
                },
            ),
        )
        if status == "failed":
            span.set_status(Status(StatusCode.ERROR, "SQL tool failed"))

        await publish_completed(producer, event, status, result)


async def main() -> None:
    consumer = AIOKafkaConsumer(
        "tool.requested",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="sql-tool-service",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            event = decode_event(msg.value)
            if event.get("tool") != "sql":
                continue

            await handle_sql_tool(producer, event)
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
