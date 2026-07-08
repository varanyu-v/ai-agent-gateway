import asyncio
import json
import os
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from opentelemetry.trace import SpanKind

from apps.observability import (
    clean_attributes,
    inject_trace_context,
    setup_observability,
    start_event_span,
)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
tracer = setup_observability("report-worker")


def decode_event(value: bytes) -> dict[str, Any]:
    return json.loads(value.decode())


async def publish_completed(
    producer: AIOKafkaProducer,
    event: dict[str, Any],
    result: dict[str, Any],
) -> None:
    completed = {
        **event,
        "status": "completed",
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
                "app.run_status": "completed",
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


async def handle_report_tool(
    producer: AIOKafkaProducer,
    event: dict[str, Any],
) -> None:
    with start_event_span(
        tracer,
        "worker.report_tool",
        event,
        attributes=clean_attributes(
            {
                "app.request_id": event.get("request_id"),
                "app.agent_id": event.get("agent_id"),
                "app.workflow": event.get("workflow"),
                "app.tool": event.get("tool"),
                "app.tool_call_id": event.get("tool_call_id"),
                "app.report_type": event.get("input", {}).get("report_type"),
                "messaging.system": "kafka",
                "messaging.destination.name": "tool.requested",
            },
        ),
    ) as span:
        report_type = event["input"]["report_type"]
        report_id = f"{event['request_id']}-{report_type}"

        span.set_attributes(
            {
                "app.report_id": report_id,
                "app.run_status": "completed",
            },
        )
        await publish_completed(
            producer,
            event,
            {
                "report_id": report_id,
                "status": "ready",
                "download_url": f"https://reports.example.com/{report_id}.pdf",
            },
        )


async def main() -> None:
    consumer = AIOKafkaConsumer(
        "tool.requested",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="report-tool-service",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            event = decode_event(msg.value)
            if event.get("tool") != "report":
                continue

            await handle_report_tool(producer, event)
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
