import asyncio
import json
import os
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


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

    await producer.send_and_wait(
        "tool.completed",
        key=event["request_id"].encode(),
        value=json.dumps(completed).encode(),
    )


async def handle_report_tool(
    producer: AIOKafkaProducer,
    event: dict[str, Any],
) -> None:
    report_type = event["input"]["report_type"]
    report_id = f"{event['request_id']}-{report_type}"

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
