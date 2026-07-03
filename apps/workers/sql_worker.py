import asyncio
import json
import os
from typing import Any

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DB_PROXY_URL = os.getenv("DB_PROXY_URL", "http://localhost:8003")


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

    await producer.send_and_wait(
        "tool.completed",
        key=event["request_id"].encode(),
        value=json.dumps(completed).encode(),
    )


async def handle_sql_tool(
    producer: AIOKafkaProducer,
    event: dict[str, Any],
) -> None:
    headers = {
        "x-request-id": event["request_id"],
        "x-tenant-id": event["tenant_id"],
        "x-user-id": event["user_id"],
    }

    async with httpx.AsyncClient(base_url=DB_PROXY_URL, timeout=60) as client:
        response = await client.post("/query", json=event["input"], headers=headers)

    status = "completed" if response.status_code == 200 else "failed"
    await publish_completed(producer, event, status, response.json())


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
