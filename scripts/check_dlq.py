"""Publish one deliberately malformed test record and verify DLQ delivery."""

import json
import time
import uuid
from confluent_kafka import Consumer, Producer
from src.common.config import config


def main() -> None:
    marker = "DLQ-TEST-" + uuid.uuid4().hex
    producer = Producer(
        {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, "acks": "all"}
    )
    delivered = []
    producer.produce(
        config.KAFKA_TOPIC,
        value=marker.encode(),
        on_delivery=lambda error, message: delivered.append(error),
    )
    if producer.flush(10) or not delivered or delivered[0] is not None:
        raise RuntimeError("Test source publication failed")
    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": marker,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([config.KAFKA_DLQ_TOPIC])
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is not None and not message.error():
                value = json.loads(message.value())
                if value["raw_payload"] == marker:
                    print(
                        json.dumps(
                            {
                                "malformed_record_routed": True,
                                "source_offset": value["source_offset"],
                            }
                        )
                    )
                    return
        raise RuntimeError("Test record was not found in DLQ")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
