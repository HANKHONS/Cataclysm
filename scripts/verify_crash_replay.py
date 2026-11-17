"""Exercise real consumer crash-after-DB behavior on an isolated retained topic.

Run inside fastapi. Uses normal PostgreSQL/MinIO sinks and separate Kafka group;
no operational offsets are reset and no data is deleted.
"""

from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


def child(mode: str) -> None:
    from src.kafka import consumer as module

    if mode == "crash":

        def crash_after_database(self, events):
            os._exit(91)

        module.ParquetLakeWriter.write_batch = crash_after_database
    else:
        original = module.Consumer

        class StopAfterCommit:
            def __init__(self, options):
                self.consumer = original(options)

            def __getattr__(self, name):
                return getattr(self.consumer, name)

            def commit(self, *args, **kwargs):
                result = self.consumer.commit(*args, **kwargs)
                module.running = False
                return result

        module.Consumer = StopAfterCommit
    module.run_consumer()


def main(output: str) -> None:
    import requests
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    from src.common.config import config
    from src.common.db import get_pool

    identifier = "CRASH-" + uuid.uuid4().hex[:16]
    topic = "verification-" + uuid.uuid4().hex[:12]
    group = topic + "-group"
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    for future in admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    ).values():
        future.result(15)
    event = dict(
        event_id=identifier,
        timestamp=datetime.now(timezone.utc).isoformat(),
        text="Synthetic crash test river flood",
        location="Crash verification",
        latitude=0,
        longitude=0,
        disaster_type="flood",
        severity="minor",
        source="synthetic_crash",
        metadata={"is_synthetic": True},
    )
    producer = Producer(
        {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, "acks": "all"}
    )
    ack = []
    producer.produce(
        topic,
        value=json.dumps(event).encode(),
        on_delivery=lambda error, msg: ack.append((error, msg.offset())),
    )
    assert producer.flush(10) == 0 and ack and ack[0][0] is None
    env = {**os.environ, "KAFKA_TOPIC": topic, "KAFKA_GROUP_ID": group}
    first = subprocess.run(
        [sys.executable, "-m", "scripts.verify_crash_replay", "--child", "crash"],
        env=env,
        timeout=60,
        capture_output=True,
    )
    assert first.returncode == 91, f"Expected crash exit 91, got {first.returncode}"
    observer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group,
            "enable.auto.commit": False,
        }
    )
    before = observer.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
    with get_pool().connection() as conn:
        count_before = conn.execute(
            "SELECT COUNT(*) AS n FROM crisis_events WHERE event_id=%s", (identifier,)
        ).fetchone()["n"]
    assert count_before == 1 and before <= ack[0][1]
    second = subprocess.run(
        [sys.executable, "-m", "scripts.verify_crash_replay", "--child", "recover"],
        env=env,
        timeout=60,
        capture_output=True,
    )
    assert second.returncode == 0, f"Recovery failed: exit {second.returncode}"
    after = observer.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
    observer.close()
    with get_pool().connection() as conn:
        count_after = conn.execute(
            "SELECT COUNT(*) AS n FROM crisis_events WHERE event_id=%s", (identifier,)
        ).fetchone()["n"]
        predictions = conn.execute(
            "SELECT COUNT(*) AS n FROM predictions WHERE event_id=%s", (identifier,)
        ).fetchone()["n"]
    assert count_after == 1 and predictions == 2 and after == ack[0][1] + 1
    detail = requests.get(
        f"http://localhost:8000/api/events/{identifier}/details", timeout=10
    )
    detail.raise_for_status()
    assert detail.json()["data_mode"] == "synthetic"
    result = {
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "event_id": identifier,
        "topic": topic,
        "group": group,
        "crash_exit": first.returncode,
        "rows_before_replay": count_before,
        "rows_after_replay": count_after,
        "offset_before_replay": before,
        "offset_after_replay": after,
        "prediction_rows": predictions,
        "scope": "Actual consumer process exits after database transaction and before raw archive/offset commit; isolated retained Kafka topic.",
    }
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    Path(output).write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", choices=["crash", "recover"])
    parser.add_argument("--output", default="docs/evidence/crash-replay.json")
    args = parser.parse_args()
    child(args.child) if args.child else main(args.output)
