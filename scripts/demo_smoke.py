"""Run inside the API container to verify live operational and lake sinks."""

import argparse
from datetime import datetime, timezone
import json
import time
import uuid
import io
import pyarrow.parquet as pq

import requests
from confluent_kafka import Consumer, TopicPartition
from src.common.config import config
from src.common.db import get_pool, close_pool
from src.storage.lake_writer import ParquetLakeWriter


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark", action="store_true")
    args = parser.parse_args()
    identifier = "DEMO-" + uuid.uuid4().hex[:16]
    event = {
        "event_id": identifier,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "text": "Synthetic demo: river flood near a residential road",
        "location": "Demo Mumbai",
        "disaster_type": "flood",
        "severity": "severe",
        "latitude": 19.076,
        "longitude": 72.8777,
        "source": "synthetic_demo",
        "metadata": {"rainfall_mm": 100, "water_level_m": 3, "is_synthetic": True},
    }
    response = requests.post("http://localhost:8000/events", json=event, timeout=10)
    response.raise_for_status()
    acknowledgement = response.json()
    client = ParquetLakeWriter()._client()
    _, _, prefix = ParquetLakeWriter._partition(event)
    deadline = time.monotonic() + 60
    try:
        while time.monotonic() < deadline:
            with get_pool().connection() as conn:
                row = conn.execute(
                    "SELECT * FROM crisis_events WHERE event_id=%s", (identifier,)
                ).fetchone()
            if row:
                try:
                    client.head_object(
                        Bucket="cataclysm-raw",
                        Key=f"{prefix}/event_id={identifier}.parquet",
                    )
                    break
                except client.exceptions.ClientError:
                    pass
            time.sleep(1)
        else:
            raise RuntimeError(
                "Event did not reach DB and raw lake within 60 seconds"
            )
        duplicate = requests.post(
            "http://localhost:8000/events", json=event, timeout=10
        )
        duplicate.raise_for_status()
        duplicate_ack = duplicate.json()
        observer = Consumer(
            {
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": "crisis-dashboard-group",
                "enable.auto.commit": False,
            }
        )
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                committed = observer.committed(
                    [TopicPartition(config.KAFKA_TOPIC, duplicate_ack["partition"])],
                    timeout=3,
                )[0].offset
                if committed > duplicate_ack["offset"]:
                    break
                time.sleep(1)
            else:
                raise RuntimeError(
                    "The duplicate record was not committed by the operational consumer"
                )
        finally:
            observer.close()
        with get_pool().connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) AS count FROM crisis_events WHERE event_id=%s",
                (identifier,),
            ).fetchone()["count"]
            assert count == 1
        result = {
            "event_id": identifier,
            "kafka": acknowledgement,
            "database": True,
            "predictions": predictions,
            "raw_lake": True,
            "duplicate_consumed_without_duplicate_row": True,
            "operational_committed_offset": committed,
        }
        if args.spark:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                objects = client.list_objects_v2(
                    Bucket="cataclysm-processed", Prefix="events-v2/"
                )
                files = [
                    item
                    for item in objects.get("Contents", [])
                    if item["Key"].endswith(".parquet")
                ]
                found = False
                for item in files:
                    body = client.get_object(
                        Bucket="cataclysm-processed", Key=item["Key"]
                    )["Body"]
                    try:
                        table = pq.ParquetFile(io.BytesIO(body.read())).read(
                            columns=["event_id"]
                        )
                        found = identifier in table.column("event_id").to_pylist()
                    finally:
                        body.close()
                    if found:
                        break
                if found:
                    result["processed_parquet_files"] = len(files)
                    result["specific_event_in_processed_parquet"] = True
                    break
                time.sleep(2)
            else:
                raise RuntimeError(
                    "No Spark processed Parquet appeared within 90 seconds"
                )
        print(json.dumps(result, indent=2))
    finally:
        close_pool()


if __name__ == "__main__":
    main()
