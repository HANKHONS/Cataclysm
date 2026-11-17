"""Probe local operational dependencies and expose Prometheus gauges."""

import time
import json
from pathlib import Path
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import Gauge, start_http_server
from src.common.config import config
from src.common.db import get_pool
from src.storage.lake_writer import ParquetLakeWriter

UP = Gauge("cataclysm_dependency_up", "Dependency probe", ["dependency"])
ROWS = Gauge("cataclysm_persisted_events", "Operational event row count")
LAG = Gauge("cataclysm_consumer_lag", "Committed consumer lag", ["partition"])
OBJECTS = Gauge("cataclysm_lake_objects", "Lake object count", ["layer"])
SPARK_DURATION = Gauge(
    "cataclysm_spark_batch_duration_ms", "Latest Spark microbatch duration", ["query"]
)
SPARK_ROWS = Gauge(
    "cataclysm_spark_processed_rows_per_second",
    "Latest Spark processing rate",
    ["query"],
)
SPARK_OBSERVED = Gauge(
    "cataclysm_spark_progress_epoch", "Last observed Spark progress epoch", ["query"]
)


def main() -> None:
    start_http_server(9108)
    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": config.KAFKA_GROUP_ID,
            "enable.auto.commit": False,
        }
    )
    client = ParquetLakeWriter()._client()
    while True:
        try:
            with get_pool().connection() as conn:
                ROWS.set(
                    conn.execute(
                        "SELECT COUNT(*) AS count FROM crisis_events"
                    ).fetchone()["count"]
                )
            UP.labels("postgres").set(1)
        except Exception:
            UP.labels("postgres").set(0)
        try:
            metadata = consumer.list_topics(config.KAFKA_TOPIC, timeout=5)
            for partition in metadata.topics[config.KAFKA_TOPIC].partitions:
                tp = TopicPartition(config.KAFKA_TOPIC, partition)
                committed = consumer.committed([tp], timeout=5)[0].offset
                low, high = consumer.get_watermark_offsets(tp, timeout=5)
                LAG.labels(str(partition)).set(max(0, high - max(low, committed)))
            UP.labels("kafka").set(1)
        except Exception:
            UP.labels("kafka").set(0)
        try:
            for layer, bucket in (
                ("raw", config.MINIO_BUCKET_RAW),
                ("processed", config.MINIO_BUCKET_PROCESSED),
                ("curated", config.MINIO_BUCKET_CURATED),
            ):
                total = sum(
                    page.get("KeyCount", 0)
                    for page in client.get_paginator("list_objects_v2").paginate(
                        Bucket=bucket
                    )
                )
                OBJECTS.labels(layer).set(total)
            UP.labels("minio").set(1)
        except Exception:
            UP.labels("minio").set(0)
        try:
            progress = json.loads(
                Path("work/spark-progress.json").read_text(encoding="utf-8")
            )
            for name, entry in progress.items():
                SPARK_DURATION.labels(name).set(
                    entry.get("durationMs", {}).get("triggerExecution", 0)
                )
                SPARK_ROWS.labels(name).set(entry.get("processedRowsPerSecond", 0))
                SPARK_OBSERVED.labels(name).set(entry["observed_epoch"])
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(15)


if __name__ == "__main__":
    main()
