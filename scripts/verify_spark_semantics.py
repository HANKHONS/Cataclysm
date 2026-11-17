"""Isolated Spark 3.5 streaming semantics test; never uses production checkpoints.

Run with spark-submit --master local[2]. Fixtures are labelled synthetic and kept
under a unique work directory. File source permits precise microbatch boundaries.
"""

import json
import sys
import uuid
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, "/opt/spark/app")
from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.types import StructType, StructField, StringType
from src.spark.streaming_job import clean_events


def main():
    root = Path("/opt/spark/app/work/spark-semantics-" + uuid.uuid4().hex[:10])
    incoming = root / "input"
    incoming.mkdir(parents=True)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("cataclysm-isolated-semantics")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    source = spark.readStream.schema(
        StructType([StructField("value", StringType())])
    ).json(str(incoming))
    cleaned = clean_events(source)
    path = str(root / "processed")
    checkpoint = str(root / "checkpoint")

    def start():
        return (
            cleaned.writeStream.format("parquet")
            .option("path", path)
            .option("checkpointLocation", checkpoint)
            .start()
        )

    query = start()
    base = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    def send(index, pairs):
        values = []
        for identifier, minutes in pairs:
            event = dict(
                event_id=identifier,
                timestamp=(base + timedelta(minutes=minutes)).isoformat(),
                text="  SYNTHETIC Flood  ",
                location="Isolated test",
                disaster_type="flood",
                severity="minor",
                source="synthetic_spark_test",
                latitude=0.0,
                longitude=0.0,
            )
            values.append(json.dumps({"value": json.dumps(event)}))
        (incoming / f"batch-{index}.json").write_text("\n".join(values))
        query.processAllAvailable()

    send(1, [("first", 0), ("first", 0)])
    query.stop()
    query = start()
    send(2, [("first", 0), ("advance", 30)])
    send(3, [("too-late", 0), ("advance-again", 60)])
    progress = query.recentProgress
    dropped = sum(
        operator.get("numRowsDroppedByWatermark", 0)
        for p in progress
        for operator in p.get("stateOperators", [])
    )
    query.stop()
    rows = spark.read.parquet(path).collect()
    ids = [row.event_id for row in rows]
    assert ids.count("first") == 1 and "too-late" not in ids and len(ids) == 3, ids
    assert all(row.text == "synthetic flood" for row in rows)
    assert dropped >= 1
    processed = (
        spark.readStream.schema(cleaned.schema)
        .parquet(path)
        .withWatermark("event_timestamp", "10 minutes")
    )
    curated = str(root / "curated")
    aggregation = (
        processed.groupBy(
            window("event_timestamp", "10 minutes", "5 minutes"),
            "disaster_type",
            "severity",
        )
        .count()
        .writeStream.format("parquet")
        .outputMode("append")
        .option("path", curated)
        .option("checkpointLocation", str(root / "aggregate-checkpoint"))
        .start()
    )
    aggregation.processAllAvailable()
    # A subsequent trigger applies the advanced watermark and finalizes windows.
    aggregation.processAllAvailable()
    aggregation.stop()
    windows = spark.read.parquet(curated).collect()
    first_windows = [
        row
        for row in windows
        if row.window.start <= base.replace(tzinfo=None) < row.window.end
    ]
    assert len(first_windows) == 2 and all(
        row["count"] == 1 for row in first_windows
    ), str(windows)
    report = {
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "root": str(root),
        "processed_event_ids": ids,
        "restart_preserved_dedup_state": True,
        "late_event_absent": True,
        "watermark_dropped_rows": dropped,
        "normalized_text": "synthetic flood",
        "finalized_windows_for_first_event": len(first_windows),
        "window_counts": [row["count"] for row in first_windows],
        "scope": "Actual clean_events with isolated file source, local[2], checkpoint restart, Parquet readback and downstream window aggregation. Production Kafka path is covered separately by demo_smoke.",
    }
    Path("/opt/spark/app/docs/evidence").mkdir(exist_ok=True)
    Path("/opt/spark/app/docs/evidence/spark-semantics.json").write_text(
        json.dumps(report, indent=2)
    )
    print(json.dumps(report, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
