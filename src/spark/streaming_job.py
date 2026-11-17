"""Checkpointed Kafka-to-MinIO Structured Streaming pipeline."""

from __future__ import annotations

import os
import json
import time
from pathlib import Path
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lower,
    trim,
    to_timestamp,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

METADATA_SCHEMA = StructType(
    [
        StructField("rainfall_mm", DoubleType()),
        StructField("water_level_m", DoubleType()),
        StructField("wind_speed_kmh", DoubleType()),
        StructField("temperature_c", DoubleType()),
    ]
)
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("text", StringType()),
        StructField("location", StringType()),
        StructField("disaster_type", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("severity", StringType()),
        StructField("source", StringType()),
        StructField("metadata", METADATA_SCHEMA),
    ]
)


class ProgressListener(StreamingQueryListener):
    def __init__(self):
        self.queries = {}

    def onQueryStarted(self, event):
        pass

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        progress["observed_epoch"] = time.time()
        self.queries[progress.get("name") or progress["id"]] = progress
        path = Path("/opt/spark/app/work/spark-progress.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.queries), encoding="utf-8")
        temporary.replace(path)


def spark_session() -> SparkSession:
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    return (
        SparkSession.builder.appName("cataclysm-streaming")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config(
            "spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "2")
        )
        .config("spark.ui.retainedJobs", "20")
        .config("spark.ui.retainedStages", "40")
        .config("spark.sql.ui.retainedExecutions", "20")
        .config("spark.sql.streaming.numRecentProgressUpdates", "20")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def clean_events(source: DataFrame) -> DataFrame:
    return (
        source.selectExpr("CAST(value AS STRING) AS payload")
        .select(from_json(col("payload"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .filter(col("event_id").isNotNull() & col("timestamp").isNotNull())
        .withColumn("event_timestamp", to_timestamp("timestamp"))
        .filter(col("event_timestamp").isNotNull())
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
        .withColumn("text", trim(lower(col("text"))))
        .withColumn("processed_at", current_timestamp())
    )


def main() -> None:
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressListener())
    source = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092"),
        )
        .option("subscribe", os.getenv("KAFKA_TOPIC", "crisis-events"))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "10000")
        .load()
    )
    events = clean_events(source)
    (
        events.writeStream.queryName("processed")
        .format("parquet")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("path", "s3a://cataclysm-processed/events-v2")
        .option("checkpointLocation", "/tmp/checkpoints/v2/processed")
        .partitionBy("disaster_type", "severity")
        .start()
    )
    # Separate stateful stages through the committed Parquet sink.
    # Parquet stores partition columns outside the file, at the end of the
    # logical read schema. Preserve that order for the file-stream reader.
    processed_schema = StructType(
        [
            field
            for field in events.schema.fields
            if field.name not in ("disaster_type", "severity")
        ]
        + [
            StructField("disaster_type", StringType()),
            StructField("severity", StringType()),
        ]
    )
    path = spark._jvm.org.apache.hadoop.fs.Path("s3a://cataclysm-processed/events-v2")
    path.getFileSystem(spark._jsc.hadoopConfiguration()).mkdirs(path)
    processed_events = (
        spark.readStream.schema(processed_schema)
        .parquet("s3a://cataclysm-processed/events-v2")
        .select("event_timestamp", "disaster_type", "severity")
        .withWatermark("event_timestamp", "10 minutes")
    )
    (
        processed_events.groupBy(
            window("event_timestamp", "10 minutes", "5 minutes"),
            "disaster_type",
            "severity",
        )
        .count()
        .writeStream.queryName("curated")
        .format("parquet")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("path", "s3a://cataclysm-curated/windowed_incident_counts-v2")
        .option("checkpointLocation", "/tmp/checkpoints/v2/curated")
        .start()
    )
    spark.streams.awaitAnyTermination()
    raise RuntimeError("A streaming query terminated; restart is required")


if __name__ == "__main__":
    main()
