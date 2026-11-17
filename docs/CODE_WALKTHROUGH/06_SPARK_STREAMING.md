# Spark Structured Streaming: two query states, two checkpoints

PySpark SQL functions construct expression trees; they do not immediately execute Python loops. StructType/StructField define the Kafka JSON contract. The nested metadata schema includes weather features only; magnitude/provenance remain available in raw/operational storage, not this v2 processed schema. Changing a persisted streaming schema requires versioned output/checkpoint reasoning.

## Session and listener

`spark_session` builds one SparkSession using configured master and S3A endpoint/credentials. Path-style access and disabled SSL match the private local MinIO endpoint, not an Internet security posture. Two shuffle partitions suit the small one-core worker workload. Retained jobs/stages/SQL executions and recent streaming progress are bounded to reduce driver history growth. Authentication settings come from environment and are never printed by application code.

`ProgressListener` retains latest progress per query name. Required start/idle/terminated callbacks are deliberately no-ops; only progress updates write measurements. `onQueryProgress` parses Spark's JSON, adds wall-clock observed_epoch and atomically replaces a temporary progress file under the shared checkout. Exporter reads that file. Idle streams can leave an older progress timestamp; it is not independently a job-liveness certificate. A crash before atomic replacement leaves the last complete file rather than partial JSON.

## clean_events, operation by operation

1. `selectExpr` casts broker value bytes to a UTF-8-like string representation expected by JSON parsing.
2. `from_json(..., EVENT_SCHEMA)` produces a struct; selecting `event.*` flattens fields.
3. The first filter removes absent ID/timestamp; parsing adds `event_timestamp`; another filter drops unparsable times.
4. `withWatermark(...,'10 minutes')` annotates event-time lateness for downstream state.
5. `dropDuplicatesWithinWatermark(['event_id'])` retains bounded identity state across microbatches. It is not a permanent set.
6. `trim(lower(text))` normalizes narrative; `current_timestamp()` appends processing time. This does not train a model or modify operational database facts.

Parsing is linear in bytes; hash-state lookup is typically constant-time per key while memory/state-store I/O scale with live distinct identities. Spark executes JVM expression/state machinery with scheduling/checkpoint overhead; this is not equivalent to a Python pandas loop.

## Production source and processed sink

The Kafka reader uses the internal bootstrap listener, topic, earliest initial offsets and a maximum of 10,000 offsets per trigger. A compatible checkpoint overrides initial-start behavior on restart. The first writer is named processed, uses append Parquet, a ten-second trigger, hazard/severity partition directories, v2 output prefix and its own v2 checkpoint. Default source data loss/retention behavior is not replaced with silent skipping. Keep driver/worker checkpoint mounts aligned.

## Curated reader and windows

Partition columns are stored in directory paths, so the explicit processed read schema moves hazard/severity to the expected logical read order. The driver ensures the prefix exists before starting the file stream. The second reader uses committed Parquet and selects event time/hazard/severity; a second watermark controls aggregation state. Grouping by `window(event_timestamp,'10 minutes','5 minutes')` and labels counts reports in overlapping half-open windows. Append output emits finalized windows only, under a different dataset/checkpoint. Current events can have no finalized curated row until event time advances.

`awaitAnyTermination` waits for either query, then raises if it terminates. Compose restarts the application rather than leaving one silent dead query. Restarting against incompatible state or sharing a checkpoint between drivers is unsafe; do not erase volumes to conceal the mismatch.

The isolated verifier uses this exact clean_events function with controlled file-source batches, its own checkpoint and real Parquet output. It restarts state, verifies duplicate suppression, advances event time, rejects a late event and reads two finalized count-one windows. The normal smoke separately checks real Kafka-to-MinIO integration. Together these establish useful bounded evidence; they are not proof of arbitrary state-scale performance or host-loss recovery.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `src/spark/streaming_job.py`

[Open source](../../src/spark/streaming_job.py) · SHA-256 `ecfc8f844f894fae740079e786e5e7acb92a67ac6434367aac27574f04b5632b`

Dependencies: `from __future__ import annotations`; `import os`; `import json`; `import time`; `from pathlib import Path`; `from pyspark.sql.streaming import StreamingQueryListener`; `from pyspark.sql import DataFrame, SparkSession`; `from pyspark.sql.functions import (     col,     current_timestamp,     from_json,     lower,     trim,     to_timestamp,     window, )`; `from pyspark.sql.types import DoubleType, StringType, StructField, StructType`

- `ProgressListener`: lines 46–67. Read its control flow with the module guide above.

- `__init__`: lines 47–48. Read its control flow with the module guide above.

- `onQueryStarted`: lines 50–51. Read its control flow with the module guide above.

- `onQueryIdle`: lines 53–54. Read its control flow with the module guide above.

- `onQueryTerminated`: lines 56–57. Read its control flow with the module guide above.

- `onQueryProgress`: lines 59–67. Read its control flow with the module guide above.

- `spark_session`: lines 70–88. Read its control flow with the module guide above.

- `clean_events`: lines 91–103. Read its control flow with the module guide above.

- `main`: lines 106–170. Read its control flow with the module guide above.


```text
0001 | """Checkpointed Kafka-to-MinIO Structured Streaming pipeline."""
0002 | 
0003 | from __future__ import annotations
0004 | 
0005 | import os
0006 | import json
0007 | import time
0008 | from pathlib import Path
0009 | from pyspark.sql.streaming import StreamingQueryListener
0010 | from pyspark.sql import DataFrame, SparkSession
0011 | from pyspark.sql.functions import (
0012 |     col,
0013 |     current_timestamp,
0014 |     from_json,
0015 |     lower,
0016 |     trim,
0017 |     to_timestamp,
0018 |     window,
0019 | )
0020 | from pyspark.sql.types import DoubleType, StringType, StructField, StructType
0021 | 
0022 | METADATA_SCHEMA = StructType(
0023 |     [
0024 |         StructField("rainfall_mm", DoubleType()),
0025 |         StructField("water_level_m", DoubleType()),
0026 |         StructField("wind_speed_kmh", DoubleType()),
0027 |         StructField("temperature_c", DoubleType()),
0028 |     ]
0029 | )
0030 | EVENT_SCHEMA = StructType(
0031 |     [
0032 |         StructField("event_id", StringType()),
0033 |         StructField("timestamp", StringType()),
0034 |         StructField("text", StringType()),
0035 |         StructField("location", StringType()),
0036 |         StructField("disaster_type", StringType()),
0037 |         StructField("latitude", DoubleType()),
0038 |         StructField("longitude", DoubleType()),
0039 |         StructField("severity", StringType()),
0040 |         StructField("source", StringType()),
0041 |         StructField("metadata", METADATA_SCHEMA),
0042 |     ]
0043 | )
0044 | 
0045 | 
0046 | class ProgressListener(StreamingQueryListener):
0047 |     def __init__(self):
0048 |         self.queries = {}
0049 | 
0050 |     def onQueryStarted(self, event):
0051 |         pass
0052 | 
0053 |     def onQueryIdle(self, event):
0054 |         pass
0055 | 
0056 |     def onQueryTerminated(self, event):
0057 |         pass
0058 | 
0059 |     def onQueryProgress(self, event):
0060 |         progress = json.loads(event.progress.json)
0061 |         progress["observed_epoch"] = time.time()
0062 |         self.queries[progress.get("name") or progress["id"]] = progress
0063 |         path = Path("/opt/spark/app/work/spark-progress.json")
0064 |         path.parent.mkdir(parents=True, exist_ok=True)
0065 |         temporary = path.with_suffix(".tmp")
0066 |         temporary.write_text(json.dumps(self.queries), encoding="utf-8")
0067 |         temporary.replace(path)
0068 | 
0069 | 
0070 | def spark_session() -> SparkSession:
0071 |     endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
0072 |     return (
0073 |         SparkSession.builder.appName("cataclysm-streaming")
0074 |         .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
0075 |         .config(
0076 |             "spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "2")
0077 |         )
0078 |         .config("spark.ui.retainedJobs", "20")
0079 |         .config("spark.ui.retainedStages", "40")
0080 |         .config("spark.sql.ui.retainedExecutions", "20")
0081 |         .config("spark.sql.streaming.numRecentProgressUpdates", "20")
0082 |         .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}")
0083 |         .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
0084 |         .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
0085 |         .config("spark.hadoop.fs.s3a.path.style.access", "true")
0086 |         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
0087 |         .getOrCreate()
0088 |     )
0089 | 
0090 | 
0091 | def clean_events(source: DataFrame) -> DataFrame:
0092 |     return (
0093 |         source.selectExpr("CAST(value AS STRING) AS payload")
0094 |         .select(from_json(col("payload"), EVENT_SCHEMA).alias("event"))
0095 |         .select("event.*")
0096 |         .filter(col("event_id").isNotNull() & col("timestamp").isNotNull())
0097 |         .withColumn("event_timestamp", to_timestamp("timestamp"))
0098 |         .filter(col("event_timestamp").isNotNull())
0099 |         .withWatermark("event_timestamp", "10 minutes")
0100 |         .dropDuplicatesWithinWatermark(["event_id"])
0101 |         .withColumn("text", trim(lower(col("text"))))
0102 |         .withColumn("processed_at", current_timestamp())
0103 |     )
0104 | 
0105 | 
0106 | def main() -> None:
0107 |     spark = spark_session()
0108 |     spark.sparkContext.setLogLevel("WARN")
0109 |     spark.streams.addListener(ProgressListener())
0110 |     source = (
0111 |         spark.readStream.format("kafka")
0112 |         .option(
0113 |             "kafka.bootstrap.servers",
0114 |             os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092"),
0115 |         )
0116 |         .option("subscribe", os.getenv("KAFKA_TOPIC", "crisis-events"))
0117 |         .option("startingOffsets", "earliest")
0118 |         .option("maxOffsetsPerTrigger", "10000")
0119 |         .load()
0120 |     )
0121 |     events = clean_events(source)
0122 |     (
0123 |         events.writeStream.queryName("processed")
0124 |         .format("parquet")
0125 |         .outputMode("append")
0126 |         .trigger(processingTime="10 seconds")
0127 |         .option("path", "s3a://cataclysm-processed/events-v2")
0128 |         .option("checkpointLocation", "/tmp/checkpoints/v2/processed")
0129 |         .partitionBy("disaster_type", "severity")
0130 |         .start()
0131 |     )
0132 |     # Separate stateful stages through the committed Parquet sink.
0133 |     # Parquet stores partition columns outside the file, at the end of the
0134 |     # logical read schema. Preserve that order for the file-stream reader.
0135 |     processed_schema = StructType(
0136 |         [
0137 |             field
0138 |             for field in events.schema.fields
0139 |             if field.name not in ("disaster_type", "severity")
0140 |         ]
0141 |         + [
0142 |             StructField("disaster_type", StringType()),
0143 |             StructField("severity", StringType()),
0144 |         ]
0145 |     )
0146 |     path = spark._jvm.org.apache.hadoop.fs.Path("s3a://cataclysm-processed/events-v2")
0147 |     path.getFileSystem(spark._jsc.hadoopConfiguration()).mkdirs(path)
0148 |     processed_events = (
0149 |         spark.readStream.schema(processed_schema)
0150 |         .parquet("s3a://cataclysm-processed/events-v2")
0151 |         .select("event_timestamp", "disaster_type", "severity")
0152 |         .withWatermark("event_timestamp", "10 minutes")
0153 |     )
0154 |     (
0155 |         processed_events.groupBy(
0156 |             window("event_timestamp", "10 minutes", "5 minutes"),
0157 |             "disaster_type",
0158 |             "severity",
0159 |         )
0160 |         .count()
0161 |         .writeStream.queryName("curated")
0162 |         .format("parquet")
0163 |         .outputMode("append")
0164 |         .trigger(processingTime="10 seconds")
0165 |         .option("path", "s3a://cataclysm-curated/windowed_incident_counts-v2")
0166 |         .option("checkpointLocation", "/tmp/checkpoints/v2/curated")
0167 |         .start()
0168 |     )
0169 |     spark.streams.awaitAnyTermination()
0170 |     raise RuntimeError("A streaming query terminated; restart is required")
0171 | 
0172 | 
0173 | if __name__ == "__main__":
0174 |     main()
```
