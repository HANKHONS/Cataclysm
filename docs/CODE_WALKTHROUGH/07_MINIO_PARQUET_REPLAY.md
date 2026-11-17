# MinIO, Arrow, Parquet and faithful replay

`EVENT_ARROW_SCHEMA` fixes raw column types rather than inferring different schemas from null-heavy individual events. PyArrow represents float telemetry and string labels, plus a millisecond timestamp column and complete supported enriched raw_payload JSON. The flattened timestamp has no timezone annotation; normalized JSON retains the instant precisely for replay.

`ParquetLakeWriter.__init__` chooses a local raw directory, optionally accepts a test S3 client, and creates the local directory. Supplying base_dir opts into local fallback; ordinary production use must not silently degrade to local files. `_client` reuses an injected/cached boto3 client, checks credentials, builds HTTP/HTTPS endpoint and path-style S3 configuration. Missing production credentials raise instead of claiming durable storage. Existing valid credentials are read from settings without printing them.

`_partition` parses ISO timestamps (Z normalized to +00:00), accepts Python datetimes and returns datetime, hazard and date/hazard prefix. Invalid timestamps fall back to current time at this helper level; production consumer validation should prevent that path. Direct callers should validate first. Crossing midnight in another timezone would affect a raw partition if bypassing normalization; gateway normalization avoids that ambiguity.

`write_batch` returns early for empty input, then loops each event. It uses null-aware `meta.get`, so a missing rainfall and a measured zero stay distinct. Parallel list-of-one values form a one-row Arrow table. The JSON field preserves provider magnitude/depth/URL, synthetic/replay flags and metadata supported by Pydantic. Object name is date/hazard path plus event ID. S3 mode writes snappy Parquet to BytesIO then PUTs bytes; explicit offline mode writes the same schema locally. One row per object is simple and replay-friendly but incurs heavy small-file overhead. This method is not a bulk dataset compactor and its returned base_dir is not proof of S3 success beyond the completed writes.

`init_buckets.main` retries `list_buckets` up to 30 times with two-second sleeps, then creates raw/processed/curated buckets. Only known already-existing-bucket errors are tolerated; permission/network failures escape. It preserves existing objects and never deletes buckets.

## Replay paths and edge cases

`replay_from_minio` validates a positive row limit, pages object listings and reads .parquet bytes with `ParquetFile`. Bodies close in finally blocks. It prefers raw_payload; legacy rows reconstruct supported flattened fields and timestamps. Replay adds `is_replay=True` without changing original event ID/time/source, then produces using the location key and acks=all. Every record waits for confirmed callback/flush; a failed delivery raises, never increments a fabricated success. The limit counts rows across objects, not just files.

`replay_from_parquet` discovers files recursively, sorts them, reads each file directly to avoid Hive directory/schema inference conflicts, and applies the same original-payload preference and replay flag. Nested file/row loops stop at the row limit. Reading/producer failure propagates with file context instead of being logged as a successful partial replay. The local regression creates a real Parquet fixture, confirms provider metadata and replay flag, and simulates unconfirmed delivery.

Restoring a previously absent ID visibly labels the stored record replay. Replaying an existing ID preserves the first accepted report and predictions; this is a recovery operation, not a revision mechanism. Old history can fall behind production Spark's watermark. A historical analytics rebuild needs its own output prefix and checkpoint so production progress and valid existing files remain intact.

Memory is bounded by one object/table for these replay paths, though listing local filenames holds that filename list. S3 listing is paginated; arbitrary huge Parquet objects would still need row-group streaming for tighter memory bounds. Always use dry-run first, compare actual reconstructed fields and retain source attribution/licensing context for any future external historical corpus. This repository does not claim an additional licensed real training corpus that is absent from the files.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `src/storage/lake_writer.py`

[Open source](../../src/storage/lake_writer.py) · SHA-256 `0bf80006c7a5370dda3855b152ef9097a7a15f6573be3acb3201eebf59c12afb`

Dependencies: `import os`; `import io`; `import json`; `import logging`; `from datetime import datetime`; `from typing import List, Dict, Any`; `import boto3`; `from botocore.config import Config as BotoConfig`; `import pyarrow as pa`; `import pyarrow.parquet as pq`; `from src.common.config import config`

- `ParquetLakeWriter`: lines 38–150. Archive raw events to MinIO as event-keyed, partitioned Parquet objects.

- `__init__`: lines 45–51. Read its control flow with the module guide above.

- `_client`: lines 53–72. Read its control flow with the module guide above.

- `_partition`: lines 75–88. Read its control flow with the module guide above.

- `write_batch`: lines 90–150. Writes a batch of events to partitioned Parquet files.


```text
0001 | import os
0002 | import io
0003 | import json
0004 | import logging
0005 | from datetime import datetime
0006 | from typing import List, Dict, Any
0007 | import boto3
0008 | from botocore.config import Config as BotoConfig
0009 | import pyarrow as pa
0010 | import pyarrow.parquet as pq
0011 | from src.common.config import config
0012 | 
0013 | logger = logging.getLogger("cataclysm.lake")
0014 | 
0015 | EVENT_ARROW_SCHEMA = pa.schema(
0016 |     [
0017 |         ("event_id", pa.string()),
0018 |         ("timestamp", pa.timestamp("ms")),
0019 |         ("disaster_type", pa.string()),
0020 |         ("severity", pa.string()),
0021 |         ("location", pa.string()),
0022 |         ("latitude", pa.float64()),
0023 |         ("longitude", pa.float64()),
0024 |         ("text", pa.string()),
0025 |         ("source", pa.string()),
0026 |         ("rainfall_mm", pa.float64()),
0027 |         ("water_level_m", pa.float64()),
0028 |         ("wind_speed_kmh", pa.float64()),
0029 |         ("temperature_c", pa.float64()),
0030 |         ("predicted_disaster_type", pa.string()),
0031 |         ("predicted_severity", pa.string()),
0032 |         ("prediction_confidence", pa.float64()),
0033 |         ("raw_payload", pa.string()),
0034 |     ]
0035 | )
0036 | 
0037 | 
0038 | class ParquetLakeWriter:
0039 |     """Archive raw events to MinIO as event-keyed, partitioned Parquet objects.
0040 | 
0041 |     Local output is explicitly opt-in for offline unit tests. Production Compose
0042 |     configures a MinIO endpoint and never silently downgrades durable storage.
0043 |     """
0044 | 
0045 |     def __init__(self, base_dir: str = None, s3_client: Any = None):
0046 |         self.base_dir = base_dir or os.path.join(
0047 |             os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "raw"
0048 |         )
0049 |         os.makedirs(self.base_dir, exist_ok=True)
0050 |         self._s3 = s3_client
0051 |         self._local_mode = base_dir is not None
0052 | 
0053 |     def _client(self):
0054 |         if self._s3 is not None:
0055 |             return self._s3
0056 |         if not config.MINIO_ACCESS_KEY or not config.MINIO_SECRET_KEY:
0057 |             if self._local_mode:
0058 |                 return None
0059 |             raise ValueError(
0060 |                 "MinIO credentials are required; supply base_dir for explicit local mode"
0061 |             )
0062 |         endpoint = config.MINIO_ENDPOINT
0063 |         if not endpoint.startswith("http"):
0064 |             endpoint = f"{'https' if config.MINIO_SECURE else 'http'}://{endpoint}"
0065 |         self._s3 = boto3.client(
0066 |             "s3",
0067 |             endpoint_url=endpoint,
0068 |             aws_access_key_id=config.MINIO_ACCESS_KEY,
0069 |             aws_secret_access_key=config.MINIO_SECRET_KEY,
0070 |             config=BotoConfig(s3={"addressing_style": "path"}),
0071 |         )
0072 |         return self._s3
0073 | 
0074 |     @staticmethod
0075 |     def _partition(event: Dict[str, Any]) -> tuple[datetime, str, str]:
0076 |         ts = event.get("timestamp")
0077 |         if isinstance(ts, str):
0078 |             try:
0079 |                 dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
0080 |             except ValueError:
0081 |                 dt = datetime.utcnow()
0082 |         elif isinstance(ts, datetime):
0083 |             dt = ts
0084 |         else:
0085 |             dt = datetime.utcnow()
0086 |         disaster = str(event.get("disaster_type", "unknown"))
0087 |         prefix = f"year={dt:%Y}/month={dt:%m}/day={dt:%d}/disaster_type={disaster}"
0088 |         return dt, disaster, prefix
0089 | 
0090 |     def write_batch(self, events: List[Dict[str, Any]]) -> str:
0091 |         """Writes a batch of events to partitioned Parquet files."""
0092 |         if not events:
0093 |             return ""
0094 | 
0095 |         client = self._client()
0096 |         for event in events:
0097 |             dt, disaster, prefix = self._partition(event)
0098 | 
0099 |             meta = event.get("metadata", {}) or {}
0100 |             record = {
0101 |                 "event_id": [event.get("event_id")],
0102 |                 "timestamp": [dt],
0103 |                 "disaster_type": [disaster],
0104 |                 "severity": [event.get("severity", "unknown")],
0105 |                 "location": [event.get("location", "unknown")],
0106 |                 "latitude": [float(event.get("latitude", 0.0))],
0107 |                 "longitude": [float(event.get("longitude", 0.0))],
0108 |                 "text": [event.get("text", "")],
0109 |                 "source": [event.get("source", "unknown")],
0110 |                 "rainfall_mm": [meta.get("rainfall_mm")],
0111 |                 "water_level_m": [meta.get("water_level_m")],
0112 |                 "wind_speed_kmh": [meta.get("wind_speed_kmh")],
0113 |                 "temperature_c": [meta.get("temperature_c")],
0114 |                 "predicted_disaster_type": [event.get("predicted_disaster_type", "")],
0115 |                 "predicted_severity": [event.get("predicted_severity", "")],
0116 |                 "prediction_confidence": [
0117 |                     float(event.get("prediction_confidence") or 0.0)
0118 |                 ],
0119 |                 "raw_payload": [json.dumps(event, default=str, sort_keys=True)],
0120 |             }
0121 | 
0122 |             table = pa.Table.from_pydict(record, schema=EVENT_ARROW_SCHEMA)
0123 |             object_name = f"{prefix}/event_id={event.get('event_id')}.parquet"
0124 |             if client is not None:
0125 |                 buffer = io.BytesIO()
0126 |                 pq.write_table(table, buffer, compression="snappy")
0127 |                 client.put_object(
0128 |                     Bucket=config.MINIO_BUCKET_RAW,
0129 |                     Key=object_name,
0130 |                     Body=buffer.getvalue(),
0131 |                     ContentType="application/octet-stream",
0132 |                 )
0133 |                 logger.debug(
0134 |                     "Archived MinIO object: s3://%s/%s",
0135 |                     config.MINIO_BUCKET_RAW,
0136 |                     object_name,
0137 |                 )
0138 |             else:
0139 |                 partition_dir = os.path.join(self.base_dir, prefix)
0140 |                 os.makedirs(partition_dir, exist_ok=True)
0141 |                 pq.write_table(
0142 |                     table,
0143 |                     os.path.join(partition_dir, object_name.split("/")[-1]),
0144 |                     compression="snappy",
0145 |                 )
0146 |                 logger.warning(
0147 |                     "MinIO credentials are absent; wrote local development archive only."
0148 |                 )
0149 | 
0150 |         return self.base_dir
```

### `src/storage/init_buckets.py`

[Open source](../../src/storage/init_buckets.py) · SHA-256 `16cc5171bb9e39dbed7f557db12fab25a7fabd88cdd2427180c4ef16f04fd03a`

Dependencies: `import time`; `from botocore.exceptions import ClientError`; `from src.common.config import config`; `from src.storage.lake_writer import ParquetLakeWriter`

- `main`: lines 9–31. Read its control flow with the module guide above.


```text
0001 | """Initialize lake buckets using the existing boto3 client, with bounded retries."""
0002 | 
0003 | import time
0004 | from botocore.exceptions import ClientError
0005 | from src.common.config import config
0006 | from src.storage.lake_writer import ParquetLakeWriter
0007 | 
0008 | 
0009 | def main() -> None:
0010 |     client = ParquetLakeWriter()._client()
0011 |     for attempt in range(30):
0012 |         try:
0013 |             client.list_buckets()
0014 |             break
0015 |         except Exception:
0016 |             if attempt == 29:
0017 |                 raise
0018 |             time.sleep(2)
0019 |     for bucket in (
0020 |         config.MINIO_BUCKET_RAW,
0021 |         config.MINIO_BUCKET_PROCESSED,
0022 |         config.MINIO_BUCKET_CURATED,
0023 |     ):
0024 |         try:
0025 |             client.create_bucket(Bucket=bucket)
0026 |         except ClientError as error:
0027 |             if error.response["Error"]["Code"] not in (
0028 |                 "BucketAlreadyOwnedByYou",
0029 |                 "BucketAlreadyExists",
0030 |             ):
0031 |                 raise
0032 | 
0033 | 
0034 | if __name__ == "__main__":
0035 |     main()
```

### `src/data/replay.py`

[Open source](../../src/data/replay.py) · SHA-256 `c444d0cbbfb853ea5979654b16c6dd35ec0f430520497db31670df77dca728b2`

Dependencies: `import os`; `import glob`; `import argparse`; `import logging`; `import json`; `import io`; `import pyarrow.parquet as pq`; `from confluent_kafka import Producer`; `from src.common.config import config`; `from src.storage.lake_writer import ParquetLakeWriter`

- `replay_from_minio`: lines 13–85. Replay at most limit rows, failing if broker delivery is unconfirmed.

- `replay_from_parquet`: lines 94–170. Read its control flow with the module guide above.


```text
0001 | import os
0002 | import glob
0003 | import argparse
0004 | import logging
0005 | import json
0006 | import io
0007 | import pyarrow.parquet as pq
0008 | from confluent_kafka import Producer
0009 | from src.common.config import config
0010 | from src.storage.lake_writer import ParquetLakeWriter
0011 | 
0012 | 
0013 | def replay_from_minio(dry_run: bool = False, limit: int = 100) -> int:
0014 |     """Replay at most limit rows, failing if broker delivery is unconfirmed."""
0015 |     if limit < 1:
0016 |         raise ValueError("limit must be positive")
0017 |     client = ParquetLakeWriter()._client()
0018 |     producer = (
0019 |         None
0020 |         if dry_run
0021 |         else Producer(
0022 |             {
0023 |                 "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
0024 |                 "acks": "all",
0025 |             }
0026 |         )
0027 |     )
0028 |     count = 0
0029 |     paginator = client.get_paginator("list_objects_v2")
0030 |     for page in paginator.paginate(Bucket=config.MINIO_BUCKET_RAW):
0031 |         for item in page.get("Contents", []):
0032 |             if not item["Key"].endswith(".parquet"):
0033 |                 continue
0034 |             body = client.get_object(Bucket=config.MINIO_BUCKET_RAW, Key=item["Key"])[
0035 |                 "Body"
0036 |             ]
0037 |             try:
0038 |                 rows = pq.ParquetFile(io.BytesIO(body.read())).read().to_pylist()
0039 |             finally:
0040 |                 body.close()
0041 |             for row in rows:
0042 |                 original = (
0043 |                     json.loads(row["raw_payload"]) if row.get("raw_payload") else None
0044 |                 )
0045 |                 event = {
0046 |                     key: row[key]
0047 |                     for key in (
0048 |                         "event_id",
0049 |                         "timestamp",
0050 |                         "disaster_type",
0051 |                         "severity",
0052 |                         "location",
0053 |                         "latitude",
0054 |                         "longitude",
0055 |                         "text",
0056 |                         "source",
0057 |                     )
0058 |                 }
0059 |                 event["timestamp"] = event["timestamp"].isoformat()
0060 |                 event["metadata"] = {
0061 |                     key: row.get(key)
0062 |                     for key in (
0063 |                         "rainfall_mm",
0064 |                         "water_level_m",
0065 |                         "wind_speed_kmh",
0066 |                         "temperature_c",
0067 |                     )
0068 |                 }
0069 |                 if original:
0070 |                     event = original
0071 |                 event.setdefault("metadata", {})["is_replay"] = True
0072 |                 if producer is not None:
0073 |                     delivery = []
0074 |                     producer.produce(
0075 |                         config.KAFKA_TOPIC,
0076 |                         key=event["location"].encode(),
0077 |                         value=json.dumps(event).encode(),
0078 |                         on_delivery=lambda error, message: delivery.append(error),
0079 |                     )
0080 |                     if producer.flush(10) or not delivery or delivery[0] is not None:
0081 |                         raise RuntimeError("Replay delivery was not confirmed")
0082 |                 count += 1
0083 |                 if count >= limit:
0084 |                     return count
0085 |     return count
0086 | 
0087 | 
0088 | logging.basicConfig(
0089 |     level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
0090 | )
0091 | logger = logging.getLogger("cataclysm.replay")
0092 | 
0093 | 
0094 | def replay_from_parquet(base_dir: str = None, dry_run: bool = False, limit: int = 100):
0095 |     raw_dir = base_dir or os.path.join(
0096 |         os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "raw"
0097 |     )
0098 |     parquet_files = glob.glob(os.path.join(raw_dir, "**", "*.parquet"), recursive=True)
0099 | 
0100 |     logger.info(f"Discovered {len(parquet_files)} Parquet partition files in {raw_dir}")
0101 |     if not parquet_files:
0102 |         logger.warning("No Parquet files found to replay.")
0103 |         return
0104 | 
0105 |     producer = (
0106 |         None
0107 |         if dry_run
0108 |         else Producer(
0109 |             {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, "acks": "all"}
0110 |         )
0111 |     )
0112 |     count = 0
0113 | 
0114 |     if limit < 1:
0115 |         raise ValueError("limit must be positive")
0116 |     for pfile in sorted(parquet_files):
0117 |         if count >= limit:
0118 |             break
0119 |         try:
0120 |             table = pq.ParquetFile(pfile).read()
0121 |             pydict = table.to_pydict()
0122 |             num_rows = len(pydict["event_id"])
0123 |             for i in range(num_rows):
0124 |                 if count >= limit:
0125 |                     break
0126 |                 event = {
0127 |                     "event_id": pydict["event_id"][i],
0128 |                     "timestamp": pydict["timestamp"][i].isoformat(),
0129 |                     "disaster_type": pydict["disaster_type"][i],
0130 |                     "severity": pydict["severity"][i],
0131 |                     "location": pydict["location"][i],
0132 |                     "latitude": pydict["latitude"][i],
0133 |                     "longitude": pydict["longitude"][i],
0134 |                     "text": pydict["text"][i],
0135 |                     "source": pydict["source"][i],
0136 |                     "metadata": {
0137 |                         "rainfall_mm": pydict["rainfall_mm"][i],
0138 |                         "water_level_m": pydict["water_level_m"][i],
0139 |                         "wind_speed_kmh": pydict["wind_speed_kmh"][i],
0140 |                         "temperature_c": pydict["temperature_c"][i],
0141 |                     },
0142 |                 }
0143 | 
0144 |                 if pydict.get("raw_payload") and pydict["raw_payload"][i]:
0145 |                     event = json.loads(pydict["raw_payload"][i])
0146 |                 event.setdefault("metadata", {})["is_replay"] = True
0147 |                 if dry_run:
0148 |                     logger.info(
0149 |                         f"[DRY-RUN] Replaying event: {event['event_id']} ({event['disaster_type']} at {event['location']})"
0150 |                     )
0151 |                 else:
0152 |                     payload = json.dumps(event).encode("utf-8")
0153 |                     key = event["location"].encode("utf-8")
0154 |                     delivery = []
0155 |                     producer.produce(
0156 |                         config.KAFKA_TOPIC,
0157 |                         key=key,
0158 |                         value=payload,
0159 |                         on_delivery=lambda error, message: delivery.append(error),
0160 |                     )
0161 |                     if producer.flush(10) or not delivery or delivery[0] is not None:
0162 |                         raise RuntimeError("Replay delivery was not confirmed")
0163 |                 count += 1
0164 |         except Exception as e:
0165 |             raise RuntimeError(f"Replay failed for {pfile}") from e
0166 | 
0167 |     if producer:
0168 |         producer.flush(timeout=5.0)
0169 |     logger.info(f"Replay complete. Processed {count} events.")
0170 |     return count
0171 | 
0172 | 
0173 | if __name__ == "__main__":
0174 |     parser = argparse.ArgumentParser(
0175 |         description="Replay historical disaster telemetry from Parquet Lake"
0176 |     )
0177 |     parser.add_argument("--dir", default=None, help="Base path to raw parquet lake")
0178 |     parser.add_argument(
0179 |         "--dry-run", action="store_true", help="Print events without sending to Kafka"
0180 |     )
0181 |     parser.add_argument(
0182 |         "--minio",
0183 |         action="store_true",
0184 |         help="Replay from the configured raw MinIO bucket",
0185 |     )
0186 |     parser.add_argument(
0187 |         "--limit", type=int, default=50, help="Maximum events to replay"
0188 |     )
0189 |     args = parser.parse_args()
0190 |     if args.minio:
0191 |         logger.info("Replayed %d rows", replay_from_minio(args.dry_run, args.limit))
0192 |     else:
0193 |         replay_from_parquet(base_dir=args.dir, dry_run=args.dry_run, limit=args.limit)
```
