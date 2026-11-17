# Operational consumer: reason about every commit boundary

Imports separate concerns: confluent-kafka provides transport, Pydantic models validate payloads, psycopg `Jsonb` adapts supported enriched JSON, inference supplies CPU predictions, and the lake writer performs durable archival. Signal handlers set a module-level running flag; they do not interrupt an in-progress transaction. Importing this module installs SIGINT/SIGTERM handlers, so execute it in a main-thread process.

## Startup and source loop

`run_consumer` initializes schema and pool, constructs the raw writer and DLQ producer, and subscribes a configured consumer with `enable.auto.commit=False` and earliest reset only when no valid committed offset exists. The operational Compose group is `crisis-dashboard-group`; independent tests use a different topic/group. Kafka poll is synchronous with a one-second timeout. Empty polls continue; partition-EOF is harmless; other broker errors log and return to polling rather than committing.

## Validation and poison messages

Each message yields bytes, partition and offset. Full CrisisEvent validation precedes model/DB operations, protecting against direct broker publishers. A validation exception constructs a DLQ envelope including source position. Raw bytes decode with replacement so invalid UTF-8 can still be diagnosed; a tombstone becomes an empty diagnostic string. The DLQ producer's callback appends its error to a local list; `flush(10)` both serves callbacks and bounds waiting. Pending records, no callback or callback error terminate the source consumer with its offset uncommitted. Successful DLQ delivery is followed by synchronous source commit. This avoids skipping poison messages whose diagnostics are not stored; it does not prevent duplicate DLQ envelopes after a crash.

## Enrichment and record construction

Text inference returns category and uncalibrated class score. Telemetry inference consumes rainfall/water/wind/temperature. The stored legacy confidence is text score only. The normalized enriched event retains original supported metadata; a separate flattened record matches SQL column placeholders. Input severity/hazard remain observations/heuristics, while prediction columns are separate. Zero scores force heuristic or unavailable per-output provenance even if a loaded artifact exists and a prediction failed. Missing all telemetry is unavailable severity rather than trained certainty.

## Transaction blocks

The event insert is parameterized and first-write wins. The consumer then reads an existing `reports.payload` for that ID; if present, that canonical payload replaces the incoming event only for archive consistency. The location insert/upsert returns an ID, avoiding a second lookup for exact duplicate triples. `reports` stores JSON and original topic/partition/offset once. Two prediction inserts store label, provenance and trained confidence; their composite primary key preserves the first predictions. Existing IDs can cause harmless extra location resolution if a conflicting location was submitted; it does not change the existing report relationship.

Connection and transaction context managers return/rollback resources correctly on exceptions. Success is set only after leaving the transaction. Database failures retry at most five times with exponential bounded delay; after exhaustion the consumer closes and raises before reading/committing later records. This is essential: skipping to a later message and committing it could also acknowledge the failed earlier offset in that partition.

## Archive and offset blocks

Only after database success does the lake writer serialize/PUT canonical raw data. A lake exception closes the consumer and escapes so Compose restarts it. Then `commit(message=msg, asynchronous=False)` records the next offset; only afterward is success logged. A graceful termination leaves the loop after the current processing block and closes the client. Unhandled commit/producer/inference exceptions can terminate the process; uncommitted work replays under the restart policy and retention assumptions.

Proof obligation: no branch may commit an ordinary source record before DB and raw success; the poison branch has its own confirmed-DLQ success condition. The real crash script exits at `write_batch`, after the DB transaction, then restarts this same loop on a dedicated group. It verifies one row and a next offset of one, not just a mock call sequence.

The loop is serial and creates a raw Parquet object per message. Throughput cannot be inferred from API acknowledgment rate. Adding batching or more consumers requires preserving partition-specific commit ordering and partial-batch failure handling. Test DB exhaustion, DLQ delivery failure, crash replay, duplicate facts and raw canonical content before such changes.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `src/kafka/consumer.py`

[Open source](../../src/kafka/consumer.py) · SHA-256 `e9b363af56afc8e3facffb926df81ca1f6e0cac6d41b8ce9a8705f4aefc9b037`

Dependencies: `import time`; `import signal`; `import logging`; `from confluent_kafka import Consumer, Producer, KafkaError`; `from src.common.config import config`; `from src.common.db import get_pool, init_db`; `from src.common.models import CrisisEvent, DLQMessage`; `from src.ml.inference import predict_disaster_type, predict_severity`; `from src.ml.inference import model_status`; `from psycopg.types.json import Jsonb`; `from src.storage.lake_writer import ParquetLakeWriter`

- `handle_sigterm`: lines 21–26. Read its control flow with the module guide above.

- `run_consumer`: lines 33–257. Read its control flow with the module guide above.


```text
0001 | import time
0002 | import signal
0003 | import logging
0004 | from confluent_kafka import Consumer, Producer, KafkaError
0005 | from src.common.config import config
0006 | from src.common.db import get_pool, init_db
0007 | from src.common.models import CrisisEvent, DLQMessage
0008 | from src.ml.inference import predict_disaster_type, predict_severity
0009 | from src.ml.inference import model_status
0010 | from psycopg.types.json import Jsonb
0011 | from src.storage.lake_writer import ParquetLakeWriter
0012 | 
0013 | logging.basicConfig(
0014 |     level=logging.INFO, format="%(asctime)s [%(levelname)s] (Consumer) %(message)s"
0015 | )
0016 | logger = logging.getLogger("cataclysm.consumer")
0017 | 
0018 | running = True
0019 | 
0020 | 
0021 | def handle_sigterm(signum, frame):
0022 |     global running
0023 |     logger.info(
0024 |         "Received termination signal. Shutting down consumer loop gracefully..."
0025 |     )
0026 |     running = False
0027 | 
0028 | 
0029 | signal.signal(signal.SIGINT, handle_sigterm)
0030 | signal.signal(signal.SIGTERM, handle_sigterm)
0031 | 
0032 | 
0033 | def run_consumer():
0034 |     global running
0035 |     logger.info(
0036 |         f"Connecting to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS} with group {config.KAFKA_GROUP_ID}"
0037 |     )
0038 | 
0039 |     # 1. Initialize DB schema
0040 |     init_db()
0041 |     pool = get_pool()
0042 |     lake_writer = ParquetLakeWriter()
0043 | 
0044 |     # 2. Producer for Dead Letter Queue
0045 |     dlq_producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
0046 | 
0047 |     # 3. Kafka Consumer Configuration
0048 |     conf = {
0049 |         "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
0050 |         "group.id": config.KAFKA_GROUP_ID,
0051 |         "auto.offset.reset": "earliest",
0052 |         "enable.auto.commit": False,  # Manual commit for At-Least-Once guarantee
0053 |     }
0054 | 
0055 |     consumer = Consumer(conf)
0056 |     consumer.subscribe([config.KAFKA_TOPIC])
0057 |     logger.info(f"Successfully subscribed to topic: '{config.KAFKA_TOPIC}'")
0058 | 
0059 |     INSERT_SQL = """
0060 |         INSERT INTO crisis_events (
0061 |             event_id, timestamp, disaster_type, severity, location,
0062 |             latitude, longitude, text, source,
0063 |             rainfall_mm, water_level_m, wind_speed_kmh, temperature_c,
0064 |             predicted_disaster_type, predicted_severity, prediction_confidence
0065 |         ) VALUES (
0066 |             %(event_id)s, %(timestamp)s, %(disaster_type)s, %(severity)s, %(location)s,
0067 |             %(latitude)s, %(longitude)s, %(text)s, %(source)s,
0068 |             %(rainfall_mm)s, %(water_level_m)s, %(wind_speed_kmh)s, %(temperature_c)s,
0069 |             %(predicted_disaster_type)s, %(predicted_severity)s, %(prediction_confidence)s
0070 |         )
0071 |         ON CONFLICT (event_id) DO NOTHING;
0072 |     """
0073 | 
0074 |     while running:
0075 |         msg = consumer.poll(timeout=1.0)
0076 |         if msg is None:
0077 |             continue
0078 |         if msg.error():
0079 |             if msg.error().code() == KafkaError._PARTITION_EOF:
0080 |                 continue
0081 |             else:
0082 |                 logger.error(f"Consumer error: {msg.error()}")
0083 |                 continue
0084 | 
0085 |         raw_bytes = msg.value()
0086 |         partition = msg.partition()
0087 |         offset = msg.offset()
0088 | 
0089 |         # Parse JSON
0090 |         try:
0091 |             event_data = CrisisEvent.model_validate_json(raw_bytes).model_dump(
0092 |                 mode="json"
0093 |             )
0094 |         except Exception as e:
0095 |             # Route to Dead Letter Queue
0096 |             logger.warning(
0097 |                 f"Malformed message at partition {partition}, offset {offset}: {e}. Diverting to DLQ."
0098 |             )
0099 |             dlq_msg = DLQMessage(
0100 |                 raw_payload=(raw_bytes or b"").decode("utf-8", errors="replace"),
0101 |                 error_type=type(e).__name__,
0102 |                 error_message=str(e),
0103 |                 source_topic=config.KAFKA_TOPIC,
0104 |                 source_partition=partition,
0105 |                 source_offset=offset,
0106 |             )
0107 |             delivery = []
0108 |             dlq_producer.produce(
0109 |                 config.KAFKA_DLQ_TOPIC,
0110 |                 value=dlq_msg.model_dump_json().encode("utf-8"),
0111 |                 on_delivery=lambda error, message: delivery.append(error),
0112 |             )
0113 |             pending = dlq_producer.flush(timeout=10.0)
0114 |             if pending or not delivery or delivery[0] is not None:
0115 |                 consumer.close()
0116 |                 raise RuntimeError(
0117 |                     "DLQ delivery failed; source offset remains uncommitted"
0118 |                 )
0119 |             consumer.commit(message=msg, asynchronous=False)
0120 |             continue
0121 | 
0122 |         # ML Enrichment
0123 |         meta = event_data.get("metadata", {}) or {}
0124 |         pred_type, type_conf = predict_disaster_type(event_data.get("text", ""))
0125 |         pred_sev, sev_conf = predict_severity(
0126 |             rainfall_mm=meta.get("rainfall_mm"),
0127 |             water_level_m=meta.get("water_level_m"),
0128 |             wind_speed_kmh=meta.get("wind_speed_kmh"),
0129 |             temperature_c=meta.get("temperature_c"),
0130 |         )
0131 | 
0132 |         # Preserve the legacy field for the classifier score only. Averaging
0133 |         # probabilities from unrelated classifiers has no joint-probability meaning.
0134 |         overall_conf = round(type_conf, 3)
0135 |         event_data["predicted_disaster_type"] = pred_type
0136 |         event_data["predicted_severity"] = pred_sev
0137 |         event_data["prediction_confidence"] = overall_conf
0138 | 
0139 |         record = {
0140 |             "event_id": event_data["event_id"],
0141 |             "timestamp": event_data["timestamp"],
0142 |             "disaster_type": event_data.get("disaster_type", pred_type),
0143 |             "severity": event_data.get("severity", pred_sev),
0144 |             "location": event_data.get("location", "Unknown"),
0145 |             "latitude": float(event_data.get("latitude", 0.0)),
0146 |             "longitude": float(event_data.get("longitude", 0.0)),
0147 |             "text": event_data.get("text", ""),
0148 |             "source": event_data.get("source", "system"),
0149 |             "rainfall_mm": meta.get("rainfall_mm"),
0150 |             "water_level_m": meta.get("water_level_m"),
0151 |             "wind_speed_kmh": meta.get("wind_speed_kmh"),
0152 |             "temperature_c": meta.get("temperature_c"),
0153 |             "predicted_disaster_type": pred_type,
0154 |             "predicted_severity": pred_sev,
0155 |             "prediction_confidence": overall_conf,
0156 |         }
0157 | 
0158 |         # Bounded retry loop for database persistence
0159 |         max_db_retries = 5
0160 |         db_success = False
0161 | 
0162 |         for attempt in range(1, max_db_retries + 1):
0163 |             try:
0164 |                 with pool.connection() as conn:
0165 |                     with conn.transaction():
0166 |                         with conn.cursor() as cur:
0167 |                             cur.execute(INSERT_SQL, record)
0168 |                             cur.execute(
0169 |                                 "SELECT payload FROM reports WHERE event_id=%s",
0170 |                                 (record["event_id"],),
0171 |                             )
0172 |                             original_report = cur.fetchone()
0173 |                             if original_report is not None:
0174 |                                 # First accepted identity owns immutable observed facts.
0175 |                                 # A retry must not overwrite its raw archive with a
0176 |                                 # different narrative or a later model version.
0177 |                                 event_data = original_report["payload"]
0178 |                             cur.execute(
0179 |                                 "INSERT INTO locations(name,latitude,longitude) VALUES (%s,%s,%s) "
0180 |                                 "ON CONFLICT(name,latitude,longitude) DO UPDATE SET name=EXCLUDED.name RETURNING id",
0181 |                                 (
0182 |                                     record["location"],
0183 |                                     record["latitude"],
0184 |                                     record["longitude"],
0185 |                                 ),
0186 |                             )
0187 |                             location_id = cur.fetchone()["id"]
0188 |                             cur.execute(
0189 |                                 "INSERT INTO reports(event_id,location_id,payload,source_topic,source_partition,source_offset) "
0190 |                                 "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(event_id) DO NOTHING",
0191 |                                 (
0192 |                                     record["event_id"],
0193 |                                     location_id,
0194 |                                     Jsonb(event_data),
0195 |                                     msg.topic(),
0196 |                                     partition,
0197 |                                     offset,
0198 |                                 ),
0199 |                             )
0200 |                             status = model_status()
0201 |                             if type_conf == 0:
0202 |                                 status["classifier"] = "heuristic"
0203 |                             if sev_conf == 0:
0204 |                                 status["severity"] = (
0205 |                                     "unavailable"
0206 |                                     if pred_sev == "unknown"
0207 |                                     else "heuristic"
0208 |                                 )
0209 |                             for name, label, confidence, provenance in (
0210 |                                 (
0211 |                                     "classifier",
0212 |                                     pred_type,
0213 |                                     type_conf,
0214 |                                     status["classifier"],
0215 |                                 ),
0216 |                                 ("severity", pred_sev, sev_conf, status["severity"]),
0217 |                             ):
0218 |                                 cur.execute(
0219 |                                     "INSERT INTO predictions(event_id,model_name,predicted_label,confidence,provenance) "
0220 |                                     "VALUES (%s,%s,%s,%s,%s) ON CONFLICT(event_id,model_name) DO NOTHING",
0221 |                                     (
0222 |                                         record["event_id"],
0223 |                                         name,
0224 |                                         label,
0225 |                                         confidence if provenance == "trained" else None,
0226 |                                         provenance,
0227 |                                     ),
0228 |                                 )
0229 |                 db_success = True
0230 |                 break
0231 |             except Exception as dbe:
0232 |                 logger.warning(
0233 |                     f"Database write attempt {attempt}/{max_db_retries} failed: {dbe}"
0234 |                 )
0235 |                 time.sleep(min(0.5 * (2 ** (attempt - 1)), 5.0))
0236 | 
0237 |         if db_success:
0238 |             try:
0239 |                 lake_writer.write_batch([event_data])
0240 |             except Exception:
0241 |                 consumer.close()
0242 |                 raise
0243 |             # Commit offset strictly after successful persistence
0244 |             consumer.commit(message=msg, asynchronous=False)
0245 |             logger.info(
0246 |                 f"Persisted event {record['event_id']} [{record['disaster_type']}] at {record['location']} (Offset: {offset})"
0247 |             )
0248 | 
0249 |         else:
0250 |             consumer.close()
0251 |             raise RuntimeError(
0252 |                 "Database retries exhausted; source offset remains uncommitted"
0253 |             )
0254 | 
0255 |     # Graceful shutdown
0256 |     consumer.close()
0257 |     logger.info("Consumer closed successfully.")
0258 | 
0259 | 
0260 | if __name__ == "__main__":
0261 |     run_consumer()
```
