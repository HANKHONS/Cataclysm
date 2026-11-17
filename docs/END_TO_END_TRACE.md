# One actual event, end to end

This trace uses the successful 10 November 2026 smoke event `DEMO-a1449ee3220b471f`. Its broker acknowledgment was partition 0, offset 81; after a second submission, the operational group's committed offset was 83. One database row, two predictions, raw storage and that exact identity in processed Parquet were observed. These values identify one run; a rerun creates a new ID and offsets.

## 1. Controlled source

`python -m scripts.demo_smoke --spark` constructs a new `DEMO-` identity and a current timezone-aware timestamp. The actual narrative was `Synthetic demo: river flood near a residential road`; source `synthetic_demo`; location `Demo Mumbai`; latitude 19.076; longitude 72.8777; reported hazard flood and severity severe. Metadata included rainfall 100 mm, water level 3 m and `is_synthetic: true`. Missing wind/temperature are missing observations, not measured zero.

```json
{"event_id":"DEMO-a1449ee3220b471f","text":"Synthetic demo: river flood near a residential road","source":"synthetic_demo","metadata":{"rainfall_mm":100,"water_level_m":3,"is_synthetic":true}}
```

This excerpt intentionally omits fields rather than inventing an exact historical timestamp. The reproducible script is the complete fixture. Input validation rejects bad IDs/coordinates/severity/nonfinite metadata before Kafka publication; manual inspection uses `/docs` or a 422 response.

## 2. HTTP boundary and broker

`src/api/main.py::ingest_event` receives a `CrisisEvent`. `model_dump(mode='json')` converts Python datetime to JSON-safe ISO text. `json.dumps(...).encode('utf-8')` creates bytes; the UTF-8 location is the Kafka key. `producer.produce` queues locally. Only `on_delivery` determines success: the handler polls callbacks until success or the 2.5-second deadline.

The observed output was `status=confirmed, partition=0, offset=81`. HTTP 200 does not prove that any consumer has polled the record. A timeout returns 503, but delivery could still occur later; retry the same event ID. Inspect a topic with the Kafka console consumer in the runbook. A source partition's offset orders records within that partition; there is no topic-wide total order.

## 3. Consumer validation and predictions

`src/kafka/consumer.py::run_consumer` calls `consumer.poll(1)`, reads bytes, and validates again through `CrisisEvent.model_validate_json`. Direct Kafka publishers can bypass HTTP, so this second boundary is necessary. A bad record becomes `DLQMessage` with original topic, partition, offset, raw bytes decoded with replacement and validation error. Source commit follows only confirmed DLQ delivery.

For a good record, `src/ml/inference.py::predict_disaster_type` runs the loaded TF-IDF/Logistic Regression pipeline on one string. `predict_severity` builds a row `[rainfall, water_level, wind, temperature]` of shape `[1,4]`, with missing single features imputed to 0/0/0/25. If all are absent it returns unknown. The legacy `prediction_confidence` is the text score only; averaging unrelated model probabilities is not meaningful. See the database's actual stored labels rather than assuming the model agrees with the reported hazard. Inspect `/api/events/DEMO-a1449ee3220b471f/details`.

## 4. Durable operational transaction

Inside `pool.connection()` and `conn.transaction()`, the consumer inserts `crisis_events`, resolves/inserts a location with a uniqueness constraint, inserts the supported enriched payload into `reports`, and writes classifier/severity rows to `predictions`. Dictionary placeholders bind values; they are not string-concatenated SQL. The event and model primary keys prevent duplicate rows. A conflicting existing ID uses first-observation ownership, including its original raw report for archival.

The transaction context commits on successful exit and rolls back on error. The consumer retries database failures five times with bounded exponential sleep, then terminates without source commit. During the observed crash experiment `CRASH-af799af880fe4a8c`, the first process exited 91 after the DB transaction. Its group had offset -1001 (no committed offset). Recovery produced offset 1, one event row and two prediction rows. This is actual process failure evidence, not a mocked exception; its isolated topic is retained.

Verify with a parameterized query through the detail endpoint or SQL shown in `RUNBOOK.md`. A DB row is visible before the raw write/offset commit, so UI visibility alone does not certify all sinks.

## 5. Raw archive and source commit

`src/storage/lake_writer.py::write_batch` flattens telemetry into an explicit Arrow schema and also stores `raw_payload`, the enriched JSON with supported metadata. `pq.write_table(..., compression='snappy')` writes an in-memory buffer. The consumer PUTs an object under `year=YYYY/month=MM/day=DD/disaster_type=flood/event_id=ID.parquet` in `cataclysm-raw`. `scripts.demo_smoke` checked that exact key exists.

Only after both DB and archive success does `consumer.commit(message=msg, asynchronous=False)` advance the group to the next offset. One archive object per identity is simple but creates tiny-file overhead. `scripts.audit_lake` reads file metadata; `demo_smoke --spark` additionally checks actual event IDs, a stronger assertion than merely counting files.

## 6. Independent Spark path

`src/spark/streaming_job.py::main` reads Kafka from its checkpoint offsets, not from PostgreSQL. `clean_events` casts bytes to string, parses a declared Spark schema, validates nonnull identity/time, parses event time, applies a 10-minute watermark and `dropDuplicatesWithinWatermark(['event_id'])`, lowercases/trims narrative and appends processing time.

The processed writer persists snappy-default Spark Parquet under `cataclysm-processed/events-v2`, partitioned by hazard/severity, with checkpoint `/tmp/checkpoints/v2/processed`. The observed smoke run found the exact event among 31 processed files. That file count is historical and will change.

The curated query reads committed processed Parquet using its partition-aware schema, reapplies an event-time watermark, groups into 10-minute windows sliding every 5 minutes plus hazard/severity, and writes final counts to `windowed_incident_counts-v2`. The smoke test does not imply immediate curated output for this event. Append-mode windows finalize only after event time advances past their ends. Do not advance the production watermark into the future simply to force output.

`evidence/spark-semantics.json` instead records an isolated actual Spark test: duplicate identity retained once across checkpoint restart; one too-late event dropped; normalized narrative read back; first event contributed to exactly two finalized windows, each count one. The file-source fixture controls microbatch boundaries; production Kafka integration is separately covered by the smoke test.

## 7. API materialization and UI

`get_recent_events` joins original metadata from `reports`; `queries.provenance` returns synthetic/replay/live/unverified and location accuracy. `/api/overview` aggregates all matching records; it is independent of the loaded 100-row page. Search, hazard, severity, source, time and geographic predicates are shared.

`events_stream` offloads a database query, fingerprints JSON-safe rows, emits changed observations and heartbeats, sleeps two seconds and repeats. Browser `console.js` uses these notifications to debounce a fresh filtered query; it does not append unpersisted form data. A real SSE database event was parsed in `scripts.verify_application`, and browser QA verified USGS labels and filters. The dashboard demo form separately waits for `/details` persistence after its broker acknowledgment.

The map marker is a rendering of the same row used in the register. Its severity color is reported severity. Model predictions appear in an explicitly separate details section. Coordinate accuracy is not invented. External tiles/CDN failure leaves a useful list and explanatory map error.

## 8. Monitoring and a manual reconstruction

API instrumentation exports HTTP counters/histograms. `src/common/exporter.py` measures committed Kafka lag, DB count, object counts and Spark progress-file values. Prometheus samples them; `/api/pipeline` requests actual instant-query results, returning null when unavailable. Current `evidence/application.json` records both scrape targets up, Grafana DB health OK and zero lag at that observation, not a perpetual health guarantee.

Reconstruct the trace: run the smoke command, copy its new ID, query details, inspect SQL rows and predictions, check the raw object and search processed files using the script. Keep event time, acknowledgment time, DB creation time and Spark processing time distinct. If a stage fails, use `FAILURE_SCENARIOS.md` and retain the original ID during retry.
