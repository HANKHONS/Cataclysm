# Implementation audit

Audit begun 10 November 2026, before implementation. Verification statuses below
describe the baseline; final evidence is recorded in VERIFICATION.md.

| Feature / source | Expected and existing behavior | Baseline evidence | Defect / improvement | Status |
|---|---|---|---|---|
| Ingestion `src/ingestion/clients.py`, `poll_sources.py` | USGS + credentialed OpenWeather normalize events; simulated generator | Adapter/unit tests pass | Source retry/backoff and malformed feature isolation need review; live credentials unavailable | partial |
| Schema `src/common/models.py` | Identity, bounded coordinates, timestamps, telemetry | Schema tests pass | Ambiguous timezone, arbitrary severity, nonfinite telemetry; provenance incomplete | partial |
| API `src/api/main.py` | Broker-confirmed POST, health, DB query, SSE | Callback polling unit test passes | No pagination/time/search/geographic scope or aggregate overview; metadata absent | partial |
| Kafka `compose.yaml`, `src/kafka/consumer.py` | KRaft, manual post-sink commits, confirmed DLQ | Failure unit tests pass | Current live crash-after-write proof pending; RF=1 has no broker redundancy | partial |
| PostgreSQL `src/common/db.py`, consumer | Transactional event/report/prediction writes, PK idempotency, pool | SQL reviewed; runtime pending | Collision policy and analytics plans need verification | partial |
| Raw lake `src/storage/lake_writer.py` | Event-keyed snappy Parquet; original enriched JSON retained | Fidelity unit test passes | Objects are replaceable, not immutable; tiny files are expensive | partial |
| Replay `src/data/replay.py` | Re-publish original IDs through Kafka | MinIO path checks acknowledgments | Local path loses metadata, incorrect row limit, unverified deliveries | partial |
| Spark `src/spark/streaming_job.py` | Watermarked bounded dedup; committed Parquet to sliding counts | Code + historical ledger | Current readback, late/drop behavior and recovery require live tests | partial |
| ML `src/ml/*` | TF-IDF LR, RF, hourly Isolation Forest | ML/anomaly unit tests pass | Small text holdout; circular synthetic severity; uncalibrated probabilities | partial |
| Dashboard `src/api/static/index.html` | Leaflet, persisted feed, SSE, simulation | Source inspected | Dark generic styling; no full-period counts, freshness or source provenance | partial |
| Monitoring `src/common/exporter.py`, `monitoring/*` | HTTP, lag, lake and Spark gauges | Source and provisioned configuration exist | Current scrape / stale-progress interpretation pending | partial |
| Azure `infra/azure/*` | Deployment preparation separate from local system | Historical compile claim only | No current cloud validation; paid deployment excluded | blocked |
| Learning documentation `docs/*` | Existing demo, ML, experiments and verification | Existing documents read | Detailed trace, walkthrough, concepts, viva, handbook missing | partial |

## Actual ownership and guarantees

FastAPI publishes keyed by location. The operational consumer enriches an event,
writes PostgreSQL in one transaction, archives raw Parquet, then commits its Kafka
offset. Spark independently consumes Kafka and writes processed Parquet; a second
query aggregates that committed dataset. Spark does **not** write PostgreSQL or run
the deployed classifiers. The API reads PostgreSQL; SSE polls it every two seconds.

This is at-least-once processing with idempotent operational identity. PostgreSQL,
MinIO and Kafka offsets do not share a transaction. Spark deduplication has bounded
event-time state, unlike the permanent PostgreSQL primary key. RF=1 `acks=all` means
one broker acknowledgment, not replication or end-to-end exactly once.

## Preservation and specification conflicts

KRaft and v2 checkpoint/dataset layout are retained. The supplied plan filename
does not exist in this checkout. Existing Git modifications and pre-existing
deleted `.gsd` files belong to the user and are not restored or reorganized.
The requested acceptance criteria override old unchecked roadmap assumptions.

## Final disposition — 10 November 2026

The confirmed replay, schema, provenance, aggregation and dashboard defects above were corrected. Final offline suite: 36 passed. Actual operational/lake/ML/processed-event smoke, subprocess crash replay (including final-code rerun), conflicting-ID raw readback, DLQ, DB/broker outages, isolated Spark restart/lateness/exact windows and monitoring checks passed. Dashboard and 24-chapter handbook were inspected on desktop/mobile, with real screenshots. Source-synchronized explanations cover 13 module groups, plus complete architecture/trace/formulas/runbook/failure/experiments/viva/report documents.

Ingestion remains partial only for credential-blocked live weather; scientific ML validity remains partial; Azure deployment is blocked/deferred. Bounded SSE, single-host availability, declared unauthenticated sources, small-file throughput and unexecuted failure/endurance cases remain documented limits. The complete feature/evidence table and precise commands are in VERIFICATION.md and RUNBOOK.md; this baseline audit is preserved rather than retroactively relabelled as all verified.
