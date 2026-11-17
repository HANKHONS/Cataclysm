# Failure scenarios: expected versus observed

Evidence is scoped to the exact tested boundary. A mocked regression proves branch behavior, not infrastructure availability. A process restart proves selected persisted state, not recovery from physical disk loss. All 10 November outage checks restored services; no volume or record was deleted.

| Scenario | Expected behavior and risk | Observed evidence | Recovery |
|---|---|---|---|
| Broker unavailable | POST waits for callback/deadline then 503; delivery may be uncertain. Consumer/Spark retry or fail. | Actual broker stop: ingestion 503; same ID retried and persisted after restoration (`evidence/dependency-outages.json`). | Start broker, wait `/ready`, retry same ID. Inspect lag and Spark. |
| Consumer crash before DB commit | Transaction rolls back; uncommitted source replays. | Exhausted DB failure/no-offset advancement covered by mocked unit test. Precise kill inside an active DB transaction was not separately executed. | Restart consumer; compare ID and transaction state. |
| Crash after DB commit before offset | Database row already visible; source replays. Archive/prediction retries must be duplicate-safe. | Actual isolated process exit 91 after DB transaction; offset -1001 before recovery, 1 after; event rows 1 before/after, predictions 2 (`evidence/crash-replay.json`). | Restart normal consumer on same group. Retained verification topic preserves evidence. |
| Crash after successful commit | Group resumes at next offset; completed record should not require replay. | Ordinary committed duplicate and durable-service restart evidence exists; exact post-commit kill instruction was not separately instrumented. | Resume same group; verify existing IDs and a fresh event. |
| PostgreSQL unavailable | GET 503; readiness 503. Consumer never commits unpersisted records. | Actual DB stop returned both 503s; readiness recovered on restart. Mocked consumer test proves no source commit after exhausted writes. | Start DB; consumer's on-failure restart retries uncommitted source. Pool acquisition bounded to 3 s. |
| MinIO unavailable | DB may already be visible; raw archival exception terminates consumer before commit. | A mocked archive-write failure proves no source commit/read-past. An actual MinIO outage was not separately forced this run. | Restore MinIO, restart consumer, same-ID replay fills raw archive. Do not mistake DB visibility for full completion. |
| Poison / invalid JSON | API schema 422 before publish; direct Kafka invalid bytes go to confirmed DLQ before source commit. | Live API 422 and malformed Kafka marker in real DLQ; failed-DLQ no-commit unit test. | Inspect original payload and validation error, correct schema, reissue with deliberate identity. DLQ can have duplicates. |
| Duplicate / conflicting ID | One stored observation; first facts/predictions own ID; canonical report supplies duplicate archive. | Live `verify_application` consumed a conflicting second narrative/severity and retained original facts. Smoke verified duplicate commit and one row. | Reuse ID for retry; assign a deliberate revision identity for corrected observations. |
| Spark restart | Checkpoint restores offsets/state; no second driver owns that checkpoint. | Isolated actual checkpoint restart retained dedup state (`spark-semantics.json`); production path exact-ID smoke passed. | Restore same query/version/checkpoint; inspect logs and new event readback. |
| Late event | PostgreSQL/raw can retain it while processed dedup drops it beyond watermark; curated counts differ legitimately. | Isolated controlled fixture dropped `too-late`, with dropped-row metric 1; readback excluded ID. | For historical analytics, use separately versioned batch/replay outputs and checkpoints. Never push future timestamps into production to force finalization. |
| Idle stream | Watermark need not advance with wall clock; latest windows remain unfinalized. | Architecture/code inspected; isolated test explicitly advanced event time to finalize two windows. | Wait for suitable event-time progression; distinguish idle from crashed through health/logs. |
| Missing weather key | Weather adapter raises before network request; no live weather claim. | Offline credential test passed. Live OpenWeather remains blocked by missing configured key. | Privately configure `.env`, refresh API container environment and run one manual request. |
| Source network / provider error | Bounded timeout; GET retry/backoff for transient statuses; failed one-shot exits nonzero. | Adapters/unit tests and successful current USGS fetch; a real provider network cut was not separately forced. | Wait/backoff, inspect provider status, preserve normalized ID on POST retry. URLs with API keys must never be logged. |
| Dashboard disconnect | EventSource retries automatically; status changes; periodic DB refresh reconciles current filtered view. | Browser observed reconnecting during API restart and connected after correction; live SSE data parsed by verifier. | Restore API/network; reload if assets changed. Bounded SSE has no durable replay cursor. |
| Map tiles or CDN unavailable | Tile fallback attempted; list/details remain usable; library absence explains missing map. | Working primary tiles visually inspected. Fallback and missing-library branches are implemented but were not forced offline in browser. | Restore network or vendor pinned dependencies for an offline demo. |
| Resource exhaustion | JVM/container may die; policy restarts driver/consumer, but recovery can lag and lose data past retention. | Historical ten-minute stability sample retained; current Docker point resources recorded. No intentional OOM test this run. | Reduce competing workloads, inspect memory/lag, preserve checkpoints; repeat targeted readback. |
| Host/disk loss | Single-host volumes provide no redundancy; data may be lost. | Not tested; no independent backup restoration evidence. | Backups and multi-host replicated services required before operational use. |

## Reproduce the executed scenarios

```powershell
docker compose exec -T fastapi python -m scripts.verify_crash_replay
.\venv\Scripts\python.exe scripts/verify_dependency_outage.py
docker compose exec -T fastapi python -m scripts.check_dlq
docker compose exec -T spark-master /opt/spark/bin/spark-submit --master local[2] /opt/spark/app/scripts/verify_spark_semantics.py
docker compose exec -T fastapi python -m scripts.verify_application
```

The dependency-outage script runs on the host and intentionally interrupts DB/broker briefly; it restores each in `finally` and waits for readiness. Do not run it during someone else's active demonstration. The crash script runs a separate actual consumer process on its own topic/group; it does not reset production offsets. The Spark script writes separate test files/state under a unique work prefix. Run smoke after recovery to establish fresh writes, not merely old-row visibility.

## What the recovery argument does and does not prove

With immutable first-observation identity, transactional DB writes, successful archive writes and commit-after-write, replay is safe for operational row cardinality. This assumes retained Kafka data, available durable storage, a compatible consumer schema/model runtime, and stable IDs. It does not prove exactly-once object PUTs, exactly-once DLQ entries, cross-sink simultaneity, authenticated sources, permanent Spark deduplication, or physical-host availability. These limits are part of the design, not hidden test failures.
