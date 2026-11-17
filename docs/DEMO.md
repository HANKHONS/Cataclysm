# 16 November 2026 demo runbook

Target 20 minutes, with 10 minutes reserved for questions. All demonstration services run locally. Azure deployment and weather polling are disabled until explicitly configured. This is a single-machine academic prototype; one broker and one object-store node are not high availability.

## Before the demo

1. Start Docker Desktop. Allocate approximately 8 GB to Docker if the host has 16 GB RAM; close memory-heavy apps. Verify the actual working set before changing Docker allocation.
2. Use the existing ignored `.env`; it contains random local credentials. Do not display it or `docker compose config` output on the projector. Use `docker compose config --quiet` for validation.
3. Run `docker compose up -d --build`. The first MinIO build compiles pinned upstream source and can take several minutes. Build and download everything before demo day.
4. Train once with `docker compose exec -T fastapi python -m src.ml.train`, then restart the API and consumer so both reload the artifacts: `docker compose restart fastapi consumer`.
5. Run `docker compose exec -T fastapi python scripts/demo_smoke.py --spark` and `docker compose exec -T fastapi python scripts/audit_lake.py`.
6. Check the dashboard, Spark master, Prometheus targets, and Grafana before presenting. Internet is needed for map tiles/CDN assets and real-source requests. Prepare actual screenshots or a recording from a successful rehearsal for an offline contingency.

## Presentation sequence

| Minutes | Action and evidence |
|---|---|
| 0–3 | Explain ingestion, Kafka partition ordering, independent consumer groups, operational DB versus lake |
| 3–6 | Dashboard at http://localhost:8000; dispatch a synthetic event and show its Kafka acknowledgement |
| 6–9 | Show `demo_smoke.py --spark`, DB records and the exact event in processed Parquet |
| 9–12 | Show MinIO at http://localhost:19001 and `audit_lake.py`; explain watermark-delayed curated output |
| 12–14 | Show stored predictions, `models/evaluation.json`, and limitations of the training data |
| 14–16 | Run `docker compose exec -T fastapi python -m src.ingestion.poll_sources --usgs` and `--nasa`; identify real source labels on map |
| 16–18 | Show http://localhost:9090/targets and Grafana at http://localhost:3000 |
| 18–20 | Show measured benchmarks and restart recovery evidence; state unresolved external validation |

## Useful commands

```powershell
docker compose ps
docker compose exec -T db psql -U cataclysm -d cataclysm -c "SELECT event_id,source,predicted_disaster_type,predicted_severity FROM crisis_events ORDER BY created_at DESC LIMIT 10"
docker compose exec -T db psql -U cataclysm -d cataclysm -c "SELECT model_name,provenance,COUNT(*) FROM predictions GROUP BY model_name,provenance"
docker compose exec -T fastapi python scripts/check_dlq.py
docker compose exec -T fastapi python -m src.data.replay --minio --dry-run --limit 2
docker compose exec -T fastapi python -m src.ml.detect_anomalies
.\venv\Scripts\python.exe scripts/recovery_check.py
```

Recovery briefly interrupts services. Demonstrate it only after a rehearsal, and follow it with a new smoke test. Never run `docker compose down -v`: it deletes the demo's named-volume data.

For the optional weather demonstration, set `OPENWEATHER_API_KEY` in the ignored `.env`, run `docker compose up -d fastapi` to refresh its environment, then make one manual call with `docker compose exec -T fastapi python -m src.ingestion.poll_sources --weather 19.076 72.8777`. Background polling is off. Do not display the key or resolved environment on the projector.

## Claims to state accurately

- API acknowledgement establishes Kafka delivery; a persisted dashboard event comes from PostgreSQL.
- Offsets commit after the operational transaction and raw archival. These independent sinks are at-least-once with idempotent event IDs, not one distributed transaction.
- Spark drops events older than its 10-minute event-time watermark. Curated 10-minute windows slide every 5 minutes and finalize only when the watermark advances. Raw/operational data can contain more records than processed/curated data.
- Both queries use 10-second triggers. Allow time for the processed and curated stages rather than expecting immediate lake updates. The driver uses a 1536 MB Java heap with bounded retained UI/query history; shared checkpoints support executor recreation.
- One machine with several workers measures process-level parallelism and contention, not scaling across independent machines.
- Text classification has a small corpus and weak measured generalization. Severity evaluation is synthetic. Neither should support real emergency decisions.
- Anomaly detection requires at least 20 historical completed hours plus a scoring hour. It uses Isolation Forest plus an explicit training-maximum guard; insufficient history is shown rather than replaced with fabricated alerts.
- Spark's measured Python-driver CPU/RSS excludes executor/JVM resources; do not call it total cluster memory.

## Shutdown

Use `docker compose stop` after practice to release CPU/RAM while preserving data. Local Docker consumes no Azure credits. Keep weather polling manual and off by default.
