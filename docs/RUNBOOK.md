# Windows / PowerShell runbook

Run from `E:\projects\catalcysm`. Start Docker Desktop and wait for its Linux engine. Existing `.env`, model artifacts, volumes, Kafka offsets and v2 datasets belong to the project: preserve them. No command below deploys cloud resources.

## Setup and start

```powershell
Set-Location E:\projects\catalcysm
docker version
# Fresh checkout only, if .env does not already exist:
python scripts/prepare_env.py
# Optional local environment; the existing venv can be reused:
python -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements-dev.txt
# Validate without printing resolved secrets:
docker compose config --quiet
docker compose up -d --build
docker compose ps
Invoke-RestMethod http://127.0.0.1:8000/ready
```

`Set-Location` selects the checkout. `docker version` must show a server, not only a client. `prepare_env.py` generates local random credentials; it does not supply a weather key. Do not print `.env` or resolved Compose environment output. Do not overwrite an existing environment file. The local dev requirements allow ranges; the container builds with the exact `requirements.lock.txt`. The first MinIO source compilation and Spark dependency resolution need Internet/time. `-d` detaches containers; `--build` rebuilds changed images. Source is bind-mounted, but Python process code/model changes still require service restart. `--quiet` checks Compose without disclosing interpolated credentials.

`/live` reports the API process; `/ready` requires DB and broker and returns 503 otherwise. `/health` preserves its descriptive 200 response with healthy/degraded fields. DB and broker have native health checks; the API now has a readiness-based Compose health check. Not every auxiliary container has a Docker health check: use the probes below.

Open dashboard `http://127.0.0.1:8000`, API contracts `/docs`, handbook `/handbook/`, Spark master `http://127.0.0.1:8080`, worker `:8081`, MinIO console `:19001`, Prometheus `:9090/targets`, Grafana `:3000`. Console passwords are in local `.env`; never display them during a presentation.

## Submit and trace synthetic data

```powershell
.\venv\Scripts\python.exe src/data/generator.py --count 20 --delay 0.5 --url http://127.0.0.1:8000/events
docker compose exec -T fastapi python -m scripts.demo_smoke --spark
docker compose exec -T fastapi python -m scripts.verify_application
docker compose exec -T fastapi python -m scripts.audit_lake
```

`--count` bounds records; `--delay` is inter-request seconds. The generator is explicitly synthetic, including simulated telemetry. `exec -T` runs inside an existing container without allocating a pseudo-terminal, suitable for scripts and captured JSON. `-m` loads the project module with package imports. Smoke creates a fresh identity, waits for operational/two-prediction/raw sinks, resubmits it and confirms committed duplicate handling. `--spark` additionally searches actual processed Parquet for that ID. Allow up to 90 seconds for Spark readback. It does not assert immediate final curated windows. The application verifier records API, identity, SSE, monitoring and SQL-plan evidence.

The dashboard's **Test event** form follows the same POST boundary. It shows broker acknowledgment separately from database persistence. Filter by its ID and inspect model provenance. Coordinates (including zero) are valid; no measured location accuracy is implied.

## Real provider observations

```powershell
docker compose exec -T fastapi python -m src.ingestion.poll_sources --usgs
# Optional one-day backfill; old data can be beyond the Spark watermark:
docker compose exec -T fastapi python -m src.ingestion.poll_sources --usgs-day
# After privately setting OPENWEATHER_API_KEY in .env:
docker compose up -d fastapi
docker compose exec -T fastapi python -m src.ingestion.poll_sources --weather 19.076 72.8777
```

USGS needs no key. The poller fetches its hour/day GeoJSON feed, keeps provider ID/time/magnitude/depth/URL and posts normalized events through FastAPI. Malformed features are skipped independently with warnings. HTTPS GET retries use exponential backoff for transient statuses; POST acknowledgment remains uncertain on network timeout, so preserve IDs. Weather requests specify metric units; wind m/s is converted to km/h. The weather key stays in an ignored environment file and is not placed in command arguments. Source-specific polling is manual by default; `--interval 600` repeats every ten minutes. A source exception fails a one-shot invocation; configure and verify credentials rather than claiming an observation was fetched.

## Kafka and operational database

```powershell
docker compose exec -T broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 --describe --topic crisis-events
docker compose exec -T broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker:9092 --describe --group crisis-dashboard-group
docker compose exec -T broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker:9092 --topic crisis-events --from-beginning --max-messages 1
docker compose exec -T db psql -U cataclysm -d cataclysm -c "SELECT event_id,source,timestamp,predicted_disaster_type,predicted_severity FROM crisis_events ORDER BY created_at DESC LIMIT 10"
docker compose exec -T db psql -U cataclysm -d cataclysm -c "SELECT model_name,provenance,COUNT(*) FROM predictions GROUP BY model_name,provenance"
```

The internal listener is `broker:9092`; host Python uses `localhost:9094`. The group command compares log end offsets with committed next offsets; lag is measured per partition. `--from-beginning --max-messages 1` is a bounded independent inspection, not an operational group reset. `psql -c` executes one query; `-U` and `-d` select the local role/database. Never reset operational offsets as a troubleshooting shortcut. Do not conflate model predictions and reported severity.

## Spark and lake

```powershell
docker compose logs --tail 80 spark-streaming spark-worker
docker compose exec -T spark-master /opt/spark/bin/spark-submit --master local[2] /opt/spark/app/scripts/verify_spark_semantics.py
docker compose exec -T fastapi python -m src.data.replay --minio --dry-run --limit 2
docker compose exec -T fastapi python -m src.data.replay --minio --limit 2
```

The production driver starts automatically; do not launch another instance against `/tmp/checkpoints/v2`. `--tail` bounds logs. The semantics verifier is independent, uses a unique fixture/checkpoint/output directory under `work/`, and checks actual restart/dedup/watermark/windows. `local[2]` uses two local executor threads for that test, not the production worker cluster. Replay dry-run reads and restores up to two rows without publishing. Actual replay preserves original identity/event time and sets `metadata.is_replay`; old IDs retain their first stored observation. Replaying old history may produce operational/raw data but be too late for current Spark state. Restore/rebuild derived history with a separately versioned dataset/checkpoint, never by deleting the existing checkpoint.

## ML, monitoring and experiments

```powershell
docker compose exec -T fastapi python -m src.ml.train
docker compose restart fastapi consumer
docker compose exec -T fastapi python -m src.ml.detect_anomalies
Invoke-RestMethod http://127.0.0.1:8000/api/models
Invoke-RestMethod http://127.0.0.1:8000/api/pipeline
docker compose exec -T prometheus promtool check config /etc/prometheus/prometheus.yml
docker compose exec -T fastapi python -m scripts.measure_current
docker stats --no-stream
```

Training saves preprocessing with each model, per-class metrics, confusion matrices, majority baselines, hashes and versioned artifacts. Previous artifacts are copied before replacement. Services cache models once; restart to load newly trained files. The anomaly job needs at least 20 completed training hours plus one scoring hour and nonconstant counts. It reports insufficient history instead of fabricating alerts. `promtool` checks configuration/rules; scrape health and actual sample values require live queries. `docker stats --no-stream` is a point sample, not a peak measurement or endurance test. The measurement runner adds 300 labelled test observations and records three bounded HTTP-to-broker runs, not maximum sustainable system capacity.

## Tests, recovery, shutdown

```powershell
.\venv\Scripts\python.exe -m pytest tests -q --basetemp=work/pytest-manual
.\venv\Scripts\python.exe -m ruff check src scripts benchmarks tests --select F
docker compose exec -T fastapi python -m scripts.verify_crash_replay
.\venv\Scripts\python.exe scripts/recovery_check.py
docker compose exec -T fastapi python -m scripts.demo_smoke --spark
docker compose stop
```

Pytest's `--basetemp` names a disposable directory which pytest manages; choose a dedicated new path under `work`, never a data/checkpoint directory. On this Windows environment the default restricted temporary directory failed, so an explicit workspace directory was used. Ruff `F` catches undefined/unused names; it is not proof of behavior. The crash verifier uses a dedicated retained Kafka topic/group and real consumer subprocesses. Recovery restarts durable services and briefly interrupts the UI, then confirms existing IDs remain; always follow with a fresh smoke event to prove new writes recover. `stop` preserves named volumes. Do not use `down -v`, prune volumes or clear buckets.

## Troubleshooting decisions

Missing Docker pipe: start Docker Desktop and wait; permission denied: run through an authorized Docker session. API 503: inspect DB/broker readiness and container logs, keep the original event ID on retry. Increasing lag: inspect consumer errors and MinIO availability, not just broker health. DB visible but lake absent: offset must remain uncommitted and consumer may restart. Spark no new curated counts: inspect watermark/progress before declaring failure; idle event time does not finalize windows. Checkpoint ownership error: stop the duplicate driver, preserve state. Map/CDN offline: use register/analytics, restore network or vendor assets before an offline demonstration. Empty last-24-hour view: change the period, and label a historical replay accurately. Missing cloud tools/credentials: local validation can continue; no paid deployment is implicit.

For a timed presentation use `DEMO.md`. For observed-versus-expected failure behavior see `FAILURE_SCENARIOS.md`.
