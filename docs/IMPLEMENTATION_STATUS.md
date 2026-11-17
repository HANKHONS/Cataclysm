# CATACLYSM execution checkpoint

Local implementation/verification pass finished 10 November 2026. Actual checkout: `E:\projects\catalcysm`. Existing user Git changes, deleted `.gsd` files, secrets, volumes and datasets were preserved. No commit, publication, paid cloud resource or Azure deployment occurred.

## Completed work

- [x] Repository/data-flow/configuration audit and baseline offline suite (18 passed).
- [x] Correct faithful local replay, acknowledgment checking and row limit.
- [x] Normalize event time, reject nonfinite metadata and constrain severity.
- [x] Preserve source metadata, explicitly label simulation/replay, isolate malformed USGS features and configure GET retry/backoff. Day backfill now marks replay.
- [x] Add parameterized source/search/time/bbox filters, pagination/sorting, full-scope overview, hourly hazard/severity trends, details, readiness and actual monitoring queries.
- [x] Enforce first-observation identity consistently across DB/predictions/raw archive and correct legacy missing-telemetry provenance.
- [x] Redesign the connected operations console and verify actual form/filter/detail/empty/map/desktop/mobile behavior; save screenshots.
- [x] Verify actual sink readback, crash/replay, DLQ, DB/broker outages, Spark state/lateness/windows, monitoring and fresh post-outage writes.
- [x] Repeat bounded benchmark three times, retain runtime/resources/repetitions and qualify measurement boundary.
- [x] Add model version preservation, hashes, confusion matrices and same-split majority baselines; rerun training.
- [x] Write architecture/trace/formulas/runbook/failure/experiments/report and 68-question viva guide.
- [x] Write 13 detailed module walkthroughs with exact source snapshots, imports, definition ranges and hashes.
- [x] Build and browser-test a 24-chapter static educational handbook with actual branching diagram, event journey, search, equations and collapsible answers.
- [x] Final offline suite: 36 passed; lint/syntax/configuration/source-sync checks and durable acceptance ledger.

## Verified artifacts / restart instructions

Start at `docs/README.md` for study order, `docs/RUNBOOK.md` for exact commands, `docs/VERIFICATION.md` for final status/boundaries and `docs/evidence/` for structured observations/screenshots. `docs/IMPLEMENTATION_AUDIT.md` preserves baseline findings and final disposition. The absent `CATACLYSM_IMPLEMENTATION_PLAN.md` was not invented; the supplied execution request governed work.

Services remain running for the local dashboard at http://127.0.0.1:8000 and handbook at /handbook/. If resuming after shutdown, start Docker Desktop, `docker compose up -d`, wait for `/ready` healthy, then run `docker compose exec -T fastapi python -m scripts.demo_smoke --spark`. Do not delete volumes or reset operational offsets. Source/model changes require restarting affected Python processes; new models were already trained/reloaded in this pass.

After source/doc edits: `python scripts/sync_source_reference.py`, `python scripts/sync_source_reference.py --check`, then `python scripts/build_handbook.py`. Use Git diff/status to distinguish this work from the substantial pre-existing modifications; do not restore the user's deleted GSD files.

## Blocked / deferred / scientifically unvalidated

- OpenWeather key presence verified false; offline integration tests pass. Privately configure it and run the documented manual observation before claiming live verification.
- Azure is undeployed; no new current compile/what-if/deployment is claimed. Paid resources require explicit authorization.
- Tiny curated text data and label-conditioned synthetic severity data cannot validate real-world predictions. Anomaly accuracy is unmeasured without ground truth.
- Single host/broker, serial small-file consumer, bounded polling SSE, unauthenticated declared source labels and untested host-loss/backup recovery remain design limits.
- Full offline asset vendoring, forced map/CDN failure, multi-hour endurance and every exact kill boundary were not executed. The failure table states which cases are observed versus inferred/mocked.

Next useful work is independently labelled data and sustained end-to-end stage/lag measurement, then authenticated provenance/revision/backup requirements. Those are evidence-driven extensions, not hidden claims of this local pass.

## Revalidation — 17 November 2026

- Fresh offline suite: 36 passed, six existing deprecation warnings.
- Docker was stopped; started Desktop and restored Compose. Host MinIO ports conflicted with unrelated applications. Defaults now use MINIO_API_PORT=19000 and MINIO_CONSOLE_PORT=19001; internal minio:9000 and named volumes remain unchanged.
- Actual smoke ID DEMO-b1526f2482c44b0c: Kafka partition 0 offset 93, one DB row/two predictions/raw object/specific processed Parquet event; duplicate committed through next offset 95 without duplicate DB row.
- Live API/filter/identity/raw canonical content/SSE checks passed; both Prometheus jobs up, Grafana database OK. Dated evidence: docs/evidence/application-2026-11-17.json.
- Public USGS fetch submitted eight observations. Weather key presence remains false; live OpenWeather is still blocked.
- Dashboard: http://127.0.0.1:8000 ; MinIO console: http://127.0.0.1:19001 ; Grafana: http://127.0.0.1:3000 ; handbook: http://127.0.0.1:8000/handbook/ .
- This is fresh local functional verification, not cloud deployment, independent ML validation, sustained end-to-end capacity or backup-restoration evidence. Those limitations remain explicit.
