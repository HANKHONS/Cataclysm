# Implementation and verification ledger

## Current local acceptance — 10 November 2026

The current implementation was inspected, modified and exercised against the existing Docker stack in `E:\projects\catalcysm`. Existing user Git changes, pre-existing deleted `.gsd` files, volumes, records and v2 datasets were preserved. No cloud deployment, paid resource creation, remote publication or Git commit occurred. The named primary implementation-plan file was absent; the supplied master request governed acceptance.

| Feature | Status | Fresh evidence and boundary |
|---|---|---|
| Input schema / normalized sources | verified locally | 36-test suite; UTC normalization, finite metadata, weather units/missing values, malformed USGS isolation; historical day backfill explicitly marked replay |
| Current USGS integration | verified | Public hour-feed fetched and four observations submitted through FastAPI; refreshed once later with another four returned feed observations; no claim that all were new unique disasters |
| OpenWeather | blocked live / verified offline | Credential presence checked without displaying it: configured=false. Key rejection, units, required observed time and missing-wind behavior tested |
| API / database query contracts | verified | Live invalid-input 422, filtering, disjoint pages, aggregate consistency, hourly hazard/severity trends and empty result behavior (`evidence/application.json`) |
| Operational persistence + raw + ML + processed lake | verified | Exact known identity in all relevant sinks, two prediction rows and committed duplicate without extra DB row (`evidence/end-to-end-smoke.json`) |
| Consumer crash after DB transaction | verified | Actual child process exit 91, no source commit (-1001), restart to next offset 1, one row before/after, two predictions. Initial and final-code runs preserved (`evidence/crash-replay.json`, `crash-replay-final.json`) |
| Conflicting identity / archive consistency | verified | Actual consumed conflicting narrative/severity preserved first facts and original raw JSON (`evidence/application.json`) |
| DLQ | verified selected paths | Actual malformed marker found in real DLQ; failed delivery/no source commit unit test. DLQ duplicates after crash remain possible |
| DB / broker outage recovery | verified | Actual stop/restore: queries and readiness 503 while DB down; POST 503 while broker down; original ID retry persisted after restoration (`evidence/dependency-outages.json`) |
| Spark semantics / checkpoint recovery | verified bounded fixture | Actual Spark 3.5.1: duplicate retained once across restart, one late row dropped, normalized text read back and two finalized count-one windows (`evidence/spark-semantics.json`). Independent fixture uses file source/local[2]; production Kafka path is separately smoke-tested |
| Raw / processed / curated readback | verified snapshot | 922 raw rows/922 files, 737 processed rows/43 files, 49 curated rows/50 files at the captured readback (`evidence/lake-readback.json`). Layers legitimately differ; file count is not row/incident count |
| Model training / inference | connected; predictive validity partial | Reproducible trained artifacts, retained previous versions, hashes, confusion matrices, same-split majority baselines and reports in `models/evaluation.json`. Text F1 0.4467/accuracy .50 on ten examples; synthetic severity F1 .9971 does not validate physical hazard prediction |
| Prediction provenance | corrected | Zero-score fallback/missing telemetry distinguished per output; 490 legacy unknown-severity prediction rows corrected to unavailable/null confidence without altering observed facts or labels |
| Dashboard | verified selected desktop/mobile interactions | Actual synthetic form acknowledgment then DB persistence; USGS labels/details/provider metadata; search/empty state; source/viewport filters; map fitting/clustering; current analytics/model/anomaly/pipeline data. Desktop 1440 and mobile 390 viewport checks had no document horizontal overflow. Register bounded to 615 desktop/500 mobile pixels |
| Handbook | verified served local reader | 24 chapters, clickable actual branching SVG, source navigation/search, eight-stage journey, 68 collapsible viva answers; online MathJax produced rendered equations. Desktop/mobile inspected; local static assets allow file-based core reader, but file:// was not separately browser-tested |
| Monitoring | verified | Both Prometheus jobs up, Grafana DB health OK, actual instant-query samples, promtool configuration and three alert rules valid. Gauge/progress age is not perpetual health or an uptime guarantee |
| Performance | measured bounded scope | Three measured 100-event/four-client container-loopback runs accepted 300/300; mean 109.31 broker acknowledgments/s, sample SD 2.94/s. Per-run p95 33.54–49.56 ms. Not sustainable end-to-end capacity |
| Azure | blocked / deferred | Existing assets inspected; tooling/subscription deployment not newly validated. Historical compile evidence retained separately. No Azure resources created or credits spent |

### Final checks and evidence

- Offline command: `.\venv\Scripts\python.exe -m pytest tests -q --basetemp=work/pytest-nov10-acceptance --tb=short`: **36 passed**, six deprecation warnings. Exact output: `evidence/pytest.txt`. Warnings concern existing FastAPI startup/shutdown hooks and legacy naive-datetime test fixtures.
- Ruff undefined/unused-name checks and Node syntax checks passed. Formatting changed source readability without introducing a runtime frontend dependency. A temporary pinned formatter remains under ignored `work`.
- Compose validates with `docker compose config --quiet`; API `/ready` reports healthy with DB and broker connected. Running service snapshot: `evidence/service-status.jsonl`.
- Source-reference snapshots are regenerated and checked with `scripts/sync_source_reference.py --check`; handbook rebuild uses canonical Markdown. Documentation/source link checks and configured-secret-value scan are performed before handoff.
- Actual screenshots: [desktop console](evidence/dashboard-desktop.jpg), [mobile console](evidence/dashboard-mobile.jpg), [desktop handbook](evidence/handbook-desktop.jpg), [mobile handbook](evidence/handbook-mobile.jpg). These are real browser captures, not mockups.
- Benchmark runtime/dependency versions and raw repetitions: `evidence/benchmark-current.json`; Docker allocation and point resource sample: `evidence/docker-runtime.json`, `evidence/container-resources.jsonl`. Older Pandas/Spark/worker-count measurements remain explicitly dated in EXPERIMENTS.md.

### Remaining limits and unexecuted cases

No global exactly-once transaction, host-level redundancy, authenticated source identity, independent physical-severity validation, calibrated probabilities, durable SSE resume cursor or backup restoration guarantee exists. Source freshness is not provider uptime. Original observations own their IDs; updates need a revision policy. Prediction rows lack full artifact hashes per result. The anomaly job excludes known simulation source conventions; arbitrary spoofed provenance remains a limitation. The source API has no public authentication/rate-limit layer. Map/CDN offline branches are implemented but were not forced offline during browser QA. Multi-hour endurance, intentional OOM/host loss, exact inside-transaction/post-offset kill and actual MinIO network outage were not separately run. See FAILURE_SCENARIOS.md for expected/observed distinctions.

The current local demonstration is implemented and verified within these boundaries. Weather credentials, paid cloud deployment and real-world ML validity remain explicit external/evidence limitations rather than successful features.

---

## Preserved historical ledger

The following 5 November material is retained as historical evidence; it is not automatically a current measurement.


This ledger replaces unsupported runtime claims in earlier project descriptions.

## Verified on 2026-11-05

- Existing virtual environment: Python 3.13.9.
- Offline pytest suite: 18 tests passed, including DB/DLQ offset safety, callback polling, archive fidelity, weather credentials/units and anomaly boundary handling.
- Live smoke tests verified HTTP acknowledgement, operational persistence, two prediction records, raw MinIO archival and the exact submitted event in processed Spark Parquet.
- Docker Desktop starts and reports server version 29.8.1.
- Old MinIO image references failed. A pinned upstream source build succeeded and replaced them; the build includes the upstream AGPL license.
- One public USGS day-feed request submitted 185 real observations. An hourly anomaly result was stored from actual history; synthetic sources are excluded.
- MinIO replay: dry-run and actual two-row replay completed. A malformed source record was verified in the real DLQ.
- Eight pre-existing operational IDs survived DB/broker/MinIO/Spark restart; a new event reached the sinks afterward.
- A scaling-induced checkpoint failure exposed missing executor mounts. Driver and worker now share the named checkpoint volume; version-2 derived prefixes preserve the earlier objects while rebuilding from Kafka.
- Worker recreation was repeated after that fix. Event `DEMO-e9c0c6f714944d57` reached its operational row, two predictions, raw object and version-2 processed Parquet.
- Prometheus API/infrastructure targets reported up; Grafana health reported its database OK.
- Prometheus's `promtool` validated the configuration and all three alert rules. Live Spark microbatch-duration gauges were present for both queries.
- Pandas/Spark runs at 100K/500K/1M and 1/2/4 worker experiments completed. Raw measurements are in ignored `work/`; see `docs/EXPERIMENTS.md` for scope.
- Azure Bicep compiled to ARM JSON locally. No Azure resources were created and no credits were spent.
- Ruff undefined-name/unused-import checks pass. The API/consumer image was built successfully using an exact container dependency lock.
- A later long-running Spark driver exhausted its heap with unbounded rapid triggering/UI retention. Both queries now use 10-second triggers, retained job/stage/query history is bounded, driver heap is 1536 MB and restart policy covers clean exits. The subsequent 10-minute stability check passed with zero restarts, including new event processing; sampled memory after startup remained approximately 917–948 MiB. Raw samples are in `work/stability.json`. This bounded check is not a multi-hour endurance guarantee.

## Reliability corrections

- API polls Kafka callbacks until success or deadline; producer buffer/Kafka exceptions return 503.
- Consumer validates the complete event schema and acknowledges DLQ delivery before source commit.
- Exhausted DB or lake writes terminate consumption before later source offsets can be committed. Compose restarts the consumer.
- Database logs no longer print the connection URL containing credentials.
- Database, broker, MinIO and Spark checkpoints have named volumes. Existing container writable-layer data must be backed up before migration to these volumes; no old volumes were deleted.
- Spark deduplication uses the bounded watermark API. Curated aggregation reads committed processed Parquet as a separate stateful stage.

## External or remaining validation

- Live OpenWeather authentication awaits the user's key. Normalization is tested offline. Background source polling remains off by default.
- Larger independently labelled ML datasets are required for useful accuracy; current small/synthetic data cannot establish real-world accuracy.
- Azure subscription what-if and deployment remain pending student-account setup and approval of actual cloud spend.
- UI visual QA found a paid tile-provider placeholder. The corrected standard OSM map, visible attribution, persisted feed, loaded-report count and model/anomaly status were visually verified.

Do not present syntax checks, generated files, heuristic confidence or synthetic test scores as production validation.

## Revalidation — 17 November 2026

- Fresh offline suite: 36 passed, six existing deprecation warnings.
- Docker was stopped; started Desktop and restored Compose. Host MinIO ports conflicted with unrelated applications. Defaults now use MINIO_API_PORT=19000 and MINIO_CONSOLE_PORT=19001; internal minio:9000 and named volumes remain unchanged.
- Actual smoke ID DEMO-b1526f2482c44b0c: Kafka partition 0 offset 93, one DB row/two predictions/raw object/specific processed Parquet event; duplicate committed through next offset 95 without duplicate DB row.
- Live API/filter/identity/raw canonical content/SSE checks passed; both Prometheus jobs up, Grafana database OK. Dated evidence: docs/evidence/application-2026-11-17.json.
- Public USGS fetch submitted eight observations. Weather key presence remains false; live OpenWeather is still blocked.
- Dashboard: http://127.0.0.1:8000 ; MinIO console: http://127.0.0.1:19001 ; Grafana: http://127.0.0.1:3000 ; handbook: http://127.0.0.1:8000/handbook/ .
- This is fresh local functional verification, not cloud deployment, independent ML validation, sustained end-to-end capacity or backup-restoration evidence. Those limitations remain explicit.
