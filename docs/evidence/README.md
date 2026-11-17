# Dated local evidence — 10 November 2026

- `pytest.txt`: final 36-test offline run and deprecation warnings.
- `end-to-end-smoke.json`: recorded actual known-ID broker/DB/prediction/raw/processed readback and duplicate result.
- `application.json`: live API/identity/canonical raw/SSE/monitoring/query-plan assertions.
- `crash-replay.json` and `crash-replay-final.json`: actual process exit after DB transaction and safe replay on separate retained topics/groups.
- `dependency-outages.json`: actual DB/broker stop/restore, 503s and same-ID recovery.
- `spark-semantics.json`: isolated actual Spark 3.5.1 checkpoint/dedup/late-row/window readback.
- `lake-readback.json`: current-prefix Parquet file/row snapshot; not unique-disaster counts.
- `benchmark-current.json`: three bounded measured runs, versions and statistics.
- `docker-runtime.json`, `container-resources.jsonl`, `service-status.jsonl`: allocation, point resource samples and running services; no indefinite uptime claim.
- Four `.jpg` files: actual desktop/mobile console/handbook captures. Viewport overrides were temporary and reset/closed after QA.

Read `../VERIFICATION.md` for scope and unexecuted cases. Fixture topics/objects and all existing state were preserved. Some PowerShell-captured text/JSON files contain a UTF-8 BOM; use `utf-8-sig` when parsing them in Python. No credentials are included.
