# Performance and experiments

## Current evidence: 10 November 2026

`evidence/benchmark-current.json` preserves the three measured runs, runtime and dependency versions. `evidence/docker-runtime.json` preserves Docker 29.8.1, 16 visible logical CPUs and an 8,172,228,608-byte Linux VM allocation. `evidence/container-resources.jsonl` is a simultaneous `docker stats --no-stream` sample. It is not a measurement of total physical host RAM, peak utilization or multihour endurance. Existing historical comparisons are preserved in `EXPERIMENTS.md`; their date and environment differ.

The current benchmark ran in the API container with Python 3.11.18, Linux/WSL2, FastAPI 0.162.0, confluent-kafka 2.16.1, psycopg 3.3.8, PyArrow 26.0.1, scikit-learn 1.9.1 and Pandas 3.1.1. Runtime versions are observations, not an instruction to upgrade. Use the existing container lock for reproduction.

### HTTP to broker acknowledgment

The workload consists of 100 unique synthetic flood events per run, one shared location key, four concurrent HTTP clients and IPv4 loopback. One preceding 100-event run warmed the service; three measured runs followed without pauses. The existing dashboard and streaming services stayed active. Input construction is included in client task scheduling; request latency starts immediately before each HTTP POST. Success means HTTP 200 after the Kafka delivery callback. It does not mean raw, DB and Spark completion. There is no claim of representative hazard traffic.

| Run | Accepted / requested | Wall s | Accepted/s | p50 ms | p95 ms |
|---|---:|---:|---:|---:|---:|
| 1 | 100/100 | 0.9260 | 107.99 | 33.00 | 44.01 |
| 2 | 100/100 | 0.9322 | 107.27 | 32.70 | 49.56 |
| 3 | 100/100 | 0.8875 | 112.68 | 32.35 | 33.54 |

Arithmetic mean of run acknowledgment rates: **109.31/s**, sample standard deviation **2.94/s**. Three small bursts are insufficient for a sustainable throughput ceiling, tail SLA, capacity recommendation or statistically stable variance estimate. Queue drainage was not included in these timings. A 100-event burst is deliberately resource-bounded. It adds synthetic persisted data rather than deleting/reusing operational identities.

The first warm-up accepted 100/100 in 1.0312 seconds, approximately 96.98/s, p50 36.43 ms and p95 70.40 ms. Excluding it from the three-run summary is an explicit warm-up policy, not selective deletion of a poor result. Network path differs from the historical Windows-host run; current container-loopback rates must not be presented as a proven backend optimization over that run.

The p95 implementation uses sorted zero-based index `int(n * .95)`, clamped to `n-1`, for accepted latencies. With 100 samples it uses the 96th order statistic. Rejected latency samples are retained in the runtime computation but not separately summarized in the current report; there were zero failures in these runs. Longer future experiments should include both populations and a published percentile convention.

### Resource sample and query plan

At the captured instant, the Spark driver was about 1.036 GiB, worker 785.6 MiB, broker 501.3 MiB, API 189 MiB, and consumer 163.2 MiB. Other containers and the Docker/host OS also consume memory. These are point samples and not memory limits or peaks. The configured driver heap is 1536 MB, executor heap 512 MB, worker allocation 1 GB; JVM RSS can differ from heap configuration. The historical ten-minute stability check is preserved separately and is not a new endurance result.

`evidence/application.json` contains `EXPLAIN (ANALYZE, FORMAT JSON)` for the recent timestamp-ordered 100-row base-table query. PostgreSQL used `idx_crisis_events_timestamp`; observed execution time was 0.088 ms in that small warmed sample. This query omits the full API's reports join and optional filters. It is supporting index evidence, not complete endpoint latency. Repeated realistic datasets and actual filtered query plans are needed before scalability claims.

### Correctness experiments are separate from speed

The smoke test checked an exact event in DB, two prediction rows, raw storage and processed Parquet, plus duplicate commit behavior. `evidence/crash-replay.json` records actual process exit between DB commit and archive/offset commit, followed by a one-row replay result. `evidence/spark-semantics.json` records actual checkpoint restart, late-event drop and two count-one finalized windows. These demonstrate selected semantics; they do not measure throughput. `evidence/application.json` verifies live filters, identity, SSE and monitoring. `evidence/dependency-outages.json` records actual DB/broker 503 behavior and same-ID retry recovery.

## Historical Pandas/Spark and worker-count comparisons

Read `EXPERIMENTS.md` for the 5 November measurements. Both engines grouped deterministic `(id % 3, id % 100)` data and averaged the numeric value. Pandas input creation was outside timing; Spark input was cached/materialized before timing. Windows Python versus Linux/JVM runtime and initialization effects are confounds. Pandas was faster at 100K, 500K and 1M rows; no evidence supports claiming it necessarily fails at one million rows.

The one/two/four-worker runs shared one physical PC. At one million rows, historical medians were 0.3065, 0.2828 and 0.2980 seconds. These imply about 1.08x and 1.03x speedup, not linear scaling. Driver-Python peak RSS excludes the JVM/executors; it cannot establish cluster memory superiority. This execution did not repeat those heavier comparisons because no change to their algorithm justified repeating them. Historical raw files remain in ignored `work`; the tracked dated summary is the available durable report.

## ML evaluation is a distinct experiment

Current reproducible training uses the same fixed stratified 40/10 text split. Macro F1 remains 0.4467, accuracy 0.50; the fire class had no correct predictions. This is weak measured generalization on an extremely small holdout. Majority-baseline macro F1, class supports and confusion matrix are now saved beside the models. Severity macro F1 is approximately 0.9971 on 300 samples drawn from the same label-conditioned synthetic generator. That score does not measure real-world damage prediction. The exact model hashes, library version and versioned artifacts are recorded in `models/evaluation.json`.

No labelled anomaly test set exists; neither contamination nor flag count is ground-truth accuracy. No calibrated confidence, end-to-end latency distribution, multi-host scaling, large historical-data ingestion throughput or cloud performance is measured.

## Reproduction commands

```powershell
docker compose exec -T fastapi python -m scripts.measure_current
docker stats --no-stream
# Optional historical comparison, only after preparing experiment dependencies:
.\venv\Scripts\python.exe benchmarks/dataframe_benchmark.py --rows 100000 500000 1000000 --repetitions 3 --output work/pandas-repeat.json
docker compose exec -T spark-master /opt/spark/bin/spark-submit --master local[2] /opt/spark/app/benchmarks/dataframe_benchmark.py --engine spark --master local[2] --output /opt/spark/app/work/spark-repeat.json
```

Use `scripts/benchmark_scaling.ps1` only in a controlled rehearsal; it changes worker count and pauses production streaming. Preserve checkpoints and restore the intended worker count afterward. Do not run parallel benchmark drivers against production checkpoints.

Throughput measures completed units per time within one boundary; latency measures one item's residence across a boundary. Performance includes both, resource use and correctness constraints. Scalability asks how they change with workload/resources. Speedup is a runtime ratio; efficiency divides it by resource count. Equations, units, examples and assumptions are in `CONCEPTS_AND_FORMULAS.md`.
