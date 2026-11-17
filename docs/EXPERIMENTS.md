# Measured experiments — 5 November 2026

Raw JSON lives in `work/` (ignored). Rerun commands below on the same machine; timings are observations rather than performance promises.

## Pandas versus Spark

Both group the same deterministic `(id % 3, id % 100)` data and compute a mean. Input construction is excluded; Spark input is materialized in cache before timing. Three measured repetitions were run, with initialization/JIT effects still present. Pandas ran in Windows/Python 3.13; Spark ran in Linux containers with Java 11. This cross-runtime difference is a confound. The experiment measures a small warm aggregation, not ingestion, disk I/O or a memory-exhaustion threshold.

| Rows | Pandas median seconds | Spark local[2] median seconds |
|---|---:|---:|
| 100,000 | 0.0096 | 0.3270 |
| 500,000 | 0.0210 | 0.2906 |
| 1,000,000 | 0.0407 | 0.2241 |

Pandas won this workload. The claim that Pandas necessarily fails at one million rows is unsupported. Spark's scheduling/serialization overhead dominates this small aggregation; distributed processing becomes useful when data, computation or recovery needs justify that overhead.

The report includes Python-process CPU time and cumulative peak RSS. Spark's figures exclude the JVM and remote executors and must not be compared as total-cluster memory consumption. They do not establish a CPU or memory scaling advantage.

## Worker scaling on one physical PC

Each worker was configured with one core and 1 GB worker memory. The application used 512 MB executors and at most the selected core count. Streaming was paused during the experiment and restored afterward. These are container/process-level runs sharing one host's CPU, RAM and disk.

| Rows | 1 worker median seconds | 2 workers | 4 workers |
|---|---:|---:|---:|
| 100,000 | 0.4727 | 0.6453 | 1.0207 |
| 500,000 | 0.3798 | 0.3372 | 0.3274 |
| 1,000,000 | 0.3065 | 0.2828 | 0.2980 |

At one million rows the measured speedups versus one worker are about 1.08x and 1.03x. This does not demonstrate linear scaling. More partitions/workers can increase scheduling cost and host contention. These small runs do not isolate every confound; use larger repeated workloads and independent hosts for stronger conclusions.

## HTTP-to-Kafka load experiment

100 synthetic events, concurrency four, one shared location key. Explicit IPv4 loopback run: 100 accepted, zero rejected, 1.768 seconds total; 56.56 acknowledged events/s; accepted latency median 64.2 ms and p95 103.5 ms. An earlier Windows `localhost` run took approximately 52 seconds. The endpoint address changed, so preserve both reports rather than treating this as an isolated server optimization. Request-address/network-path effects require further profiling.

These measurements stop at broker acknowledgement. Consumer lag and lake completion must be inspected separately. No sub-5-ms latency or maximum sustainable throughput claim is established.

## ML comparison

`work/model-comparison.json` records TF-IDF Logistic Regression, Random Forest and XGBoost on the same 10-example text holdout, plus Random Forest/XGBoost on a synthetic severity holdout. Training and inference timings are recorded. Corpus size and synthetic severity labels prevent useful claims about real-world accuracy. This comparison's vectorizer differs from the deployed training configuration; do not interchange their scores.

## Reproduce

```powershell
.\venv\Scripts\python.exe benchmarks/dataframe_benchmark.py --rows 100000 500000 1000000 --output work/pandas-benchmark.json
docker compose exec -T spark-master /opt/spark/bin/spark-submit --master local[2] /opt/spark/app/benchmarks/dataframe_benchmark.py --engine spark --master local[2] --output /opt/spark/app/work/spark-benchmark.json
.\scripts\benchmark_scaling.ps1
.\venv\Scripts\python.exe benchmarks/kafka_load.py --count 100 --concurrency 4 --output work/kafka-load-ipv4.json
.\venv\Scripts\python.exe -m benchmarks.compare_models --output work/model-comparison.json
```

Install `requirements-experiments.txt` for local experiment dependencies. Create `work/` before specifying output files. Rerunning the load experiment adds labelled synthetic records; the anomaly job excludes those sources.
