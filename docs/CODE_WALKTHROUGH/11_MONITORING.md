# Monitoring: inspect what each number measures

`exporter.py` defines Prometheus Gauges with deliberately bounded label dimensions: dependency, Kafka partition, lake layer and query name. Exporting arbitrary event IDs as metric labels would grow cardinality without bound; event identities belong in logs/evidence, not metrics. start_http_server binds exporter port 9108 inside the Compose network.

Its main loop first counts operational event rows and sets the PostgreSQL probe. Next it gets topic partitions, committed offsets and low/high watermarks to publish clamped committed lag per partition, then Kafka probe success. It pages S3 object listings for raw/processed/curated counts and publishes MinIO probe status. Object totals include metadata objects and preserved older versions; they are not Parquet row counts. `scripts.audit_lake` separately reads Parquet metadata using current prefixes.

Finally it reads the latest per-query progress JSON, exports triggerExecution duration in milliseconds, processedRowsPerSecond and observed epoch. Missing/corrupt progress leaves previous samples rather than inventing successful progress. The exporter sleeps 15 seconds after sequential probes; slow probes can lengthen the actual loop. Prometheus scrape interval adds additional observation delay. Gauge values can persist from a previous successful probe, so pair them with dependency health and age.

API Instrumentator separately exports request counts, latency histograms and process metrics from actual responses. `prometheus.yml` scrapes the API and infrastructure exporter; `alerts.yml` defines the configured alert expressions. A valid config does not prove successful scraping. Grafana provisioning registers the datasource and a dashboard of actual PromQL queries (requests, 5xx, up, lag, objects, Spark duration and API process RSS). No hardcoded operational values are present. Live evidence observed both scrape jobs up and Grafana database OK; future service state must be refreshed.

`/api/pipeline` wraps a fixed set of instant queries for the operation console. `sum(rate(http_requests_total[5m]))` is all instrumented HTTP traffic, including polling/health; it is not report-ingestion rate. `time()-min(progress_epoch)` reflects the older query's latest progress and can rise during idle input. Maximum Spark batch duration combines current per-query latest values, not a historical p95. Committed consumer lag covers the operational group only. Definitions and units must accompany any presentation.

Exporter DB pool and S3/Kafka clients are reused; serial probes trade simplicity for observation delay. Bucket listings scale with object count, making the raw tiny-file layout increasingly expensive even for monitoring. Future scale requires sampled/aggregated storage metrics and a proper Spark status interface, justified by measurements.

To modify, add a bounded, defined metric with units and failure behavior, verify raw `/metrics`, check promtool syntax/rules, then inspect real Prometheus samples/Grafana health. Do not interpret `up=1`, an old gauge, or a container running state as complete pipeline correctness.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `src/common/exporter.py`

[Open source](../../src/common/exporter.py) · SHA-256 `10c2b1c61046cb9211bfe0530330bee0830b1525504378042c108399f6ee8962`

Dependencies: `import time`; `import json`; `from pathlib import Path`; `from confluent_kafka import Consumer, TopicPartition`; `from prometheus_client import Gauge, start_http_server`; `from src.common.config import config`; `from src.common.db import get_pool`; `from src.storage.lake_writer import ParquetLakeWriter`

- `main`: lines 29–88. Read its control flow with the module guide above.


```text
0001 | """Probe local operational dependencies and expose Prometheus gauges."""
0002 | 
0003 | import time
0004 | import json
0005 | from pathlib import Path
0006 | from confluent_kafka import Consumer, TopicPartition
0007 | from prometheus_client import Gauge, start_http_server
0008 | from src.common.config import config
0009 | from src.common.db import get_pool
0010 | from src.storage.lake_writer import ParquetLakeWriter
0011 | 
0012 | UP = Gauge("cataclysm_dependency_up", "Dependency probe", ["dependency"])
0013 | ROWS = Gauge("cataclysm_persisted_events", "Operational event row count")
0014 | LAG = Gauge("cataclysm_consumer_lag", "Committed consumer lag", ["partition"])
0015 | OBJECTS = Gauge("cataclysm_lake_objects", "Lake object count", ["layer"])
0016 | SPARK_DURATION = Gauge(
0017 |     "cataclysm_spark_batch_duration_ms", "Latest Spark microbatch duration", ["query"]
0018 | )
0019 | SPARK_ROWS = Gauge(
0020 |     "cataclysm_spark_processed_rows_per_second",
0021 |     "Latest Spark processing rate",
0022 |     ["query"],
0023 | )
0024 | SPARK_OBSERVED = Gauge(
0025 |     "cataclysm_spark_progress_epoch", "Last observed Spark progress epoch", ["query"]
0026 | )
0027 | 
0028 | 
0029 | def main() -> None:
0030 |     start_http_server(9108)
0031 |     consumer = Consumer(
0032 |         {
0033 |             "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
0034 |             "group.id": config.KAFKA_GROUP_ID,
0035 |             "enable.auto.commit": False,
0036 |         }
0037 |     )
0038 |     client = ParquetLakeWriter()._client()
0039 |     while True:
0040 |         try:
0041 |             with get_pool().connection() as conn:
0042 |                 ROWS.set(
0043 |                     conn.execute(
0044 |                         "SELECT COUNT(*) AS count FROM crisis_events"
0045 |                     ).fetchone()["count"]
0046 |                 )
0047 |             UP.labels("postgres").set(1)
0048 |         except Exception:
0049 |             UP.labels("postgres").set(0)
0050 |         try:
0051 |             metadata = consumer.list_topics(config.KAFKA_TOPIC, timeout=5)
0052 |             for partition in metadata.topics[config.KAFKA_TOPIC].partitions:
0053 |                 tp = TopicPartition(config.KAFKA_TOPIC, partition)
0054 |                 committed = consumer.committed([tp], timeout=5)[0].offset
0055 |                 low, high = consumer.get_watermark_offsets(tp, timeout=5)
0056 |                 LAG.labels(str(partition)).set(max(0, high - max(low, committed)))
0057 |             UP.labels("kafka").set(1)
0058 |         except Exception:
0059 |             UP.labels("kafka").set(0)
0060 |         try:
0061 |             for layer, bucket in (
0062 |                 ("raw", config.MINIO_BUCKET_RAW),
0063 |                 ("processed", config.MINIO_BUCKET_PROCESSED),
0064 |                 ("curated", config.MINIO_BUCKET_CURATED),
0065 |             ):
0066 |                 total = sum(
0067 |                     page.get("KeyCount", 0)
0068 |                     for page in client.get_paginator("list_objects_v2").paginate(
0069 |                         Bucket=bucket
0070 |                     )
0071 |                 )
0072 |                 OBJECTS.labels(layer).set(total)
0073 |             UP.labels("minio").set(1)
0074 |         except Exception:
0075 |             UP.labels("minio").set(0)
0076 |         try:
0077 |             progress = json.loads(
0078 |                 Path("work/spark-progress.json").read_text(encoding="utf-8")
0079 |             )
0080 |             for name, entry in progress.items():
0081 |                 SPARK_DURATION.labels(name).set(
0082 |                     entry.get("durationMs", {}).get("triggerExecution", 0)
0083 |                 )
0084 |                 SPARK_ROWS.labels(name).set(entry.get("processedRowsPerSecond", 0))
0085 |                 SPARK_OBSERVED.labels(name).set(entry["observed_epoch"])
0086 |         except (OSError, ValueError, KeyError):
0087 |             pass
0088 |         time.sleep(15)
0089 | 
0090 | 
0091 | if __name__ == "__main__":
0092 |     main()
```

### `monitoring/prometheus.yml`

[Open source](../../monitoring/prometheus.yml) · SHA-256 `7ae442b1dc2ca8d0e2b457139c333fe072b0cdfeb449dfa2043e7ba1c639f0bf`


```text
0001 | global:
0002 |   scrape_interval: 15s
0003 | rule_files:
0004 |   - /etc/prometheus/alerts.yml
0005 | scrape_configs:
0006 |   - job_name: cataclysm-api
0007 |     metrics_path: /metrics
0008 |     static_configs:
0009 |       - targets: [fastapi:8000]
0010 |   - job_name: cataclysm-infrastructure
0011 |     static_configs:
0012 |       - targets: [exporter:9108]
```

### `monitoring/alerts.yml`

[Open source](../../monitoring/alerts.yml) · SHA-256 `8fe289019a9e4bfb55b7f9d2ee23b8fb16687b277414f0aef6006e0b7f42957d`


```text
0001 | groups:
0002 |   - name: cataclysm
0003 |     rules:
0004 |       - alert: ApiUnavailable
0005 |         expr: up{job="cataclysm-api"} == 0
0006 |         for: 1m
0007 |         labels: {severity: critical}
0008 |         annotations: {summary: "Cataclysm API is unavailable"}
0009 |       - alert: DependencyUnavailable
0010 |         expr: cataclysm_dependency_up == 0
0011 |         for: 1m
0012 |         labels: {severity: warning}
0013 |         annotations: {summary: "A dependency probe failed"}
0014 |       - alert: ConsumerLag
0015 |         expr: sum(cataclysm_consumer_lag) > 1000
0016 |         for: 2m
0017 |         labels: {severity: warning}
0018 |         annotations: {summary: "Consumer lag exceeds 1000 records"}
```

### `monitoring/grafana/provisioning/dashboards/api.json`

[Open source](../../monitoring/grafana/provisioning/dashboards/api.json) · SHA-256 `7ddd345502f0ebda51ac7749a70babcbcdb7e807110b0f117bfdece635b33ab3`


```text
0001 | {
0002 |   "uid": "cataclysm-api",
0003 |   "title": "Cataclysm API",
0004 |   "schemaVersion": 39,
0005 |   "version": 1,
0006 |   "refresh": "15s",
0007 |   "time": {"from": "now-1h", "to": "now"},
0008 |   "panels": [
0009 |     {"id": 1, "title": "Requests per second", "type": "timeseries", "gridPos": {"x": 0, "y": 0, "w": 12, "h": 8}, "targets": [{"expr": "sum(rate(http_requests_total[5m])) by (handler)", "legendFormat": "{{handler}}"}]},
0010 |     {"id": 2, "title": "HTTP 5xx responses per second", "type": "timeseries", "gridPos": {"x": 12, "y": 0, "w": 12, "h": 8}, "targets": [{"expr": "sum(rate(http_requests_total{status=~\"5..\"}[5m]))", "legendFormat": "5xx"}]},
0011 |     {"id": 3, "title": "API availability", "type": "stat", "gridPos": {"x": 0, "y": 8, "w": 6, "h": 5}, "targets": [{"expr": "up{job=\"cataclysm-api\"}"}]},
0012 |     {"id": 4, "title": "Committed Kafka consumer lag", "type": "timeseries", "gridPos": {"x": 6, "y": 8, "w": 9, "h": 5}, "targets": [{"expr": "cataclysm_consumer_lag", "legendFormat": "Partition {{partition}}"}]},
0013 |     {"id": 5, "title": "Lake objects by layer", "type": "timeseries", "gridPos": {"x": 15, "y": 8, "w": 9, "h": 5}, "targets": [{"expr": "cataclysm_lake_objects", "legendFormat": "{{layer}}"}]},
0014 |     {"id": 6, "title": "Spark microbatch duration (ms)", "type": "timeseries", "gridPos": {"x": 0, "y": 13, "w": 12, "h": 7}, "targets": [{"expr": "cataclysm_spark_batch_duration_ms", "legendFormat": "{{query}}"}]},
0015 |     {"id": 7, "title": "API process resident memory", "type": "timeseries", "gridPos": {"x": 12, "y": 13, "w": 12, "h": 7}, "targets": [{"expr": "process_resident_memory_bytes{job=\"cataclysm-api\"}"}]}
0016 |   ]
0017 | }
```

### `monitoring/grafana/provisioning/dashboards/provider.yml`

[Open source](../../monitoring/grafana/provisioning/dashboards/provider.yml) · SHA-256 `6143ffaef30514440cab4d016410e00c7defb7ceb091468bb2b3328d9574e4ad`


```text
0001 | apiVersion: 1
0002 | providers:
0003 |   - name: Cataclysm
0004 |     folder: Cataclysm
0005 |     type: file
0006 |     options:
0007 |       path: /etc/grafana/provisioning/dashboards
```

### `monitoring/grafana/provisioning/datasources/prometheus.yml`

[Open source](../../monitoring/grafana/provisioning/datasources/prometheus.yml) · SHA-256 `f50c135b9d5102b3d286683340fc6a6a1bb8f6c5215ef3a6e541ac0f02848ecf`


```text
0001 | apiVersion: 1
0002 | datasources:
0003 |   - name: Prometheus
0004 |     type: prometheus
0005 |     access: proxy
0006 |     url: http://prometheus:9090
0007 |     isDefault: true
```
