# Code walkthrough

Study chapters in this order. Prose explains ownership, control flow, edge cases, costs and safe changes. Each chapter then includes exact numbered source snapshots, import lists, definition ranges and SHA-256 hashes. Refresh after source changes with `python scripts/sync_source_reference.py`; verify synchronization with `--check`. These snapshots support code reading; canonical executable files remain in src/scripts/infra.

- [Configuration and Docker: read values before starting processes](01_CONFIGURATION_AND_DOCKER.md)
- [Sources: preserve identity, observation time and units](02_SOURCES_AND_GENERATION.md)
- [API and schemas: an acknowledgment is a precise boundary](03_API_AND_SCHEMAS.md)
- [Operational consumer: reason about every commit boundary](04_KAFKA_CONSUMER.md)
- [PostgreSQL: constraints are part of the algorithm](05_POSTGRESQL.md)
- [Spark Structured Streaming: two query states, two checkpoints](06_SPARK_STREAMING.md)
- [MinIO, Arrow, Parquet and faithful replay](07_MINIO_PARQUET_REPLAY.md)
- [Training: distinguish a connected model from a validated target](08_ML_TRAINING.md)
- [Inference and hourly anomaly state](09_ML_INFERENCE_AND_ANOMALIES.md)
- [Dashboard: data ownership and interaction flow](10_DASHBOARD.md)
- [Monitoring: inspect what each number measures](11_MONITORING.md)
- [Tests and recovery: match evidence to the failure boundary](12_TESTS_AND_RECOVERY.md)
- [Deployment and benchmarks: configuration is not deployment evidence](13_DEPLOYMENT_AND_BENCHMARKS.md)
