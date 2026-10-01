# Cataclysm — Architecture

## Overview

During a disaster (flood, earthquake, cyclone, fire), information arrives from many places at once. This data is messy, fast, and high-volume. Cataclysm collects all of it, processes it in real-time, classifies the disaster type, predicts severity, and shows results on a dashboard.

## Data Flow

1. **Data Sources** — Citizen reports (simulated), Weather API (OpenWeatherMap), Earthquake API (USGS), Sensor data (simulated water level, smoke, temperature)
2. **FastAPI** — Receives data via REST API, validates it (correct format? required fields?), sends valid events to Kafka
3. **Kafka** — Streams events reliably. Holds data safely even if Spark is busy or crashes. Handles thousands of events/sec. Decouples receiving data from processing data.
4. **Spark** — Reads from Kafka in real-time. Cleans messy data, removes duplicates, aggregates (e.g. "47 flood reports from Zone A in 10 min"), joins weather + reports. Also does batch processing on historical data.
5. **Data Lake (MinIO)** — Stores ALL data in layers: /raw (untouched original), /processed (cleaned), /curated (ready for ML). Uses Parquet format (compressed, fast for analytics).
6. **PostgreSQL** — Stores structured, queryable data: incidents, predictions, locations. The dashboard and API query this.
7. **ML Models** — Incident Classifier: "Water entering houses" → FLOOD. Severity Predictor: rainfall + report count + water level → CRITICAL.
8. **Dashboard** — Map with incident locations (color-coded by severity), active incidents list, trends over time, anomaly alerts.
9. **Monitoring (Prometheus + Grafana)** — Tracks API response time, Kafka messages/sec, Spark processing time, CPU, memory, errors.

```
Data Sources → FastAPI → Kafka → Spark → Data Lake + PostgreSQL → ML → Dashboard → Monitoring
```

## Why Two Storage Systems?

**Data Lake (MinIO):** Stores everything in bulk — millions of raw events. Cheap storage. Good for batch processing and ML training. You don't query it directly from the app.

**PostgreSQL:** Stores structured data that the app needs to query. Example: "Show me all critical floods in Mumbai." Supports SQL (SELECT, JOIN, WHERE). The dashboard reads from here.

Different storage for different purposes. You don't dump 5 million raw events into PostgreSQL. You don't run SQL dashboard queries on Parquet files.

## Containers (Docker Compose)

| Container | What it runs |
|---|---|
| fastapi | Python API server |
| kafka | Message streaming |
| zookeeper | Required by Kafka for coordination |
| spark-master | Spark driver (plans the work) |
| spark-worker | Spark executor (does the work) — scalable to 2, 4 workers |
| postgres | PostgreSQL database |
| minio | S3-compatible Data Lake (local) |

One command starts everything: `docker compose up`

## Cloud

Will be deployed on Azure VM (compute) + Azure Blob Storage (replaces MinIO as Data Lake in production). Azure Student credits — $100 free, no credit card needed.