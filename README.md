# Cataclysm 

Cataclysm ingests real-time crisis data from multiple sources (citizen reports, weather APIs, sensors), streams them through Kafka, processes them with Spark, stores raw data in a Data Lake (Parquet) and structured results in PostgreSQL, classifies incidents and predicts severity using ML, and displays everything on a live dashboard.

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python, FastAPI |
| Streaming | Apache Kafka |
| Processing | Apache Spark (Structured Streaming + Batch) |
| Data Lake | MinIO, Parquet |
| Database | PostgreSQL |
| ML | scikit-learn |
| Containers | Docker, Docker Compose |
| Cloud | Azure |
| Monitoring | Prometheus, Grafana |
| Dashboard | TBD (Streamlit or React) |

## Architecture

```
Data Sources (reports, weather API, sensors)
       ↓
    FastAPI (receives + validates)
       ↓
     Kafka (streams events reliably)
       ↓
     Spark (cleans, transforms, aggregates)
       ↓
  ┌────┴────┐
  ↓         ↓
Data Lake  PostgreSQL
(raw data) (incidents, predictions)
  └────┬────┘
       ↓
   ML Models (classify disaster type, predict severity)
       ↓
   Dashboard (map, incidents, alerts)
       ↓
   Monitoring (system health metrics)
```

## Team

Solo developer (currently)