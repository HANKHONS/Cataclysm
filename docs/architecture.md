# Cataclysm — System Architecture & Design Specification

## 1. Overview
Current verified behavior and operational caveats are recorded in `docs/VERIFICATION.md` and `docs/DEMO.md`; those take precedence over the original design narrative below. The operational consumer writes PostgreSQL and raw MinIO objects before committing offsets. Spark independently writes `events-v2/`, then a second query reads that committed Parquet and produces `windowed_incident_counts-v2/`. Driver and executors share the checkpoint volume. The SSE feed polls persisted DB records; ingestion acknowledgements do not imply downstream persistence. The relational additions are `locations`, `reports`, `predictions` and `anomaly_windows`, preserving the existing `crisis_events` table.

**Cataclysm** is a distributed, real-time crisis intelligence platform that ingests high-velocity disaster telemetry (floods, cyclones, earthquakes, wildfires, infrastructure incidents), streams them through Apache Kafka, processes them with Apache Spark, archives raw events in a MinIO Parquet Data Lake, maintains queryable relational records in PostgreSQL, runs ML classification/severity models, and renders live alerts on an interactive geospatial dashboard via Server-Sent Events (SSE).

---

## 2. End-to-End Data Flow

```
[Sources: Generator / Sensors / APIs]
                  │
                  ▼ HTTP POST /events
         [FastAPI Gateway] ─── SSE Stream ───► [Interactive Leaflet Dashboard]
                  │
                  ▼ (Producer with Delivery Callbacks, acks=all)
          [Kafka Broker]
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
 [Python Consumer]    [Apache Spark 3.5.1]
  - ML Inference       - 10-Min Watermark
  - DLQ Error Route    - Stateful Dedup
  - Idempotent Write   - Sliding Windows
        │                   │
        ├───────────────────┘
        ▼
   ┌────┴───────────────┐
   ▼                    ▼
[PostgreSQL 15]     [MinIO Parquet Lake]
(Live Queries)      (Long-Term Archival)
```

1. **Ingestion & Validation**:
   - Telemetry arrives at `POST /events`.
   - Validated via Pydantic (`CrisisEvent`, `Metadata`).
   - Acknowledged only after Kafka broker confirms partition write.
2. **Reliable Message Streaming**:
   - Apache Kafka 4.3.1 KRaft broker with topic `crisis-events` (3 partitions).
   - Topic `crisis-events-dlq` receives malformed payloads with error diagnostic headers.
3. **Consumer & Persistence**:
   - `enable.auto.commit=False` for at-least-once processing.
   - Idempotent upserts (`ON CONFLICT (event_id) DO NOTHING` or `DO UPDATE`) via `psycopg-pool`.
   - Commits Kafka offsets strictly after database transaction commits.
4. **Machine Learning Intelligence**:
   - Text classification: Categorizes unstructured report text into disaster categories.
   - Severity prediction: Evaluates physics telemetry (`rainfall_mm`, `water_level_m`, `wind_speed_kmh`, `temperature_c`) to classify threat levels.
5. **Data Lake Storage (MinIO S3)**:
   - Snappy-compressed Parquet files partitioned by `year=YYYY/month=MM/day=DD/disaster_type=X/`.
   - Disaster replay tool enables rapid state recovery and backfilling.
6. **Real-Time Geospatial Visualization**:
   - Leaflet.js interactive dark-mode map.
   - Live Server-Sent Events (SSE) `/api/events/stream` automatically plots events and updates live feeds in real time.

---

## 3. Reliability & Fault Tolerance Guarantees

| Failure Scenario | System Behavior & Mitigation |
|---|---|
| **Broker temporarily down** | API returns `503 Service Unavailable`; client retry triggered; zero false acknowledgments. |
| **Malformed / corrupted JSON** | Diverted to `crisis-events-dlq` topic; consumer commits offset and continues processing. |
| **PostgreSQL container restart** | Consumer executes bounded exponential backoff retries without crashing or committing offset. |
| **Duplicate message delivery** | Table uses `PRIMARY KEY (event_id)` with `ON CONFLICT DO NOTHING/UPDATE` (idempotent). |
| **Complete database loss** | Parquet replay CLI (`src/data/replay.py`) reconstructs PostgreSQL records from MinIO S3 data lake. |
