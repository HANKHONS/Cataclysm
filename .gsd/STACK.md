# Technology Stack: Cataclysm

## 1. Core Technologies & Runtime

| Component | Technology | Version / Specification | Source Citation |
|---|---|---|---|
| **Runtime** | Python | `3.11-slim` (Docker base) | [Dockerfile:1](file:///e:/catalcysm/Dockerfile#L1) |
| **Backend Framework** | FastAPI | Unpinned in requirements (`fastapi`) | [requirements.txt:1](file:///e:/catalcysm/requirements.txt#L1) |
| **ASGI Server** | Uvicorn | Unpinned in requirements (`uvicorn`) | [requirements.txt:2](file:///e:/catalcysm/requirements.txt#L2) |
| **Data Validation** | Pydantic | Unpinned in requirements (`pydantic`) | [requirements.txt:3](file:///e:/catalcysm/requirements.txt#L3) |
| **Message Streaming** | Apache Kafka (KRaft mode) | `apache/kafka:4.3.1` container image | [compose.yaml:23](file:///e:/catalcysm/compose.yaml#L23) |
| **Kafka Client** | confluent-kafka | Unpinned in requirements (`confluent-kafka`) | [requirements.txt:4](file:///e:/catalcysm/requirements.txt#L4) |
| **Relational Database** | PostgreSQL | `postgres:15` container image | [compose.yaml:14](file:///e:/catalcysm/compose.yaml#L14) |
| **Database Driver** | psycopg (v3) | `psycopg[binary]` | [requirements.txt:5](file:///e:/catalcysm/requirements.txt#L5) |
| **Frontend UI** | Static HTML / Vanilla JS | Served by FastAPI via `FileResponse` | [src/api/main.py:49-54](file:///e:/catalcysm/src/api/main.py#L49-L54), [src/api/static/index.html:1-246](file:///e:/catalcysm/src/api/static/index.html#L1-L246) |
| **Styling** | Tailwind CSS CDN | Loaded dynamically via `<script src="https://cdn.tailwindcss.com">` | [src/api/static/index.html:8](file:///e:/catalcysm/src/api/static/index.html#L8) |
| **Containerization** | Docker Compose | Compose file specification format | [compose.yaml:1-40](file:///e:/catalcysm/compose.yaml#L1-L40) |

---

## 2. Dependencies Analysis

### Declared Dependencies (`requirements.txt`)
- `fastapi` ([requirements.txt:1](file:///e:/catalcysm/requirements.txt#L1)) — Web framework for HTTP ingest and querying.
- `uvicorn` ([requirements.txt:2](file:///e:/catalcysm/requirements.txt#L2)) — ASGI web server running `src.api.main:app` ([Dockerfile:6](file:///e:/catalcysm/Dockerfile#L6)).
- `pydantic` ([requirements.txt:3](file:///e:/catalcysm/requirements.txt#L3)) — Schema definitions (`Metadata`, `CrisisEvent`) and validation ([src/api/main.py:31-47](file:///e:/catalcysm/src/api/main.py#L31-L47)).
- `confluent-kafka` ([requirements.txt:4](file:///e:/catalcysm/requirements.txt#L4)) — C-based Kafka client used in API producer ([src/api/main.py:7,24](file:///e:/catalcysm/src/api/main.py#L7)) and standalone consumer ([src/kafka/consumer.py:1,39](file:///e:/catalcysm/src/kafka/consumer.py#L1)).
- `psycopg[binary]` ([requirements.txt:5](file:///e:/catalcysm/requirements.txt#L5)) — PostgreSQL 3.x driver with pre-compiled C extensions used in API query handler ([src/api/main.py:8,73](file:///e:/catalcysm/src/api/main.py#L8)) and consumer ([src/kafka/consumer.py:3,6](file:///e:/catalcysm/src/kafka/consumer.py#L3)).

### Undeclared Dependencies
- `requests` — Imported in [src/data/generator.py:5](file:///e:/catalcysm/src/data/generator.py#L5) for `requests.post("http://localhost:8000/events", ...)` ([src/data/generator.py:147](file:///e:/catalcysm/src/data/generator.py#L147)), but **not listed in requirements.txt**. Running `generator.py` in an environment installing strictly from `requirements.txt` will raise `ModuleNotFoundError: No module named 'requests'`.

---

## 3. Network & Port Configurations

| Service | Container Port | Host Port | Protocol / Purpose | Citation |
|---|---|---|---|---|
| `fastapi` | `8000` | `8000` | HTTP (REST API & static web portal) | [compose.yaml:5](file:///e:/catalcysm/compose.yaml#L5) |
| `db` (PostgreSQL) | `5432` | `5433` | PostgreSQL wire protocol | [compose.yaml:20](file:///e:/catalcysm/compose.yaml#L20) |
| `broker` (Kafka Internal) | `9092` | Not mapped to host | PLAINTEXT internal docker broker listener (`broker:9092`) | [compose.yaml:30-31](file:///e:/catalcysm/compose.yaml#L30-L31) |
| `broker` (Kafka Controller) | `9093` | Not mapped to host | PLAINTEXT KRaft controller listener (`broker:9093`) | [compose.yaml:30,35](file:///e:/catalcysm/compose.yaml#L30) |
| `broker` (Kafka External) | `9094` | `9094` | PLAINTEXT external host listener (`localhost:9094`) | [compose.yaml:26,30-31](file:///e:/catalcysm/compose.yaml#L26) |

---

## 4. Environment Variables Configuration

| Variable | Scope | Default / Value in Repo | Description & Source Citation |
|---|---|---|---|
| `DATABASE_URL` | `fastapi` service | `postgresql://cataclysm:78520@db:5432/cataclysm` | Set in docker-compose for internal container network ([compose.yaml:7](file:///e:/catalcysm/compose.yaml#L7)), fallback in API ([src/api/main.py:27](file:///e:/catalcysm/src/api/main.py#L27)) |
| `POSTGRES_USER` | `db` service | `cataclysm` | Database superuser ([compose.yaml:16](file:///e:/catalcysm/compose.yaml#L16)) |
| `POSTGRES_PASSWORD` | `db` service | `"78520"` | Database superuser password ([compose.yaml:17](file:///e:/catalcysm/compose.yaml#L17)) |
| `POSTGRES_DB` | `db` service | `cataclysm` | Target database name ([compose.yaml:18](file:///e:/catalcysm/compose.yaml#L18)) |
| `KAFKA_BOOTSTRAP_SERVERS` | `fastapi` service | `broker:9092` (fallback) | Used by producer in API ([src/api/main.py:23](file:///e:/catalcysm/src/api/main.py#L23)). Note: not explicitly set in `compose.yaml` fastapi service. |
| `KAFKA_NODE_ID` | `broker` service | `1` | Kafka cluster broker ID ([compose.yaml:28](file:///e:/catalcysm/compose.yaml#L28)) |
| `KAFKA_PROCESS_ROLES` | `broker` service | `broker,controller` | Combined KRaft node role ([compose.yaml:29](file:///e:/catalcysm/compose.yaml#L29)) |
| `KAFKA_LISTENERS` | `broker` service | `INTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094` | Multi-listener interface binding ([compose.yaml:30](file:///e:/catalcysm/compose.yaml#L30)) |
| `KAFKA_ADVERTISED_LISTENERS` | `broker` service | `INTERNAL://broker:9092,EXTERNAL://localhost:9094` | Advertised addresses for internal docker network & host ([compose.yaml:31](file:///e:/catalcysm/compose.yaml#L31)) |
| `KAFKA_NUM_PARTITIONS` | `broker` service | `3` | Default topic partition count ([compose.yaml:40](file:///e:/catalcysm/compose.yaml#L40)) |

---

## 5. Planned vs Currently Implemented Stack

| Layer | Roadmap / Documentation Plan (`README.md`, `docs/architecture.md`) | Currently Implemented Status |
|---|---|---|
| **Stream Ingestion** | FastAPI, Apache Kafka | **Implemented** ([compose.yaml:1-40](file:///e:/catalcysm/compose.yaml#L1-L40), [src/api/main.py:1-103](file:///e:/catalcysm/src/api/main.py#L1-L103)) |
| **Stream Processing** | Apache Spark (Structured Streaming + Batch) | **Not implemented**. Temporary Python consumer script ([src/kafka/consumer.py:1-94](file:///e:/catalcysm/src/kafka/consumer.py#L1-L94)) directly writes raw events to PostgreSQL. |
| **Data Lake** | MinIO (local S3), Parquet storage (`/raw`, `/processed`, `/curated`) | **Not implemented**. No MinIO service in `compose.yaml`; no Parquet generation code. |
| **Database** | PostgreSQL 15 | **Implemented** ([compose.yaml:13-21](file:///e:/catalcysm/compose.yaml#L13-L21), [src/kafka/consumer.py:13-29](file:///e:/catalcysm/src/kafka/consumer.py#L13-L29)) |
| **Machine Learning** | scikit-learn (Disaster classification & Severity prediction) | **Not implemented**. No ML training scripts or inference pipeline present. |
| **Dashboard** | Streamlit or React dashboard (TBD in [README.md:18](file:///e:/catalcysm/README.md#L18)) | **Partially implemented**. Single-page HTML/Tailwind/Vanilla JS portal served at `/` ([src/api/static/index.html](file:///e:/catalcysm/src/api/static/index.html)). |
| **Monitoring** | Prometheus & Grafana | **Not implemented**. No metrics exporter or telemetry scraping in place. |
| **Cloud Deployment** | Azure VM + Azure Blob Storage | **Not implemented**. Local Docker Compose development environment only. |
