# Configuration and Docker: read values before starting processes

## What each boundary owns

`src/common/config.py` imports `os` for environment lookup, `Path` to locate the checkout independently of the shell directory, `quote` to URL-encode the fallback database password, and `load_dotenv` for the ignored local file. `load_dotenv(..., override=False)` preserves explicitly supplied container/process variables. The `Config` class evaluates typed class attributes at import, so changing `.env` after startup does not refresh a running process. `config = Config()` provides one shared settings object. String environment values are converted to int/float for pool sizes and timeouts; invalid values fail early. Boolean MinIO TLS is parsed by comparing lowercase text with `true`.

The fallback DB URL targets host port 5433; Compose explicitly supplies the internal `db:5432` URL. Likewise Kafka host clients use port 9094, whereas containers use `broker:9092`. MinIO host endpoint defaults to port 19000 (MINIO_API_PORT); internal endpoint is `minio:9000`. A container's localhost means itself, not the Windows host. These distinctions explain most connectivity errors better than changing source code randomly.

## Compose, block by block

`fastapi` and `consumer` reuse one Python image and bind-mount this checkout. Their different commands select API versus consumer execution; model files are shared through the mount but cached in each process. DB/broker dependencies wait for native health before launch. API readiness is independently checked with a small stdlib HTTP request. `restart: on-failure` restarts the consumer after bounded persistence failure without advancing its offset. Dependencies are startup ordering, not continuous availability guarantees.

The database stores its data directory on `postgres-data`. Broker settings combine KRaft controller/broker roles; INTERNAL/EXTERNAL/CONTROLLER listener names map protocols and advertised addresses. Controller traffic is not a client bootstrap endpoint. One broker, three default partitions and replication factors one are intentional local limits. `kafka-data` retains logs and group offsets. The configuration does not create a replicated cluster by naming several listeners.

Spark master runs its master class; worker advertises one core and 1 GB worker memory. Streaming driver uses `spark-submit`, a 1536 MB driver heap, 512 MB executor and one total executor core. Kafka/Hadoop connector packages resolve into the `spark-ivy` volume. Both driver and worker mount `spark-checkpoints` at the same path; executors can perform checkpoint I/O. `unless-stopped` covers both failed and clean driver exits. The source code raises if a query terminates so silent partial operation is not treated as success.

MinIO runs a pinned upstream source build and stores `/data` on a named volume. The init container creates three buckets with bounded retries; existing-bucket responses are accepted. Spark waits for successful init, rather than assuming container process start means storage is initialized. Prometheus and Grafana mount read-only configuration and durable data volumes. Exporter and anomaly worker are separate process workloads; their restarts do not recreate the database.

## Image and secret details

The Python Dockerfile installs curl, copies the exact container lock, installs dependencies, then copies the application. The checkout mount supersedes copied source during development. MinIO's multistage Dockerfile compiles a fixed Git commit using bounded Go parallelism, then copies only binary/license into a Debian runtime. This costs more on the first build but avoids relying on an unavailable historical image tag.

`.env.example` lists variable names/placeholders, not usable production credentials. `prepare_env.main` refuses to replace an existing `.env`, generates cryptographically random hex secrets, and uses exclusive creation mode `x`. Its optional rotation branch requires an explicit existing DB URL and changes a role; it is not needed for ordinary setup and should not be used casually. This execution preserved existing secrets.

## Modification, complexity and verification

Adding a setting requires choosing a host/container default, validating its type and passing it explicitly in Compose where needed. Restart/recreate the affected service to apply it. `docker compose config --quiet` checks syntax without printing secrets. Verify actual readiness rather than just `running`. Named-volume persistence does not prove backup recovery. Total service RSS includes JVM/Python/native buffers; heap flags alone do not bound complete host use. See the runbook for commands and failure documentation for retention/availability assumptions.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `src/common/config.py`

[Open source](../../src/common/config.py) · SHA-256 `b2d4050beb1c5924dc840b6a229261cf9fe769403f3675069a86288952825fca`

Dependencies: `import os`; `from pathlib import Path`; `from urllib.parse import quote`; `from dotenv import load_dotenv`

- `Config`: lines 12–58. Read its control flow with the module guide above.


```text
0001 | import os
0002 | from pathlib import Path
0003 | from urllib.parse import quote
0004 | 
0005 | from dotenv import load_dotenv
0006 | 
0007 | load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)
0008 | 
0009 | DEFAULT_INGESTION_API_URL = "http://127.0.0.1:8000/events"
0010 | 
0011 | 
0012 | class Config:
0013 |     # Database
0014 |     DATABASE_URL: str = os.getenv(
0015 |         "DATABASE_URL",
0016 |         "postgresql://cataclysm:"
0017 |         + quote(os.getenv("POSTGRES_PASSWORD", ""), safe="")
0018 |         + "@127.0.0.1:5433/cataclysm",
0019 |     )
0020 |     DB_POOL_MIN_SIZE: int = int(os.getenv("DB_POOL_MIN_SIZE", "2"))
0021 |     DB_POOL_MAX_SIZE: int = int(os.getenv("DB_POOL_MAX_SIZE", "10"))
0022 | 
0023 |     # Kafka
0024 |     KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
0025 |         "KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"
0026 |     )
0027 |     KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "crisis-events")
0028 |     KAFKA_DLQ_TOPIC: str = os.getenv("KAFKA_DLQ_TOPIC", "crisis-events-dlq")
0029 |     KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "crisis-intelligence-group")
0030 | 
0031 |     # MinIO / S3 Data Lake
0032 |     MINIO_ENDPOINT: str = os.getenv(
0033 |         "MINIO_ENDPOINT", "localhost:" + os.getenv("MINIO_API_PORT", "19000")
0034 |     )
0035 |     MINIO_ACCESS_KEY: str = os.getenv(
0036 |         "MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "")
0037 |     )
0038 |     MINIO_SECRET_KEY: str = os.getenv(
0039 |         "MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "")
0040 |     )
0041 |     MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
0042 |     MINIO_BUCKET_RAW: str = os.getenv("MINIO_BUCKET_RAW", "cataclysm-raw")
0043 |     MINIO_BUCKET_PROCESSED: str = os.getenv(
0044 |         "MINIO_BUCKET_PROCESSED", "cataclysm-processed"
0045 |     )
0046 |     MINIO_BUCKET_CURATED: str = os.getenv("MINIO_BUCKET_CURATED", "cataclysm-curated")
0047 | 
0048 |     # Spark
0049 |     SPARK_MASTER_URL: str = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
0050 | 
0051 |     # External source credentials. USGS is public; OpenWeather requires this key.
0052 |     OPENWEATHER_API_KEY: str = os.getenv("OPENWEATHER_API_KEY", "")
0053 |     INGESTION_API_URL: str = os.getenv(
0054 |         "INGESTION_API_URL", DEFAULT_INGESTION_API_URL
0055 |     )
0056 |     INGESTION_TIMEOUT_SECONDS: float = float(
0057 |         os.getenv("INGESTION_TIMEOUT_SECONDS", "15")
0058 |     )
0059 | 
0060 | 
0061 | config = Config()
```

### `compose.yaml`

[Open source](../../compose.yaml) · SHA-256 `fd100d8a18627811dcc62352ec90f5b42716d56d2e27d59a59a94834199732d6`


```text
0001 | services:
0002 |   fastapi:
0003 |     image: catalcysm-fastapi:demo
0004 |     build: .
0005 |     container_name: fastapi
0006 |     ports:
0007 |       - "127.0.0.1:8000:8000"
0008 |     environment:
0009 |       DATABASE_URL: postgresql://cataclysm:${POSTGRES_PASSWORD:?set in .env}@db:5432/cataclysm
0010 |       KAFKA_BOOTSTRAP_SERVERS: broker:9092
0011 |       KAFKA_TOPIC: crisis-events
0012 |       KAFKA_DLQ_TOPIC: crisis-events-dlq
0013 |       PYTHONPATH: /app
0014 |       PYTHONUNBUFFERED: "1"
0015 |       MINIO_ENDPOINT: minio:9000
0016 |       MINIO_ACCESS_KEY: ${MINIO_ROOT_USER:?set in .env}
0017 |       MINIO_SECRET_KEY: ${MINIO_ROOT_PASSWORD:?set in .env}
0018 |       OPENWEATHER_API_KEY: ${OPENWEATHER_API_KEY:-}
0019 |       INGESTION_API_URL: http://127.0.0.1:8000/events
0020 |     volumes:
0021 |       - .:/app
0022 |     healthcheck:
0023 |       test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/ready', timeout=5)"]
0024 |       interval: 15s
0025 |       timeout: 7s
0026 |       retries: 3
0027 |       start_period: 30s
0028 |     depends_on:
0029 |       db:
0030 |         condition: service_healthy
0031 |       broker:
0032 |         condition: service_healthy
0033 | 
0034 |   db:
0035 |     image: postgres:15
0036 |     volumes:
0037 |       - postgres-data:/var/lib/postgresql/data
0038 |     container_name: db
0039 |     environment:
0040 |       POSTGRES_USER: cataclysm
0041 |       POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?set in .env}
0042 |       POSTGRES_DB: cataclysm
0043 |     ports:
0044 |       - "127.0.0.1:5433:5432"
0045 |     healthcheck:
0046 |       test: ["CMD-SHELL", "pg_isready -U cataclysm -d cataclysm"]
0047 |       interval: 5s
0048 |       timeout: 5s
0049 |       retries: 5
0050 |       start_period: 5s
0051 | 
0052 |   broker:
0053 |     image: apache/kafka:4.3.1
0054 |     volumes:
0055 |       - kafka-data:/var/lib/kafka/data
0056 |     container_name: broker
0057 |     ports:
0058 |       - "127.0.0.1:9094:9094"
0059 |     environment:
0060 |       KAFKA_NODE_ID: 1
0061 |       KAFKA_LOG_DIRS: /var/lib/kafka/data
0062 |       KAFKA_PROCESS_ROLES: broker,controller
0063 |       KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094
0064 |       KAFKA_ADVERTISED_LISTENERS: INTERNAL://broker:9092,EXTERNAL://localhost:9094
0065 |       KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
0066 |       KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
0067 |       KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
0068 |       KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker:9093
0069 |       KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
0070 |       KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
0071 |       KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
0072 |       KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
0073 |       KAFKA_NUM_PARTITIONS: 3
0074 |     healthcheck:
0075 |       test: ["CMD-SHELL", "kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1 || /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1 || exit 1"]
0076 |       interval: 10s
0077 |       timeout: 5s
0078 |       retries: 6
0079 |       start_period: 15s
0080 | 
0081 |   consumer:
0082 |     image: catalcysm-fastapi:demo
0083 |     restart: on-failure
0084 |     build: .
0085 |     container_name: consumer
0086 |     command: python src/kafka/consumer.py
0087 |     environment:
0088 |       DATABASE_URL: postgresql://cataclysm:${POSTGRES_PASSWORD:?set in .env}@db:5432/cataclysm
0089 |       KAFKA_BOOTSTRAP_SERVERS: broker:9092
0090 |       KAFKA_TOPIC: crisis-events
0091 |       KAFKA_DLQ_TOPIC: crisis-events-dlq
0092 |       KAFKA_GROUP_ID: crisis-dashboard-group
0093 |       PYTHONPATH: /app
0094 |       PYTHONUNBUFFERED: "1"
0095 |       MINIO_ENDPOINT: minio:9000
0096 |       MINIO_ACCESS_KEY: ${MINIO_ROOT_USER:?set in .env}
0097 |       MINIO_SECRET_KEY: ${MINIO_ROOT_PASSWORD:?set in .env}
0098 |     volumes:
0099 |       - .:/app
0100 |     depends_on:
0101 |       db:
0102 |         condition: service_healthy
0103 |       broker:
0104 |         condition: service_healthy
0105 | 
0106 |   spark-master:
0107 |     image: apache/spark:3.5.1
0108 |     container_name: spark-master
0109 |     hostname: spark-master
0110 |     user: root
0111 |     environment:
0112 |       - SPARK_NO_DAEMONIZE=true
0113 |     command: >
0114 |       /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
0115 |       --host spark-master
0116 |       --port 7077
0117 |       --webui-port 8080
0118 |     ports:
0119 |       - "127.0.0.1:7077:7077"
0120 |       - "127.0.0.1:8080:8080"
0121 |     volumes:
0122 |       - .:/opt/spark/app
0123 | 
0124 |   spark-worker:
0125 |     image: apache/spark:3.5.1
0126 |     container_name: spark-worker
0127 |     hostname: spark-worker
0128 |     user: root
0129 |     depends_on:
0130 |       - spark-master
0131 |     environment:
0132 |       - SPARK_NO_DAEMONIZE=true
0133 |     command: >
0134 |       /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker
0135 |       --cores 1
0136 |       --memory 1G
0137 |       --webui-port 8081
0138 |       spark://spark-master:7077
0139 |     ports:
0140 |       - "127.0.0.1:8081:8081"
0141 |     volumes:
0142 |       - .:/opt/spark/app
0143 |       - spark-checkpoints:/tmp/checkpoints
0144 | 
0145 |   minio:
0146 |     image: cataclysm-minio:source-2025-10-15
0147 |     build:
0148 |       context: ./infra/minio
0149 |     container_name: minio
0150 |     volumes:
0151 |       - minio-data:/data
0152 |     ports:
0153 |       - "127.0.0.1:${MINIO_API_PORT:-19000}:9000"
0154 |       - "127.0.0.1:${MINIO_CONSOLE_PORT:-19001}:9001"
0155 |     environment:
0156 |       MINIO_ROOT_USER: ${MINIO_ROOT_USER:?set in .env}
0157 |       MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:?set in .env}
0158 |     command: server /data --console-address ":9001"
0159 | 
0160 |   minio-init:
0161 |     image: catalcysm-fastapi:demo
0162 |     build: .
0163 |     container_name: minio-init
0164 |     depends_on:
0165 |       - minio
0166 |     command: python -m src.storage.init_buckets
0167 |     environment:
0168 |       MINIO_ENDPOINT: minio:9000
0169 |       MINIO_ACCESS_KEY: ${MINIO_ROOT_USER:?set in .env}
0170 |       MINIO_SECRET_KEY: ${MINIO_ROOT_PASSWORD:?set in .env}
0171 | 
0172 |   spark-streaming:
0173 |     restart: unless-stopped
0174 |     image: apache/spark:3.5.1
0175 |     container_name: spark-streaming
0176 |     user: root
0177 |     depends_on:
0178 |       broker: {condition: service_healthy}
0179 |       minio-init: {condition: service_completed_successfully}
0180 |       spark-master: {condition: service_started}
0181 |     environment:
0182 |       SPARK_LOCAL_HOSTNAME: spark-streaming
0183 |       KAFKA_BOOTSTRAP_SERVERS: broker:9092
0184 |       SPARK_MASTER_URL: spark://spark-master:7077
0185 |       MINIO_ENDPOINT: minio:9000
0186 |       MINIO_ACCESS_KEY: ${MINIO_ROOT_USER:?set in .env}
0187 |       MINIO_SECRET_KEY: ${MINIO_ROOT_PASSWORD:?set in .env}
0188 |     command: >
0189 |       /opt/spark/bin/spark-submit --master spark://spark-master:7077
0190 |       --driver-memory 1536m
0191 |       --conf spark.driver.host=spark-streaming --conf spark.driver.bindAddress=0.0.0.0
0192 |       --conf spark.executor.memory=512m --conf spark.cores.max=1
0193 |       --conf spark.jars.ivy=/tmp/ivy
0194 |       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4
0195 |       /opt/spark/app/src/spark/streaming_job.py
0196 |     volumes:
0197 |       - .:/opt/spark/app
0198 |       - spark-checkpoints:/tmp/checkpoints
0199 |       - spark-ivy:/tmp/ivy
0200 | 
0201 |   prometheus:
0202 |     image: prom/prometheus:v3.0.1
0203 |     ports: ["127.0.0.1:9090:9090"]
0204 |     volumes:
0205 |       - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
0206 |       - ./monitoring/alerts.yml:/etc/prometheus/alerts.yml:ro
0207 |       - prometheus-data:/prometheus
0208 | 
0209 |   exporter:
0210 |     image: catalcysm-fastapi:demo
0211 |     build: .
0212 |     command: python -m src.common.exporter
0213 |     restart: on-failure
0214 |     environment:
0215 |       DATABASE_URL: postgresql://cataclysm:${POSTGRES_PASSWORD:?set in .env}@db:5432/cataclysm
0216 |       KAFKA_BOOTSTRAP_SERVERS: broker:9092
0217 |       KAFKA_GROUP_ID: crisis-dashboard-group
0218 |       MINIO_ENDPOINT: minio:9000
0219 |       MINIO_ACCESS_KEY: ${MINIO_ROOT_USER:?set in .env}
0220 |       MINIO_SECRET_KEY: ${MINIO_ROOT_PASSWORD:?set in .env}
0221 |     volumes: [.:/app]
0222 | 
0223 |   anomaly-worker:
0224 |     image: catalcysm-fastapi:demo
0225 |     build: .
0226 |     command: python -m src.ml.detect_anomalies --interval 300
0227 |     restart: on-failure
0228 |     depends_on:
0229 |       db: {condition: service_healthy}
0230 |     environment:
0231 |       DATABASE_URL: postgresql://cataclysm:${POSTGRES_PASSWORD:?set in .env}@db:5432/cataclysm
0232 |     volumes: [.:/app]
0233 | 
0234 |   grafana:
0235 |     image: grafana/grafana:11.4.0
0236 |     ports: ["127.0.0.1:3000:3000"]
0237 |     environment:
0238 |       GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:?set in .env}
0239 |     volumes:
0240 |       - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
0241 |       - grafana-data:/var/lib/grafana
0242 | 
0243 | volumes:
0244 |   postgres-data:
0245 |   kafka-data:
0246 |   minio-data:
0247 |   spark-checkpoints:
0248 |   spark-ivy:
0249 |   prometheus-data:
0250 |   grafana-data:
```

### `Dockerfile`

[Open source](../../Dockerfile) · SHA-256 `f4259d9e8396399af4d937b592cbdc1b4663d96dcdcd46c578a075bd50ee1e23`


```text
0001 | FROM python:3.11-slim
0002 | 
0003 | WORKDIR /app
0004 | 
0005 | ENV PYTHONUNBUFFERED=1 \
0006 |     PYTHONPATH=/app \
0007 |     PYTHONDONTWRITEBYTECODE=1
0008 | 
0009 | # Install curl for healthchecks
0010 | RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
0011 | 
0012 | COPY requirements.txt .
0013 | COPY requirements.lock.txt .
0014 | RUN pip install --no-cache-dir -r requirements.lock.txt
0015 | 
0016 | COPY . .
0017 | 
0018 | EXPOSE 8000
0019 | 
0020 | CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### `infra/minio/Dockerfile`

[Open source](../../infra/minio/Dockerfile) · SHA-256 `6b23ce0551ebaec5b70dfb7ed84eee8dcc0d9039b5bbf6afdbe54c7b9549ae83`


```text
0001 | FROM golang:1.24-bookworm AS build
0002 | ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
0003 | RUN git clone --filter=blob:none https://github.com/minio/minio.git /source
0004 | WORKDIR /source
0005 | RUN git checkout ${MINIO_COMMIT}
0006 | ENV CGO_ENABLED=0 GOMAXPROCS=2
0007 | RUN go build -p 2 -trimpath -o /out/minio .
0008 | 
0009 | FROM debian:bookworm-slim
0010 | RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
0011 | COPY --from=build /out/minio /usr/local/bin/minio
0012 | COPY --from=build /source/LICENSE /usr/share/doc/minio/LICENSE
0013 | EXPOSE 9000 9001
0014 | ENTRYPOINT ["minio"]
```

### `requirements.txt`

[Open source](../../requirements.txt) · SHA-256 `e059cea3ac35f08862bfa6303c0c255fd65f32fb14c91f30ab483c4827412cc8`


```text
0001 | fastapi>=0.115.0
0002 | uvicorn[standard]>=0.31.0
0003 | pydantic>=2.9.2
0004 | confluent-kafka>=2.6.0
0005 | psycopg[binary]>=3.2.3
0006 | psycopg-pool>=3.2.3
0007 | requests>=2.32.3
0008 | pyarrow>=17.0.0
0009 | scikit-learn>=1.5.2
0010 | joblib>=1.4.2
0011 | numpy>=2.0.0
0012 | pandas>=2.2.0
0013 | boto3>=1.35.0
0014 | prometheus-fastapi-instrumentator>=7.0.0
0015 | prometheus-client>=0.21.0
0016 | python-dotenv>=1.2.4
```

### `requirements.lock.txt`

[Open source](../../requirements.lock.txt) · SHA-256 `2386fd94a0a75fcbb240866436dedf1b7759bf46a005ed9d684bdfb9d9dec5bd`


```text
0001 | annotated-doc==0.0.5
0002 | annotated-types==0.8.0
0003 | anyio==4.16.0
0004 | boto3==1.43.124
0005 | botocore==1.43.124
0006 | certifi==2026.10.23
0007 | charset-normalizer==3.5.2
0008 | click==8.5.0
0009 | cloudpickle==3.1.2
0010 | confluent-kafka==2.16.1
0011 | fastapi==0.162.0
0012 | h11==0.16.0
0013 | httptools==0.9.0
0014 | idna==3.20
0015 | jmespath==1.1.0
0016 | joblib==1.6.0
0017 | narwhals==2.28.0
0018 | numpy==2.4.6
0019 | opentelemetry-api==1.45.2
0020 | packaging==26.3
0021 | pandas==3.1.1
0022 | prometheus-fastapi-instrumentator==8.1.0
0023 | prometheus_client==0.26.0
0024 | psycopg==3.3.8
0025 | psycopg-binary==3.3.8
0026 | psycopg-pool==3.3.3
0027 | pyarrow==26.0.1
0028 | pydantic==2.14.2
0029 | pydantic_core==2.50.2
0030 | python-dateutil==2.9.0.post0
0031 | python-dotenv==1.2.4
0032 | PyYAML==6.0.3
0033 | requests==2.34.2
0034 | s3transfer==0.19.3
0035 | scikit-learn==1.9.1
0036 | scipy==1.17.1
0037 | six==1.17.0
0038 | starlette==1.10.0
0039 | threadpoolctl==3.7.0
0040 | typing-inspection==0.4.5
0041 | typing_extensions==4.16.0
0042 | urllib3==2.8.0
0043 | uvicorn==0.54.0
0044 | uvloop==0.23.0
0045 | watchfiles==1.3.0
0046 | websockets==17.4
```

### `scripts/prepare_env.py`

[Open source](../../scripts/prepare_env.py) · SHA-256 `041ad410cfd9fcd049af3bfeee6af8457f92951faebb6f50dbc179c72f262357`

Dependencies: `import argparse`; `import os`; `from pathlib import Path`; `import secrets`

- `main`: lines 9–37. Read its control flow with the module guide above.


```text
0001 | """Create local secrets once; optionally rotate an existing development DB role."""
0002 | 
0003 | import argparse
0004 | import os
0005 | from pathlib import Path
0006 | import secrets
0007 | 
0008 | 
0009 | def main() -> None:
0010 |     parser = argparse.ArgumentParser()
0011 |     parser.add_argument("--rotate-existing-db", action="store_true")
0012 |     args = parser.parse_args()
0013 |     target = Path(__file__).resolve().parents[1] / ".env"
0014 |     if target.exists():
0015 |         raise SystemExit(".env already exists; preserved it")
0016 |     values = {
0017 |         "POSTGRES_PASSWORD": secrets.token_hex(24),
0018 |         "MINIO_ROOT_USER": "cataclysm-local",
0019 |         "MINIO_ROOT_PASSWORD": secrets.token_hex(24),
0020 |         "GRAFANA_ADMIN_PASSWORD": secrets.token_hex(24),
0021 |         "OPENWEATHER_API_KEY": "",
0022 |     }
0023 |     if args.rotate_existing_db:
0024 |         import psycopg
0025 |         from psycopg import sql
0026 | 
0027 |         with psycopg.connect(os.environ["CURRENT_DATABASE_URL"]) as conn:
0028 |             conn.execute(
0029 |                 sql.SQL("ALTER ROLE cataclysm PASSWORD {}").format(
0030 |                     sql.Literal(values["POSTGRES_PASSWORD"])
0031 |                 )
0032 |             )
0033 |     with target.open("x", encoding="utf-8") as stream:
0034 |         stream.write(
0035 |             "\n".join(f"{key}={value}" for key, value in values.items()) + "\n"
0036 |         )
0037 |     print("Created ignored .env with random local secrets; weather key is empty")
0038 | 
0039 | 
0040 | if __name__ == "__main__":
0041 |     main()
```
