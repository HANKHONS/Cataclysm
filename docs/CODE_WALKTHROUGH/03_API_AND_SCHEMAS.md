# API and schemas: an acknowledgment is a precise boundary

## Typed input and metadata

`BaseModel` provides parsing and validation, `Field` supplies constraints/descriptions, `Literal` restricts severity and `field_validator` normalizes time. `Metadata` keeps supported telemetry, provider fields, synthetic/replay flags, location accuracy and severity method; NaN/infinity are disallowed. Missing telemetry remains `None`. `CrisisEvent` bounds IDs/text/location/hazard strings and coordinates; its timestamp validator makes legacy naive input explicitly UTC and converts aware input to UTC. Pydantic defaults ignore unsupported extra metadata: this is a defined supported schema, not arbitrary JSON preservation. Extend it deliberately if a new source field must survive archival.

`EnrichedCrisisEvent` documents prediction fields but the consumer currently enriches dictionaries directly, so this class is not evidence of a separate executed stage. `DLQMessage` records diagnostic bytes/error/context with an aware UTC failure timestamp.

## Application setup and synchronous work

`FastAPI` registers path-operation decorators and OpenAPI contracts. Instrumentator wraps actual requests and exposes `/metrics`. CORS lists allowed origins; it is a browser policy, not authentication or authorization. `/static` serves the three frontend assets; `/handbook` and `/documentation` serve intended learning material. Module-level Producer construction shares one client across worker-thread routes. The ordinary `def` handlers run through FastAPI's threadpool, keeping blocking psycopg/Kafka calls outside the async event loop.

Startup attempts schema initialization and logs failure without exposing connection secrets; a degraded API can start but `/ready` returns failure. Shutdown flushes producer briefly and closes the DB pool. Existing `on_event` hooks are deprecated in the installed FastAPI release; they still work and emit warnings. A future lifespan migration is maintenance, not a current behavioral test failure.

## Producer handler, small logical blocks

`ingest_event` creates a per-request threading Event and delivery-result dictionary. The nested callback records error or partition/offset and sets the event. Serialization uses Pydantic JSON mode, then UTF-8; location bytes become the key. `produce` can raise buffer/Kafka exceptions immediately and returns 503 in that case. `poll(0)` serves already-ready callbacks. A monotonic deadline avoids wall-clock jumps; polling in bounded slices services the callback until success/deadline. A confirmed callback returns the original ID/partition/offset; otherwise the route logs an identity-level error and returns 503. This route performs no DB or direct UI insert. Its 2.5-second deadline is not a guarantee that a timed-out queued record will never arrive.

`healthcheck` probes broker metadata and `SELECT 1`, returning connected/degraded fields. `/live` answers process liveness; `/ready` maps dependency failure to HTTP 503. `serve_dashboard` returns the static index. `get_models` reports cached-model provenance; `get_anomalies` reads completed-window records. `/incidents` remains a bounded alias of recent events for compatibility.

## Query builder and query routes

`event_filter` builds only fixed-column predicates; all user values become parameters. Search spans text/location/ID using `ILIKE` and wildcard matching. `%`/`_` in search remain SQL pattern wildcards, not arbitrary SQL injection. Since is inclusive, until exclusive; naive query times follow the documented UTC convention. A bbox is west,south,east,north and must be ordered/in range; antimeridian boxes require two queries. Invalid syntax produces 422 before storage access. `read_rows` acquires a pooled connection and converts storage failures into 503. `provenance` checks replay first, then known synthetic flags/sources, then provider names; unknown sources are unverified. It serializes timestamps including created_at so SSE fingerprints remain JSON-safe.

`get_recent_events` selects operational columns and metadata through a left join on the original report. A stable timestamp/ID order supports newest/oldest pagination; limit is 1–500 and offset bounded. Results remain a list to preserve the existing API contract. The overview applies the same filters but counts every matching row, groups severity/hazard/source, returns source latest-event times, hourly event-time counts and up to 1,000 hourly hazard/severity trend groups. Its recent-persistence field counts unique IDs created in the last 60 seconds inside the selected filters. It does not measure broker throughput. Multiple aggregate SQL statements are not a repeatable-read snapshot and can differ momentarily under concurrent ingestion.

`event_details` reads one ID, returns 404 when absent, adds per-model prediction rows and original metadata. It is the form's persistence check. `pipeline_metrics` makes four bounded Prometheus instant queries and returns actual finite samples or null. A null value means unavailable, never a made-up zero. The legacy health route is separate so monitoring-server failure does not itself declare the API's DB/broker dead.

## SSE generator and cost

The async `events_stream` returns a `StreamingResponse` over a nested async generator. It first emits `connected`, then offloads a latest-500 query with `asyncio.to_thread`, reverses rows to emit older-before-newer, compares JSON fingerprints by ID, emits changed rows and a heartbeat, then awaits two seconds. Storage HTTP errors become a specific storage-error SSE event; cancellation exits cleanly. The previous fingerprint map is replaced, so it stays bounded to the current query window.

Per client the polling query/encoding costs roughly O(500 × payload size) every two seconds plus DB sorting/join; many clients duplicate that work. There is no shared durable cursor or Last-Event-ID resume. Browser refresh supplies source-of-truth reconciliation. Validate changes through callback/schema/query unit tests, the live API verifier and browser interactions; a parsed health response alone is insufficient.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `src/common/models.py`

[Open source](../../src/common/models.py) · SHA-256 `52c0a718c6a41bab02288e6b00c136e72300cc77b4a5a3ea4a21b5d267564c98`

Dependencies: `from datetime import datetime, timezone`; `from typing import Literal, Optional`; `from pydantic import BaseModel, ConfigDict, Field, field_validator`

- `Metadata`: lines 6–30. Read its control flow with the module guide above.

- `CrisisEvent`: lines 33–87. Read its control flow with the module guide above.

- `normalize_timestamp`: lines 38–42. Legacy naive timestamps mean UTC; normalize aware inputs to UTC.

- `EnrichedCrisisEvent`: lines 90–100. Read its control flow with the module guide above.

- `DLQMessage`: lines 103–110. Read its control flow with the module guide above.


```text
0001 | from datetime import datetime, timezone
0002 | from typing import Literal, Optional
0003 | from pydantic import BaseModel, ConfigDict, Field, field_validator
0004 | 
0005 | 
0006 | class Metadata(BaseModel):
0007 |     model_config = ConfigDict(allow_inf_nan=False)
0008 |     magnitude: Optional[float] = None
0009 |     depth_km: Optional[float] = None
0010 |     provider_url: Optional[str] = None
0011 |     is_synthetic: bool = False
0012 |     is_replay: bool = False
0013 |     location_accuracy: Optional[str] = None
0014 |     severity_method: Optional[str] = None
0015 |     measurement_kind: Optional[str] = None
0016 |     precipitation_mm: Optional[float] = None
0017 |     precipitation_interval_seconds: Optional[int] = None
0018 |     attribution: Optional[str] = None
0019 |     rainfall_mm: Optional[float] = Field(
0020 |         default=None, description="Rainfall in millimeters"
0021 |     )
0022 |     water_level_m: Optional[float] = Field(
0023 |         default=None, description="Water level in meters"
0024 |     )
0025 |     wind_speed_kmh: Optional[float] = Field(
0026 |         default=None, description="Wind speed in km/h"
0027 |     )
0028 |     temperature_c: Optional[float] = Field(
0029 |         default=None, description="Temperature in Celsius"
0030 |     )
0031 | 
0032 | 
0033 | class CrisisEvent(BaseModel):
0034 |     model_config = ConfigDict(allow_inf_nan=False)
0035 | 
0036 |     @field_validator("timestamp")
0037 |     @classmethod
0038 |     def normalize_timestamp(cls, value: datetime) -> datetime:
0039 |         """Legacy naive timestamps mean UTC; normalize aware inputs to UTC."""
0040 |         if value.tzinfo is None:
0041 |             value = value.replace(tzinfo=timezone.utc)
0042 |         return value.astimezone(timezone.utc)
0043 | 
0044 |     event_id: str = Field(
0045 |         ...,
0046 |         min_length=1,
0047 |         max_length=50,
0048 |         pattern=r"^[A-Za-z0-9_.-]+$",
0049 |         description="Unique event identifier (UUID or hash)",
0050 |     )
0051 |     timestamp: datetime = Field(
0052 |         ..., description="Timestamp of the incident in ISO format"
0053 |     )
0054 |     text: str = Field(
0055 |         ...,
0056 |         min_length=1,
0057 |         max_length=10000,
0058 |         description="Descriptive narrative of the disaster event",
0059 |     )
0060 |     location: str = Field(
0061 |         ..., min_length=1, max_length=100, description="City or region name"
0062 |     )
0063 |     disaster_type: str = Field(
0064 |         ...,
0065 |         pattern=r"^[a-z_]+$",
0066 |         max_length=50,
0067 |         description="flood, earthquake, cyclone, fire, infrastructure, weather, or unknown",
0068 |     )
0069 |     latitude: float = Field(..., ge=-90.0, le=90.0, description="Geographic latitude")
0070 |     longitude: float = Field(
0071 |         ..., ge=-180.0, le=180.0, description="Geographic longitude"
0072 |     )
0073 |     severity: Literal["minor", "moderate", "severe", "critical", "unknown"] = Field(
0074 |         ...,
0075 |         min_length=1,
0076 |         max_length=20,
0077 |         description="minor, moderate, severe, or critical",
0078 |     )
0079 |     source: str = Field(
0080 |         ...,
0081 |         min_length=1,
0082 |         max_length=50,
0083 |         description="citizen_report, weather_station, usgs_feed, or iot_sensor",
0084 |     )
0085 |     metadata: Metadata = Field(
0086 |         default_factory=Metadata, description="Physical and weather telemetry"
0087 |     )
0088 | 
0089 | 
0090 | class EnrichedCrisisEvent(CrisisEvent):
0091 |     predicted_disaster_type: Optional[str] = Field(
0092 |         default=None, description="ML-predicted disaster classification"
0093 |     )
0094 |     predicted_severity: Optional[str] = Field(
0095 |         default=None, description="ML-predicted severity category"
0096 |     )
0097 |     prediction_confidence: Optional[float] = Field(
0098 |         default=None,
0099 |         description="Uncalibrated classifier score, not a probability of correctness",
0100 |     )
0101 | 
0102 | 
0103 | class DLQMessage(BaseModel):
0104 |     raw_payload: str
0105 |     error_type: str
0106 |     error_message: str
0107 |     failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
0108 |     source_topic: str
0109 |     source_partition: Optional[int] = None
0110 |     source_offset: Optional[int] = None
```

### `src/api/main.py`

[Open source](../../src/api/main.py) · SHA-256 `61e21dc719d6a1cfceca5aa1f0eca818b6ae57034453c26c37a240a4d8e68fab`

Dependencies: `import os`; `import json`; `import time`; `import asyncio`; `import logging`; `from typing import Annotated, Literal, Optional, AsyncGenerator`; `from datetime import datetime, timezone`; `from threading import Event`; `from fastapi import FastAPI, HTTPException, Query, status`; `from fastapi.responses import FileResponse, StreamingResponse`; `from fastapi.middleware.cors import CORSMiddleware`; `from fastapi.staticfiles import StaticFiles`; `from confluent_kafka import Producer, KafkaException`; `from src.common.config import config`; `from src.common.models import CrisisEvent`; `from src.common.db import get_pool, init_db, close_pool`; `from src.ml.inference import model_status`; `from src.api.queries import event_filter, read_rows, provenance`; `from prometheus_fastapi_instrumentator import Instrumentator`

- `startup_event`: lines 68–73. Read its control flow with the module guide above.

- `shutdown_event`: lines 77–79. Read its control flow with the module guide above.

- `serve_dashboard`: lines 83–87. Serves the interactive crisis operations dashboard.

- `get_models`: lines 91–92. Read its control flow with the module guide above.

- `get_anomalies`: lines 96–99. Read its control flow with the module guide above.

- `get_incidents`: lines 103–104. Read its control flow with the module guide above.

- `healthcheck`: lines 108–131. Comprehensive health probe verifying PostgreSQL and Kafka availability.

- `liveness`: lines 135–136. Read its control flow with the module guide above.

- `readiness`: lines 140–144. Read its control flow with the module guide above.

- `ingest_event`: lines 148–206. Ingests disaster event telemetry and produces to Kafka with delivery acknowledgment.

- `on_delivery`: lines 161–169. Read its control flow with the module guide above.

- `get_recent_events`: lines 210–234. Bounded rows, stable ordering and original metadata; totals use /overview.

- `get_overview`: lines 238–290. Aggregate all matching persisted reports, independently of feed pagination.

- `event_details`: lines 294–305. Read its control flow with the module guide above.

- `pipeline_metrics`: lines 309–339. Read actual Prometheus samples; unavailable metrics remain null.

- `events_stream`: lines 343–375. Server-Sent Events (SSE) live stream endpoint for real-time dashboard updates.

- `event_generator`: lines 346–365. Read its control flow with the module guide above.


```text
0001 | import os
0002 | import json
0003 | import time
0004 | import asyncio
0005 | import logging
0006 | from typing import Annotated, Literal, Optional, AsyncGenerator
0007 | from datetime import datetime, timezone
0008 | from threading import Event
0009 | 
0010 | from fastapi import FastAPI, HTTPException, Query, status
0011 | from fastapi.responses import FileResponse, StreamingResponse
0012 | from fastapi.middleware.cors import CORSMiddleware
0013 | from fastapi.staticfiles import StaticFiles
0014 | from confluent_kafka import Producer, KafkaException
0015 | 
0016 | from src.common.config import config
0017 | from src.common.models import CrisisEvent
0018 | from src.common.db import get_pool, init_db, close_pool
0019 | from src.ml.inference import model_status
0020 | from src.api.queries import event_filter, read_rows, provenance
0021 | from prometheus_fastapi_instrumentator import Instrumentator
0022 | 
0023 | logging.basicConfig(
0024 |     level=logging.INFO, format="%(asctime)s [%(levelname)s] (API) %(message)s"
0025 | )
0026 | logger = logging.getLogger("cataclysm.api")
0027 | 
0028 | app = FastAPI(
0029 |     title="Cataclysm Crisis Intelligence API",
0030 |     description="Real-Time Disaster Telemetry Ingestion, Streaming & Intelligence Platform",
0031 |     version="1.0.0",
0032 | )
0033 | Instrumentator().instrument(app).expose(
0034 |     app, endpoint="/metrics", include_in_schema=False
0035 | )
0036 | 
0037 | app.add_middleware(
0038 |     CORSMiddleware,
0039 |     allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "http://localhost:8000").split(","),
0040 |     allow_credentials=False,
0041 |     allow_methods=["*"],
0042 |     allow_headers=["*"],
0043 | )
0044 | 
0045 | STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
0046 | STATIC_INDEX = os.path.join(STATIC_DIR, "index.html")
0047 | app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
0048 | DOCS_DIR = os.path.abspath(os.path.join(STATIC_DIR, "..", "..", "..", "docs"))
0049 | app.mount(
0050 |     "/handbook",
0051 |     StaticFiles(directory=os.path.join(DOCS_DIR, "handbook"), html=True),
0052 |     name="handbook",
0053 | )
0054 | app.mount("/documentation", StaticFiles(directory=DOCS_DIR), name="documentation")
0055 | 
0056 | # Kafka Producer with Acknowledgment
0057 | producer = Producer(
0058 |     {
0059 |         "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
0060 |         "acks": "all",  # Strongest durability guarantee
0061 |         "retries": 3,
0062 |         "retry.backoff.ms": 100,
0063 |     }
0064 | )
0065 | 
0066 | 
0067 | @app.on_event("startup")
0068 | def startup_event():
0069 |     try:
0070 |         init_db()
0071 |         logger.info("Database initialized on startup.")
0072 |     except Exception as e:
0073 |         logger.warning(f"Database init warning (will retry on query): {e}")
0074 | 
0075 | 
0076 | @app.on_event("shutdown")
0077 | def shutdown_event():
0078 |     producer.flush(timeout=3.0)
0079 |     close_pool()
0080 | 
0081 | 
0082 | @app.get("/")
0083 | def serve_dashboard():
0084 |     """Serves the interactive crisis operations dashboard."""
0085 |     if os.path.exists(STATIC_INDEX):
0086 |         return FileResponse(STATIC_INDEX)
0087 |     return {"message": "Cataclysm Disaster API is online. Static dashboard not found."}
0088 | 
0089 | 
0090 | @app.get("/api/models")
0091 | def get_models():
0092 |     return model_status()
0093 | 
0094 | 
0095 | @app.get("/api/anomalies")
0096 | def get_anomalies():
0097 |     return read_rows(
0098 |         "SELECT * FROM anomaly_windows ORDER BY window_start DESC LIMIT 100"
0099 |     )
0100 | 
0101 | 
0102 | @app.get("/incidents")
0103 | def get_incidents(limit: int = Query(default=50, ge=1, le=500)):
0104 |     return get_recent_events(limit=limit)
0105 | 
0106 | 
0107 | @app.get("/health")
0108 | def healthcheck():
0109 |     """Comprehensive health probe verifying PostgreSQL and Kafka availability."""
0110 |     db_ok = False
0111 |     kafka_ok = False
0112 |     try:
0113 |         kafka_ok = bool(producer.list_topics(timeout=2).brokers)
0114 |     except Exception:
0115 |         pass
0116 |     try:
0117 |         pool = get_pool()
0118 |         with pool.connection() as conn:
0119 |             with conn.cursor() as cur:
0120 |                 cur.execute("SELECT 1;")
0121 |                 db_ok = True
0122 |     except Exception as e:
0123 |         logger.debug(f"Health DB probe failed: {e}")
0124 | 
0125 |     return {
0126 |         "status": "healthy" if db_ok and kafka_ok else "degraded",
0127 |         "database": "connected" if db_ok else "unreachable",
0128 |         "broker": config.KAFKA_BOOTSTRAP_SERVERS,
0129 |         "kafka_connected": kafka_ok,
0130 |         "timestamp": datetime.now(timezone.utc).isoformat(),
0131 |     }
0132 | 
0133 | 
0134 | @app.get("/live")
0135 | def liveness():
0136 |     return {"status": "alive"}
0137 | 
0138 | 
0139 | @app.get("/ready")
0140 | def readiness():
0141 |     result = healthcheck()
0142 |     if result["status"] != "healthy":
0143 |         raise HTTPException(503, "Database or broker unavailable")
0144 |     return result
0145 | 
0146 | 
0147 | @app.post("/events", status_code=status.HTTP_200_OK)
0148 | def ingest_event(event: CrisisEvent):
0149 |     """
0150 |     Ingests disaster event telemetry and produces to Kafka with delivery acknowledgment.
0151 |     Does NOT return 200 OK until Kafka confirms persistence on the broker partition.
0152 |     """
0153 |     delivery_event = Event()
0154 |     delivery_result = {
0155 |         "success": False,
0156 |         "partition": None,
0157 |         "offset": None,
0158 |         "error": None,
0159 |     }
0160 | 
0161 |     def on_delivery(err, msg):
0162 |         if err is not None:
0163 |             delivery_result["error"] = str(err)
0164 |             delivery_result["success"] = False
0165 |         else:
0166 |             delivery_result["success"] = True
0167 |             delivery_result["partition"] = msg.partition()
0168 |             delivery_result["offset"] = msg.offset()
0169 |         delivery_event.set()
0170 | 
0171 |     # Serialize event
0172 |     event_dict = event.model_dump(mode="json")
0173 |     payload = json.dumps(event_dict).encode("utf-8")
0174 |     key = event.location.encode("utf-8")
0175 | 
0176 |     try:
0177 |         producer.produce(
0178 |             topic=config.KAFKA_TOPIC, key=key, value=payload, on_delivery=on_delivery
0179 |         )
0180 |         producer.poll(0)
0181 |     except (KafkaException, BufferError) as ke:
0182 |         raise HTTPException(
0183 |             status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
0184 |             detail=f"Kafka producer buffer error: {ke}",
0185 |         )
0186 | 
0187 |     # Bounded wait for broker delivery confirmation (2.0s timeout)
0188 |     deadline = time.monotonic() + 2.5
0189 |     while not delivery_event.is_set() and time.monotonic() < deadline:
0190 |         producer.poll(min(0.1, max(0.0, deadline - time.monotonic())))
0191 |     confirmed = delivery_event.is_set()
0192 | 
0193 |     if not confirmed or not delivery_result["success"]:
0194 |         err_msg = delivery_result.get("error") or "Broker acknowledgment timeout"
0195 |         logger.error(f"Failed delivery for event {event.event_id}: {err_msg}")
0196 |         raise HTTPException(
0197 |             status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
0198 |             detail=f"Message delivery to Kafka failed: {err_msg}",
0199 |         )
0200 | 
0201 |     return {
0202 |         "status": "confirmed",
0203 |         "event_id": event.event_id,
0204 |         "partition": delivery_result["partition"],
0205 |         "offset": delivery_result["offset"],
0206 |     }
0207 | 
0208 | 
0209 | @app.get("/api/events")
0210 | def get_recent_events(
0211 |     limit: Annotated[int, Query(ge=1, le=500)] = 50,
0212 |     disaster_type: Optional[str] = None,
0213 |     severity: Optional[str] = None,
0214 |     source: Optional[str] = None,
0215 |     q: Annotated[Optional[str], Query(max_length=200)] = None,
0216 |     since: Optional[datetime] = None,
0217 |     until: Optional[datetime] = None,
0218 |     bbox: Optional[str] = None,
0219 |     offset: Annotated[int, Query(ge=0, le=100000)] = 0,
0220 |     sort: Literal["newest", "oldest"] = "newest",
0221 | ):
0222 |     """Bounded rows, stable ordering and original metadata; totals use /overview."""
0223 |     predicate, params = event_filter(
0224 |         disaster_type, severity, source, q, since, until, bbox
0225 |     )
0226 |     direction = "DESC" if sort == "newest" else "ASC"
0227 |     rows = read_rows(
0228 |         "SELECT e.*, COALESCE(r.payload->'metadata', '{}'::jsonb) AS metadata "
0229 |         "FROM crisis_events e LEFT JOIN reports r USING(event_id)"
0230 |         + predicate
0231 |         + f" ORDER BY e.timestamp {direction}, e.event_id {direction} LIMIT %s OFFSET %s",
0232 |         params + [limit, offset],
0233 |     )
0234 |     return [provenance(row) for row in rows]
0235 | 
0236 | 
0237 | @app.get("/api/overview")
0238 | def get_overview(
0239 |     disaster_type: Optional[str] = None,
0240 |     severity: Optional[str] = None,
0241 |     source: Optional[str] = None,
0242 |     q: Annotated[Optional[str], Query(max_length=200)] = None,
0243 |     since: Optional[datetime] = None,
0244 |     until: Optional[datetime] = None,
0245 |     bbox: Optional[str] = None,
0246 | ):
0247 |     """Aggregate all matching persisted reports, independently of feed pagination."""
0248 |     predicate, params = event_filter(
0249 |         disaster_type, severity, source, q, since, until, bbox
0250 |     )
0251 |     base = " FROM crisis_events e" + predicate
0252 |     total = read_rows(
0253 |         "SELECT COUNT(*) AS total, MAX(timestamp) AS latest_event, MAX(created_at) AS latest_ingested, "
0254 |         "COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1 minute') AS ingested_last_minute"
0255 |         + base,
0256 |         params,
0257 |     )[0]
0258 |     for name, column in (
0259 |         ("severity", "severity"),
0260 |         ("categories", "disaster_type"),
0261 |         ("sources", "source"),
0262 |     ):
0263 |         total[name] = read_rows(
0264 |             f"SELECT {column} AS label, COUNT(*) AS count, MAX(timestamp) AS latest_event"
0265 |             + base
0266 |             + f" GROUP BY {column} ORDER BY count DESC",
0267 |             params,
0268 |         )
0269 |     total["timeline"] = read_rows(
0270 |         "SELECT date_trunc('hour', timestamp) AS hour, COUNT(*) AS count"
0271 |         + base
0272 |         + " GROUP BY hour ORDER BY hour DESC LIMIT 168",
0273 |         params,
0274 |     )
0275 |     total["trends"] = read_rows(
0276 |         "SELECT date_trunc('hour', timestamp) AS hour, disaster_type, severity, COUNT(*) AS count"
0277 |         + base
0278 |         + " GROUP BY hour, disaster_type, severity ORDER BY hour DESC, disaster_type, severity LIMIT 1000",
0279 |         params,
0280 |     )
0281 |     total["scope"] = {
0282 |         "since": since,
0283 |         "until": until,
0284 |         "timeline_bin": "1 hour",
0285 |         "timeline_max_bins": 168,
0286 |     }
0287 |     total["rate_note"] = (
0288 |         "Persisted unique rows created in the last minute within selected event filters; not broker throughput."
0289 |     )
0290 |     return total
0291 | 
0292 | 
0293 | @app.get("/api/events/{event_id}/details")
0294 | def event_details(event_id: str):
0295 |     rows = read_rows(
0296 |         "SELECT e.*, COALESCE(r.payload->'metadata', '{}'::jsonb) AS metadata FROM crisis_events e LEFT JOIN reports r USING(event_id) WHERE e.event_id=%s",
0297 |         [event_id],
0298 |     )
0299 |     if not rows:
0300 |         raise HTTPException(404, "Event not found")
0301 |     result = provenance(rows[0])
0302 |     result["predictions"] = read_rows(
0303 |         "SELECT * FROM predictions WHERE event_id=%s ORDER BY model_name", [event_id]
0304 |     )
0305 |     return result
0306 | 
0307 | 
0308 | @app.get("/api/pipeline")
0309 | def pipeline_metrics():
0310 |     """Read actual Prometheus samples; unavailable metrics remain null."""
0311 |     import requests
0312 | 
0313 |     expressions = {
0314 |         "consumer_lag": "sum(cataclysm_consumer_lag)",
0315 |         "spark_progress_age_seconds": "time() - min(cataclysm_spark_progress_epoch)",
0316 |         "spark_batch_duration_ms": "max(cataclysm_spark_batch_duration_ms)",
0317 |         "http_requests_per_second": "sum(rate(http_requests_total[5m]))",
0318 |     }
0319 |     values = {}
0320 |     for name, expression in expressions.items():
0321 |         try:
0322 |             response = requests.get(
0323 |                 os.getenv("PROMETHEUS_URL", "http://prometheus:9090") + "/api/v1/query",
0324 |                 params={"query": expression},
0325 |                 timeout=1,
0326 |             )
0327 |             response.raise_for_status()
0328 |             samples = response.json()["data"]["result"]
0329 |             value = float(samples[0]["value"][1]) if samples else None
0330 |             import math
0331 | 
0332 |             values[name] = value if value is not None and math.isfinite(value) else None
0333 |         except (requests.RequestException, ValueError, KeyError, IndexError):
0334 |             values[name] = None
0335 |     return {
0336 |         "metrics": values,
0337 |         "source": "Prometheus instant queries",
0338 |         "observed_at": datetime.now(timezone.utc).isoformat(),
0339 |     }
0340 | 
0341 | 
0342 | @app.get("/api/events/stream")
0343 | async def events_stream() -> StreamingResponse:
0344 |     """Server-Sent Events (SSE) live stream endpoint for real-time dashboard updates."""
0345 | 
0346 |     async def event_generator() -> AsyncGenerator[str, None]:
0347 |         seen = {}
0348 |         try:
0349 |             yield 'event: connected\ndata: {"status":"connected"}\n\n'
0350 |             while True:
0351 |                 try:
0352 |                     rows = await asyncio.to_thread(get_recent_events, 500)
0353 |                     current = {}
0354 |                     for data in reversed(rows):
0355 |                         fingerprint = json.dumps(data, sort_keys=True)
0356 |                         current[data["event_id"]] = fingerprint
0357 |                         if seen.get(data["event_id"]) != fingerprint:
0358 |                             yield f"event: crisis_event\ndata: {json.dumps(data)}\n\n"
0359 |                     seen = current
0360 |                     yield ": heartbeat\n\n"
0361 |                 except HTTPException:
0362 |                     yield 'event: storage_error\ndata: {"status":"unavailable"}\n\n'
0363 |                 await asyncio.sleep(2)
0364 |         except asyncio.CancelledError:
0365 |             pass
0366 | 
0367 |     return StreamingResponse(
0368 |         event_generator(),
0369 |         media_type="text/event-stream",
0370 |         headers={
0371 |             "Cache-Control": "no-cache",
0372 |             "Connection": "keep-alive",
0373 |             "X-Accel-Buffering": "no",
0374 |         },
0375 |     )
```

### `src/api/queries.py`

[Open source](../../src/api/queries.py) · SHA-256 `6657049ee8fe0b74fcaa6c766e0644bf108340b0acb003976079aa82deb905cb`

Dependencies: `from datetime import datetime, timezone`; `from typing import Any`; `from fastapi import HTTPException`; `from src.common.db import get_pool`

- `event_filter`: lines 11–67. Build an AND predicate; bbox is west,south,east,north in degrees.

- `read_rows`: lines 70–76. Execute a read in a worker thread when called from a synchronous route.

- `provenance`: lines 79–104. Expose observation mode without relabeling unverified submissions live.


```text
0001 | """Shared parameterized event filters for operational queries and aggregates."""
0002 | 
0003 | from datetime import datetime, timezone
0004 | from typing import Any
0005 | 
0006 | from fastapi import HTTPException
0007 | 
0008 | from src.common.db import get_pool
0009 | 
0010 | 
0011 | def event_filter(
0012 |     disaster_type: str | None = None,
0013 |     severity: str | None = None,
0014 |     source: str | None = None,
0015 |     q: str | None = None,
0016 |     since: datetime | None = None,
0017 |     until: datetime | None = None,
0018 |     bbox: str | None = None,
0019 | ) -> tuple[str, list[Any]]:
0020 |     """Build an AND predicate; bbox is west,south,east,north in degrees."""
0021 |     conditions: list[str] = []
0022 |     params: list[Any] = []
0023 |     for column, value in (
0024 |         ("disaster_type", disaster_type),
0025 |         ("severity", severity),
0026 |         ("source", source),
0027 |     ):
0028 |         if value:
0029 |             conditions.append(f"e.{column} = %s")
0030 |             params.append(value)
0031 |     if q:
0032 |         conditions.append(
0033 |             "(e.text ILIKE %s OR e.location ILIKE %s OR e.event_id ILIKE %s)"
0034 |         )
0035 |         params.extend([f"%{q}%"] * 3)
0036 |     for column, operator, value in (
0037 |         ("timestamp", ">=", since),
0038 |         ("timestamp", "<", until),
0039 |     ):
0040 |         if value:
0041 |             conditions.append(f"e.{column} {operator} %s")
0042 |             params.append(
0043 |                 value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
0044 |             )
0045 |     if (
0046 |         since
0047 |         and until
0048 |         and params
0049 |         and (since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since)
0050 |         >= (until.replace(tzinfo=timezone.utc) if until.tzinfo is None else until)
0051 |     ):
0052 |         raise HTTPException(422, "since must precede until")
0053 |     if bbox:
0054 |         try:
0055 |             west, south, east, north = map(float, bbox.split(","))
0056 |             if not (-180 <= west <= east <= 180 and -90 <= south <= north <= 90):
0057 |                 raise ValueError
0058 |         except ValueError as exc:
0059 |             raise HTTPException(
0060 |                 422,
0061 |                 "bbox must be west,south,east,north; antimeridian boxes require two queries",
0062 |             ) from exc
0063 |         conditions.append(
0064 |             "e.longitude BETWEEN %s AND %s AND e.latitude BETWEEN %s AND %s"
0065 |         )
0066 |         params.extend([west, east, south, north])
0067 |     return (" WHERE " + " AND ".join(conditions) if conditions else ""), params
0068 | 
0069 | 
0070 | def read_rows(sql: str, params: list[Any] | tuple = ()) -> list[dict]:
0071 |     """Execute a read in a worker thread when called from a synchronous route."""
0072 |     try:
0073 |         with get_pool().connection() as conn:
0074 |             return conn.execute(sql, params).fetchall()
0075 |     except Exception as exc:
0076 |         raise HTTPException(503, "Event storage unavailable") from exc
0077 | 
0078 | 
0079 | def provenance(row: dict) -> dict:
0080 |     """Expose observation mode without relabeling unverified submissions live."""
0081 |     meta = row.get("metadata") or {}
0082 |     source = row["source"]
0083 |     if meta.get("is_replay"):
0084 |         mode = "replay"
0085 |     elif (
0086 |         meta.get("is_synthetic")
0087 |         or source.startswith("synthetic")
0088 |         or source in {"operator_manual_dispatch", "integration", "test"}
0089 |     ):
0090 |         mode = "synthetic"
0091 |     elif source == "open_meteo":
0092 |         mode = "weather_estimate"
0093 |     elif source in {"usgs_feed", "openweather"}:
0094 |         mode = "live"
0095 |     else:
0096 |         mode = "unverified"
0097 |     row["data_mode"] = mode
0098 |     row["location_accuracy"] = (
0099 |         meta.get("location_accuracy") or "Source coordinates; accuracy not supplied"
0100 |     )
0101 |     for key in ("timestamp", "created_at"):
0102 |         if isinstance(row.get(key), datetime):
0103 |             row[key] = row[key].isoformat()
0104 |     return row
```
