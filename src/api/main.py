import os
import json
import time
import asyncio
import logging
from typing import Annotated, Literal, Optional, AsyncGenerator
from datetime import datetime, timezone
from threading import Event

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from confluent_kafka import Producer, KafkaException

from src.common.config import config
from src.common.models import CrisisEvent
from src.common.db import get_pool, init_db, close_pool
from src.api.queries import event_filter, read_rows, provenance
from prometheus_fastapi_instrumentator import Instrumentator

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] (API) %(message)s"
)
logger = logging.getLogger("cataclysm.api")

app = FastAPI(
    title="Crisis Intelligence Platform API",
    description="Real-Time Disaster Telemetry Ingestion, Streaming & Geospatial Platform",
    version="1.0.0",
)
Instrumentator().instrument(app).expose(
    app, endpoint="/metrics", include_in_schema=False
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "http://localhost:8000").split(","),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
STATIC_INDEX = os.path.join(STATIC_DIR, "index.html")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
DOCS_DIR = os.path.abspath(os.path.join(STATIC_DIR, "..", "..", "..", "docs"))
app.mount(
    "/handbook",
    StaticFiles(directory=os.path.join(DOCS_DIR, "handbook"), html=True),
    name="handbook",
)
app.mount("/documentation", StaticFiles(directory=DOCS_DIR), name="documentation")

# Kafka Producer with Acknowledgment
producer = Producer(
    {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",  # Strongest durability guarantee
        "retries": 3,
        "retry.backoff.ms": 100,
    }
)


@app.on_event("startup")
def startup_event():
    try:
        init_db()
        logger.info("Database initialized on startup.")
    except Exception as e:
        logger.warning(f"Database init warning (will retry on query): {e}")


@app.on_event("shutdown")
def shutdown_event():
    producer.flush(timeout=3.0)
    close_pool()


@app.get("/")
def serve_dashboard():
    """Serves the interactive crisis operations dashboard."""
    if os.path.exists(STATIC_INDEX):
        return FileResponse(STATIC_INDEX)
    return {"message": "Crisis Intelligence Platform API is online. Static dashboard not found."}


@app.get("/incidents")
def get_incidents(limit: int = Query(default=50, ge=1, le=500)):
    return get_recent_events(limit=limit)


@app.get("/health")
def healthcheck():
    """Comprehensive health probe verifying PostgreSQL and Kafka availability."""
    db_ok = False
    kafka_ok = False
    try:
        kafka_ok = bool(producer.list_topics(timeout=2).brokers)
    except Exception:
        pass
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                db_ok = True
    except Exception as e:
        logger.debug(f"Health DB probe failed: {e}")

    return {
        "status": "healthy" if db_ok and kafka_ok else "degraded",
        "database": "connected" if db_ok else "unreachable",
        "broker": config.KAFKA_BOOTSTRAP_SERVERS,
        "kafka_connected": kafka_ok,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/live")
def liveness():
    return {"status": "alive"}


@app.get("/ready")
def readiness():
    result = healthcheck()
    if result["status"] != "healthy":
        raise HTTPException(503, "Database or broker unavailable")
    return result


@app.post("/events", status_code=status.HTTP_200_OK)
def ingest_event(event: CrisisEvent):
    """
    Ingests disaster event telemetry and produces to Kafka with delivery acknowledgment.
    Does NOT return 200 OK until Kafka confirms persistence on the broker partition.
    """
    delivery_event = Event()
    delivery_result = {
        "success": False,
        "partition": None,
        "offset": None,
        "error": None,
    }

    def on_delivery(err, msg):
        if err is not None:
            delivery_result["error"] = str(err)
            delivery_result["success"] = False
        else:
            delivery_result["success"] = True
            delivery_result["partition"] = msg.partition()
            delivery_result["offset"] = msg.offset()
        delivery_event.set()

    # Serialize event
    event_dict = event.model_dump(mode="json")
    payload = json.dumps(event_dict).encode("utf-8")
    key = event.location.encode("utf-8")

    try:
        producer.produce(
            topic=config.KAFKA_TOPIC, key=key, value=payload, on_delivery=on_delivery
        )
        producer.poll(0)
    except (KafkaException, BufferError) as ke:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Kafka producer buffer error: {ke}",
        )

    # Bounded wait for broker delivery confirmation (2.0s timeout)
    deadline = time.monotonic() + 2.5
    while not delivery_event.is_set() and time.monotonic() < deadline:
        producer.poll(min(0.1, max(0.0, deadline - time.monotonic())))
    confirmed = delivery_event.is_set()

    if not confirmed or not delivery_result["success"]:
        err_msg = delivery_result.get("error") or "Broker acknowledgment timeout"
        logger.error(f"Failed delivery for event {event.event_id}: {err_msg}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Message delivery to Kafka failed: {err_msg}",
        )

    return {
        "status": "confirmed",
        "event_id": event.event_id,
        "partition": delivery_result["partition"],
        "offset": delivery_result["offset"],
    }


@app.get("/api/events")
def get_recent_events(
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    disaster_type: Optional[str] = None,
    severity: Optional[str] = None,
    source: Optional[str] = None,
    q: Annotated[Optional[str], Query(max_length=200)] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    bbox: Optional[str] = None,
    offset: Annotated[int, Query(ge=0, le=100000)] = 0,
    sort: Literal["newest", "oldest"] = "newest",
):
    """Bounded rows, stable ordering and original metadata; totals use /overview."""
    predicate, params = event_filter(
        disaster_type, severity, source, q, since, until, bbox
    )
    direction = "DESC" if sort == "newest" else "ASC"
    rows = read_rows(
        "SELECT e.*, COALESCE(r.payload->'metadata', '{}'::jsonb) AS metadata "
        "FROM crisis_events e LEFT JOIN reports r USING(event_id)"
        + predicate
        + f" ORDER BY e.timestamp {direction}, e.event_id {direction} LIMIT %s OFFSET %s",
        params + [limit, offset],
    )
    return [provenance(row) for row in rows]


@app.get("/api/overview")
def get_overview(
    disaster_type: Optional[str] = None,
    severity: Optional[str] = None,
    source: Optional[str] = None,
    q: Annotated[Optional[str], Query(max_length=200)] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    bbox: Optional[str] = None,
):
    """Aggregate all matching persisted reports, independently of feed pagination."""
    predicate, params = event_filter(
        disaster_type, severity, source, q, since, until, bbox
    )
    base = " FROM crisis_events e" + predicate
    total = read_rows(
        "SELECT COUNT(*) AS total, MAX(timestamp) AS latest_event, MAX(created_at) AS latest_ingested, "
        "COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1 minute') AS ingested_last_minute"
        + base,
        params,
    )[0]
    for name, column in (
        ("severity", "severity"),
        ("categories", "disaster_type"),
        ("sources", "source"),
    ):
        total[name] = read_rows(
            f"SELECT {column} AS label, COUNT(*) AS count, MAX(timestamp) AS latest_event"
            + base
            + f" GROUP BY {column} ORDER BY count DESC",
            params,
        )
    total["timeline"] = read_rows(
        "SELECT date_trunc('hour', timestamp) AS hour, COUNT(*) AS count"
        + base
        + " GROUP BY hour ORDER BY hour DESC LIMIT 168",
        params,
    )
    total["trends"] = read_rows(
        "SELECT date_trunc('hour', timestamp) AS hour, disaster_type, severity, COUNT(*) AS count"
        + base
        + " GROUP BY hour, disaster_type, severity ORDER BY hour DESC, disaster_type, severity LIMIT 1000",
        params,
    )
    total["scope"] = {
        "since": since,
        "until": until,
        "timeline_bin": "1 hour",
        "timeline_max_bins": 168,
    }
    total["rate_note"] = (
        "Persisted unique rows created in the last minute within selected event filters; not broker throughput."
    )
    return total


@app.get("/api/events/{event_id}/details")
def event_details(event_id: str):
    rows = read_rows(
        "SELECT e.*, COALESCE(r.payload->'metadata', '{}'::jsonb) AS metadata FROM crisis_events e LEFT JOIN reports r USING(event_id) WHERE e.event_id=%s",
        [event_id],
    )
    if not rows:
        raise HTTPException(404, "Event not found")
    return provenance(rows[0])


@app.get("/api/pipeline")
def pipeline_metrics():
    """Read actual Prometheus samples; unavailable metrics remain null."""
    import requests

    expressions = {
        "consumer_lag": "sum(cataclysm_consumer_lag)",
        "spark_progress_age_seconds": "time() - min(cataclysm_spark_progress_epoch)",
        "spark_batch_duration_ms": "max(cataclysm_spark_batch_duration_ms)",
        "http_requests_per_second": "sum(rate(http_requests_total[5m]))",
    }
    values = {}
    for name, expression in expressions.items():
        try:
            response = requests.get(
                os.getenv("PROMETHEUS_URL", "http://prometheus:9090") + "/api/v1/query",
                params={"query": expression},
                timeout=1,
            )
            response.raise_for_status()
            samples = response.json()["data"]["result"]
            value = float(samples[0]["value"][1]) if samples else None
            import math

            values[name] = value if value is not None and math.isfinite(value) else None
        except (requests.RequestException, ValueError, KeyError, IndexError):
            values[name] = None
    return {
        "metrics": values,
        "source": "Prometheus instant queries",
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/events/stream")
async def events_stream() -> StreamingResponse:
    """Server-Sent Events (SSE) live stream endpoint for real-time dashboard updates."""

    async def event_generator() -> AsyncGenerator[str, None]:
        seen = {}
        try:
            yield 'event: connected\ndata: {"status":"connected"}\n\n'
            while True:
                try:
                    rows = await asyncio.to_thread(get_recent_events, 500)
                    current = {}
                    for data in reversed(rows):
                        fingerprint = json.dumps(data, sort_keys=True)
                        current[data["event_id"]] = fingerprint
                        if seen.get(data["event_id"]) != fingerprint:
                            yield f"event: crisis_event\ndata: {json.dumps(data)}\n\n"
                    seen = current
                    yield ": heartbeat\n\n"
                except HTTPException:
                    yield 'event: storage_error\ndata: {"status":"unavailable"}\n\n'
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
