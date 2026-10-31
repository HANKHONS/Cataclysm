import os
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
from confluent_kafka import Producer
import psycopg
from psycopg.rows import dict_row

app = FastAPI(title="Disaster Stream API", description="Real-Time Disaster Streaming API")

# Enable CORS for web dashboards
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Kafka Producer
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# PostgreSQL Connection String
DB_URL = os.getenv("DATABASE_URL", "postgresql://cataclysm:78520@db:5432/cataclysm")

STATIC_INDEX = os.path.join(os.path.dirname(__file__), "static", "index.html")

class Metadata(BaseModel):
    rainfall_mm: float | None = None
    water_level_m: float | None = None
    wind_speed_kmh: float | None = None
    temperature_c: float | None = None

class CrisisEvent(BaseModel):
    event_id: str 
    timestamp: datetime 
    text: str 
    location: str 
    disaster_type: str 
    latitude: float 
    longitude: float 
    severity: str 
    source: str
    metadata: Metadata

@app.get("/")
def dashboard():
    """Serves the live React.js Dashboard"""
    if os.path.exists(STATIC_INDEX):
        return FileResponse(STATIC_INDEX)
    return {"message": "Cataclysm Disaster API is online. Dashboard not found."}

@app.get("/health")
def status():
    return {"status": "ok"}

@app.post("/events")
def ingest_event(event: CrisisEvent):
    """Ingests a crisis event and publishes it to Kafka"""
    event_key = event.location.encode('utf-8')
    event_payload = event.model_dump_json().encode('utf-8')
    producer.produce('crisis-events', key=event_key, value=event_payload)
    producer.flush()
    return {"status": "Event sent to kafka successfully"}

@app.get("/api/events")
def get_recent_events():
    """Queries latest 50 disaster events from PostgreSQL for the React Dashboard"""
    try:
        with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        event_id, 
                        timestamp, 
                        disaster_type, 
                        severity, 
                        location, 
                        latitude, 
                        longitude, 
                        text, 
                        source, 
                        rainfall_mm, 
                        water_level_m, 
                        wind_speed_kmh, 
                        temperature_c 
                    FROM crisis_events 
                    ORDER BY timestamp DESC 
                    LIMIT 50;
                """)
                rows = cur.fetchall()
                # Format timestamps to ISO strings for frontend
                for row in rows:
                    if isinstance(row.get('timestamp'), datetime):
                        row['timestamp'] = row['timestamp'].isoformat()
                return rows
    except Exception as e:
        # If table doesn't exist yet or connection fails, return empty list gracefully
        return []
