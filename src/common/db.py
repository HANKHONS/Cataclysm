import logging
import time
from typing import Optional
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
from src.common.config import config

logger = logging.getLogger("cataclysm.db")

_pool: Optional[ConnectionPool] = None

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS crisis_events (
    event_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    disaster_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    location VARCHAR(100) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    text TEXT NOT NULL,
    source VARCHAR(50) NOT NULL,
    rainfall_mm FLOAT,
    water_level_m FLOAT,
    wind_speed_kmh FLOAT,
    temperature_c FLOAT,
    predicted_disaster_type VARCHAR(50),
    predicted_severity VARCHAR(20),
    prediction_confidence FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crisis_events_timestamp ON crisis_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_crisis_events_disaster ON crisis_events(disaster_type, severity);
CREATE INDEX IF NOT EXISTS idx_crisis_events_coords ON crisis_events(latitude, longitude);

CREATE TABLE IF NOT EXISTS locations (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    UNIQUE(name, latitude, longitude)
);
CREATE TABLE IF NOT EXISTS reports (
    event_id VARCHAR(50) PRIMARY KEY REFERENCES crisis_events(event_id),
    location_id BIGINT REFERENCES locations(id),
    payload JSONB NOT NULL,
    source_topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS predictions (
    event_id VARCHAR(50) REFERENCES crisis_events(event_id),
    model_name TEXT NOT NULL,
    predicted_label TEXT NOT NULL,
    confidence DOUBLE PRECISION,
    provenance TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(event_id, model_name)
);
CREATE TABLE IF NOT EXISTS anomaly_windows (
    window_start TIMESTAMPTZ PRIMARY KEY,
    report_count INTEGER NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    training_windows INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            conninfo=config.DATABASE_URL,
            min_size=config.DB_POOL_MIN_SIZE,
            max_size=config.DB_POOL_MAX_SIZE,
            timeout=3.0,
            kwargs={"row_factory": dict_row, "connect_timeout": 3},
            open=False,
        )
        _pool.open()
        logger.info("Database connection pool opened")
    return _pool


def init_db(max_retries: int = 10, retry_delay: float = 2.0):
    """Initializes tables and indexes with retry logic for container startup."""
    for attempt in range(1, max_retries + 1):
        try:
            pool = get_pool()
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(SCHEMA_SQL)
                conn.commit()
            logger.info("Database schema verified and initialized.")
            return
        except Exception as e:
            logger.warning(
                f"Database initialization attempt {attempt}/{max_retries} failed: {e}"
            )
            if attempt == max_retries:
                logger.error("Failed to initialize database after maximum retries.")
                raise e
            time.sleep(retry_delay)


def close_pool():
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
        logger.info("Database connection pool closed.")
