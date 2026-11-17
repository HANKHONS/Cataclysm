# PostgreSQL: constraints are part of the algorithm

`src/common/db.py` imports the psycopg pool, dictionary row factory and shared settings. `_pool` is process-local, initially None. `get_pool` lazily creates a pool with configured min/max sizes, three-second acquisition timeout and three-second connection timeout. `open=False` followed by explicit `open()` makes lifecycle visible. API/consumer/exporter/anomaly processes have separate pools; ten per process can become many connections, so count processes when sizing. Lazy creation is not guarded by a process-local lock; startup normally initializes it before query concurrency, but a future multiprocess/thread refactor should make initialization explicit.

## Schema, statement by statement

`crisis_events` stores one row per safe event identity. `TIMESTAMP WITH TIME ZONE` represents an instant; the session formats it according to timezone. Declared VARCHAR bounds agree with the API. Latitude/longitude use floating point and telemetry permits NULL; schema constraints do not mirror all Pydantic coordinate/severity checks for arbitrary direct SQL clients. Created_at defaults to first persistence and is not source event time.

The timestamp descending index supports latest-event queries. The `(disaster_type,severity)` composite index is naturally useful for its leading hazard predicate and sometimes combinations; it is not automatically optimal for severity-only filters. The coordinate index is a plain multicolumn B-tree, not PostGIS or an R-tree; rectangular predicates do not establish sophisticated spatial indexing. Verify actual query plans under representative distributions.

`locations` gives exact `(name,latitude,longitude)` triples a unique surrogate ID. It does not merge near coordinates or alternate place names. `reports.event_id` is both primary key and FK into events; its location FK and JSONB payload preserve the supported enriched report and original Kafka position. JSONB permits structured metadata access for the dashboard without widening every event column. `predictions` has a composite `(event_id,model_name)` key and stores label, score/provenance and creation time. It currently does not persist artifact SHA per prediction, a remaining audit limitation. `anomaly_windows` keys one completed hour and stores count, score, flag and training-history size.

## Initialization and reads

`init_db` runs CREATE TABLE/INDEX IF NOT EXISTS in a pooled connection, explicitly commits, retries bounded startup failures and raises after its final attempt. This is idempotent bootstrap, not a versioned migration framework. A future column/type change needs a deliberate migration; IF NOT EXISTS does not alter an existing incompatible table. `close_pool` closes the process pool and resets its global reference for clean shutdown.

Consumer transaction boundaries are explained in chapter 04. API queries bind values, join by identity and return dictionaries. The current evidence's timestamp-index scan is for a base query with 100 returned rows, not the entire joined/filter endpoint. Count/group/search operations can become the dominant cost as history grows. Large offset skips are not free; introduce keyset pagination only with an API/UX migration and tests for ties.

For safe modification, preserve keys and first-observation policy, add a migration for existing state, and rerun duplicate/conflicting-ID/crash evidence. Never drop the table or volume to make schema initialization pass. The dependency outage test exercised actual 503/readiness and restoration; no independent backup restore or disk-loss test exists.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `src/common/db.py`

[Open source](../../src/common/db.py) · SHA-256 `91d898996765db5c8d324dcf98aee693961a5654c93c9fad9f3887ae315a99e5`

Dependencies: `import logging`; `import time`; `from typing import Optional`; `from psycopg_pool import ConnectionPool`; `from psycopg.rows import dict_row`; `from src.common.config import config`

- `get_pool`: lines 72–85. Read its control flow with the module guide above.

- `init_db`: lines 88–106. Initializes tables and indexes with retry logic for container startup.

- `close_pool`: lines 109–114. Read its control flow with the module guide above.


```text
0001 | import logging
0002 | import time
0003 | from typing import Optional
0004 | from psycopg_pool import ConnectionPool
0005 | from psycopg.rows import dict_row
0006 | from src.common.config import config
0007 | 
0008 | logger = logging.getLogger("cataclysm.db")
0009 | 
0010 | _pool: Optional[ConnectionPool] = None
0011 | 
0012 | SCHEMA_SQL = """
0013 | CREATE TABLE IF NOT EXISTS crisis_events (
0014 |     event_id VARCHAR(50) PRIMARY KEY,
0015 |     timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
0016 |     disaster_type VARCHAR(50) NOT NULL,
0017 |     severity VARCHAR(20) NOT NULL,
0018 |     location VARCHAR(100) NOT NULL,
0019 |     latitude FLOAT NOT NULL,
0020 |     longitude FLOAT NOT NULL,
0021 |     text TEXT NOT NULL,
0022 |     source VARCHAR(50) NOT NULL,
0023 |     rainfall_mm FLOAT,
0024 |     water_level_m FLOAT,
0025 |     wind_speed_kmh FLOAT,
0026 |     temperature_c FLOAT,
0027 |     predicted_disaster_type VARCHAR(50),
0028 |     predicted_severity VARCHAR(20),
0029 |     prediction_confidence FLOAT,
0030 |     created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
0031 | );
0032 | 
0033 | CREATE INDEX IF NOT EXISTS idx_crisis_events_timestamp ON crisis_events(timestamp DESC);
0034 | CREATE INDEX IF NOT EXISTS idx_crisis_events_disaster ON crisis_events(disaster_type, severity);
0035 | CREATE INDEX IF NOT EXISTS idx_crisis_events_coords ON crisis_events(latitude, longitude);
0036 | 
0037 | CREATE TABLE IF NOT EXISTS locations (
0038 |     id BIGSERIAL PRIMARY KEY,
0039 |     name TEXT NOT NULL,
0040 |     latitude DOUBLE PRECISION NOT NULL,
0041 |     longitude DOUBLE PRECISION NOT NULL,
0042 |     UNIQUE(name, latitude, longitude)
0043 | );
0044 | CREATE TABLE IF NOT EXISTS reports (
0045 |     event_id VARCHAR(50) PRIMARY KEY REFERENCES crisis_events(event_id),
0046 |     location_id BIGINT REFERENCES locations(id),
0047 |     payload JSONB NOT NULL,
0048 |     source_topic TEXT NOT NULL,
0049 |     source_partition INTEGER NOT NULL,
0050 |     source_offset BIGINT NOT NULL
0051 | );
0052 | CREATE TABLE IF NOT EXISTS predictions (
0053 |     event_id VARCHAR(50) REFERENCES crisis_events(event_id),
0054 |     model_name TEXT NOT NULL,
0055 |     predicted_label TEXT NOT NULL,
0056 |     confidence DOUBLE PRECISION,
0057 |     provenance TEXT NOT NULL,
0058 |     created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
0059 |     PRIMARY KEY(event_id, model_name)
0060 | );
0061 | CREATE TABLE IF NOT EXISTS anomaly_windows (
0062 |     window_start TIMESTAMPTZ PRIMARY KEY,
0063 |     report_count INTEGER NOT NULL,
0064 |     score DOUBLE PRECISION NOT NULL,
0065 |     is_anomaly BOOLEAN NOT NULL,
0066 |     training_windows INTEGER NOT NULL,
0067 |     created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
0068 | );
0069 | """
0070 | 
0071 | 
0072 | def get_pool() -> ConnectionPool:
0073 |     global _pool
0074 |     if _pool is None:
0075 |         _pool = ConnectionPool(
0076 |             conninfo=config.DATABASE_URL,
0077 |             min_size=config.DB_POOL_MIN_SIZE,
0078 |             max_size=config.DB_POOL_MAX_SIZE,
0079 |             timeout=3.0,
0080 |             kwargs={"row_factory": dict_row, "connect_timeout": 3},
0081 |             open=False,
0082 |         )
0083 |         _pool.open()
0084 |         logger.info("Database connection pool opened")
0085 |     return _pool
0086 | 
0087 | 
0088 | def init_db(max_retries: int = 10, retry_delay: float = 2.0):
0089 |     """Initializes tables and indexes with retry logic for container startup."""
0090 |     for attempt in range(1, max_retries + 1):
0091 |         try:
0092 |             pool = get_pool()
0093 |             with pool.connection() as conn:
0094 |                 with conn.cursor() as cur:
0095 |                     cur.execute(SCHEMA_SQL)
0096 |                 conn.commit()
0097 |             logger.info("Database schema verified and initialized.")
0098 |             return
0099 |         except Exception as e:
0100 |             logger.warning(
0101 |                 f"Database initialization attempt {attempt}/{max_retries} failed: {e}"
0102 |             )
0103 |             if attempt == max_retries:
0104 |                 logger.error("Failed to initialize database after maximum retries.")
0105 |                 raise e
0106 |             time.sleep(retry_delay)
0107 | 
0108 | 
0109 | def close_pool():
0110 |     global _pool
0111 |     if _pool is not None:
0112 |         _pool.close()
0113 |         _pool = None
0114 |         logger.info("Database connection pool closed.")
```
