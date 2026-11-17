import os
import io
import json
import logging
from datetime import datetime
from typing import List, Dict, Any
import boto3
from botocore.config import Config as BotoConfig
import pyarrow as pa
import pyarrow.parquet as pq
from src.common.config import config

logger = logging.getLogger("cataclysm.lake")

EVENT_ARROW_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("timestamp", pa.timestamp("ms")),
        ("disaster_type", pa.string()),
        ("severity", pa.string()),
        ("location", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("text", pa.string()),
        ("source", pa.string()),
        ("rainfall_mm", pa.float64()),
        ("water_level_m", pa.float64()),
        ("wind_speed_kmh", pa.float64()),
        ("temperature_c", pa.float64()),
        ("predicted_disaster_type", pa.string()),
        ("predicted_severity", pa.string()),
        ("prediction_confidence", pa.float64()),
        ("raw_payload", pa.string()),
    ]
)


class ParquetLakeWriter:
    """Archive raw events to MinIO as event-keyed, partitioned Parquet objects.

    Local output is explicitly opt-in for offline unit tests. Production Compose
    configures a MinIO endpoint and never silently downgrades durable storage.
    """

    def __init__(self, base_dir: str = None, s3_client: Any = None):
        self.base_dir = base_dir or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "raw"
        )
        os.makedirs(self.base_dir, exist_ok=True)
        self._s3 = s3_client
        self._local_mode = base_dir is not None

    def _client(self):
        if self._s3 is not None:
            return self._s3
        if not config.MINIO_ACCESS_KEY or not config.MINIO_SECRET_KEY:
            if self._local_mode:
                return None
            raise ValueError(
                "MinIO credentials are required; supply base_dir for explicit local mode"
            )
        endpoint = config.MINIO_ENDPOINT
        if not endpoint.startswith("http"):
            endpoint = f"{'https' if config.MINIO_SECURE else 'http'}://{endpoint}"
        self._s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=config.MINIO_ACCESS_KEY,
            aws_secret_access_key=config.MINIO_SECRET_KEY,
            config=BotoConfig(s3={"addressing_style": "path"}),
        )
        return self._s3

    @staticmethod
    def _partition(event: Dict[str, Any]) -> tuple[datetime, str, str]:
        ts = event.get("timestamp")
        if isinstance(ts, str):
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                dt = datetime.utcnow()
        elif isinstance(ts, datetime):
            dt = ts
        else:
            dt = datetime.utcnow()
        disaster = str(event.get("disaster_type", "unknown"))
        prefix = f"year={dt:%Y}/month={dt:%m}/day={dt:%d}/disaster_type={disaster}"
        return dt, disaster, prefix

    def write_batch(self, events: List[Dict[str, Any]]) -> str:
        """Writes a batch of events to partitioned Parquet files."""
        if not events:
            return ""

        client = self._client()
        for event in events:
            dt, disaster, prefix = self._partition(event)

            meta = event.get("metadata", {}) or {}
            record = {
                "event_id": [event.get("event_id")],
                "timestamp": [dt],
                "disaster_type": [disaster],
                "severity": [event.get("severity", "unknown")],
                "location": [event.get("location", "unknown")],
                "latitude": [float(event.get("latitude", 0.0))],
                "longitude": [float(event.get("longitude", 0.0))],
                "text": [event.get("text", "")],
                "source": [event.get("source", "unknown")],
                "rainfall_mm": [meta.get("rainfall_mm")],
                "water_level_m": [meta.get("water_level_m")],
                "wind_speed_kmh": [meta.get("wind_speed_kmh")],
                "temperature_c": [meta.get("temperature_c")],
                "predicted_disaster_type": [event.get("predicted_disaster_type", "")],
                "predicted_severity": [event.get("predicted_severity", "")],
                "prediction_confidence": [
                    float(event.get("prediction_confidence") or 0.0)
                ],
                "raw_payload": [json.dumps(event, default=str, sort_keys=True)],
            }

            table = pa.Table.from_pydict(record, schema=EVENT_ARROW_SCHEMA)
            object_name = f"{prefix}/event_id={event.get('event_id')}.parquet"
            if client is not None:
                buffer = io.BytesIO()
                pq.write_table(table, buffer, compression="snappy")
                client.put_object(
                    Bucket=config.MINIO_BUCKET_RAW,
                    Key=object_name,
                    Body=buffer.getvalue(),
                    ContentType="application/octet-stream",
                )
                logger.debug(
                    "Archived MinIO object: s3://%s/%s",
                    config.MINIO_BUCKET_RAW,
                    object_name,
                )
            else:
                partition_dir = os.path.join(self.base_dir, prefix)
                os.makedirs(partition_dir, exist_ok=True)
                pq.write_table(
                    table,
                    os.path.join(partition_dir, object_name.split("/")[-1]),
                    compression="snappy",
                )
                logger.warning(
                    "MinIO credentials are absent; wrote local development archive only."
                )

        return self.base_dir
