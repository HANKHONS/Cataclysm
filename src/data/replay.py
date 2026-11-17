import os
import glob
import argparse
import logging
import json
import io
import pyarrow.parquet as pq
from confluent_kafka import Producer
from src.common.config import config
from src.storage.lake_writer import ParquetLakeWriter


def replay_from_minio(dry_run: bool = False, limit: int = 100) -> int:
    """Replay at most limit rows, failing if broker delivery is unconfirmed."""
    if limit < 1:
        raise ValueError("limit must be positive")
    client = ParquetLakeWriter()._client()
    producer = (
        None
        if dry_run
        else Producer(
            {
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                "acks": "all",
            }
        )
    )
    count = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=config.MINIO_BUCKET_RAW):
        for item in page.get("Contents", []):
            if not item["Key"].endswith(".parquet"):
                continue
            body = client.get_object(Bucket=config.MINIO_BUCKET_RAW, Key=item["Key"])[
                "Body"
            ]
            try:
                rows = pq.ParquetFile(io.BytesIO(body.read())).read().to_pylist()
            finally:
                body.close()
            for row in rows:
                original = (
                    json.loads(row["raw_payload"]) if row.get("raw_payload") else None
                )
                event = {
                    key: row[key]
                    for key in (
                        "event_id",
                        "timestamp",
                        "disaster_type",
                        "severity",
                        "location",
                        "latitude",
                        "longitude",
                        "text",
                        "source",
                    )
                }
                event["timestamp"] = event["timestamp"].isoformat()
                event["metadata"] = {
                    key: row.get(key)
                    for key in (
                        "rainfall_mm",
                        "water_level_m",
                        "wind_speed_kmh",
                        "temperature_c",
                    )
                }
                if original:
                    event = original
                event.setdefault("metadata", {})["is_replay"] = True
                if producer is not None:
                    delivery = []
                    producer.produce(
                        config.KAFKA_TOPIC,
                        key=event["location"].encode(),
                        value=json.dumps(event).encode(),
                        on_delivery=lambda error, message: delivery.append(error),
                    )
                    if producer.flush(10) or not delivery or delivery[0] is not None:
                        raise RuntimeError("Replay delivery was not confirmed")
                count += 1
                if count >= limit:
                    return count
    return count


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("cataclysm.replay")


def replay_from_parquet(base_dir: str = None, dry_run: bool = False, limit: int = 100):
    raw_dir = base_dir or os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "raw"
    )
    parquet_files = glob.glob(os.path.join(raw_dir, "**", "*.parquet"), recursive=True)

    logger.info(f"Discovered {len(parquet_files)} Parquet partition files in {raw_dir}")
    if not parquet_files:
        logger.warning("No Parquet files found to replay.")
        return

    producer = (
        None
        if dry_run
        else Producer(
            {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, "acks": "all"}
        )
    )
    count = 0

    if limit < 1:
        raise ValueError("limit must be positive")
    for pfile in sorted(parquet_files):
        if count >= limit:
            break
        try:
            table = pq.ParquetFile(pfile).read()
            pydict = table.to_pydict()
            num_rows = len(pydict["event_id"])
            for i in range(num_rows):
                if count >= limit:
                    break
                event = {
                    "event_id": pydict["event_id"][i],
                    "timestamp": pydict["timestamp"][i].isoformat(),
                    "disaster_type": pydict["disaster_type"][i],
                    "severity": pydict["severity"][i],
                    "location": pydict["location"][i],
                    "latitude": pydict["latitude"][i],
                    "longitude": pydict["longitude"][i],
                    "text": pydict["text"][i],
                    "source": pydict["source"][i],
                    "metadata": {
                        "rainfall_mm": pydict["rainfall_mm"][i],
                        "water_level_m": pydict["water_level_m"][i],
                        "wind_speed_kmh": pydict["wind_speed_kmh"][i],
                        "temperature_c": pydict["temperature_c"][i],
                    },
                }

                if pydict.get("raw_payload") and pydict["raw_payload"][i]:
                    event = json.loads(pydict["raw_payload"][i])
                event.setdefault("metadata", {})["is_replay"] = True
                if dry_run:
                    logger.info(
                        f"[DRY-RUN] Replaying event: {event['event_id']} ({event['disaster_type']} at {event['location']})"
                    )
                else:
                    payload = json.dumps(event).encode("utf-8")
                    key = event["location"].encode("utf-8")
                    delivery = []
                    producer.produce(
                        config.KAFKA_TOPIC,
                        key=key,
                        value=payload,
                        on_delivery=lambda error, message: delivery.append(error),
                    )
                    if producer.flush(10) or not delivery or delivery[0] is not None:
                        raise RuntimeError("Replay delivery was not confirmed")
                count += 1
        except Exception as e:
            raise RuntimeError(f"Replay failed for {pfile}") from e

    if producer:
        producer.flush(timeout=5.0)
    logger.info(f"Replay complete. Processed {count} events.")
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Replay historical disaster telemetry from Parquet Lake"
    )
    parser.add_argument("--dir", default=None, help="Base path to raw parquet lake")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print events without sending to Kafka"
    )
    parser.add_argument(
        "--minio",
        action="store_true",
        help="Replay from the configured raw MinIO bucket",
    )
    parser.add_argument(
        "--limit", type=int, default=50, help="Maximum events to replay"
    )
    args = parser.parse_args()
    if args.minio:
        logger.info("Replayed %d rows", replay_from_minio(args.dry_run, args.limit))
    else:
        replay_from_parquet(base_dir=args.dir, dry_run=args.dry_run, limit=args.limit)
