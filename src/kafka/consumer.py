import time
import signal
import logging
from confluent_kafka import Consumer, Producer, KafkaError
from src.common.config import config
from src.common.db import get_pool, init_db
from src.common.models import CrisisEvent, DLQMessage
from psycopg.types.json import Jsonb
from src.storage.lake_writer import ParquetLakeWriter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] (Consumer) %(message)s"
)
logger = logging.getLogger("cataclysm.consumer")

running = True


def handle_sigterm(signum, frame):
    global running
    logger.info(
        "Received termination signal. Shutting down consumer loop gracefully..."
    )
    running = False


signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)


def run_consumer():
    global running
    logger.info(
        f"Connecting to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS} with group {config.KAFKA_GROUP_ID}"
    )

    # 1. Initialize DB schema
    init_db()
    pool = get_pool()
    lake_writer = ParquetLakeWriter()

    # 2. Producer for Dead Letter Queue
    dlq_producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})

    # 3. Kafka Consumer Configuration
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": config.KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Manual commit for At-Least-Once guarantee
    }

    consumer = Consumer(conf)
    consumer.subscribe([config.KAFKA_TOPIC])
    logger.info(f"Successfully subscribed to topic: '{config.KAFKA_TOPIC}'")

    INSERT_SQL = """
        INSERT INTO crisis_events (
            event_id, timestamp, disaster_type, severity, location,
            latitude, longitude, text, source,
            rainfall_mm, water_level_m, wind_speed_kmh, temperature_c,
            predicted_disaster_type, predicted_severity, prediction_confidence
        ) VALUES (
            %(event_id)s, %(timestamp)s, %(disaster_type)s, %(severity)s, %(location)s,
            %(latitude)s, %(longitude)s, %(text)s, %(source)s,
            %(rainfall_mm)s, %(water_level_m)s, %(wind_speed_kmh)s, %(temperature_c)s,
            %(predicted_disaster_type)s, %(predicted_severity)s, %(prediction_confidence)s
        )
        ON CONFLICT (event_id) DO NOTHING;
    """

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(f"Consumer error: {msg.error()}")
                continue

        raw_bytes = msg.value()
        partition = msg.partition()
        offset = msg.offset()

        # Parse JSON
        try:
            event_data = CrisisEvent.model_validate_json(raw_bytes).model_dump(
                mode="json"
            )
        except Exception as e:
            # Route to Dead Letter Queue
            logger.warning(
                f"Malformed message at partition {partition}, offset {offset}: {e}. Diverting to DLQ."
            )
            dlq_msg = DLQMessage(
                raw_payload=(raw_bytes or b"").decode("utf-8", errors="replace"),
                error_type=type(e).__name__,
                error_message=str(e),
                source_topic=config.KAFKA_TOPIC,
                source_partition=partition,
                source_offset=offset,
            )
            delivery = []
            dlq_producer.produce(
                config.KAFKA_DLQ_TOPIC,
                value=dlq_msg.model_dump_json().encode("utf-8"),
                on_delivery=lambda error, message: delivery.append(error),
            )
            pending = dlq_producer.flush(timeout=10.0)
            if pending or not delivery or delivery[0] is not None:
                consumer.close()
                raise RuntimeError(
                    "DLQ delivery failed; source offset remains uncommitted"
                )
            consumer.commit(message=msg, asynchronous=False)
            continue

        # Operational telemetry normalization
        meta = event_data.get("metadata", {}) or {}
        record = {
            "event_id": event_data["event_id"],
            "timestamp": event_data["timestamp"],
            "disaster_type": event_data.get("disaster_type", "unknown"),
            "severity": event_data.get("severity", "unknown"),
            "location": event_data.get("location", "Unknown"),
            "latitude": float(event_data.get("latitude", 0.0)),
            "longitude": float(event_data.get("longitude", 0.0)),
            "text": event_data.get("text", ""),
            "source": event_data.get("source", "system"),
            "rainfall_mm": meta.get("rainfall_mm"),
            "water_level_m": meta.get("water_level_m"),
            "wind_speed_kmh": meta.get("wind_speed_kmh"),
            "temperature_c": meta.get("temperature_c"),
            "predicted_disaster_type": None,
            "predicted_severity": None,
            "prediction_confidence": None,
        }

        # Bounded retry loop for database persistence
        max_db_retries = 5
        db_success = False

        for attempt in range(1, max_db_retries + 1):
            try:
                with pool.connection() as conn:
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.execute(INSERT_SQL, record)
                            cur.execute(
                                "SELECT payload FROM reports WHERE event_id=%s",
                                (record["event_id"],),
                            )
                            original_report = cur.fetchone()
                            if original_report is not None:
                                # First accepted identity owns immutable observed facts.
                                # A retry must not overwrite its raw archive with a
                                # different narrative or a later model version.
                                event_data = original_report["payload"]
                            cur.execute(
                                "INSERT INTO locations(name,latitude,longitude) VALUES (%s,%s,%s) "
                                "ON CONFLICT(name,latitude,longitude) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                                (
                                    record["location"],
                                    record["latitude"],
                                    record["longitude"],
                                ),
                            )
                            location_id = cur.fetchone()["id"]
                            cur.execute(
                                "INSERT INTO reports(event_id,location_id,payload,source_topic,source_partition,source_offset) "
                                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(event_id) DO NOTHING",
                                (
                                    record["event_id"],
                                    location_id,
                                    Jsonb(event_data),
                                    msg.topic(),
                                    partition,
                                    offset,
                                ),
                            )
                db_success = True
                break
            except Exception as dbe:
                logger.warning(
                    f"Database write attempt {attempt}/{max_db_retries} failed: {dbe}"
                )
                time.sleep(min(0.5 * (2 ** (attempt - 1)), 5.0))

        if db_success:
            try:
                lake_writer.write_batch([event_data])
            except Exception:
                consumer.close()
                raise
            # Commit offset strictly after successful persistence
            consumer.commit(message=msg, asynchronous=False)
            logger.info(
                f"Persisted event {record['event_id']} [{record['disaster_type']}] at {record['location']} (Offset: {offset})"
            )

        else:
            consumer.close()
            raise RuntimeError(
                "Database retries exhausted; source offset remains uncommitted"
            )

    # Graceful shutdown
    consumer.close()
    logger.info("Consumer closed successfully.")


if __name__ == "__main__":
    run_consumer()
