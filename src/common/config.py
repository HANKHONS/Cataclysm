import os
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)

DEFAULT_INGESTION_API_URL = "http://127.0.0.1:8000/events"


class Config:
    # Database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://cataclysm:"
        + quote(os.getenv("POSTGRES_PASSWORD", ""), safe="")
        + "@127.0.0.1:5433/cataclysm",
    )
    DB_POOL_MIN_SIZE: int = int(os.getenv("DB_POOL_MIN_SIZE", "2"))
    DB_POOL_MAX_SIZE: int = int(os.getenv("DB_POOL_MAX_SIZE", "10"))

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"
    )
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "crisis-events")
    KAFKA_DLQ_TOPIC: str = os.getenv("KAFKA_DLQ_TOPIC", "crisis-events-dlq")
    KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "crisis-intelligence-group")

    # MinIO / S3 Data Lake
    MINIO_ENDPOINT: str = os.getenv(
        "MINIO_ENDPOINT", "localhost:" + os.getenv("MINIO_API_PORT", "19000")
    )
    MINIO_ACCESS_KEY: str = os.getenv(
        "MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "")
    )
    MINIO_SECRET_KEY: str = os.getenv(
        "MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "")
    )
    MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    MINIO_BUCKET_RAW: str = os.getenv("MINIO_BUCKET_RAW", "cataclysm-raw")
    MINIO_BUCKET_PROCESSED: str = os.getenv(
        "MINIO_BUCKET_PROCESSED", "cataclysm-processed"
    )
    MINIO_BUCKET_CURATED: str = os.getenv("MINIO_BUCKET_CURATED", "cataclysm-curated")

    # Spark
    SPARK_MASTER_URL: str = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    # External source credentials. USGS is public; OpenWeather requires this key.
    OPENWEATHER_API_KEY: str = os.getenv("OPENWEATHER_API_KEY", "")
    INGESTION_API_URL: str = os.getenv(
        "INGESTION_API_URL", DEFAULT_INGESTION_API_URL
    )
    INGESTION_TIMEOUT_SECONDS: float = float(
        os.getenv("INGESTION_TIMEOUT_SECONDS", "15")
    )


config = Config()
