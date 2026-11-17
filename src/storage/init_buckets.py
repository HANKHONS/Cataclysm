"""Initialize lake buckets using the existing boto3 client, with bounded retries."""

import time
from botocore.exceptions import ClientError
from src.common.config import config
from src.storage.lake_writer import ParquetLakeWriter


def main() -> None:
    client = ParquetLakeWriter()._client()
    for attempt in range(30):
        try:
            client.list_buckets()
            break
        except Exception:
            if attempt == 29:
                raise
            time.sleep(2)
    for bucket in (
        config.MINIO_BUCKET_RAW,
        config.MINIO_BUCKET_PROCESSED,
        config.MINIO_BUCKET_CURATED,
    ):
        try:
            client.create_bucket(Bucket=bucket)
        except ClientError as error:
            if error.response["Error"]["Code"] not in (
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            ):
                raise


if __name__ == "__main__":
    main()
