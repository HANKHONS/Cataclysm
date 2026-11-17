"""Read lake Parquet contents and count records, ignoring metadata objects."""

import io
import json
import pyarrow.parquet as pq
from src.storage.lake_writer import ParquetLakeWriter


def main() -> None:
    client = ParquetLakeWriter()._client()
    report = {}
    for layer in ("raw", "processed", "curated"):
        bucket = "cataclysm-" + layer
        files = rows = 0
        prefix = {
            "raw": "",
            "processed": "events-v2/",
            "curated": "windowed_incident_counts-v2/",
        }[layer]
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix
        ):
            for item in page.get("Contents", []):
                if not item["Key"].endswith(".parquet"):
                    continue
                body = client.get_object(Bucket=bucket, Key=item["Key"])["Body"]
                try:
                    rows += pq.ParquetFile(io.BytesIO(body.read())).metadata.num_rows
                    files += 1
                finally:
                    body.close()
        report[layer] = {"parquet_files": files, "rows": rows}
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
