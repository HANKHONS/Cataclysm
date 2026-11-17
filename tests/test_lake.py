"""Verify archive fidelity and protection against object-key path injection."""

import io

import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from src.common.models import CrisisEvent
from src.storage.lake_writer import ParquetLakeWriter


def test_archive_preserves_missing_and_zero_telemetry():
    class Client:
        def put_object(self, **kwargs):
            self.body = kwargs["Body"]

    client = Client()
    ParquetLakeWriter(s3_client=client).write_batch(
        [
            {
                "event_id": "test",
                "timestamp": "2026-11-05T00:00:00Z",
                "disaster_type": "flood",
                "metadata": {"temperature_c": 0.0},
            }
        ]
    )
    row = pq.ParquetFile(io.BytesIO(client.body)).read().to_pylist()[0]
    assert row["rainfall_mm"] is None
    assert row["temperature_c"] == 0.0


def test_event_id_cannot_escape_archive_directory():
    with pytest.raises(ValidationError):
        CrisisEvent(
            event_id="../../outside",
            timestamp="2026-11-05T00:00:00Z",
            text="test",
            location="test",
            disaster_type="flood",
            latitude=0,
            longitude=0,
            severity="minor",
            source="test",
        )
