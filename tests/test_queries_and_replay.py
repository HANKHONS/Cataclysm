"""Regression tests for query safety, provenance, timestamps and replay fidelity."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from src.api.queries import event_filter, provenance
from src.common.models import CrisisEvent, Metadata
from src.data.replay import replay_from_parquet
from src.storage.lake_writer import ParquetLakeWriter


def event():
    return dict(
        event_id="replay-test",
        timestamp="2026-11-10T01:00:00+05:30",
        text="test flood",
        location="test",
        disaster_type="flood",
        latitude=0,
        longitude=0,
        severity="minor",
        source="usgs_feed",
        metadata={"provider_url": "https://example.org/event", "magnitude": 3.1},
    )


def test_timestamp_is_normalized_to_utc():
    value = CrisisEvent(**event()).timestamp
    assert value == datetime(2026, 11, 9, 19, 30, tzinfo=timezone.utc)
    payload = event()
    payload["timestamp"] = "2026-11-10T01:00:00"
    assert CrisisEvent(**payload).timestamp.tzinfo == timezone.utc


@pytest.mark.parametrize("field", ["rainfall_mm", "temperature_c", "magnitude"])
def test_nonfinite_metadata_rejected(field):
    with pytest.raises(ValidationError):
        Metadata(**{field: float("nan")})


def test_filter_parameters_cannot_become_sql():
    sql, params = event_filter(q="'; DROP TABLE reports; --", bbox="-1,-2,3,4")
    assert "DROP" not in sql
    assert "DROP" in params[0]
    assert params[-4:] == [-1, 3, -2, 4]


@pytest.mark.parametrize("bbox", ["1,2,3", "nan,0,1,2", "10,0,-10,1", "0,-91,1,2"])
def test_invalid_bbox_rejected(bbox):
    with pytest.raises(HTTPException):
        event_filter(bbox=bbox)


def test_provenance_does_not_promote_unknown_sources_to_live():
    assert provenance({"source": "citizen_report"})["data_mode"] == "unverified"
    assert provenance({"source": "usgs_feed"})["data_mode"] == "live"
    assert (
        provenance({"source": "usgs_feed", "metadata": {"is_replay": True}})[
            "data_mode"
        ]
        == "replay"
    )
    assert provenance({"source": "synthetic_demo"})["data_mode"] == "synthetic"


def test_sse_rows_remain_json_serializable():
    import json

    row = provenance(
        {
            "source": "test",
            "timestamp": datetime.now(timezone.utc),
            "created_at": datetime.now(timezone.utc),
        }
    )
    assert isinstance(json.loads(json.dumps(row))["created_at"], str)


def test_usgs_bad_feature_does_not_discard_good_neighbor():
    from src.ingestion.clients import usgs_features_to_events

    good = {
        "id": "valid",
        "properties": {"time": 1700000000000, "mag": 3},
        "geometry": {"coordinates": [77, 12]},
    }
    bad = {
        "id": "invalid",
        "properties": {"time": "not numeric"},
        "geometry": {"coordinates": [77, 12]},
    }
    assert [
        e["event_id"] for e in usgs_features_to_events({"features": [bad, good]})
    ] == ["USGS-valid"]


def test_local_replay_preserves_metadata_and_limits_rows(tmp_path):
    payload = event()
    # Explicit local mode without credentials, independent of the user's .env.
    with patch("src.storage.lake_writer.config.MINIO_ACCESS_KEY", ""):
        ParquetLakeWriter(base_dir=str(tmp_path)).write_batch([payload])
    producer = MagicMock()

    def deliver(*args, **kwargs):
        import json

        restored = json.loads(kwargs["value"])
        assert restored["metadata"]["magnitude"] == 3.1
        assert (
            restored["metadata"]["provider_url"] == payload["metadata"]["provider_url"]
        )
        assert restored["metadata"]["is_replay"] is True
        kwargs["on_delivery"](None, None)

    producer.produce.side_effect = deliver
    producer.flush.return_value = 0
    with patch("src.data.replay.Producer", return_value=producer):
        assert replay_from_parquet(str(tmp_path), limit=1) == 1
    producer.produce.assert_called_once()


def test_replay_delivery_failure_is_not_reported_success(tmp_path):
    with patch("src.storage.lake_writer.config.MINIO_ACCESS_KEY", ""):
        ParquetLakeWriter(base_dir=str(tmp_path)).write_batch([event()])
    producer = MagicMock()
    producer.flush.return_value = 1
    with patch("src.data.replay.Producer", return_value=producer):
        with pytest.raises(RuntimeError, match="Replay failed"):
            replay_from_parquet(str(tmp_path))
