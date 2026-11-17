"""Regression checks: a failed sink must never advance source offsets."""

from unittest.mock import MagicMock, patch
import pytest
from src.kafka import consumer as module


def test_database_failure_does_not_commit_or_read_past_record():
    kafka = MagicMock()
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = b'{"event_id":"failure-test","timestamp":"2026-11-05T00:00:00Z","text":"flood","location":"test","disaster_type":"flood","severity":"minor","latitude":0,"longitude":0,"source":"test"}'
    kafka.poll.return_value = message
    pool = MagicMock()
    pool.connection.side_effect = ConnectionError("simulated database outage")
    with (
        patch.object(module, "Consumer", return_value=kafka),
        patch.object(module, "Producer"),
        patch.object(module, "init_db"),
        patch.object(module, "get_pool", return_value=pool),
        patch.object(module.time, "sleep"),
        patch.object(module, "predict_disaster_type", return_value=("flood", 0)),
        patch.object(module, "predict_severity", return_value=("minor", 0)),
    ):
        with pytest.raises(RuntimeError, match="Database retries exhausted"):
            module.run_consumer()
    kafka.commit.assert_not_called()
    assert kafka.poll.call_count == 1
    kafka.close.assert_called_once()


def test_failed_dlq_delivery_does_not_commit_source():
    kafka = MagicMock()
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = b"not-json"
    message.partition.return_value = 0
    message.offset.return_value = 1
    kafka.poll.return_value = message
    producer = MagicMock()
    producer.flush.return_value = 1
    with (
        patch.object(module, "Consumer", return_value=kafka),
        patch.object(module, "Producer", return_value=producer),
        patch.object(module, "init_db"),
        patch.object(module, "get_pool"),
    ):
        with pytest.raises(RuntimeError, match="DLQ delivery failed"):
            module.run_consumer()
    kafka.commit.assert_not_called()


def test_archive_failure_after_transaction_does_not_commit():
    kafka = MagicMock()
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = b'{"event_id":"archive-failure","timestamp":"2026-11-10T00:00:00Z","text":"flood","location":"test","disaster_type":"flood","severity":"minor","latitude":0,"longitude":0,"source":"synthetic_test"}'
    kafka.poll.return_value = message
    pool = MagicMock()
    cursor = pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [None, {"id": 1}]
    lake = MagicMock()
    lake.write_batch.side_effect = OSError("simulated unavailable archive")
    with (
        patch.object(module, "Consumer", return_value=kafka),
        patch.object(module, "Producer"),
        patch.object(module, "init_db"),
        patch.object(module, "get_pool", return_value=pool),
        patch.object(module, "ParquetLakeWriter", return_value=lake),
        patch.object(module, "predict_disaster_type", return_value=("flood", 0)),
        patch.object(module, "predict_severity", return_value=("unknown", 0)),
        patch.object(
            module,
            "model_status",
            return_value={"classifier": "heuristic", "severity": "heuristic"},
        ),
    ):
        with pytest.raises(OSError, match="unavailable archive"):
            module.run_consumer()
    kafka.commit.assert_not_called()
    kafka.close.assert_called_once()
    assert kafka.poll.call_count == 1
