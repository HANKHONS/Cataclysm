"""Regression tests for callback polling without a live Kafka broker."""

from unittest.mock import patch

from src.api import main
from src.common.models import CrisisEvent


def test_ingestion_polls_until_delivery_callback():
    class Message:
        def partition(self):
            return 1

        def offset(self):
            return 42

    class Producer:
        callback = None

        def produce(self, **kwargs):
            self.callback = kwargs["on_delivery"]

        def poll(self, timeout):
            if timeout > 0:
                self.callback(None, Message())

    event = CrisisEvent(
        event_id="test-delivery",
        timestamp="2026-11-05T00:00:00Z",
        text="river flood",
        location="test",
        disaster_type="flood",
        severity="minor",
        latitude=12,
        longitude=77,
        source="test",
    )
    with patch.object(main, "producer", Producer()):
        result = main.ingest_event(event)
    assert result["offset"] == 42
    assert result["status"] == "confirmed"
