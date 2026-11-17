import unittest
from datetime import datetime
from pydantic import ValidationError
from src.common.models import CrisisEvent, Metadata, DLQMessage

class TestCrisisSchemas(unittest.TestCase):
    def test_valid_crisis_event(self):
        event = CrisisEvent(
            event_id="EVT-TEST01",
            timestamp=datetime.utcnow(),
            text="Severe waterlogging in downtown",
            location="Mumbai",
            disaster_type="flood",
            latitude=19.0760,
            longitude=72.8777,
            severity="critical",
            source="citizen_report",
            metadata=Metadata(rainfall_mm=120.5, water_level_m=3.2)
        )
        self.assertEqual(event.event_id, "EVT-TEST01")
        self.assertEqual(event.disaster_type, "flood")
        self.assertEqual(event.metadata.rainfall_mm, 120.5)

    def test_invalid_coordinates(self):
        with self.assertRaises(ValidationError):
            CrisisEvent(
                event_id="EVT-BADCOORD",
                timestamp=datetime.utcnow(),
                text="Invalid latitude",
                location="Unknown",
                disaster_type="flood",
                latitude=150.0,  # Invalid (>90)
                longitude=72.0,
                severity="minor",
                source="sensor",
                metadata=Metadata()
            )

    def test_dlq_message_serialization(self):
        dlq = DLQMessage(
            raw_payload="invalid-bytes",
            error_type="JSONDecodeError",
            error_message="Expecting value",
            source_topic="crisis-events",
            source_partition=1,
            source_offset=42
        )
        self.assertEqual(dlq.source_topic, "crisis-events")
        self.assertEqual(dlq.source_partition, 1)
        self.assertEqual(dlq.source_offset, 42)

if __name__ == "__main__":
    unittest.main()
