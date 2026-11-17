# Tests and recovery: match evidence to the failure boundary

The offline pytest suite uses mocks only where actual network services would make unit tests nondeterministic. Schema tests check IDs/coordinates and serialization. Delivery test uses a callback-producing fake to prove the API polls rather than waits without servicing callbacks. Consumer reliability tests exhaust database retries or fail DLQ flush and assert no source commit/read-past. Lake tests read real in-memory Parquet and distinguish missing/zero values. Credential tests reject missing weather key before request and verify metric-unit/wind conversion. ML tests force heuristic mode, so ignored model artifacts do not silently change their expectations. Anomaly tests cover training validity and boundary extrapolation guard.

The added query/replay tests check UTC normalization, finite metadata, parameter isolation, invalid bbox values (including NaN), unknown-versus-live/synthetic/replay provenance, SSE JSON serializability, malformed USGS feature isolation, local real Parquet metadata restoration and unconfirmed replay failure. A test that only mirrors an expression would not prove a useful boundary; these focus on observed defect paths. Workspace `--basetemp` avoids a restricted Windows temporary-directory issue; pytest manages only that dedicated disposable path.

## Live scripts, exact responsibilities

- `demo_smoke`: creates a labelled event, POSTs, waits for DB/two predictions/exact raw object, resubmits and checks operational group commit plus one row, optionally searches actual processed files for the ID. File existence alone is not its Spark assertion.
- `check_dlq`: publishes a uniquely marked malformed Kafka payload, verifies producer callback, consumes an independent DLQ group and matches the exact diagnostic marker/source offset. It preserves operational offsets.
- `audit_lake`: pages current-layer files and sums Parquet metadata row counts; excludes non-Parquet metadata and uses v2 processed/curated prefixes. It is aggregate readback, weaker than matching a known ID/window.
- `verify_application`: checks live 422/empty/search/pagination/aggregate sums, provider labels, a consumed conflicting-ID retry, details predictions, actual parsed SSE, both monitoring scrape jobs, Grafana DB health and SQL query-plan output. It writes structured evidence.
- `verify_crash_replay`: creates a dedicated topic/group, starts this actual consumer in a subprocess whose raw-write entry calls os._exit(91), verifies DB row/no committed offset, then restarts normal processing with a wrapper that ends after real commit. The two child modes isolate the precise real boundary. Existing production group offsets are untouched.
- `verify_spark_semantics`: invokes actual clean_events with deterministic JSON-file microbatches, separate directories/checkpoint and local[2] Spark. It restarts dedup state, sends late/advance fixtures, checks normalized rows and state metric, then verifies two exact final count-one windows. It does not claim this isolated source tests production Kafka connectivity; smoke covers that.
- `verify_dependency_outage`: host script stops/restores DB, observes GET/ready 503, then stops/restores broker, observes POST 503 and retries the same identity. Finally blocks restore each service and readiness. It intentionally causes brief downtime and does not delete data.
- `recovery_check`: snapshots up to 500 visible IDs, restarts DB/broker/MinIO/streaming and waits for those IDs. This establishes old-row persistence for that bounded snapshot, not raw/lake restoration or new write success; run smoke afterward.
- `check_stability`: samples selected container restart/memory state for a configured duration without submitting an event; run smoke separately to verify fresh writes. A finite stability test is not indefinite uptime evidence.

All live scripts add labelled test records or retained isolated fixtures; they do not reset offsets, truncate tables or clear buckets. Timeouts are explicit, and assertions failing should remain failures. Some service outage branches remain source/mocked evidence only; the failure-scenario table distinguishes them instead of claiming every row has been executed.

Before changing commit/state behavior, run the narrow regression first, then the corresponding actual service fixture and fresh smoke. Full pytest success cannot prove running Docker services or UI behavior. Conversely, one smoke success cannot replace negative-path tests. Preserve raw JSON outputs with date/scope and avoid embedding local secrets or connection URLs in reports.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `tests/test_anomaly.py`

[Open source](../../tests/test_anomaly.py) · SHA-256 `259e08f4c39997e1380e5101f187cecc939d21604f5a12d6b8a5a5c05fc1b69c`

Dependencies: `import pytest`; `from src.ml.anomaly import train_report_rate_detector, anomaly_scores`

- `test_anomaly_requires_history`: lines 5–7. Read its control flow with the module guide above.

- `test_report_spike_is_anomalous`: lines 10–14. Read its control flow with the module guide above.


```text
0001 | import pytest
0002 | from src.ml.anomaly import train_report_rate_detector, anomaly_scores
0003 | 
0004 | 
0005 | def test_anomaly_requires_history():
0006 |     with pytest.raises(ValueError):
0007 |         train_report_rate_detector([1, 2])
0008 | 
0009 | 
0010 | def test_report_spike_is_anomalous():
0011 |     model = train_report_rate_detector([8, 9, 10, 11, 12] * 20)
0012 |     result = anomaly_scores(model, [10, 1000])
0013 |     assert not result[0]["is_anomaly"]
0014 |     assert result[1]["is_anomaly"]
```

### `tests/test_consumer_reliability.py`

[Open source](../../tests/test_consumer_reliability.py) · SHA-256 `d91623a3c939db3584b1bd98e5bbb6a58bd47fa57c14a78c8f353a749922ba37`

Dependencies: `from unittest.mock import MagicMock, patch`; `import pytest`; `from src.kafka import consumer as module`

- `test_database_failure_does_not_commit_or_read_past_record`: lines 8–29. Read its control flow with the module guide above.

- `test_failed_dlq_delivery_does_not_commit_source`: lines 32–50. Read its control flow with the module guide above.

- `test_archive_failure_after_transaction_does_not_commit`: lines 53–82. Read its control flow with the module guide above.


```text
0001 | """Regression checks: a failed sink must never advance source offsets."""
0002 | 
0003 | from unittest.mock import MagicMock, patch
0004 | import pytest
0005 | from src.kafka import consumer as module
0006 | 
0007 | 
0008 | def test_database_failure_does_not_commit_or_read_past_record():
0009 |     kafka = MagicMock()
0010 |     message = MagicMock()
0011 |     message.error.return_value = None
0012 |     message.value.return_value = b'{"event_id":"failure-test","timestamp":"2026-11-05T00:00:00Z","text":"flood","location":"test","disaster_type":"flood","severity":"minor","latitude":0,"longitude":0,"source":"test"}'
0013 |     kafka.poll.return_value = message
0014 |     pool = MagicMock()
0015 |     pool.connection.side_effect = ConnectionError("simulated database outage")
0016 |     with (
0017 |         patch.object(module, "Consumer", return_value=kafka),
0018 |         patch.object(module, "Producer"),
0019 |         patch.object(module, "init_db"),
0020 |         patch.object(module, "get_pool", return_value=pool),
0021 |         patch.object(module.time, "sleep"),
0022 |         patch.object(module, "predict_disaster_type", return_value=("flood", 0)),
0023 |         patch.object(module, "predict_severity", return_value=("minor", 0)),
0024 |     ):
0025 |         with pytest.raises(RuntimeError, match="Database retries exhausted"):
0026 |             module.run_consumer()
0027 |     kafka.commit.assert_not_called()
0028 |     assert kafka.poll.call_count == 1
0029 |     kafka.close.assert_called_once()
0030 | 
0031 | 
0032 | def test_failed_dlq_delivery_does_not_commit_source():
0033 |     kafka = MagicMock()
0034 |     message = MagicMock()
0035 |     message.error.return_value = None
0036 |     message.value.return_value = b"not-json"
0037 |     message.partition.return_value = 0
0038 |     message.offset.return_value = 1
0039 |     kafka.poll.return_value = message
0040 |     producer = MagicMock()
0041 |     producer.flush.return_value = 1
0042 |     with (
0043 |         patch.object(module, "Consumer", return_value=kafka),
0044 |         patch.object(module, "Producer", return_value=producer),
0045 |         patch.object(module, "init_db"),
0046 |         patch.object(module, "get_pool"),
0047 |     ):
0048 |         with pytest.raises(RuntimeError, match="DLQ delivery failed"):
0049 |             module.run_consumer()
0050 |     kafka.commit.assert_not_called()
0051 | 
0052 | 
0053 | def test_archive_failure_after_transaction_does_not_commit():
0054 |     kafka = MagicMock()
0055 |     message = MagicMock()
0056 |     message.error.return_value = None
0057 |     message.value.return_value = b'{"event_id":"archive-failure","timestamp":"2026-11-10T00:00:00Z","text":"flood","location":"test","disaster_type":"flood","severity":"minor","latitude":0,"longitude":0,"source":"synthetic_test"}'
0058 |     kafka.poll.return_value = message
0059 |     pool = MagicMock()
0060 |     cursor = pool.connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
0061 |     cursor.fetchone.side_effect = [None, {"id": 1}]
0062 |     lake = MagicMock()
0063 |     lake.write_batch.side_effect = OSError("simulated unavailable archive")
0064 |     with (
0065 |         patch.object(module, "Consumer", return_value=kafka),
0066 |         patch.object(module, "Producer"),
0067 |         patch.object(module, "init_db"),
0068 |         patch.object(module, "get_pool", return_value=pool),
0069 |         patch.object(module, "ParquetLakeWriter", return_value=lake),
0070 |         patch.object(module, "predict_disaster_type", return_value=("flood", 0)),
0071 |         patch.object(module, "predict_severity", return_value=("unknown", 0)),
0072 |         patch.object(
0073 |             module,
0074 |             "model_status",
0075 |             return_value={"classifier": "heuristic", "severity": "heuristic"},
0076 |         ),
0077 |     ):
0078 |         with pytest.raises(OSError, match="unavailable archive"):
0079 |             module.run_consumer()
0080 |     kafka.commit.assert_not_called()
0081 |     kafka.close.assert_called_once()
0082 |     assert kafka.poll.call_count == 1
```

### `tests/test_delivery.py`

[Open source](../../tests/test_delivery.py) · SHA-256 `d5ecec2dca0e57b6fad01f2e762fe1987f489406ef50e38c0e7fab5fb72f99c3`

Dependencies: `from unittest.mock import patch`; `from src.api import main`; `from src.common.models import CrisisEvent`

- `test_ingestion_polls_until_delivery_callback`: lines 9–41. Read its control flow with the module guide above.

- `Message`: lines 10–15. Read its control flow with the module guide above.

- `partition`: lines 11–12. Read its control flow with the module guide above.

- `offset`: lines 14–15. Read its control flow with the module guide above.

- `Producer`: lines 17–25. Read its control flow with the module guide above.

- `produce`: lines 20–21. Read its control flow with the module guide above.

- `poll`: lines 23–25. Read its control flow with the module guide above.


```text
0001 | """Regression tests for callback polling without a live Kafka broker."""
0002 | 
0003 | from unittest.mock import patch
0004 | 
0005 | from src.api import main
0006 | from src.common.models import CrisisEvent
0007 | 
0008 | 
0009 | def test_ingestion_polls_until_delivery_callback():
0010 |     class Message:
0011 |         def partition(self):
0012 |             return 1
0013 | 
0014 |         def offset(self):
0015 |             return 42
0016 | 
0017 |     class Producer:
0018 |         callback = None
0019 | 
0020 |         def produce(self, **kwargs):
0021 |             self.callback = kwargs["on_delivery"]
0022 | 
0023 |         def poll(self, timeout):
0024 |             if timeout > 0:
0025 |                 self.callback(None, Message())
0026 | 
0027 |     event = CrisisEvent(
0028 |         event_id="test-delivery",
0029 |         timestamp="2026-11-05T00:00:00Z",
0030 |         text="river flood",
0031 |         location="test",
0032 |         disaster_type="flood",
0033 |         severity="minor",
0034 |         latitude=12,
0035 |         longitude=77,
0036 |         source="test",
0037 |     )
0038 |     with patch.object(main, "producer", Producer()):
0039 |         result = main.ingest_event(event)
0040 |     assert result["offset"] == 42
0041 |     assert result["status"] == "confirmed"
```

### `tests/test_ingestion.py`

[Open source](../../tests/test_ingestion.py) · SHA-256 `dd46c3fed9a324e4b898bac4f69f8ddcaeb97e6f2f0d5352952fd339250f128d`

Dependencies: `from src.ingestion.clients import (     usgs_features_to_events,     weather_to_event, )`

- `test_usgs_normalization_preserves_coordinates_and_id`: lines 7–24. Read its control flow with the module guide above.

- `test_weather_converts_mps_to_kmh`: lines 27–40. Read its control flow with the module guide above.


```text
0001 | from src.ingestion.clients import (
0002 |     usgs_features_to_events,
0003 |     weather_to_event,
0004 | )
0005 | 
0006 | 
0007 | def test_usgs_normalization_preserves_coordinates_and_id():
0008 |     events = usgs_features_to_events(
0009 |         {
0010 |             "features": [
0011 |                 {
0012 |                     "id": "abc",
0013 |                     "properties": {
0014 |                         "mag": 6.1,
0015 |                         "time": 1_700_000_000_000,
0016 |                         "place": "test",
0017 |                     },
0018 |                     "geometry": {"coordinates": [77.0, 12.0]},
0019 |                 }
0020 |             ]
0021 |         }
0022 |     )
0023 |     assert events[0]["event_id"] == "USGS-abc"
0024 |     assert events[0]["severity"] == "severe"
0025 | 
0026 | 
0027 | def test_weather_converts_mps_to_kmh():
0028 |     event = weather_to_event(
0029 |         {
0030 |             "id": 1,
0031 |             "dt": 1_700_000_000,
0032 |             "name": "test",
0033 |             "coord": {},
0034 |             "wind": {"speed": 10},
0035 |             "main": {"temp": 20},
0036 |         },
0037 |         12,
0038 |         77,
0039 |     )
0040 |     assert event["metadata"]["wind_speed_kmh"] == 36.0
```

### `tests/test_lake.py`

[Open source](../../tests/test_lake.py) · SHA-256 `6bfc2f92886152bf990b0db54328592eba0effd2e2d74ddce85877677e15fe82`

Dependencies: `import io`; `import pyarrow.parquet as pq`; `import pytest`; `from pydantic import ValidationError`; `from src.common.models import CrisisEvent`; `from src.storage.lake_writer import ParquetLakeWriter`

- `test_archive_preserves_missing_and_zero_telemetry`: lines 13–31. Read its control flow with the module guide above.

- `Client`: lines 14–16. Read its control flow with the module guide above.

- `put_object`: lines 15–16. Read its control flow with the module guide above.

- `test_event_id_cannot_escape_archive_directory`: lines 34–46. Read its control flow with the module guide above.


```text
0001 | """Verify archive fidelity and protection against object-key path injection."""
0002 | 
0003 | import io
0004 | 
0005 | import pyarrow.parquet as pq
0006 | import pytest
0007 | from pydantic import ValidationError
0008 | 
0009 | from src.common.models import CrisisEvent
0010 | from src.storage.lake_writer import ParquetLakeWriter
0011 | 
0012 | 
0013 | def test_archive_preserves_missing_and_zero_telemetry():
0014 |     class Client:
0015 |         def put_object(self, **kwargs):
0016 |             self.body = kwargs["Body"]
0017 | 
0018 |     client = Client()
0019 |     ParquetLakeWriter(s3_client=client).write_batch(
0020 |         [
0021 |             {
0022 |                 "event_id": "test",
0023 |                 "timestamp": "2026-11-05T00:00:00Z",
0024 |                 "disaster_type": "flood",
0025 |                 "metadata": {"temperature_c": 0.0},
0026 |             }
0027 |         ]
0028 |     )
0029 |     row = pq.ParquetFile(io.BytesIO(client.body)).read().to_pylist()[0]
0030 |     assert row["rainfall_mm"] is None
0031 |     assert row["temperature_c"] == 0.0
0032 | 
0033 | 
0034 | def test_event_id_cannot_escape_archive_directory():
0035 |     with pytest.raises(ValidationError):
0036 |         CrisisEvent(
0037 |             event_id="../../outside",
0038 |             timestamp="2026-11-05T00:00:00Z",
0039 |             text="test",
0040 |             location="test",
0041 |             disaster_type="flood",
0042 |             latitude=0,
0043 |             longitude=0,
0044 |             severity="minor",
0045 |             source="test",
0046 |         )
```

### `tests/test_ml.py`

[Open source](../../tests/test_ml.py) · SHA-256 `c1596a37ef0f0fc7bebfdfd5942e551dac8759d0ba9f5d9c20c7cfe7204cdc78`

Dependencies: `import unittest`; `from unittest.mock import patch`; `from src.ml.inference import predict_disaster_type, predict_severity`

- `TestMLInference`: lines 6–34. Read its control flow with the module guide above.

- `setUp`: lines 7–12. Read its control flow with the module guide above.

- `test_flood_text_prediction`: lines 14–18. Read its control flow with the module guide above.

- `test_earthquake_text_prediction`: lines 20–24. Read its control flow with the module guide above.

- `test_critical_severity_prediction`: lines 26–29. Read its control flow with the module guide above.

- `test_minor_severity_prediction`: lines 31–34. Read its control flow with the module guide above.


```text
0001 | import unittest
0002 | from unittest.mock import patch
0003 | from src.ml.inference import predict_disaster_type, predict_severity
0004 | 
0005 | 
0006 | class TestMLInference(unittest.TestCase):
0007 |     def setUp(self):
0008 |         self.state = patch.multiple(
0009 |             "src.ml.inference", _models_loaded=True, _text_model=None, _sev_model=None
0010 |         )
0011 |         self.state.start()
0012 |         self.addCleanup(self.state.stop)
0013 | 
0014 |     def test_flood_text_prediction(self):
0015 |         text = "The river has burst its banks and rising floodwaters are submerging houses."
0016 |         pred_type, conf = predict_disaster_type(text)
0017 |         self.assertEqual(pred_type, "flood")
0018 |         self.assertEqual(conf, 0.0)
0019 | 
0020 |     def test_earthquake_text_prediction(self):
0021 |         text = "Strong seismic tremors and shaking reported, magnitude 6.2 on the Richter scale."
0022 |         pred_type, conf = predict_disaster_type(text)
0023 |         self.assertEqual(pred_type, "earthquake")
0024 |         self.assertEqual(conf, 0.0)
0025 | 
0026 |     def test_critical_severity_prediction(self):
0027 |         sev, conf = predict_severity(rainfall_mm=250.0, water_level_m=6.5)
0028 |         self.assertEqual(sev, "critical")
0029 |         self.assertEqual(conf, 0.0)
0030 | 
0031 |     def test_minor_severity_prediction(self):
0032 |         sev, conf = predict_severity(rainfall_mm=10.0, water_level_m=0.2)
0033 |         self.assertEqual(sev, "minor")
0034 |         self.assertEqual(conf, 0.0)
0035 | 
0036 | 
0037 | if __name__ == "__main__":
0038 |     unittest.main()
```

### `tests/test_open_meteo.py`

[Open source](../../tests/test_open_meteo.py) · SHA-256 `493786386aa695270da99c422bc7095dac26af18663e1adbf366204a43d9ad53`

Dependencies: `from unittest.mock import Mock`; `import pytest`; `from src.ingestion.clients import fetch_open_meteo, open_meteo_to_event`; `from src.api.queries import provenance`

- `payload`: lines 8–11. Read its control flow with the module guide above.

- `test_weather_identity_units_and_model_provenance`: lines 14–21. Read its control flow with the module guide above.

- `test_unknown_units_rejected`: lines 24–28. Read its control flow with the module guide above.

- `test_missing_provider_time_rejected`: lines 31–35. Read its control flow with the module guide above.

- `test_request_is_keyless_and_uses_explicit_time_and_units`: lines 38–45. Read its control flow with the module guide above.


```text
0001 | """Weather-model attribution, temporal support and missing-value regressions."""
0002 | from unittest.mock import Mock
0003 | import pytest
0004 | from src.ingestion.clients import fetch_open_meteo, open_meteo_to_event
0005 | from src.api.queries import provenance
0006 | 
0007 | 
0008 | def payload():
0009 |     return {"latitude": 19.1, "longitude": 72.9,
0010 |             "current_units": {"temperature_2m": "°C", "wind_speed_10m": "km/h", "precipitation": "mm"},
0011 |             "current": {"time": 1700000000, "interval": 900, "temperature_2m": 0, "wind_speed_10m": 0, "precipitation": 1.5}}
0012 | 
0013 | 
0014 | def test_weather_identity_units_and_model_provenance():
0015 |     event = open_meteo_to_event(payload(), 19.076, 72.8777)
0016 |     assert event == open_meteo_to_event(payload(), 19.076, 72.8777)
0017 |     assert event["metadata"]["temperature_c"] == 0
0018 |     assert event["metadata"]["rainfall_mm"] is None
0019 |     assert event["metadata"]["precipitation_interval_seconds"] == 900
0020 |     assert event["severity"] == "unknown"
0021 |     assert provenance(event)["data_mode"] == "weather_estimate"
0022 | 
0023 | 
0024 | def test_unknown_units_rejected():
0025 |     data = payload()
0026 |     data["current_units"]["wind_speed_10m"] = "m/s"
0027 |     with pytest.raises(ValueError, match="units"):
0028 |         open_meteo_to_event(data, 19, 72)
0029 | 
0030 | 
0031 | def test_missing_provider_time_rejected():
0032 |     data = payload()
0033 |     del data["current"]["time"]
0034 |     with pytest.raises(KeyError):
0035 |         open_meteo_to_event(data, 19, 72)
0036 | 
0037 | 
0038 | def test_request_is_keyless_and_uses_explicit_time_and_units():
0039 |     session = Mock()
0040 |     session.get.return_value.json.return_value = payload()
0041 |     fetch_open_meteo(session, 19, 72, 5)
0042 |     params = session.get.call_args.kwargs["params"]
0043 |     assert params["timeformat"] == "unixtime"
0044 |     assert params["wind_speed_unit"] == "kmh"
0045 |     assert "appid" not in params
```

### `tests/test_queries_and_replay.py`

[Open source](../../tests/test_queries_and_replay.py) · SHA-256 `801dd92f601a89538fa5034b2913f2063772e0f73ee12cd9ce468ce14a3e37ec`

Dependencies: `from datetime import datetime, timezone`; `from unittest.mock import MagicMock, patch`; `import pytest`; `from fastapi import HTTPException`; `from pydantic import ValidationError`; `from src.api.queries import event_filter, provenance`; `from src.common.models import CrisisEvent, Metadata`; `from src.data.replay import replay_from_parquet`; `from src.storage.lake_writer import ParquetLakeWriter`

- `event`: lines 16–28. Read its control flow with the module guide above.

- `test_timestamp_is_normalized_to_utc`: lines 31–36. Read its control flow with the module guide above.

- `test_nonfinite_metadata_rejected`: lines 40–42. Read its control flow with the module guide above.

- `test_filter_parameters_cannot_become_sql`: lines 45–49. Read its control flow with the module guide above.

- `test_invalid_bbox_rejected`: lines 53–55. Read its control flow with the module guide above.

- `test_provenance_does_not_promote_unknown_sources_to_live`: lines 58–67. Read its control flow with the module guide above.

- `test_sse_rows_remain_json_serializable`: lines 70–80. Read its control flow with the module guide above.

- `test_usgs_bad_feature_does_not_discard_good_neighbor`: lines 83–98. Read its control flow with the module guide above.

- `test_local_replay_preserves_metadata_and_limits_rows`: lines 101–123. Read its control flow with the module guide above.

- `deliver`: lines 108–117. Read its control flow with the module guide above.

- `test_replay_delivery_failure_is_not_reported_success`: lines 126–133. Read its control flow with the module guide above.


```text
0001 | """Regression tests for query safety, provenance, timestamps and replay fidelity."""
0002 | 
0003 | from datetime import datetime, timezone
0004 | from unittest.mock import MagicMock, patch
0005 | 
0006 | import pytest
0007 | from fastapi import HTTPException
0008 | from pydantic import ValidationError
0009 | 
0010 | from src.api.queries import event_filter, provenance
0011 | from src.common.models import CrisisEvent, Metadata
0012 | from src.data.replay import replay_from_parquet
0013 | from src.storage.lake_writer import ParquetLakeWriter
0014 | 
0015 | 
0016 | def event():
0017 |     return dict(
0018 |         event_id="replay-test",
0019 |         timestamp="2026-11-10T01:00:00+05:30",
0020 |         text="test flood",
0021 |         location="test",
0022 |         disaster_type="flood",
0023 |         latitude=0,
0024 |         longitude=0,
0025 |         severity="minor",
0026 |         source="usgs_feed",
0027 |         metadata={"provider_url": "https://example.org/event", "magnitude": 3.1},
0028 |     )
0029 | 
0030 | 
0031 | def test_timestamp_is_normalized_to_utc():
0032 |     value = CrisisEvent(**event()).timestamp
0033 |     assert value == datetime(2026, 11, 9, 19, 30, tzinfo=timezone.utc)
0034 |     payload = event()
0035 |     payload["timestamp"] = "2026-11-10T01:00:00"
0036 |     assert CrisisEvent(**payload).timestamp.tzinfo == timezone.utc
0037 | 
0038 | 
0039 | @pytest.mark.parametrize("field", ["rainfall_mm", "temperature_c", "magnitude"])
0040 | def test_nonfinite_metadata_rejected(field):
0041 |     with pytest.raises(ValidationError):
0042 |         Metadata(**{field: float("nan")})
0043 | 
0044 | 
0045 | def test_filter_parameters_cannot_become_sql():
0046 |     sql, params = event_filter(q="'; DROP TABLE reports; --", bbox="-1,-2,3,4")
0047 |     assert "DROP" not in sql
0048 |     assert "DROP" in params[0]
0049 |     assert params[-4:] == [-1, 3, -2, 4]
0050 | 
0051 | 
0052 | @pytest.mark.parametrize("bbox", ["1,2,3", "nan,0,1,2", "10,0,-10,1", "0,-91,1,2"])
0053 | def test_invalid_bbox_rejected(bbox):
0054 |     with pytest.raises(HTTPException):
0055 |         event_filter(bbox=bbox)
0056 | 
0057 | 
0058 | def test_provenance_does_not_promote_unknown_sources_to_live():
0059 |     assert provenance({"source": "citizen_report"})["data_mode"] == "unverified"
0060 |     assert provenance({"source": "usgs_feed"})["data_mode"] == "live"
0061 |     assert (
0062 |         provenance({"source": "usgs_feed", "metadata": {"is_replay": True}})[
0063 |             "data_mode"
0064 |         ]
0065 |         == "replay"
0066 |     )
0067 |     assert provenance({"source": "synthetic_demo"})["data_mode"] == "synthetic"
0068 | 
0069 | 
0070 | def test_sse_rows_remain_json_serializable():
0071 |     import json
0072 | 
0073 |     row = provenance(
0074 |         {
0075 |             "source": "test",
0076 |             "timestamp": datetime.now(timezone.utc),
0077 |             "created_at": datetime.now(timezone.utc),
0078 |         }
0079 |     )
0080 |     assert isinstance(json.loads(json.dumps(row))["created_at"], str)
0081 | 
0082 | 
0083 | def test_usgs_bad_feature_does_not_discard_good_neighbor():
0084 |     from src.ingestion.clients import usgs_features_to_events
0085 | 
0086 |     good = {
0087 |         "id": "valid",
0088 |         "properties": {"time": 1700000000000, "mag": 3},
0089 |         "geometry": {"coordinates": [77, 12]},
0090 |     }
0091 |     bad = {
0092 |         "id": "invalid",
0093 |         "properties": {"time": "not numeric"},
0094 |         "geometry": {"coordinates": [77, 12]},
0095 |     }
0096 |     assert [
0097 |         e["event_id"] for e in usgs_features_to_events({"features": [bad, good]})
0098 |     ] == ["USGS-valid"]
0099 | 
0100 | 
0101 | def test_local_replay_preserves_metadata_and_limits_rows(tmp_path):
0102 |     payload = event()
0103 |     # Explicit local mode without credentials, independent of the user's .env.
0104 |     with patch("src.storage.lake_writer.config.MINIO_ACCESS_KEY", ""):
0105 |         ParquetLakeWriter(base_dir=str(tmp_path)).write_batch([payload])
0106 |     producer = MagicMock()
0107 | 
0108 |     def deliver(*args, **kwargs):
0109 |         import json
0110 | 
0111 |         restored = json.loads(kwargs["value"])
0112 |         assert restored["metadata"]["magnitude"] == 3.1
0113 |         assert (
0114 |             restored["metadata"]["provider_url"] == payload["metadata"]["provider_url"]
0115 |         )
0116 |         assert restored["metadata"]["is_replay"] is True
0117 |         kwargs["on_delivery"](None, None)
0118 | 
0119 |     producer.produce.side_effect = deliver
0120 |     producer.flush.return_value = 0
0121 |     with patch("src.data.replay.Producer", return_value=producer):
0122 |         assert replay_from_parquet(str(tmp_path), limit=1) == 1
0123 |     producer.produce.assert_called_once()
0124 | 
0125 | 
0126 | def test_replay_delivery_failure_is_not_reported_success(tmp_path):
0127 |     with patch("src.storage.lake_writer.config.MINIO_ACCESS_KEY", ""):
0128 |         ParquetLakeWriter(base_dir=str(tmp_path)).write_batch([event()])
0129 |     producer = MagicMock()
0130 |     producer.flush.return_value = 1
0131 |     with patch("src.data.replay.Producer", return_value=producer):
0132 |         with pytest.raises(RuntimeError, match="Replay failed"):
0133 |             replay_from_parquet(str(tmp_path))
```

### `tests/test_schemas.py`

[Open source](../../tests/test_schemas.py) · SHA-256 `b56f4c31a76be145cd58955816a2906dc319358658e2d08231268bb0ec273859`

Dependencies: `import unittest`; `from datetime import datetime`; `from pydantic import ValidationError`; `from src.common.models import CrisisEvent, Metadata, DLQMessage`

- `TestCrisisSchemas`: lines 6–50. Read its control flow with the module guide above.

- `test_valid_crisis_event`: lines 7–22. Read its control flow with the module guide above.

- `test_invalid_coordinates`: lines 24–37. Read its control flow with the module guide above.

- `test_dlq_message_serialization`: lines 39–50. Read its control flow with the module guide above.


```text
0001 | import unittest
0002 | from datetime import datetime
0003 | from pydantic import ValidationError
0004 | from src.common.models import CrisisEvent, Metadata, DLQMessage
0005 | 
0006 | class TestCrisisSchemas(unittest.TestCase):
0007 |     def test_valid_crisis_event(self):
0008 |         event = CrisisEvent(
0009 |             event_id="EVT-TEST01",
0010 |             timestamp=datetime.utcnow(),
0011 |             text="Severe waterlogging in downtown",
0012 |             location="Mumbai",
0013 |             disaster_type="flood",
0014 |             latitude=19.0760,
0015 |             longitude=72.8777,
0016 |             severity="critical",
0017 |             source="citizen_report",
0018 |             metadata=Metadata(rainfall_mm=120.5, water_level_m=3.2)
0019 |         )
0020 |         self.assertEqual(event.event_id, "EVT-TEST01")
0021 |         self.assertEqual(event.disaster_type, "flood")
0022 |         self.assertEqual(event.metadata.rainfall_mm, 120.5)
0023 | 
0024 |     def test_invalid_coordinates(self):
0025 |         with self.assertRaises(ValidationError):
0026 |             CrisisEvent(
0027 |                 event_id="EVT-BADCOORD",
0028 |                 timestamp=datetime.utcnow(),
0029 |                 text="Invalid latitude",
0030 |                 location="Unknown",
0031 |                 disaster_type="flood",
0032 |                 latitude=150.0,  # Invalid (>90)
0033 |                 longitude=72.0,
0034 |                 severity="minor",
0035 |                 source="sensor",
0036 |                 metadata=Metadata()
0037 |             )
0038 | 
0039 |     def test_dlq_message_serialization(self):
0040 |         dlq = DLQMessage(
0041 |             raw_payload="invalid-bytes",
0042 |             error_type="JSONDecodeError",
0043 |             error_message="Expecting value",
0044 |             source_topic="crisis-events",
0045 |             source_partition=1,
0046 |             source_offset=42
0047 |         )
0048 |         self.assertEqual(dlq.source_topic, "crisis-events")
0049 |         self.assertEqual(dlq.source_partition, 1)
0050 |         self.assertEqual(dlq.source_offset, 42)
0051 | 
0052 | if __name__ == "__main__":
0053 |     unittest.main()
```

### `tests/test_source_credentials.py`

[Open source](../../tests/test_source_credentials.py) · SHA-256 `3a815f273436c736dd732a77708f7ed8e55191096d08444206ed2ab62418a2b3`

Dependencies: `from unittest.mock import Mock`; `import pytest`; `from src.common.config import DEFAULT_INGESTION_API_URL, config`; `from src.ingestion.clients import fetch_weather`; `from src.ingestion.poll_sources import submit`

- `test_default_ingestion_endpoint_targets_event_route`: lines 10–11. Read its control flow with the module guide above.

- `test_submission_posts_normalized_event_to_configured_endpoint`: lines 14–26. Read its control flow with the module guide above.

- `test_weather_requires_key_before_making_request`: lines 29–33. Read its control flow with the module guide above.

- `test_weather_requests_metric_units_and_converts_wind`: lines 36–47. Read its control flow with the module guide above.

- `test_weather_does_not_invent_observation_time`: lines 50–54. Read its control flow with the module guide above.

- `test_missing_wind_is_not_measured_calm`: lines 57–63. Read its control flow with the module guide above.

- `test_historical_day_feed_is_marked_replay`: lines 66–81. Read its control flow with the module guide above.


```text
0001 | from unittest.mock import Mock
0002 | 
0003 | import pytest
0004 | 
0005 | from src.common.config import DEFAULT_INGESTION_API_URL, config
0006 | from src.ingestion.clients import fetch_weather
0007 | from src.ingestion.poll_sources import submit
0008 | 
0009 | 
0010 | def test_default_ingestion_endpoint_targets_event_route():
0011 |     assert DEFAULT_INGESTION_API_URL == "http://127.0.0.1:8000/events"
0012 | 
0013 | 
0014 | def test_submission_posts_normalized_event_to_configured_endpoint():
0015 |     session = Mock()
0016 |     session.post.return_value.raise_for_status.return_value = None
0017 |     event = {"event_id": "OWM-test"}
0018 | 
0019 |     accepted = submit(session, [event], DEFAULT_INGESTION_API_URL)
0020 | 
0021 |     assert accepted == 1
0022 |     session.post.assert_called_once_with(
0023 |         DEFAULT_INGESTION_API_URL,
0024 |         json=event,
0025 |         timeout=config.INGESTION_TIMEOUT_SECONDS,
0026 |     )
0027 | 
0028 | 
0029 | def test_weather_requires_key_before_making_request():
0030 |     session = Mock()
0031 |     with pytest.raises(ValueError, match="OPENWEATHER_API_KEY"):
0032 |         fetch_weather(session, "", 12, 77, 3)
0033 |     session.get.assert_not_called()
0034 | 
0035 | 
0036 | def test_weather_requests_metric_units_and_converts_wind():
0037 |     session = Mock()
0038 |     session.get.return_value.json.return_value = {
0039 |         "id": 1,
0040 |         "dt": 1700000000,
0041 |         "main": {"temp": 20},
0042 |         "wind": {"speed": 10},
0043 |     }
0044 |     result = fetch_weather(session, "offline-test-key", 12, 77, 3)
0045 |     assert session.get.call_args.kwargs["params"]["units"] == "metric"
0046 |     assert result["metadata"]["wind_speed_kmh"] == 36
0047 |     assert result["severity"] == "unknown"
0048 | 
0049 | 
0050 | def test_weather_does_not_invent_observation_time():
0051 |     from src.ingestion.clients import weather_to_event
0052 | 
0053 |     with pytest.raises(ValueError, match="observation timestamp"):
0054 |         weather_to_event({}, 12, 77)
0055 | 
0056 | 
0057 | def test_missing_wind_is_not_measured_calm():
0058 |     from src.ingestion.clients import weather_to_event
0059 | 
0060 |     assert (
0061 |         weather_to_event({"dt": 1700000000}, 12, 77)["metadata"]["wind_speed_kmh"]
0062 |         is None
0063 |     )
0064 | 
0065 | 
0066 | def test_historical_day_feed_is_marked_replay():
0067 |     from src.ingestion.clients import fetch_usgs
0068 | 
0069 |     session = Mock()
0070 |     session.get.return_value.json.return_value = {
0071 |         "features": [
0072 |             {
0073 |                 "id": "historical",
0074 |                 "properties": {"time": 1700000000000, "mag": 3},
0075 |                 "geometry": {"coordinates": [77, 12]},
0076 |             }
0077 |         ]
0078 |     }
0079 |     events = fetch_usgs(session, timeout=3, day=True)
0080 |     assert events[0]["metadata"]["is_replay"] is True
0081 |     assert "all_day" in session.get.call_args.args[0]
```

### `scripts/demo_smoke.py`

[Open source](../../scripts/demo_smoke.py) · SHA-256 `97fffe2b86af1791eaae10358dd3dfeab674f9ec668c8a34b8d869d9deffcc00`

Dependencies: `import argparse`; `from datetime import datetime, timezone`; `import json`; `import time`; `import uuid`; `import io`; `import pyarrow.parquet as pq`; `import requests`; `from confluent_kafka import Consumer, TopicPartition`; `from src.common.config import config`; `from src.common.db import get_pool, close_pool`; `from src.storage.lake_writer import ParquetLakeWriter`

- `main`: lines 18–144. Read its control flow with the module guide above.


```text
0001 | """Run inside the API container to verify live operational and lake sinks."""
0002 | 
0003 | import argparse
0004 | from datetime import datetime, timezone
0005 | import json
0006 | import time
0007 | import uuid
0008 | import io
0009 | import pyarrow.parquet as pq
0010 | 
0011 | import requests
0012 | from confluent_kafka import Consumer, TopicPartition
0013 | from src.common.config import config
0014 | from src.common.db import get_pool, close_pool
0015 | from src.storage.lake_writer import ParquetLakeWriter
0016 | 
0017 | 
0018 | def main() -> None:
0019 |     parser = argparse.ArgumentParser()
0020 |     parser.add_argument("--spark", action="store_true")
0021 |     args = parser.parse_args()
0022 |     identifier = "DEMO-" + uuid.uuid4().hex[:16]
0023 |     event = {
0024 |         "event_id": identifier,
0025 |         "timestamp": datetime.now(timezone.utc).isoformat(),
0026 |         "text": "Synthetic demo: river flood near a residential road",
0027 |         "location": "Demo Mumbai",
0028 |         "disaster_type": "flood",
0029 |         "severity": "severe",
0030 |         "latitude": 19.076,
0031 |         "longitude": 72.8777,
0032 |         "source": "synthetic_demo",
0033 |         "metadata": {"rainfall_mm": 100, "water_level_m": 3, "is_synthetic": True},
0034 |     }
0035 |     response = requests.post("http://localhost:8000/events", json=event, timeout=10)
0036 |     response.raise_for_status()
0037 |     acknowledgement = response.json()
0038 |     client = ParquetLakeWriter()._client()
0039 |     _, _, prefix = ParquetLakeWriter._partition(event)
0040 |     deadline = time.monotonic() + 60
0041 |     try:
0042 |         while time.monotonic() < deadline:
0043 |             with get_pool().connection() as conn:
0044 |                 row = conn.execute(
0045 |                     "SELECT * FROM crisis_events WHERE event_id=%s", (identifier,)
0046 |                 ).fetchone()
0047 |                 predictions = conn.execute(
0048 |                     "SELECT COUNT(*) AS count FROM predictions WHERE event_id=%s",
0049 |                     (identifier,),
0050 |                 ).fetchone()["count"]
0051 |             if row and predictions == 2:
0052 |                 try:
0053 |                     client.head_object(
0054 |                         Bucket="cataclysm-raw",
0055 |                         Key=f"{prefix}/event_id={identifier}.parquet",
0056 |                     )
0057 |                     break
0058 |                 except client.exceptions.ClientError:
0059 |                     pass
0060 |             time.sleep(1)
0061 |         else:
0062 |             raise RuntimeError(
0063 |                 "Event did not reach DB, prediction table and raw lake within 60 seconds"
0064 |             )
0065 |         duplicate = requests.post(
0066 |             "http://localhost:8000/events", json=event, timeout=10
0067 |         )
0068 |         duplicate.raise_for_status()
0069 |         duplicate_ack = duplicate.json()
0070 |         observer = Consumer(
0071 |             {
0072 |                 "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
0073 |                 "group.id": "crisis-dashboard-group",
0074 |                 "enable.auto.commit": False,
0075 |             }
0076 |         )
0077 |         try:
0078 |             deadline = time.monotonic() + 30
0079 |             while time.monotonic() < deadline:
0080 |                 committed = observer.committed(
0081 |                     [TopicPartition(config.KAFKA_TOPIC, duplicate_ack["partition"])],
0082 |                     timeout=3,
0083 |                 )[0].offset
0084 |                 if committed > duplicate_ack["offset"]:
0085 |                     break
0086 |                 time.sleep(1)
0087 |             else:
0088 |                 raise RuntimeError(
0089 |                     "The duplicate record was not committed by the operational consumer"
0090 |                 )
0091 |         finally:
0092 |             observer.close()
0093 |         with get_pool().connection() as conn:
0094 |             count = conn.execute(
0095 |                 "SELECT COUNT(*) AS count FROM crisis_events WHERE event_id=%s",
0096 |                 (identifier,),
0097 |             ).fetchone()["count"]
0098 |             assert count == 1
0099 |         result = {
0100 |             "event_id": identifier,
0101 |             "kafka": acknowledgement,
0102 |             "database": True,
0103 |             "predictions": predictions,
0104 |             "raw_lake": True,
0105 |             "duplicate_consumed_without_duplicate_row": True,
0106 |             "operational_committed_offset": committed,
0107 |         }
0108 |         if args.spark:
0109 |             deadline = time.monotonic() + 90
0110 |             while time.monotonic() < deadline:
0111 |                 objects = client.list_objects_v2(
0112 |                     Bucket="cataclysm-processed", Prefix="events-v2/"
0113 |                 )
0114 |                 files = [
0115 |                     item
0116 |                     for item in objects.get("Contents", [])
0117 |                     if item["Key"].endswith(".parquet")
0118 |                 ]
0119 |                 found = False
0120 |                 for item in files:
0121 |                     body = client.get_object(
0122 |                         Bucket="cataclysm-processed", Key=item["Key"]
0123 |                     )["Body"]
0124 |                     try:
0125 |                         table = pq.ParquetFile(io.BytesIO(body.read())).read(
0126 |                             columns=["event_id"]
0127 |                         )
0128 |                         found = identifier in table.column("event_id").to_pylist()
0129 |                     finally:
0130 |                         body.close()
0131 |                     if found:
0132 |                         break
0133 |                 if found:
0134 |                     result["processed_parquet_files"] = len(files)
0135 |                     result["specific_event_in_processed_parquet"] = True
0136 |                     break
0137 |                 time.sleep(2)
0138 |             else:
0139 |                 raise RuntimeError(
0140 |                     "No Spark processed Parquet appeared within 90 seconds"
0141 |                 )
0142 |         print(json.dumps(result, indent=2))
0143 |     finally:
0144 |         close_pool()
0145 | 
0146 | 
0147 | if __name__ == "__main__":
0148 |     main()
```

### `scripts/check_dlq.py`

[Open source](../../scripts/check_dlq.py) · SHA-256 `5cb17bdefd50b57f3c085fbbba7a00df6cb145e828c973a1cfb593d3b2fbba2f`

Dependencies: `import json`; `import time`; `import uuid`; `from confluent_kafka import Consumer, Producer`; `from src.common.config import config`

- `main`: lines 10–50. Read its control flow with the module guide above.


```text
0001 | """Publish one deliberately malformed test record and verify DLQ delivery."""
0002 | 
0003 | import json
0004 | import time
0005 | import uuid
0006 | from confluent_kafka import Consumer, Producer
0007 | from src.common.config import config
0008 | 
0009 | 
0010 | def main() -> None:
0011 |     marker = "DLQ-TEST-" + uuid.uuid4().hex
0012 |     producer = Producer(
0013 |         {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, "acks": "all"}
0014 |     )
0015 |     delivered = []
0016 |     producer.produce(
0017 |         config.KAFKA_TOPIC,
0018 |         value=marker.encode(),
0019 |         on_delivery=lambda error, message: delivered.append(error),
0020 |     )
0021 |     if producer.flush(10) or not delivered or delivered[0] is not None:
0022 |         raise RuntimeError("Test source publication failed")
0023 |     consumer = Consumer(
0024 |         {
0025 |             "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
0026 |             "group.id": marker,
0027 |             "auto.offset.reset": "earliest",
0028 |             "enable.auto.commit": False,
0029 |         }
0030 |     )
0031 |     consumer.subscribe([config.KAFKA_DLQ_TOPIC])
0032 |     deadline = time.monotonic() + 30
0033 |     try:
0034 |         while time.monotonic() < deadline:
0035 |             message = consumer.poll(1)
0036 |             if message is not None and not message.error():
0037 |                 value = json.loads(message.value())
0038 |                 if value["raw_payload"] == marker:
0039 |                     print(
0040 |                         json.dumps(
0041 |                             {
0042 |                                 "malformed_record_routed": True,
0043 |                                 "source_offset": value["source_offset"],
0044 |                             }
0045 |                         )
0046 |                     )
0047 |                     return
0048 |         raise RuntimeError("Test record was not found in DLQ")
0049 |     finally:
0050 |         consumer.close()
0051 | 
0052 | 
0053 | if __name__ == "__main__":
0054 |     main()
```

### `scripts/audit_lake.py`

[Open source](../../scripts/audit_lake.py) · SHA-256 `bf7d51bfdbc1f0fdd4900c8a8ba991f16e372bd0126f60ec6f2b496883dccac2`

Dependencies: `import io`; `import json`; `import pyarrow.parquet as pq`; `from src.storage.lake_writer import ParquetLakeWriter`

- `main`: lines 9–33. Read its control flow with the module guide above.


```text
0001 | """Read lake Parquet contents and count records, ignoring metadata objects."""
0002 | 
0003 | import io
0004 | import json
0005 | import pyarrow.parquet as pq
0006 | from src.storage.lake_writer import ParquetLakeWriter
0007 | 
0008 | 
0009 | def main() -> None:
0010 |     client = ParquetLakeWriter()._client()
0011 |     report = {}
0012 |     for layer in ("raw", "processed", "curated"):
0013 |         bucket = "cataclysm-" + layer
0014 |         files = rows = 0
0015 |         prefix = {
0016 |             "raw": "",
0017 |             "processed": "events-v2/",
0018 |             "curated": "windowed_incident_counts-v2/",
0019 |         }[layer]
0020 |         for page in client.get_paginator("list_objects_v2").paginate(
0021 |             Bucket=bucket, Prefix=prefix
0022 |         ):
0023 |             for item in page.get("Contents", []):
0024 |                 if not item["Key"].endswith(".parquet"):
0025 |                     continue
0026 |                 body = client.get_object(Bucket=bucket, Key=item["Key"])["Body"]
0027 |                 try:
0028 |                     rows += pq.ParquetFile(io.BytesIO(body.read())).metadata.num_rows
0029 |                     files += 1
0030 |                 finally:
0031 |                     body.close()
0032 |         report[layer] = {"parquet_files": files, "rows": rows}
0033 |     print(json.dumps(report, indent=2))
0034 | 
0035 | 
0036 | if __name__ == "__main__":
0037 |     main()
```

### `scripts/recovery_check.py`

[Open source](../../scripts/recovery_check.py) · SHA-256 `12ad8a8ab8b20634cb32cebb35575da5f1d782f3b0e6ad0234c83922d5679ef2`

Dependencies: `import json`; `import subprocess`; `import time`; `import requests`

- `main`: lines 9–41. Read its control flow with the module guide above.


```text
0001 | """Restart local durable services and verify existing operational IDs survive."""
0002 | 
0003 | import json
0004 | import subprocess
0005 | import time
0006 | import requests
0007 | 
0008 | 
0009 | def main() -> None:
0010 |     base = "http://127.0.0.1:8000"
0011 |     response = requests.get(base + "/api/events?limit=500", timeout=10)
0012 |     response.raise_for_status()
0013 |     identifiers = {event["event_id"] for event in response.json()}
0014 |     if not identifiers:
0015 |         raise RuntimeError("Run demo_smoke before the recovery check")
0016 |     subprocess.run(
0017 |         ["docker", "compose", "restart", "db", "broker", "minio", "spark-streaming"],
0018 |         check=True,
0019 |     )
0020 |     deadline = time.monotonic() + 90
0021 |     while time.monotonic() < deadline:
0022 |         try:
0023 |             response = requests.get(base + "/api/events?limit=500", timeout=5)
0024 |             response.raise_for_status()
0025 |             actual = {event["event_id"] for event in response.json()}
0026 |             if identifiers <= actual:
0027 |                 print(
0028 |                     json.dumps(
0029 |                         {
0030 |                             "preserved_event_ids": len(identifiers),
0031 |                             "database_restart_recovery": True,
0032 |                             "note": "Run demo_smoke --spark next to verify writes after restart",
0033 |                         },
0034 |                         indent=2,
0035 |                     )
0036 |                 )
0037 |                 return
0038 |         except requests.RequestException:
0039 |             pass
0040 |         time.sleep(2)
0041 |     raise RuntimeError("Stored IDs did not become available after restart")
0042 | 
0043 | 
0044 | if __name__ == "__main__":
0045 |     main()
```

### `scripts/check_stability.py`

[Open source](../../scripts/check_stability.py) · SHA-256 `b7d3f09df1e3247ad8206824f0c0ad6a8ea63f6a71d615e990a75c1ddba81baa`

Dependencies: `import argparse`; `import json`; `from pathlib import Path`; `import subprocess`; `import time`

- `inspect`: lines 10–18. Read its control flow with the module guide above.

- `main`: lines 21–62. Read its control flow with the module guide above.


```text
0001 | """Bounded local streaming endurance check; fails on exits or restarts."""
0002 | 
0003 | import argparse
0004 | import json
0005 | from pathlib import Path
0006 | import subprocess
0007 | import time
0008 | 
0009 | 
0010 | def inspect():
0011 |     result = subprocess.run(
0012 |         ["docker", "inspect", "spark-streaming", "--format", "{{json .}}"],
0013 |         capture_output=True,
0014 |         text=True,
0015 |         check=True,
0016 |     )
0017 |     state = json.loads(result.stdout)
0018 |     return state["State"]["Running"], state["RestartCount"]
0019 | 
0020 | 
0021 | def main() -> None:
0022 |     parser = argparse.ArgumentParser()
0023 |     parser.add_argument("--seconds", type=int, default=600)
0024 |     args = parser.parse_args()
0025 |     running, baseline = inspect()
0026 |     if not running:
0027 |         raise RuntimeError("Streaming is not running")
0028 |     started = time.monotonic()
0029 |     samples = []
0030 |     while time.monotonic() - started < args.seconds:
0031 |         running, restarts = inspect()
0032 |         if not running or restarts != baseline:
0033 |             raise RuntimeError("Streaming exited or restarted during stability check")
0034 |         usage = subprocess.run(
0035 |             [
0036 |                 "docker",
0037 |                 "stats",
0038 |                 "spark-streaming",
0039 |                 "--no-stream",
0040 |                 "--format",
0041 |                 "{{json .}}",
0042 |             ],
0043 |             capture_output=True,
0044 |             text=True,
0045 |             check=True,
0046 |         )
0047 |         sample = {
0048 |             "elapsed_seconds": round(time.monotonic() - started, 1),
0049 |             **json.loads(usage.stdout),
0050 |         }
0051 |         samples.append(sample)
0052 |         print(json.dumps(sample), flush=True)
0053 |         time.sleep(30)
0054 |     output = {
0055 |         "duration_seconds": time.monotonic() - started,
0056 |         "restarts": 0,
0057 |         "samples": samples,
0058 |     }
0059 |     Path("work/stability.json").write_text(
0060 |         json.dumps(output, indent=2), encoding="utf-8"
0061 |     )
0062 |     print("Stability check passed", flush=True)
0063 | 
0064 | 
0065 | if __name__ == "__main__":
0066 |     main()
```

### `scripts/correct_prediction_provenance.py`

[Open source](../../scripts/correct_prediction_provenance.py) · SHA-256 `a92f5b3bc935574ae5d16a5e94f86516d18db72ff2cd1a2ceac22182040f4bbb`

Dependencies: `import json`; `from src.common.db import get_pool`

- `main`: lines 8–28. Unknown severity is unavailable; zero-score keyword output is heuristic.


```text
0001 | """Correct legacy zero-score provenance without changing observation/model labels."""
0002 | 
0003 | import json
0004 | 
0005 | from src.common.db import get_pool
0006 | 
0007 | 
0008 | def main() -> None:
0009 |     """Unknown severity is unavailable; zero-score keyword output is heuristic."""
0010 |     with get_pool().connection() as conn:
0011 |         unavailable = conn.execute(
0012 |             "UPDATE predictions SET provenance='unavailable', confidence=NULL "
0013 |             "WHERE model_name='severity' AND predicted_label='unknown' "
0014 |             "AND provenance='trained' AND COALESCE(confidence,0)=0"
0015 |         ).rowcount
0016 |         heuristic = conn.execute(
0017 |             "UPDATE predictions SET provenance='heuristic', confidence=NULL "
0018 |             "WHERE provenance='trained' AND confidence=0 "
0019 |             "AND predicted_label <> 'unknown'"
0020 |         ).rowcount
0021 |     print(
0022 |         json.dumps(
0023 |             {
0024 |                 "legacy_unavailable_corrected": unavailable,
0025 |                 "legacy_heuristic_corrected": heuristic,
0026 |             }
0027 |         )
0028 |     )
0029 | 
0030 | 
0031 | if __name__ == "__main__":
0032 |     main()
```

### `scripts/verify_application.py`

[Open source](../../scripts/verify_application.py) · SHA-256 `ef33680e86eac329749fa2c88ca7decfa0e5de1762905417dd629bca453ff364`

Dependencies: `from __future__ import annotations`; `import json`; `import time`; `import uuid`; `from pathlib import Path`; `from datetime import datetime, timezone`; `import requests`; `from confluent_kafka import Consumer, TopicPartition`; `from src.common.config import config`; `from src.common.db import get_pool`

- `main`: lines 15–165. Read its control flow with the module guide above.

- `get`: lines 18–21. Read its control flow with the module guide above.


```text
0001 | """Verify the running API, persistence identity, SSE and monitoring contracts."""
0002 | 
0003 | from __future__ import annotations
0004 | import json
0005 | import time
0006 | import uuid
0007 | from pathlib import Path
0008 | from datetime import datetime, timezone
0009 | import requests
0010 | from confluent_kafka import Consumer, TopicPartition
0011 | from src.common.config import config
0012 | from src.common.db import get_pool
0013 | 
0014 | 
0015 | def main():
0016 |     base = "http://localhost:8000"
0017 | 
0018 |     def get(path, **kwargs):
0019 |         r = requests.get(base + path, timeout=15, **kwargs)
0020 |         r.raise_for_status()
0021 |         return r.json()
0022 | 
0023 |     deadline = time.monotonic() + 45
0024 |     while True:
0025 |         try:
0026 |             if get("/live")["status"] == "alive":
0027 |                 break
0028 |         except requests.RequestException:
0029 |             if time.monotonic() >= deadline:
0030 |                 raise
0031 |         time.sleep(1)
0032 |     assert get("/ready")["status"] == "healthy"
0033 |     assert (
0034 |         requests.post(
0035 |             base + "/events", json={"event_id": "invalid"}, timeout=5
0036 |         ).status_code
0037 |         == 422
0038 |     )
0039 |     assert (
0040 |         requests.get(base + "/api/events?bbox=10,0,-10,1", timeout=5).status_code == 422
0041 |     )
0042 |     assert requests.get(base + "/api/events?limit=501", timeout=5).status_code == 422
0043 |     assert get("/api/events?q=THIS-WILL-NOT-MATCH-" + uuid.uuid4().hex) == []
0044 |     summary = get("/api/overview")
0045 |     assert summary["total"] == sum(x["count"] for x in summary["severity"])
0046 |     assert summary["total"] == sum(x["count"] for x in summary["sources"])
0047 |     assert summary["trends"] and all(row["count"] > 0 for row in summary["trends"])
0048 |     first = get("/api/events?limit=3")
0049 |     second = get("/api/events?limit=3&offset=3")
0050 |     assert not ({x["event_id"] for x in first} & {x["event_id"] for x in second})
0051 |     usgs = get("/api/events?source=usgs_feed&limit=5")
0052 |     assert usgs and all(
0053 |         x["source"] == "usgs_feed" and x["data_mode"] == "live" for x in usgs
0054 |     )
0055 |     identifier = "IDENTITY-" + uuid.uuid4().hex[:16]
0056 |     event = dict(
0057 |         event_id=identifier,
0058 |         timestamp=datetime.now(timezone.utc).isoformat(),
0059 |         location="Identity test",
0060 |         latitude=0,
0061 |         longitude=0,
0062 |         text="Original synthetic flood",
0063 |         disaster_type="flood",
0064 |         severity="minor",
0065 |         source="synthetic_verification",
0066 |         metadata={
0067 |             "is_synthetic": True,
0068 |             "location_accuracy": "Controlled zero-coordinate fixture",
0069 |         },
0070 |     )
0071 |     r = requests.post(base + "/events", json=event, timeout=10)
0072 |     r.raise_for_status()
0073 |     deadline = time.monotonic() + 30
0074 |     while time.monotonic() < deadline:
0075 |         r = requests.get(base + f"/api/events/{identifier}/details", timeout=5)
0076 |         if r.ok:
0077 |             break
0078 |         time.sleep(0.5)
0079 |     r.raise_for_status()
0080 |     conflict = {**event, "text": "Conflicting fire narrative", "severity": "critical"}
0081 |     r = requests.post(base + "/events", json=conflict, timeout=10)
0082 |     r.raise_for_status()
0083 |     ack = r.json()
0084 |     observer = Consumer(
0085 |         {
0086 |             "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
0087 |             "group.id": "crisis-dashboard-group",
0088 |             "enable.auto.commit": False,
0089 |         }
0090 |     )
0091 |     deadline = time.monotonic() + 30
0092 |     while time.monotonic() < deadline:
0093 |         committed = observer.committed(
0094 |             [TopicPartition(config.KAFKA_TOPIC, ack["partition"])], timeout=3
0095 |         )[0].offset
0096 |         if committed > ack["offset"]:
0097 |             break
0098 |         time.sleep(0.5)
0099 |     observer.close()
0100 |     assert committed > ack["offset"]
0101 |     detail = get(f"/api/events/{identifier}/details")
0102 |     assert detail["text"] == event["text"] and detail["severity"] == "minor"
0103 |     assert detail["data_mode"] == "synthetic" and len(detail["predictions"]) == 2
0104 |     import io
0105 |     import pyarrow.parquet as pq
0106 |     from src.storage.lake_writer import ParquetLakeWriter
0107 | 
0108 |     writer = ParquetLakeWriter()
0109 |     _, _, prefix = writer._partition(event)
0110 |     body = writer._client().get_object(
0111 |         Bucket=config.MINIO_BUCKET_RAW, Key=f"{prefix}/event_id={identifier}.parquet"
0112 |     )["Body"]
0113 |     try:
0114 |         archived = pq.ParquetFile(io.BytesIO(body.read())).read().to_pylist()[0]
0115 |     finally:
0116 |         body.close()
0117 |     assert json.loads(archived["raw_payload"])["text"] == event["text"]
0118 |     assert archived["severity"] == "minor"
0119 |     # Read a real SSE event and close the HTTP connection deterministically.
0120 |     with requests.get(
0121 |         base + "/api/events/stream", stream=True, timeout=(5, 15)
0122 |     ) as response:
0123 |         response.raise_for_status()
0124 |         saw_event = False
0125 |         for line in response.iter_lines(chunk_size=1, decode_unicode=True):
0126 |             if line.startswith("event: crisis_event"):
0127 |                 saw_event = True
0128 |             if saw_event and line.startswith("data: "):
0129 |                 streamed = json.loads(line[6:])
0130 |                 assert "event_id" in streamed
0131 |                 break
0132 |         assert saw_event
0133 |     targets = requests.get("http://prometheus:9090/api/v1/targets", timeout=10).json()[
0134 |         "data"
0135 |     ]["activeTargets"]
0136 |     assert targets and all(x["health"] == "up" for x in targets)
0137 |     grafana = requests.get("http://grafana:3000/api/health", timeout=10).json()
0138 |     assert grafana["database"] == "ok"
0139 |     pipeline = get("/api/pipeline")
0140 |     with get_pool().connection() as conn:
0141 |         plan = conn.execute(
0142 |             "EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM crisis_events ORDER BY timestamp DESC LIMIT 100"
0143 |         ).fetchone()
0144 |     report = dict(
0145 |         observed_at=datetime.now(timezone.utc).isoformat(),
0146 |         invalid_payload_rejected=True,
0147 |         invalid_bbox_rejected=True,
0148 |         pagination_disjoint=True,
0149 |         overview_consistent=True,
0150 |         live_usgs_rows=len(usgs),
0151 |         identity_event=identifier,
0152 |         first_observation_wins=True,
0153 |         raw_first_observation_wins=True,
0154 |         sse_database_event_received=True,
0155 |         prometheus_targets=[
0156 |             {"job": x["labels"].get("job"), "health": x["health"]} for x in targets
0157 |         ],
0158 |         grafana_database=grafana["database"],
0159 |         pipeline=pipeline,
0160 |         query_plan=plan,
0161 |     )
0162 |     Path("docs/evidence/application.json").write_text(
0163 |         json.dumps(report, indent=2, default=str)
0164 |     )
0165 |     print(json.dumps({k: v for k, v in report.items() if k != "query_plan"}, indent=2))
0166 | 
0167 | 
0168 | if __name__ == "__main__":
0169 |     main()
```

### `scripts/verify_crash_replay.py`

[Open source](../../scripts/verify_crash_replay.py) · SHA-256 `efd609b9f813d031b3c5d1f11c9708f1504dd01d5709e6588820fda8869220fa`

Dependencies: `from __future__ import annotations`; `import argparse`; `import json`; `import os`; `import subprocess`; `import sys`; `import uuid`; `from datetime import datetime, timezone`; `from pathlib import Path`

- `child`: lines 18–43. Read its control flow with the module guide above.

- `crash_after_database`: lines 23–24. Read its control flow with the module guide above.

- `StopAfterCommit`: lines 30–40. Read its control flow with the module guide above.

- `__init__`: lines 31–32. Read its control flow with the module guide above.

- `__getattr__`: lines 34–35. Read its control flow with the module guide above.

- `commit`: lines 37–40. Read its control flow with the module guide above.

- `main`: lines 46–141. Read its control flow with the module guide above.


```text
0001 | """Exercise real consumer crash-after-DB behavior on an isolated retained topic.
0002 | 
0003 | Run inside fastapi. Uses normal PostgreSQL/MinIO sinks and separate Kafka group;
0004 | no operational offsets are reset and no data is deleted.
0005 | """
0006 | 
0007 | from __future__ import annotations
0008 | import argparse
0009 | import json
0010 | import os
0011 | import subprocess
0012 | import sys
0013 | import uuid
0014 | from datetime import datetime, timezone
0015 | from pathlib import Path
0016 | 
0017 | 
0018 | def child(mode: str) -> None:
0019 |     from src.kafka import consumer as module
0020 | 
0021 |     if mode == "crash":
0022 | 
0023 |         def crash_after_database(self, events):
0024 |             os._exit(91)
0025 | 
0026 |         module.ParquetLakeWriter.write_batch = crash_after_database
0027 |     else:
0028 |         original = module.Consumer
0029 | 
0030 |         class StopAfterCommit:
0031 |             def __init__(self, options):
0032 |                 self.consumer = original(options)
0033 | 
0034 |             def __getattr__(self, name):
0035 |                 return getattr(self.consumer, name)
0036 | 
0037 |             def commit(self, *args, **kwargs):
0038 |                 result = self.consumer.commit(*args, **kwargs)
0039 |                 module.running = False
0040 |                 return result
0041 | 
0042 |         module.Consumer = StopAfterCommit
0043 |     module.run_consumer()
0044 | 
0045 | 
0046 | def main(output: str) -> None:
0047 |     import requests
0048 |     from confluent_kafka import Consumer, Producer, TopicPartition
0049 |     from confluent_kafka.admin import AdminClient, NewTopic
0050 |     from src.common.config import config
0051 |     from src.common.db import get_pool
0052 | 
0053 |     identifier = "CRASH-" + uuid.uuid4().hex[:16]
0054 |     topic = "verification-" + uuid.uuid4().hex[:12]
0055 |     group = topic + "-group"
0056 |     admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
0057 |     for future in admin.create_topics(
0058 |         [NewTopic(topic, num_partitions=1, replication_factor=1)]
0059 |     ).values():
0060 |         future.result(15)
0061 |     event = dict(
0062 |         event_id=identifier,
0063 |         timestamp=datetime.now(timezone.utc).isoformat(),
0064 |         text="Synthetic crash test river flood",
0065 |         location="Crash verification",
0066 |         latitude=0,
0067 |         longitude=0,
0068 |         disaster_type="flood",
0069 |         severity="minor",
0070 |         source="synthetic_crash",
0071 |         metadata={"is_synthetic": True},
0072 |     )
0073 |     producer = Producer(
0074 |         {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, "acks": "all"}
0075 |     )
0076 |     ack = []
0077 |     producer.produce(
0078 |         topic,
0079 |         value=json.dumps(event).encode(),
0080 |         on_delivery=lambda error, msg: ack.append((error, msg.offset())),
0081 |     )
0082 |     assert producer.flush(10) == 0 and ack and ack[0][0] is None
0083 |     env = {**os.environ, "KAFKA_TOPIC": topic, "KAFKA_GROUP_ID": group}
0084 |     first = subprocess.run(
0085 |         [sys.executable, "-m", "scripts.verify_crash_replay", "--child", "crash"],
0086 |         env=env,
0087 |         timeout=60,
0088 |         capture_output=True,
0089 |     )
0090 |     assert first.returncode == 91, f"Expected crash exit 91, got {first.returncode}"
0091 |     observer = Consumer(
0092 |         {
0093 |             "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
0094 |             "group.id": group,
0095 |             "enable.auto.commit": False,
0096 |         }
0097 |     )
0098 |     before = observer.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
0099 |     with get_pool().connection() as conn:
0100 |         count_before = conn.execute(
0101 |             "SELECT COUNT(*) AS n FROM crisis_events WHERE event_id=%s", (identifier,)
0102 |         ).fetchone()["n"]
0103 |     assert count_before == 1 and before <= ack[0][1]
0104 |     second = subprocess.run(
0105 |         [sys.executable, "-m", "scripts.verify_crash_replay", "--child", "recover"],
0106 |         env=env,
0107 |         timeout=60,
0108 |         capture_output=True,
0109 |     )
0110 |     assert second.returncode == 0, f"Recovery failed: exit {second.returncode}"
0111 |     after = observer.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
0112 |     observer.close()
0113 |     with get_pool().connection() as conn:
0114 |         count_after = conn.execute(
0115 |             "SELECT COUNT(*) AS n FROM crisis_events WHERE event_id=%s", (identifier,)
0116 |         ).fetchone()["n"]
0117 |         predictions = conn.execute(
0118 |             "SELECT COUNT(*) AS n FROM predictions WHERE event_id=%s", (identifier,)
0119 |         ).fetchone()["n"]
0120 |     assert count_after == 1 and predictions == 2 and after == ack[0][1] + 1
0121 |     detail = requests.get(
0122 |         f"http://localhost:8000/api/events/{identifier}/details", timeout=10
0123 |     )
0124 |     detail.raise_for_status()
0125 |     assert detail.json()["data_mode"] == "synthetic"
0126 |     result = {
0127 |         "observed_at": datetime.now(timezone.utc).isoformat(),
0128 |         "event_id": identifier,
0129 |         "topic": topic,
0130 |         "group": group,
0131 |         "crash_exit": first.returncode,
0132 |         "rows_before_replay": count_before,
0133 |         "rows_after_replay": count_after,
0134 |         "offset_before_replay": before,
0135 |         "offset_after_replay": after,
0136 |         "prediction_rows": predictions,
0137 |         "scope": "Actual consumer process exits after database transaction and before raw archive/offset commit; isolated retained Kafka topic.",
0138 |     }
0139 |     Path(output).parent.mkdir(parents=True, exist_ok=True)
0140 |     Path(output).write_text(json.dumps(result, indent=2), encoding="utf-8")
0141 |     print(json.dumps(result, indent=2))
0142 | 
0143 | 
0144 | if __name__ == "__main__":
0145 |     parser = argparse.ArgumentParser(description=__doc__)
0146 |     parser.add_argument("--child", choices=["crash", "recover"])
0147 |     parser.add_argument("--output", default="docs/evidence/crash-replay.json")
0148 |     args = parser.parse_args()
0149 |     child(args.child) if args.child else main(args.output)
```

### `scripts/verify_spark_semantics.py`

[Open source](../../scripts/verify_spark_semantics.py) · SHA-256 `8ea99c4ee2e7b7e58214ff6d1e4a39170f2188df6cfa5b99c7f03fb1b996684a`

Dependencies: `import json`; `import sys`; `import uuid`; `from pathlib import Path`; `from datetime import datetime, timezone, timedelta`; `from pyspark.sql import SparkSession`; `from pyspark.sql.functions import window`; `from pyspark.sql.types import StructType, StructField, StringType`; `from src.spark.streaming_job import clean_events`

- `main`: lines 20–134. Read its control flow with the module guide above.

- `start`: lines 38–44. Read its control flow with the module guide above.

- `send`: lines 49–65. Read its control flow with the module guide above.


```text
0001 | """Isolated Spark 3.5 streaming semantics test; never uses production checkpoints.
0002 | 
0003 | Run with spark-submit --master local[2]. Fixtures are labelled synthetic and kept
0004 | under a unique work directory. File source permits precise microbatch boundaries.
0005 | """
0006 | 
0007 | import json
0008 | import sys
0009 | import uuid
0010 | from pathlib import Path
0011 | from datetime import datetime, timezone, timedelta
0012 | 
0013 | sys.path.insert(0, "/opt/spark/app")
0014 | from pyspark.sql import SparkSession
0015 | from pyspark.sql.functions import window
0016 | from pyspark.sql.types import StructType, StructField, StringType
0017 | from src.spark.streaming_job import clean_events
0018 | 
0019 | 
0020 | def main():
0021 |     root = Path("/opt/spark/app/work/spark-semantics-" + uuid.uuid4().hex[:10])
0022 |     incoming = root / "input"
0023 |     incoming.mkdir(parents=True)
0024 |     spark = (
0025 |         SparkSession.builder.master("local[2]")
0026 |         .appName("cataclysm-isolated-semantics")
0027 |         .config("spark.sql.shuffle.partitions", "2")
0028 |         .getOrCreate()
0029 |     )
0030 |     spark.sparkContext.setLogLevel("ERROR")
0031 |     source = spark.readStream.schema(
0032 |         StructType([StructField("value", StringType())])
0033 |     ).json(str(incoming))
0034 |     cleaned = clean_events(source)
0035 |     path = str(root / "processed")
0036 |     checkpoint = str(root / "checkpoint")
0037 | 
0038 |     def start():
0039 |         return (
0040 |             cleaned.writeStream.format("parquet")
0041 |             .option("path", path)
0042 |             .option("checkpointLocation", checkpoint)
0043 |             .start()
0044 |         )
0045 | 
0046 |     query = start()
0047 |     base = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
0048 | 
0049 |     def send(index, pairs):
0050 |         values = []
0051 |         for identifier, minutes in pairs:
0052 |             event = dict(
0053 |                 event_id=identifier,
0054 |                 timestamp=(base + timedelta(minutes=minutes)).isoformat(),
0055 |                 text="  SYNTHETIC Flood  ",
0056 |                 location="Isolated test",
0057 |                 disaster_type="flood",
0058 |                 severity="minor",
0059 |                 source="synthetic_spark_test",
0060 |                 latitude=0.0,
0061 |                 longitude=0.0,
0062 |             )
0063 |             values.append(json.dumps({"value": json.dumps(event)}))
0064 |         (incoming / f"batch-{index}.json").write_text("\n".join(values))
0065 |         query.processAllAvailable()
0066 | 
0067 |     send(1, [("first", 0), ("first", 0)])
0068 |     query.stop()
0069 |     query = start()
0070 |     send(2, [("first", 0), ("advance", 30)])
0071 |     send(3, [("too-late", 0), ("advance-again", 60)])
0072 |     progress = query.recentProgress
0073 |     dropped = sum(
0074 |         operator.get("numRowsDroppedByWatermark", 0)
0075 |         for p in progress
0076 |         for operator in p.get("stateOperators", [])
0077 |     )
0078 |     query.stop()
0079 |     rows = spark.read.parquet(path).collect()
0080 |     ids = [row.event_id for row in rows]
0081 |     assert ids.count("first") == 1 and "too-late" not in ids and len(ids) == 3, ids
0082 |     assert all(row.text == "synthetic flood" for row in rows)
0083 |     assert dropped >= 1
0084 |     processed = (
0085 |         spark.readStream.schema(cleaned.schema)
0086 |         .parquet(path)
0087 |         .withWatermark("event_timestamp", "10 minutes")
0088 |     )
0089 |     curated = str(root / "curated")
0090 |     aggregation = (
0091 |         processed.groupBy(
0092 |             window("event_timestamp", "10 minutes", "5 minutes"),
0093 |             "disaster_type",
0094 |             "severity",
0095 |         )
0096 |         .count()
0097 |         .writeStream.format("parquet")
0098 |         .outputMode("append")
0099 |         .option("path", curated)
0100 |         .option("checkpointLocation", str(root / "aggregate-checkpoint"))
0101 |         .start()
0102 |     )
0103 |     aggregation.processAllAvailable()
0104 |     # A subsequent trigger applies the advanced watermark and finalizes windows.
0105 |     aggregation.processAllAvailable()
0106 |     aggregation.stop()
0107 |     windows = spark.read.parquet(curated).collect()
0108 |     first_windows = [
0109 |         row
0110 |         for row in windows
0111 |         if row.window.start <= base.replace(tzinfo=None) < row.window.end
0112 |     ]
0113 |     assert len(first_windows) == 2 and all(
0114 |         row["count"] == 1 for row in first_windows
0115 |     ), str(windows)
0116 |     report = {
0117 |         "observed_at": datetime.now(timezone.utc).isoformat(),
0118 |         "spark_version": spark.version,
0119 |         "root": str(root),
0120 |         "processed_event_ids": ids,
0121 |         "restart_preserved_dedup_state": True,
0122 |         "late_event_absent": True,
0123 |         "watermark_dropped_rows": dropped,
0124 |         "normalized_text": "synthetic flood",
0125 |         "finalized_windows_for_first_event": len(first_windows),
0126 |         "window_counts": [row["count"] for row in first_windows],
0127 |         "scope": "Actual clean_events with isolated file source, local[2], checkpoint restart, Parquet readback and downstream window aggregation. Production Kafka path is covered separately by demo_smoke.",
0128 |     }
0129 |     Path("/opt/spark/app/docs/evidence").mkdir(exist_ok=True)
0130 |     Path("/opt/spark/app/docs/evidence/spark-semantics.json").write_text(
0131 |         json.dumps(report, indent=2)
0132 |     )
0133 |     print(json.dumps(report, indent=2))
0134 |     spark.stop()
0135 | 
0136 | 
0137 | if __name__ == "__main__":
0138 |     main()
```

### `scripts/verify_dependency_outage.py`

[Open source](../../scripts/verify_dependency_outage.py) · SHA-256 `9234d4985e70815b06517f1ace3293c81759755190d3c1a733e5e78c33e31d1a`

Dependencies: `import json`; `import subprocess`; `import time`; `import uuid`; `from datetime import datetime, timezone`; `from pathlib import Path`; `import requests`

- `compose`: lines 14–15. Read its control flow with the module guide above.

- `wait_ready`: lines 18–27. Read its control flow with the module guide above.

- `main`: lines 30–86. Read its control flow with the module guide above.


```text
0001 | """Brief reversible DB/broker outage rehearsal. Run on host, not in a container."""
0002 | 
0003 | import json
0004 | import subprocess
0005 | import time
0006 | import uuid
0007 | from datetime import datetime, timezone
0008 | from pathlib import Path
0009 | import requests
0010 | 
0011 | BASE = "http://127.0.0.1:8000"
0012 | 
0013 | 
0014 | def compose(*args):
0015 |     subprocess.run(["docker", "compose", *args], check=True, capture_output=True)
0016 | 
0017 | 
0018 | def wait_ready():
0019 |     deadline = time.monotonic() + 90
0020 |     while time.monotonic() < deadline:
0021 |         try:
0022 |             if requests.get(BASE + "/ready", timeout=10).status_code == 200:
0023 |                 return
0024 |         except requests.RequestException:
0025 |             pass
0026 |         time.sleep(2)
0027 |     raise RuntimeError("Service did not recover")
0028 | 
0029 | 
0030 | def main():
0031 |     report = {"observed_at": datetime.now(timezone.utc).isoformat()}
0032 |     try:
0033 |         compose("stop", "db")
0034 |         report["database_outage_events_status"] = requests.get(
0035 |             BASE + "/api/events", timeout=15
0036 |         ).status_code
0037 |         report["database_outage_ready_status"] = requests.get(
0038 |             BASE + "/ready", timeout=15
0039 |         ).status_code
0040 |         assert (
0041 |             report["database_outage_events_status"] == 503
0042 |             and report["database_outage_ready_status"] == 503
0043 |         )
0044 |     finally:
0045 |         compose("start", "db")
0046 |         wait_ready()
0047 |     event = dict(
0048 |         event_id="OUTAGE-" + uuid.uuid4().hex[:16],
0049 |         timestamp=datetime.now(timezone.utc).isoformat(),
0050 |         text="Synthetic broker outage fixture",
0051 |         location="Outage test",
0052 |         latitude=0,
0053 |         longitude=0,
0054 |         disaster_type="flood",
0055 |         severity="minor",
0056 |         source="synthetic_outage",
0057 |         metadata={"is_synthetic": True},
0058 |     )
0059 |     try:
0060 |         compose("stop", "broker")
0061 |         report["broker_outage_ingest_status"] = requests.post(
0062 |             BASE + "/events", json=event, timeout=15
0063 |         ).status_code
0064 |         assert report["broker_outage_ingest_status"] == 503
0065 |     finally:
0066 |         compose("start", "broker")
0067 |         wait_ready()
0068 |     response = requests.post(BASE + "/events", json=event, timeout=15)
0069 |     response.raise_for_status()
0070 |     deadline = time.monotonic() + 60
0071 |     while time.monotonic() < deadline:
0072 |         r = requests.get(BASE + f"/api/events/{event['event_id']}/details", timeout=10)
0073 |         if r.ok:
0074 |             break
0075 |         time.sleep(1)
0076 |     r.raise_for_status()
0077 |     report.update(
0078 |         event_id=event["event_id"],
0079 |         same_id_retry_persisted=True,
0080 |         recovered_readiness=True,
0081 |         note="503 acknowledgment timeout is uncertain acceptance; retry uses original identity. Services restored in finally blocks.",
0082 |     )
0083 |     Path("docs/evidence/dependency-outages.json").write_text(
0084 |         json.dumps(report, indent=2)
0085 |     )
0086 |     print(json.dumps(report, indent=2))
0087 | 
0088 | 
0089 | if __name__ == "__main__":
0090 |     main()
```
