from unittest.mock import Mock

import pytest

from src.common.config import DEFAULT_INGESTION_API_URL, config
from src.ingestion.clients import fetch_weather
from src.ingestion.poll_sources import submit


def test_default_ingestion_endpoint_targets_event_route():
    assert DEFAULT_INGESTION_API_URL == "http://127.0.0.1:8000/events"


def test_submission_posts_normalized_event_to_configured_endpoint():
    session = Mock()
    session.post.return_value.raise_for_status.return_value = None
    event = {"event_id": "OWM-test"}

    accepted = submit(session, [event], DEFAULT_INGESTION_API_URL)

    assert accepted == 1
    session.post.assert_called_once_with(
        DEFAULT_INGESTION_API_URL,
        json=event,
        timeout=config.INGESTION_TIMEOUT_SECONDS,
    )


def test_weather_requires_key_before_making_request():
    session = Mock()
    with pytest.raises(ValueError, match="OPENWEATHER_API_KEY"):
        fetch_weather(session, "", 12, 77, 3)
    session.get.assert_not_called()


def test_weather_requests_metric_units_and_converts_wind():
    session = Mock()
    session.get.return_value.json.return_value = {
        "id": 1,
        "dt": 1700000000,
        "main": {"temp": 20},
        "wind": {"speed": 10},
    }
    result = fetch_weather(session, "offline-test-key", 12, 77, 3)
    assert session.get.call_args.kwargs["params"]["units"] == "metric"
    assert result["metadata"]["wind_speed_kmh"] == 36
    assert result["severity"] == "unknown"


def test_weather_does_not_invent_observation_time():
    from src.ingestion.clients import weather_to_event

    with pytest.raises(ValueError, match="observation timestamp"):
        weather_to_event({}, 12, 77)


def test_missing_wind_is_not_measured_calm():
    from src.ingestion.clients import weather_to_event

    assert (
        weather_to_event({"dt": 1700000000}, 12, 77)["metadata"]["wind_speed_kmh"]
        is None
    )


def test_historical_day_feed_is_marked_replay():
    from src.ingestion.clients import fetch_usgs

    session = Mock()
    session.get.return_value.json.return_value = {
        "features": [
            {
                "id": "historical",
                "properties": {"time": 1700000000000, "mag": 3},
                "geometry": {"coordinates": [77, 12]},
            }
        ]
    }
    events = fetch_usgs(session, timeout=3, day=True)
    assert events[0]["metadata"]["is_replay"] is True
    assert "all_day" in session.get.call_args.args[0]
