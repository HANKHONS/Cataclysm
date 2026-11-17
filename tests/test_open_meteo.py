"""Weather-model attribution, temporal support and missing-value regressions."""
from unittest.mock import Mock
import pytest
from src.ingestion.clients import fetch_open_meteo, open_meteo_to_event
from src.api.queries import provenance


def payload():
    return {"latitude": 19.1, "longitude": 72.9,
            "current_units": {"temperature_2m": "°C", "wind_speed_10m": "km/h", "precipitation": "mm"},
            "current": {"time": 1700000000, "interval": 900, "temperature_2m": 0, "wind_speed_10m": 0, "precipitation": 1.5}}


def test_weather_identity_units_and_model_provenance():
    event = open_meteo_to_event(payload(), 19.076, 72.8777)
    assert event == open_meteo_to_event(payload(), 19.076, 72.8777)
    assert event["metadata"]["temperature_c"] == 0
    assert event["metadata"]["rainfall_mm"] is None
    assert event["metadata"]["precipitation_interval_seconds"] == 900
    assert event["severity"] == "unknown"
    assert provenance(event)["data_mode"] == "weather_estimate"


def test_unknown_units_rejected():
    data = payload()
    data["current_units"]["wind_speed_10m"] = "m/s"
    with pytest.raises(ValueError, match="units"):
        open_meteo_to_event(data, 19, 72)


def test_missing_provider_time_rejected():
    data = payload()
    del data["current"]["time"]
    with pytest.raises(KeyError):
        open_meteo_to_event(data, 19, 72)


def test_request_is_keyless_and_uses_explicit_time_and_units():
    session = Mock()
    session.get.return_value.json.return_value = payload()
    fetch_open_meteo(session, 19, 72, 5)
    params = session.get.call_args.kwargs["params"]
    assert params["timeformat"] == "unixtime"
    assert params["wind_speed_unit"] == "kmh"
    assert "appid" not in params
