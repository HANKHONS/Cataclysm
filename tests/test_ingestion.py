from src.ingestion.clients import (
    gdacs_to_events,
    nasa_eonet_to_events,
    usgs_features_to_events,
    weather_to_event,
)
from src.api.queries import provenance


def test_usgs_normalization_preserves_coordinates_and_id():
    events = usgs_features_to_events(
        {
            "features": [
                {
                    "id": "abc",
                    "properties": {
                        "mag": 6.1,
                        "time": 1_700_000_000_000,
                        "place": "test",
                    },
                    "geometry": {"coordinates": [77.0, 12.0]},
                }
            ]
        }
    )
    assert events[0]["event_id"] == "USGS-abc"
    assert events[0]["severity"] == "severe"


def test_weather_converts_mps_to_kmh():
    event = weather_to_event(
        {
            "id": 1,
            "dt": 1_700_000_000,
            "name": "test",
            "coord": {},
            "wind": {"speed": 10},
            "main": {"temp": 20},
        },
        12,
        77,
    )
    assert event["metadata"]["wind_speed_kmh"] == 36.0


def test_nasa_eonet_normalization_and_live_provenance():
    payload = {
        "events": [
            {
                "id": "EONET_9999",
                "title": "Wildfire - Oregon Cascades",
                "link": "https://eonet.gsfc.nasa.gov/api/v3/events/EONET_9999",
                "categories": [{"id": "wildfires", "title": "Wildfires"}],
                "geometry": [
                    {
                        "magnitudeValue": 120.0,
                        "date": "2026-11-17T12:00:00Z",
                        "type": "Point",
                        "coordinates": [-121.5, 44.2],
                    }
                ],
            }
        ]
    }
    events = nasa_eonet_to_events(payload)
    assert len(events) == 1
    ev = events[0]
    assert ev["event_id"] == "NASA-EONET_9999"
    assert ev["disaster_type"] == "fire"
    assert ev["latitude"] == 44.2
    assert ev["longitude"] == -121.5
    assert ev["source"] == "nasa_eonet"
    assert ev["severity"] == "severe"
    assert provenance(ev)["data_mode"] == "live"


def test_nasa_eonet_skips_malformed_without_exception():
    payload = {
        "events": [
            {"id": "BAD_1", "geometry": []},
            {"id": "BAD_2", "geometry": [{"coordinates": [999.0, 999.0]}]},
            {
                "id": "GOOD_1",
                "title": "Flood Event",
                "categories": [{"id": "floods", "title": "Floods"}],
                "geometry": [
                    {
                        "date": "2026-11-17T10:00:00Z",
                        "coordinates": [88.36, 22.57],
                    }
                ],
            },
        ]
    }
    events = nasa_eonet_to_events(payload)
    assert len(events) == 1
    assert events[0]["event_id"] == "NASA-GOOD_1"
    assert events[0]["disaster_type"] == "flood"


def test_gdacs_normalization_and_live_provenance():
    payload = {
        "features": [
            {
                "geometry": {"type": "Point", "coordinates": [87.3, 12.2]},
                "properties": {
                    "eventtype": "TC",
                    "eventid": 1001351,
                    "name": "Tropical Cyclone AKVAN-26",
                    "alertlevel": "Orange",
                    "country": "India",
                    "fromdate": "2026-11-16T18:00:00",
                    "severitydata": {"severity": 83.3, "severityunit": "km/h"},
                },
            }
        ]
    }
    events = gdacs_to_events(payload)
    assert len(events) == 1
    ev = events[0]
    assert ev["event_id"] == "GDACS-TC-1001351"
    assert ev["disaster_type"] == "cyclone"
    assert ev["latitude"] == 12.2
    assert ev["longitude"] == 87.3
    assert ev["source"] == "gdacs"
    assert ev["severity"] == "severe"
    assert ev["metadata"]["wind_speed_kmh"] == 83.3
    assert provenance(ev)["data_mode"] == "live"


