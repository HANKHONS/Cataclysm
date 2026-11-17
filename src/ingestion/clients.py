"""Public-source adapters. They normalize source-specific payloads to CrisisEvent.

No source response is treated as a ground-truth severity label. The USGS
magnitude and weather telemetry are preserved in metadata and labels remain
the platform's downstream model outputs.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

import requests
from src.common.models import CrisisEvent

USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
NASA_EONET_URL = "https://eonet.gsfc.nasa.gov/api/v3/events"
GDACS_URL = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"

NASA_CATEGORY_MAP = {
    "wildfires": "fire",
    "severeStorms": "cyclone",
    "volcanoes": "volcano",
    "floods": "flood",
    "earthquakes": "earthquake",
    "landslides": "infrastructure",
    "snow": "weather",
    "tempExtremes": "weather",
    "seaLakeIce": "weather",
    "dustHaze": "weather",
    "waterColor": "weather",
    "manmade": "infrastructure",
}

GDACS_TYPE_MAP = {
    "TC": "cyclone",
    "FL": "flood",
    "WF": "fire",
    "VO": "volcano",
    "EQ": "earthquake",
    "DR": "weather",
}


def open_meteo_to_event(
    payload: dict[str, Any], latitude: float, longitude: float
) -> dict[str, Any]:
    """Normalize model-derived current conditions without inventing sensor readings."""
    current = payload["current"]
    units = payload["current_units"]
    if (
        units.get("temperature_2m") != "°C"
        or units.get("wind_speed_10m") != "km/h"
        or units.get("precipitation") != "mm"
    ):
        raise ValueError("Unexpected Open-Meteo weather units")
    timestamp = datetime.fromtimestamp(current["time"], tz=timezone.utc).isoformat()
    identity = f"{latitude}-{longitude}-{timestamp}"
    event = {
        "event_id": "OM-" + hashlib.sha256(identity.encode()).hexdigest()[:24],
        "timestamp": timestamp,
        "text": f"Open-Meteo model-derived weather conditions: temperature {current.get('temperature_2m')} C, wind {current.get('wind_speed_10m')} km/h. Not a confirmed disaster.",
        "location": f"Weather grid near {latitude:.4f}, {longitude:.4f}",
        "latitude": payload["latitude"],
        "longitude": payload["longitude"],
        "disaster_type": "weather",
        "severity": "unknown",
        "source": "open_meteo",
        "metadata": {
            "temperature_c": current.get("temperature_2m"),
            "wind_speed_kmh": current.get("wind_speed_10m"),
            "rainfall_mm": None,
            "water_level_m": None,
            "precipitation_mm": current.get("precipitation"),
            "precipitation_interval_seconds": current["interval"],
            "measurement_kind": "model_derived_weather",
            "severity_method": "unassessed",
            "location_accuracy": "Weather model grid cell; not a measured incident location",
            "provider_url": "https://open-meteo.com/",
            "attribution": "Weather data by Open-Meteo, CC BY 4.0",
        },
    }
    return CrisisEvent.model_validate(event).model_dump(mode="json")


def fetch_open_meteo(
    session: requests.Session, latitude: float, longitude: float, timeout: float
) -> dict[str, Any]:
    """Fetch a public, noncommercial-use weather model API without credentials."""
    response = session.get(
        OPEN_METEO_URL,
        params={
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,precipitation,wind_speed_10m",
            "wind_speed_unit": "kmh",
            "temperature_unit": "celsius",
            "timeformat": "unixtime",
            "timezone": "UTC",
            "forecast_days": 1,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return open_meteo_to_event(response.json(), latitude, longitude)


def severity_from_magnitude(magnitude: float | None) -> str:
    value = magnitude or 0.0
    if value >= 7.0:
        return "critical"
    if value >= 6.0:
        return "severe"
    if value >= 4.5:
        return "moderate"
    return "minor"


def _normalize_usgs_features(payload: dict[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for feature in payload.get("features", []):
        properties = feature.get("properties") or {}
        coordinates = (feature.get("geometry") or {}).get("coordinates") or []
        if len(coordinates) < 2:
            continue
        magnitude = properties.get("mag")
        timestamp_ms = properties.get("time")
        if timestamp_ms is None:
            continue
        event_id = str(
            feature.get("id")
            or hashlib.sha256(json.dumps(feature, sort_keys=True).encode()).hexdigest()[
                :24
            ]
        )
        events.append(
            {
                "event_id": f"USGS-{event_id}",
                "timestamp": datetime.fromtimestamp(
                    timestamp_ms / 1000, tz=timezone.utc
                ).isoformat(),
                "text": f"USGS earthquake: {properties.get('title', properties.get('place', 'unknown location'))}",
                "location": (properties.get("place") or "USGS reported location")[:100],
                "disaster_type": "earthquake",
                "latitude": float(coordinates[1]),
                "longitude": float(coordinates[0]),
                "severity": severity_from_magnitude(magnitude),
                "source": "usgs_feed",
                "metadata": {
                    "severity_method": "magnitude_threshold_heuristic_not_damage",
                    "magnitude": magnitude,
                    "depth_km": coordinates[2] if len(coordinates) > 2 else None,
                    "provider_url": properties.get("url"),
                    "rainfall_mm": None,
                    "water_level_m": None,
                    "wind_speed_kmh": None,
                    "temperature_c": None,
                },
            }
        )
    return events


def usgs_features_to_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Reject malformed observations independently, preserving valid neighbours."""
    events = []
    for feature in payload.get("features", []):
        try:
            normalized = _normalize_usgs_features({"features": [feature]})
            events.extend(
                CrisisEvent.model_validate(event).model_dump(mode="json")
                for event in normalized
            )
        except (TypeError, ValueError, KeyError, AttributeError, OverflowError):
            logging.getLogger(__name__).warning("Skipped malformed USGS observation")
    return events


def weather_to_event(
    payload: dict[str, Any], latitude: float, longitude: float
) -> dict[str, Any]:
    rain = (payload.get("rain") or {}).get("1h", 0.0)
    wind_mps = (payload.get("wind") or {}).get("speed")
    temperature = (payload.get("main") or {}).get("temp")
    observed = payload.get("dt")
    if not observed:
        raise ValueError("Weather response lacks an observation timestamp")
    timestamp = (
        datetime.fromtimestamp(observed, tz=timezone.utc).isoformat()
        if observed
        else datetime.now(timezone.utc).isoformat()
    )
    identity = f"{payload.get('id', 'location')}-{latitude}-{longitude}-{observed}"
    return {
        "event_id": f"OWM-{hashlib.sha256(identity.encode()).hexdigest()[:24]}",
        "timestamp": timestamp,
        "text": f"OpenWeather observation for {payload.get('name', 'configured location')}: {((payload.get('weather') or [{}])[0]).get('description', 'weather observation')}",
        "location": payload.get("name") or "configured location",
        "disaster_type": "weather",
        "latitude": float(payload.get("coord", {}).get("lat", latitude)),
        "longitude": float(payload.get("coord", {}).get("lon", longitude)),
        "severity": "unknown",
        "source": "openweather",
        "metadata": {
            "severity_method": "unassessed",
            "rainfall_mm": float(rain or 0.0),
            "water_level_m": None,
            "wind_speed_kmh": float(wind_mps) * 3.6 if wind_mps is not None else None,
            "temperature_c": float(temperature) if temperature is not None else None,
        },
    }


def fetch_usgs(
    session: requests.Session, timeout: float, day: bool = False
) -> list[dict[str, Any]]:
    url = USGS_URL.replace("all_hour", "all_day") if day else USGS_URL
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    events = usgs_features_to_events(response.json())
    if day:
        for event in events:
            event["metadata"]["is_replay"] = True
    return events


def fetch_weather(
    session: requests.Session,
    api_key: str,
    latitude: float,
    longitude: float,
    timeout: float,
) -> dict[str, Any]:
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY is required for weather ingestion")
    response = session.get(
        OPENWEATHER_URL,
        params={"lat": latitude, "lon": longitude, "appid": api_key, "units": "metric"},
        timeout=timeout,
    )
    response.raise_for_status()
    return weather_to_event(response.json(), latitude, longitude)


def nasa_eonet_to_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize NASA EONET v3 natural events (wildfires, storms, volcanoes) to CrisisEvent."""
    events: list[dict[str, Any]] = []
    for item in payload.get("events", []):
        try:
            geom_list = item.get("geometry") or []
            if not geom_list:
                continue
            # Use the latest observation geometry
            latest_geom = geom_list[-1]
            coords = latest_geom.get("coordinates") or []
            if len(coords) < 2:
                continue
            if isinstance(coords[0], (int, float)) and isinstance(coords[1], (int, float)):
                lon, lat = float(coords[0]), float(coords[1])
            elif isinstance(coords[0], list) and len(coords[0]) >= 2:
                first = coords[0]
                while isinstance(first[0], list):
                    first = first[0]
                lon, lat = float(first[0]), float(first[1])
            else:
                continue

            if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
                continue

            date_str = latest_geom.get("date")
            if not date_str:
                date_str = datetime.now(timezone.utc).isoformat()

            raw_id = str(item.get("id") or "")
            clean_id = re.sub(r"[^A-Za-z0-9_.-]", "_", raw_id)
            if not clean_id:
                clean_id = hashlib.sha256(
                    json.dumps(item, sort_keys=True).encode()
                ).hexdigest()[:20]
            event_id = f"NASA-{clean_id}"[:50]

            categories = item.get("categories") or []
            primary_cat = categories[0].get("id") if categories else "unknown"
            disaster_type = NASA_CATEGORY_MAP.get(primary_cat, "unknown")
            cat_titles = ", ".join(
                c.get("title", "") for c in categories if c.get("title")
            )

            title = item.get("title") or "NASA EONET natural event"
            location = title[:100]
            text = f"NASA EONET observation: {title}. Categories: {cat_titles or primary_cat}."

            mag_val = latest_geom.get("magnitudeValue")
            mag_float = float(mag_val) if mag_val is not None else None

            severity = "moderate"
            if mag_float and mag_float > 100:
                severity = "severe"

            normalized = {
                "event_id": event_id,
                "timestamp": date_str,
                "text": text[:1000],
                "location": location,
                "disaster_type": disaster_type,
                "latitude": lat,
                "longitude": lon,
                "severity": severity,
                "source": "nasa_eonet",
                "metadata": {
                    "severity_method": "nasa_eonet_observatory_tracking",
                    "magnitude": mag_float,
                    "depth_km": None,
                    "provider_url": item.get("link") or "https://eonet.gsfc.nasa.gov/",
                    "location_accuracy": "Satellite / observatory reported event location",
                    "attribution": "NASA Earth Observatory Natural Event Tracker (EONET)",
                    "rainfall_mm": None,
                    "water_level_m": None,
                    "wind_speed_kmh": mag_float if primary_cat == "severeStorms" else None,
                    "temperature_c": None,
                },
            }
            events.append(CrisisEvent.model_validate(normalized).model_dump(mode="json"))
        except (TypeError, ValueError, KeyError, AttributeError, OverflowError) as exc:
            logging.getLogger(__name__).warning("Skipped malformed NASA EONET event: %s", exc)
    return events


def fetch_nasa_eonet(
    session: requests.Session, timeout: float, limit: int = 50, days: int = 30
) -> list[dict[str, Any]]:
    """Fetch active global natural events from NASA EONET (with automatic GDACS fallback if NASA API is down)."""
    try:
        response = session.get(
            NASA_EONET_URL,
            params={"status": "open", "limit": limit, "days": days},
            timeout=timeout,
        )
        response.raise_for_status()
        return nasa_eonet_to_events(response.json())
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "NASA EONET API unavailable (%s); falling back to GDACS UN/EC global feed.",
            exc,
        )
        return fetch_gdacs(session, timeout)


def gdacs_to_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize GDACS (UN & European Commission) global alerts to CrisisEvent."""
    events: list[dict[str, Any]] = []
    for feature in payload.get("features", []):
        try:
            geom = feature.get("geometry") or {}
            coords = geom.get("coordinates") or []
            if len(coords) < 2:
                continue
            lon, lat = float(coords[0]), float(coords[1])
            if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
                continue

            props = feature.get("properties") or {}
            event_type = props.get("eventtype") or "EQ"
            event_id_num = props.get("eventid") or hashlib.sha256(
                json.dumps(feature, sort_keys=True).encode()
            ).hexdigest()[:12]
            clean_event_id = f"GDACS-{event_type}-{event_id_num}"[:50]

            date_str = (
                props.get("todate")
                or props.get("fromdate")
                or props.get("datemodified")
            )
            if date_str:
                try:
                    dt = datetime.fromisoformat(date_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    timestamp = dt.isoformat()
                except Exception:
                    timestamp = datetime.now(timezone.utc).isoformat()
            else:
                timestamp = datetime.now(timezone.utc).isoformat()

            alert_level = (props.get("alertlevel") or "").lower()
            if "red" in alert_level:
                severity = "critical"
            elif "orange" in alert_level:
                severity = "severe"
            elif "green" in alert_level:
                severity = "moderate"
            else:
                severity = "moderate"

            name = (
                props.get("name") or props.get("eventname") or "Global Disaster Alert"
            )
            country = props.get("country") or "Global"
            location = f"{name} ({country})"[:100]

            htmldesc = (
                props.get("htmldescription") or props.get("description") or name
            )
            clean_text = re.sub(r"<[^>]+>", "", htmldesc).strip()
            text = f"GDACS observation: {clean_text}"[:1000]

            sev_data = props.get("severitydata") or {}
            sev_unit = sev_data.get("severityunit", "")
            sev_val = sev_data.get("severity")
            sev_float = float(sev_val) if sev_val is not None else None

            wind_kmh = sev_float if sev_unit == "km/h" else None
            rain_mm = sev_float if sev_unit == "mm" else None
            water_m = sev_float if sev_unit == "m" else None

            urls = props.get("url") or {}
            provider_url = (
                urls.get("report") or urls.get("details") or "https://www.gdacs.org/"
            )

            disaster_type = GDACS_TYPE_MAP.get(event_type, "unknown")

            normalized = {
                "event_id": clean_event_id,
                "timestamp": timestamp,
                "text": text,
                "location": location,
                "disaster_type": disaster_type,
                "latitude": lat,
                "longitude": lon,
                "severity": severity,
                "source": "gdacs",
                "metadata": {
                    "severity_method": f"gdacs_{alert_level or 'unassessed'}_alert_score",
                    "magnitude": float(props.get("alertscore", 0.0) or 0.0),
                    "depth_km": None,
                    "provider_url": provider_url,
                    "location_accuracy": "Centroid coordinates from UN/EC GDACS global monitoring",
                    "attribution": "Global Disaster Alert and Coordination System (GDACS - UN/EC)",
                    "rainfall_mm": rain_mm,
                    "water_level_m": water_m,
                    "wind_speed_kmh": wind_kmh,
                    "temperature_c": None,
                },
            }
            events.append(
                CrisisEvent.model_validate(normalized).model_dump(mode="json")
            )
        except (TypeError, ValueError, KeyError, AttributeError, OverflowError) as exc:
            logging.getLogger(__name__).warning("Skipped malformed GDACS event: %s", exc)
    return events


def fetch_gdacs(session: requests.Session, timeout: float) -> list[dict[str, Any]]:
    """Fetch active global disasters (cyclones, floods, wildfires, volcanoes) from UN/EC GDACS."""
    response = session.get(GDACS_URL, timeout=timeout)
    response.raise_for_status()
    return gdacs_to_events(response.json())

