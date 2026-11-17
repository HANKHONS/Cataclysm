# Sources: preserve identity, observation time and units

## Generator

`generate_event(city_override, disaster_override)` chooses a city and hazard, samples coordinates near that city and generates a narrative from hazard-specific templates. Optional overrides let controlled callers narrow the sample. Severity is a synthetic label; telemetry is sampled from chosen ranges, not observed sensors. The returned plain dictionary includes a new ID, UTC-formatted timestamp, source `synthetic_generator`, `is_synthetic=True` and simulated-location accuracy. This is the project's sensor/citizen simulation; there is no claim of real IoT hardware.

The generator's `main` uses argparse to bound count and delay, requests to POST each event, and checks HTTP success rather than appending directly to the browser. It prints partition/offset for broker confirmation. Its synchronous loop naturally limits the offer rate; one run's wall time includes deliberate sleep and cannot be called server throughput. Randomness is useful for demonstrations but not a controlled benchmark corpus.

## USGS normalization

`severity_from_magnitude` maps magnitude thresholds 4.5/6/7 to input severity labels. This is a heuristic, not provider-supplied damage or earthquake intensity. Magnitude is retained separately, and metadata states the severity method. `_normalize_usgs_features` walks GeoJSON features, checks coordinate count and event time, retains provider identity or a deterministic sorted-JSON hash fallback, converts milliseconds since epoch to aware UTC time, reverses GeoJSON longitude/latitude into named fields, and retains depth/URL. Truncating location to 100 characters meets the input contract.

The public wrapper isolates features: normalize and Pydantic-validate one at a time, append valid results, warn and continue after malformed values. This prevents one broken timestamp from discarding valid neighbors. Missing geometry/time can be skipped. The valid-neighbor regression tests this distinction. Feature parsing is linear in feature count and total narrative/payload size; returning a list holds the current feed in memory, acceptable for a bounded public feed but not an arbitrarily large historical dump.

`fetch_usgs(session, timeout, day)` selects the public hour/day URL, performs a bounded GET, checks status and normalizes JSON. Provider IDs keep repeated polls idempotent. Day-feed historical backfill sets is_replay so newly restored observations are not presented as current live arrivals. Older observations may update at the provider while preserving ID; current first-observation storage deliberately does not implement provider-revision tracking. A future version should define updates explicitly instead of silently replacing canonical reports.

## Weather normalization

`fetch_weather` rejects an empty key before any request. It sends latitude/longitude, the private key and `units=metric` as query parameters; URLs must not be logged because they include the key. It checks HTTP status before JSON parsing. `weather_to_event` requires an actual observation timestamp rather than substituting arrival time, converts wind m/s to km/h by multiplying 3.6, preserves absent wind/temperature as missing and uses unassessed severity. Its hash identity incorporates provider/location coordinates and observation time. The weather description is a provider observation narrative, not a disaster declaration.

Rain absent from this current-weather response is treated as zero reported hourly rain; that convention differs from an unavailable sensor feed and should not be generalized. No water-level measurement is invented. The weather integration is offline-tested; current live authentication remains blocked by the absent key.

## Poller and failure handling

`submit` iterates normalized events and POSTs them to the gateway with a timeout, incrementing accepted count only after a successful HTTP response. `main` selects sources, endpoint and optional repeat interval. No source selected is a CLI error; repeat intervals below ten minutes are rejected to avoid aggressive polling. The HTTPS adapter retries GET for 429/5xx transient statuses with bounded exponential backoff. It does not blindly retry POST with a new ID. A request exception logs a generic error without the credential-bearing URL; a one-shot exits nonzero while repeated polling sleeps before another iteration.

USGS and weather execute within one polling iteration, so a failed earlier request can prevent the later source in that iteration. Separate scheduled source processes would isolate those failures, but no unmeasured scheduler is introduced. Validate an adapter change with normalization/credential/malformed-neighbor unit tests, then one manual public request and actual persisted details. Source labels are declared provenance, not cryptographic provider authentication.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `src/data/generator.py`

[Open source](../../src/data/generator.py) · SHA-256 `cbfdadb9ce5ec228c26f0776feea68fae7d2421026bc7cc122b6958528f460af`

Dependencies: `import time`; `import uuid`; `import random`; `import argparse`; `import requests`; `from datetime import datetime`

- `generate_event`: lines 55–127. Read its control flow with the module guide above.

- `main`: lines 130–175. Read its control flow with the module guide above.


```text
0001 | import time
0002 | import uuid
0003 | import random
0004 | import argparse
0005 | import requests
0006 | from datetime import datetime
0007 | 
0008 | CITIES = [
0009 |     {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777, "typical": "flood"},
0010 |     {"name": "Chennai", "lat": 13.0827, "lon": 80.2707, "typical": "cyclone"},
0011 |     {"name": "Guwahati", "lat": 26.1445, "lon": 91.7362, "typical": "flood"},
0012 |     {"name": "Kochi", "lat": 9.9312, "lon": 76.2673, "typical": "flood"},
0013 |     {"name": "Delhi", "lat": 28.7041, "lon": 77.1025, "typical": "earthquake"},
0014 |     {"name": "Dehradun", "lat": 30.3165, "lon": 78.0322, "typical": "earthquake"},
0015 |     {"name": "Puri", "lat": 19.8135, "lon": 85.8312, "typical": "cyclone"},
0016 |     {"name": "Nagpur", "lat": 21.1458, "lon": 79.0882, "typical": "fire"},
0017 |     {"name": "Bengaluru", "lat": 12.9716, "lon": 77.5946, "typical": "infrastructure"},
0018 |     {"name": "Kolkata", "lat": 22.5726, "lon": 88.3639, "typical": "cyclone"},
0019 | ]
0020 | 
0021 | DISASTER_DESCRIPTIONS = {
0022 |     "flood": [
0023 |         "Severe waterlogging reported in {location}, ground floors inundated after intense rainfall.",
0024 |         "River overflowing past critical danger mark in {location}, rescue boats deployed.",
0025 |         "Flash floods triggered by torrential rain sweep through residential colonies in {location}.",
0026 |         "Water supply disrupted and main roads submerged following dam release near {location}.",
0027 |     ],
0028 |     "earthquake": [
0029 |         "Strong seismic tremors felt in {location}, buildings swaying and residents fleeing into open fields.",
0030 |         "Moderate earthquake strikes near {location}, cracks observed on older masonry structures.",
0031 |         "Multiple aftershocks recorded across {location} district following initial magnitude 5.8 quake.",
0032 |         "Structural damage and power outage reported across several neighborhoods in {location}.",
0033 |     ],
0034 |     "cyclone": [
0035 |         "Fierce gales reaching 140 km/h uprooting trees and electric pylons along coast of {location}.",
0036 |         "Severe storm surge and tidal flooding inundate low-lying coastal belts of {location}.",
0037 |         "High-velocity cyclone winds causing extensive damage to roofs and communications in {location}.",
0038 |         "Heavy squall with blinding rains battering ports and coastal roads near {location}.",
0039 |     ],
0040 |     "fire": [
0041 |         "Large commercial building inferno burning out of control in industrial zone of {location}.",
0042 |         "Dry brush fire spreading rapidly along periphery of {location} due to high wind gusts.",
0043 |         "Chemical warehouse blaze producing thick dark smoke plume over {location} skyline.",
0044 |         "Residential complex fire prompts urgent evacuation of hundreds of families in {location}.",
0045 |     ],
0046 |     "infrastructure": [
0047 |         "Major arterial flyover developed critical concrete crack, closed to traffic in {location}.",
0048 |         "Main power transmission substation failure causes blackout across north sector of {location}.",
0049 |         "Underground water pipeline burst flooding rail tracks and disrupting transit in {location}.",
0050 |         "Highway retaining wall collapse blocks multi-lane freight corridor near {location}.",
0051 |     ],
0052 | }
0053 | 
0054 | 
0055 | def generate_event(city_override=None, disaster_override=None):
0056 |     city = random.choice(CITIES) if not city_override else city_override
0057 |     disaster = (
0058 |         disaster_override
0059 |         if disaster_override
0060 |         else random.choice(["flood", "earthquake", "cyclone", "fire", "infrastructure"])
0061 |     )
0062 | 
0063 |     # Realistic jitter around city coordinates (+/- 0.05 deg ~ 5km)
0064 |     lat = round(city["lat"] + random.uniform(-0.04, 0.04), 4)
0065 |     lon = round(city["lon"] + random.uniform(-0.04, 0.04), 4)
0066 | 
0067 |     # Severity distribution
0068 |     severity = random.choices(
0069 |         ["minor", "moderate", "severe", "critical"], weights=[0.3, 0.35, 0.25, 0.1]
0070 |     )[0]
0071 | 
0072 |     # Telemetry based on disaster
0073 |     if disaster == "flood":
0074 |         meta = {
0075 |             "rainfall_mm": round(random.uniform(60, 320), 1),
0076 |             "water_level_m": round(random.uniform(1.2, 7.5), 1),
0077 |             "wind_speed_kmh": round(random.uniform(10, 45), 1),
0078 |             "temperature_c": round(random.uniform(22, 32), 1),
0079 |         }
0080 |     elif disaster == "cyclone":
0081 |         meta = {
0082 |             "rainfall_mm": round(random.uniform(90, 380), 1),
0083 |             "water_level_m": round(random.uniform(1.5, 6.0), 1),
0084 |             "wind_speed_kmh": round(random.uniform(85, 210), 1),
0085 |             "temperature_c": round(random.uniform(22, 30), 1),
0086 |         }
0087 |     elif disaster == "fire":
0088 |         meta = {
0089 |             "rainfall_mm": 0.0,
0090 |             "water_level_m": 0.0,
0091 |             "wind_speed_kmh": round(random.uniform(15, 55), 1),
0092 |             "temperature_c": round(random.uniform(38, 52), 1),
0093 |         }
0094 |     elif disaster == "earthquake":
0095 |         meta = {
0096 |             "rainfall_mm": round(random.uniform(0, 10), 1),
0097 |             "water_level_m": 0.0,
0098 |             "wind_speed_kmh": round(random.uniform(0, 20), 1),
0099 |             "temperature_c": round(random.uniform(15, 35), 1),
0100 |         }
0101 |     else:  # infrastructure
0102 |         meta = {
0103 |             "rainfall_mm": round(random.uniform(0, 30), 1),
0104 |             "water_level_m": round(random.uniform(0, 1.0), 1),
0105 |             "wind_speed_kmh": round(random.uniform(5, 30), 1),
0106 |             "temperature_c": round(random.uniform(20, 38), 1),
0107 |         }
0108 | 
0109 |     template = random.choice(DISASTER_DESCRIPTIONS[disaster])
0110 |     text = template.format(location=city["name"])
0111 | 
0112 |     return {
0113 |         "event_id": f"EVT-{uuid.uuid4().hex[:8].upper()}",
0114 |         "timestamp": datetime.utcnow().isoformat() + "Z",
0115 |         "text": text,
0116 |         "location": city["name"],
0117 |         "disaster_type": disaster,
0118 |         "latitude": lat,
0119 |         "longitude": lon,
0120 |         "severity": severity,
0121 |         "source": "synthetic_generator",
0122 |         "metadata": {
0123 |             **meta,
0124 |             "is_synthetic": True,
0125 |             "location_accuracy": "Simulated coordinates near city center",
0126 |         },
0127 |     }
0128 | 
0129 | 
0130 | def main():
0131 |     parser = argparse.ArgumentParser(
0132 |         description="Cataclysm Synthetic Crisis Event Generator"
0133 |     )
0134 |     parser.add_argument(
0135 |         "--count", type=int, default=10, help="Number of disaster events to generate"
0136 |     )
0137 |     parser.add_argument(
0138 |         "--delay", type=float, default=0.5, help="Delay in seconds between events"
0139 |     )
0140 |     parser.add_argument(
0141 |         "--url", default="http://localhost:8000/events", help="FastAPI ingest endpoint"
0142 |     )
0143 |     args = parser.parse_args()
0144 | 
0145 |     print("===========================================================")
0146 |     print(f" Cataclysm Event Generator — Dispatching {args.count} Events")
0147 |     print(f" Target Endpoint: {args.url}")
0148 |     print("===========================================================\n")
0149 | 
0150 |     success = 0
0151 |     failed = 0
0152 | 
0153 |     for i in range(1, args.count + 1):
0154 |         event = generate_event()
0155 |         try:
0156 |             res = requests.post(args.url, json=event, timeout=4.0)
0157 |             if res.status_code == 200:
0158 |                 data = res.json()
0159 |                 print(
0160 |                     f"[{i}/{args.count}] ✅ Ingested {event['event_id']} | {event['disaster_type'].upper()} ({event['severity']}) in {event['location']} -> Kafka P{data.get('partition')}@O{data.get('offset')}"
0161 |                 )
0162 |                 success += 1
0163 |             else:
0164 |                 print(
0165 |                     f"[{i}/{args.count}] ❌ Ingest failed: HTTP {res.status_code} - {res.text}"
0166 |                 )
0167 |                 failed += 1
0168 |         except Exception as e:
0169 |             print(f"[{i}/{args.count}] ⚠️ Request failed: {e}")
0170 |             failed += 1
0171 | 
0172 |         if i < args.count and args.delay > 0:
0173 |             time.sleep(args.delay)
0174 | 
0175 |     print(f"\nDispatch Summary: {success} Succeeded, {failed} Failed.")
0176 | 
0177 | 
0178 | if __name__ == "__main__":
0179 |     main()
```

### `src/ingestion/clients.py`

[Open source](../../src/ingestion/clients.py) · SHA-256 `1390a65ed58fd5e438d21b1f9fd362d645f8dc39ee7455b0af13126742c6c3b0`

Dependencies: `from __future__ import annotations`; `import hashlib`; `import json`; `import logging`; `from datetime import datetime, timezone`; `from typing import Any`; `import requests`; `from src.common.models import CrisisEvent`

- `open_meteo_to_event`: lines 24–62. Normalize model-derived current conditions without inventing sensor readings.

- `fetch_open_meteo`: lines 65–84. Fetch a public, noncommercial-use weather model API without credentials.

- `severity_from_magnitude`: lines 87–95. Read its control flow with the module guide above.

- `_normalize_usgs_features`: lines 98–140. Read its control flow with the module guide above.

- `usgs_features_to_events`: lines 143–155. Reject malformed observations independently, preserving valid neighbours.

- `weather_to_event`: lines 158–190. Read its control flow with the module guide above.

- `fetch_usgs`: lines 193–203. Read its control flow with the module guide above.

- `fetch_weather`: lines 206–221. Read its control flow with the module guide above.


```text
0001 | """Public-source adapters. They normalize source-specific payloads to CrisisEvent.
0002 | 
0003 | No source response is treated as a ground-truth severity label. The USGS
0004 | magnitude and weather telemetry are preserved in metadata and labels remain
0005 | the platform's downstream model outputs.
0006 | """
0007 | 
0008 | from __future__ import annotations
0009 | 
0010 | import hashlib
0011 | import json
0012 | import logging
0013 | from datetime import datetime, timezone
0014 | from typing import Any
0015 | 
0016 | import requests
0017 | from src.common.models import CrisisEvent
0018 | 
0019 | USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
0020 | OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
0021 | OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
0022 | 
0023 | 
0024 | def open_meteo_to_event(
0025 |     payload: dict[str, Any], latitude: float, longitude: float
0026 | ) -> dict[str, Any]:
0027 |     """Normalize model-derived current conditions without inventing sensor readings."""
0028 |     current = payload["current"]
0029 |     units = payload["current_units"]
0030 |     if (
0031 |         units.get("temperature_2m") != "°C"
0032 |         or units.get("wind_speed_10m") != "km/h"
0033 |         or units.get("precipitation") != "mm"
0034 |     ):
0035 |         raise ValueError("Unexpected Open-Meteo weather units")
0036 |     timestamp = datetime.fromtimestamp(current["time"], tz=timezone.utc).isoformat()
0037 |     identity = f"{latitude}-{longitude}-{timestamp}"
0038 |     event = {
0039 |         "event_id": "OM-" + hashlib.sha256(identity.encode()).hexdigest()[:24],
0040 |         "timestamp": timestamp,
0041 |         "text": f"Open-Meteo model-derived weather conditions: temperature {current.get('temperature_2m')} C, wind {current.get('wind_speed_10m')} km/h. Not a confirmed disaster.",
0042 |         "location": f"Weather grid near {latitude:.4f}, {longitude:.4f}",
0043 |         "latitude": payload["latitude"],
0044 |         "longitude": payload["longitude"],
0045 |         "disaster_type": "weather",
0046 |         "severity": "unknown",
0047 |         "source": "open_meteo",
0048 |         "metadata": {
0049 |             "temperature_c": current.get("temperature_2m"),
0050 |             "wind_speed_kmh": current.get("wind_speed_10m"),
0051 |             "rainfall_mm": None,
0052 |             "water_level_m": None,
0053 |             "precipitation_mm": current.get("precipitation"),
0054 |             "precipitation_interval_seconds": current["interval"],
0055 |             "measurement_kind": "model_derived_weather",
0056 |             "severity_method": "unassessed",
0057 |             "location_accuracy": "Weather model grid cell; not a measured incident location",
0058 |             "provider_url": "https://open-meteo.com/",
0059 |             "attribution": "Weather data by Open-Meteo, CC BY 4.0",
0060 |         },
0061 |     }
0062 |     return CrisisEvent.model_validate(event).model_dump(mode="json")
0063 | 
0064 | 
0065 | def fetch_open_meteo(
0066 |     session: requests.Session, latitude: float, longitude: float, timeout: float
0067 | ) -> dict[str, Any]:
0068 |     """Fetch a public, noncommercial-use weather model API without credentials."""
0069 |     response = session.get(
0070 |         OPEN_METEO_URL,
0071 |         params={
0072 |             "latitude": latitude,
0073 |             "longitude": longitude,
0074 |             "current": "temperature_2m,precipitation,wind_speed_10m",
0075 |             "wind_speed_unit": "kmh",
0076 |             "temperature_unit": "celsius",
0077 |             "timeformat": "unixtime",
0078 |             "timezone": "UTC",
0079 |             "forecast_days": 1,
0080 |         },
0081 |         timeout=timeout,
0082 |     )
0083 |     response.raise_for_status()
0084 |     return open_meteo_to_event(response.json(), latitude, longitude)
0085 | 
0086 | 
0087 | def severity_from_magnitude(magnitude: float | None) -> str:
0088 |     value = magnitude or 0.0
0089 |     if value >= 7.0:
0090 |         return "critical"
0091 |     if value >= 6.0:
0092 |         return "severe"
0093 |     if value >= 4.5:
0094 |         return "moderate"
0095 |     return "minor"
0096 | 
0097 | 
0098 | def _normalize_usgs_features(payload: dict[str, Any]) -> list[dict[str, Any]]:
0099 |     events: list[dict[str, Any]] = []
0100 |     for feature in payload.get("features", []):
0101 |         properties = feature.get("properties") or {}
0102 |         coordinates = (feature.get("geometry") or {}).get("coordinates") or []
0103 |         if len(coordinates) < 2:
0104 |             continue
0105 |         magnitude = properties.get("mag")
0106 |         timestamp_ms = properties.get("time")
0107 |         if timestamp_ms is None:
0108 |             continue
0109 |         event_id = str(
0110 |             feature.get("id")
0111 |             or hashlib.sha256(json.dumps(feature, sort_keys=True).encode()).hexdigest()[
0112 |                 :24
0113 |             ]
0114 |         )
0115 |         events.append(
0116 |             {
0117 |                 "event_id": f"USGS-{event_id}",
0118 |                 "timestamp": datetime.fromtimestamp(
0119 |                     timestamp_ms / 1000, tz=timezone.utc
0120 |                 ).isoformat(),
0121 |                 "text": f"USGS earthquake: {properties.get('title', properties.get('place', 'unknown location'))}",
0122 |                 "location": (properties.get("place") or "USGS reported location")[:100],
0123 |                 "disaster_type": "earthquake",
0124 |                 "latitude": float(coordinates[1]),
0125 |                 "longitude": float(coordinates[0]),
0126 |                 "severity": severity_from_magnitude(magnitude),
0127 |                 "source": "usgs_feed",
0128 |                 "metadata": {
0129 |                     "severity_method": "magnitude_threshold_heuristic_not_damage",
0130 |                     "magnitude": magnitude,
0131 |                     "depth_km": coordinates[2] if len(coordinates) > 2 else None,
0132 |                     "provider_url": properties.get("url"),
0133 |                     "rainfall_mm": None,
0134 |                     "water_level_m": None,
0135 |                     "wind_speed_kmh": None,
0136 |                     "temperature_c": None,
0137 |                 },
0138 |             }
0139 |         )
0140 |     return events
0141 | 
0142 | 
0143 | def usgs_features_to_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
0144 |     """Reject malformed observations independently, preserving valid neighbours."""
0145 |     events = []
0146 |     for feature in payload.get("features", []):
0147 |         try:
0148 |             normalized = _normalize_usgs_features({"features": [feature]})
0149 |             events.extend(
0150 |                 CrisisEvent.model_validate(event).model_dump(mode="json")
0151 |                 for event in normalized
0152 |             )
0153 |         except (TypeError, ValueError, KeyError, AttributeError, OverflowError):
0154 |             logging.getLogger(__name__).warning("Skipped malformed USGS observation")
0155 |     return events
0156 | 
0157 | 
0158 | def weather_to_event(
0159 |     payload: dict[str, Any], latitude: float, longitude: float
0160 | ) -> dict[str, Any]:
0161 |     rain = (payload.get("rain") or {}).get("1h", 0.0)
0162 |     wind_mps = (payload.get("wind") or {}).get("speed")
0163 |     temperature = (payload.get("main") or {}).get("temp")
0164 |     observed = payload.get("dt")
0165 |     if not observed:
0166 |         raise ValueError("Weather response lacks an observation timestamp")
0167 |     timestamp = (
0168 |         datetime.fromtimestamp(observed, tz=timezone.utc).isoformat()
0169 |         if observed
0170 |         else datetime.now(timezone.utc).isoformat()
0171 |     )
0172 |     identity = f"{payload.get('id', 'location')}-{latitude}-{longitude}-{observed}"
0173 |     return {
0174 |         "event_id": f"OWM-{hashlib.sha256(identity.encode()).hexdigest()[:24]}",
0175 |         "timestamp": timestamp,
0176 |         "text": f"OpenWeather observation for {payload.get('name', 'configured location')}: {((payload.get('weather') or [{}])[0]).get('description', 'weather observation')}",
0177 |         "location": payload.get("name") or "configured location",
0178 |         "disaster_type": "weather",
0179 |         "latitude": float(payload.get("coord", {}).get("lat", latitude)),
0180 |         "longitude": float(payload.get("coord", {}).get("lon", longitude)),
0181 |         "severity": "unknown",
0182 |         "source": "openweather",
0183 |         "metadata": {
0184 |             "severity_method": "unassessed",
0185 |             "rainfall_mm": float(rain or 0.0),
0186 |             "water_level_m": None,
0187 |             "wind_speed_kmh": float(wind_mps) * 3.6 if wind_mps is not None else None,
0188 |             "temperature_c": float(temperature) if temperature is not None else None,
0189 |         },
0190 |     }
0191 | 
0192 | 
0193 | def fetch_usgs(
0194 |     session: requests.Session, timeout: float, day: bool = False
0195 | ) -> list[dict[str, Any]]:
0196 |     url = USGS_URL.replace("all_hour", "all_day") if day else USGS_URL
0197 |     response = session.get(url, timeout=timeout)
0198 |     response.raise_for_status()
0199 |     events = usgs_features_to_events(response.json())
0200 |     if day:
0201 |         for event in events:
0202 |             event["metadata"]["is_replay"] = True
0203 |     return events
0204 | 
0205 | 
0206 | def fetch_weather(
0207 |     session: requests.Session,
0208 |     api_key: str,
0209 |     latitude: float,
0210 |     longitude: float,
0211 |     timeout: float,
0212 | ) -> dict[str, Any]:
0213 |     if not api_key:
0214 |         raise ValueError("OPENWEATHER_API_KEY is required for weather ingestion")
0215 |     response = session.get(
0216 |         OPENWEATHER_URL,
0217 |         params={"lat": latitude, "lon": longitude, "appid": api_key, "units": "metric"},
0218 |         timeout=timeout,
0219 |     )
0220 |     response.raise_for_status()
0221 |     return weather_to_event(response.json(), latitude, longitude)
```

### `src/ingestion/poll_sources.py`

[Open source](../../src/ingestion/poll_sources.py) · SHA-256 `6dd9b57b61cd311afb502b7b9c7ca0cbf4cafc44ad8134eed19305a59450fbb6`

Dependencies: `from __future__ import annotations`; `import argparse`; `import logging`; `import time`; `from collections.abc import Iterable`; `import requests`; `from requests.adapters import HTTPAdapter`; `from urllib3.util.retry import Retry`; `from src.common.config import config`; `from src.ingestion.clients import fetch_open_meteo, fetch_usgs, fetch_weather`

- `submit`: lines 20–28. Read its control flow with the module guide above.

- `main`: lines 31–128. Read its control flow with the module guide above.


```text
0001 | """Poll USGS/OpenWeather and submit normalized events through the API gateway."""
0002 | 
0003 | from __future__ import annotations
0004 | 
0005 | import argparse
0006 | import logging
0007 | import time
0008 | from collections.abc import Iterable
0009 | 
0010 | import requests
0011 | from requests.adapters import HTTPAdapter
0012 | from urllib3.util.retry import Retry
0013 | 
0014 | from src.common.config import config
0015 | from src.ingestion.clients import fetch_open_meteo, fetch_usgs, fetch_weather
0016 | 
0017 | LOGGER = logging.getLogger(__name__)
0018 | 
0019 | 
0020 | def submit(session: requests.Session, events: Iterable[dict], endpoint: str) -> int:
0021 |     accepted = 0
0022 |     for event in events:
0023 |         response = session.post(
0024 |             endpoint, json=event, timeout=config.INGESTION_TIMEOUT_SECONDS
0025 |         )
0026 |         response.raise_for_status()
0027 |         accepted += 1
0028 |     return accepted
0029 | 
0030 | 
0031 | def main() -> None:
0032 |     parser = argparse.ArgumentParser(
0033 |         description="Ingest real public crisis-source observations."
0034 |     )
0035 |     parser.add_argument(
0036 |         "--usgs", action="store_true", help="Fetch USGS all-hour GeoJSON feed."
0037 |     )
0038 |     parser.add_argument(
0039 |         "--usgs-day",
0040 |         action="store_true",
0041 |         help="One-day public USGS backfill for historical analysis.",
0042 |     )
0043 |     parser.add_argument(
0044 |         "--weather",
0045 |         nargs=2,
0046 |         type=float,
0047 |         metavar=("LAT", "LON"),
0048 |         help="Fetch one OpenWeather current observation.",
0049 |     )
0050 |     parser.add_argument("--endpoint", default=config.INGESTION_API_URL)
0051 |     parser.add_argument(
0052 |         "--open-meteo",
0053 |         nargs=2,
0054 |         type=float,
0055 |         metavar=("LAT", "LON"),
0056 |         help="Keyless model-derived weather conditions; not disaster observations.",
0057 |     )
0058 |     parser.add_argument(
0059 |         "--interval",
0060 |         type=float,
0061 |         default=0,
0062 |         help="Seconds between polls; zero runs once.",
0063 |     )
0064 |     args = parser.parse_args()
0065 |     if not args.usgs and not args.usgs_day and not args.weather and not args.open_meteo:
0066 |         parser.error(
0067 |             "select --usgs, --usgs-day, --weather LAT LON, or --open-meteo LAT LON"
0068 |         )
0069 |     for coordinates in (args.weather, args.open_meteo):
0070 |         if coordinates and not (
0071 |             -90 <= coordinates[0] <= 90 and -180 <= coordinates[1] <= 180
0072 |         ):
0073 |             parser.error("latitude/longitude outside valid ranges")
0074 |     if args.weather and not config.OPENWEATHER_API_KEY:
0075 |         parser.error(
0076 |             "OPENWEATHER_API_KEY is missing; configure it privately or use --open-meteo"
0077 |         )
0078 |     if args.interval and args.interval < 600:
0079 |         parser.error("continuous polling interval must be at least 600 seconds")
0080 |     with requests.Session() as session:
0081 |         session.mount(
0082 |             "https://",
0083 |             HTTPAdapter(
0084 |                 max_retries=Retry(
0085 |                     total=3,
0086 |                     backoff_factor=0.5,
0087 |                     status_forcelist=[429, 500, 502, 503, 504],
0088 |                     allowed_methods=["GET"],
0089 |                 )
0090 |             ),
0091 |         )
0092 |         while True:
0093 |             try:
0094 |                 if args.open_meteo:
0095 |                     event = fetch_open_meteo(
0096 |                         session, *args.open_meteo, config.INGESTION_TIMEOUT_SECONDS
0097 |                     )
0098 |                     submit(session, [event], args.endpoint)
0099 |                     LOGGER.info(
0100 |                         "Submitted model-derived weather event %s", event["event_id"]
0101 |                     )
0102 |                 if args.usgs or args.usgs_day:
0103 |                     count = submit(
0104 |                         session,
0105 |                         fetch_usgs(
0106 |                             session, config.INGESTION_TIMEOUT_SECONDS, day=args.usgs_day
0107 |                         ),
0108 |                         args.endpoint,
0109 |                     )
0110 |                     LOGGER.info("Submitted %d USGS events", count)
0111 |                 if args.weather:
0112 |                     event = fetch_weather(
0113 |                         session,
0114 |                         config.OPENWEATHER_API_KEY,
0115 |                         args.weather[0],
0116 |                         args.weather[1],
0117 |                         config.INGESTION_TIMEOUT_SECONDS,
0118 |                     )
0119 |                     submit(session, [event], args.endpoint)
0120 |                     LOGGER.info("Submitted one OpenWeather observation")
0121 |             except (requests.RequestException, ValueError, KeyError, TypeError):
0122 |                 # Do not log request URLs: weather query strings contain the key.
0123 |                 LOGGER.error("Source request or ingestion failed")
0124 |                 if not args.interval:
0125 |                     raise SystemExit(1)
0126 |             if not args.interval:
0127 |                 break
0128 |             time.sleep(args.interval)
0129 | 
0130 | 
0131 | if __name__ == "__main__":
0132 |     logging.basicConfig(
0133 |         level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
0134 |     )
0135 |     main()
```
