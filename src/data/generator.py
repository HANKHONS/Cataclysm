import time
import uuid
import random
import argparse
import requests
from datetime import datetime

CITIES = [
    {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777, "typical": "flood"},
    {"name": "Chennai", "lat": 13.0827, "lon": 80.2707, "typical": "cyclone"},
    {"name": "Guwahati", "lat": 26.1445, "lon": 91.7362, "typical": "flood"},
    {"name": "Kochi", "lat": 9.9312, "lon": 76.2673, "typical": "flood"},
    {"name": "Delhi", "lat": 28.7041, "lon": 77.1025, "typical": "earthquake"},
    {"name": "Dehradun", "lat": 30.3165, "lon": 78.0322, "typical": "earthquake"},
    {"name": "Puri", "lat": 19.8135, "lon": 85.8312, "typical": "cyclone"},
    {"name": "Nagpur", "lat": 21.1458, "lon": 79.0882, "typical": "fire"},
    {"name": "Bengaluru", "lat": 12.9716, "lon": 77.5946, "typical": "infrastructure"},
    {"name": "Kolkata", "lat": 22.5726, "lon": 88.3639, "typical": "cyclone"},
]

DISASTER_DESCRIPTIONS = {
    "flood": [
        "Severe waterlogging reported in {location}, ground floors inundated after intense rainfall.",
        "River overflowing past critical danger mark in {location}, rescue boats deployed.",
        "Flash floods triggered by torrential rain sweep through residential colonies in {location}.",
        "Water supply disrupted and main roads submerged following dam release near {location}.",
    ],
    "earthquake": [
        "Strong seismic tremors felt in {location}, buildings swaying and residents fleeing into open fields.",
        "Moderate earthquake strikes near {location}, cracks observed on older masonry structures.",
        "Multiple aftershocks recorded across {location} district following initial magnitude 5.8 quake.",
        "Structural damage and power outage reported across several neighborhoods in {location}.",
    ],
    "cyclone": [
        "Fierce gales reaching 140 km/h uprooting trees and electric pylons along coast of {location}.",
        "Severe storm surge and tidal flooding inundate low-lying coastal belts of {location}.",
        "High-velocity cyclone winds causing extensive damage to roofs and communications in {location}.",
        "Heavy squall with blinding rains battering ports and coastal roads near {location}.",
    ],
    "fire": [
        "Large commercial building inferno burning out of control in industrial zone of {location}.",
        "Dry brush fire spreading rapidly along periphery of {location} due to high wind gusts.",
        "Chemical warehouse blaze producing thick dark smoke plume over {location} skyline.",
        "Residential complex fire prompts urgent evacuation of hundreds of families in {location}.",
    ],
    "infrastructure": [
        "Major arterial flyover developed critical concrete crack, closed to traffic in {location}.",
        "Main power transmission substation failure causes blackout across north sector of {location}.",
        "Underground water pipeline burst flooding rail tracks and disrupting transit in {location}.",
        "Highway retaining wall collapse blocks multi-lane freight corridor near {location}.",
    ],
}


def generate_event(city_override=None, disaster_override=None):
    city = random.choice(CITIES) if not city_override else city_override
    disaster = (
        disaster_override
        if disaster_override
        else random.choice(["flood", "earthquake", "cyclone", "fire", "infrastructure"])
    )

    # Realistic jitter around city coordinates (+/- 0.05 deg ~ 5km)
    lat = round(city["lat"] + random.uniform(-0.04, 0.04), 4)
    lon = round(city["lon"] + random.uniform(-0.04, 0.04), 4)

    # Severity distribution
    severity = random.choices(
        ["minor", "moderate", "severe", "critical"], weights=[0.3, 0.35, 0.25, 0.1]
    )[0]

    # Telemetry based on disaster
    if disaster == "flood":
        meta = {
            "rainfall_mm": round(random.uniform(60, 320), 1),
            "water_level_m": round(random.uniform(1.2, 7.5), 1),
            "wind_speed_kmh": round(random.uniform(10, 45), 1),
            "temperature_c": round(random.uniform(22, 32), 1),
        }
    elif disaster == "cyclone":
        meta = {
            "rainfall_mm": round(random.uniform(90, 380), 1),
            "water_level_m": round(random.uniform(1.5, 6.0), 1),
            "wind_speed_kmh": round(random.uniform(85, 210), 1),
            "temperature_c": round(random.uniform(22, 30), 1),
        }
    elif disaster == "fire":
        meta = {
            "rainfall_mm": 0.0,
            "water_level_m": 0.0,
            "wind_speed_kmh": round(random.uniform(15, 55), 1),
            "temperature_c": round(random.uniform(38, 52), 1),
        }
    elif disaster == "earthquake":
        meta = {
            "rainfall_mm": round(random.uniform(0, 10), 1),
            "water_level_m": 0.0,
            "wind_speed_kmh": round(random.uniform(0, 20), 1),
            "temperature_c": round(random.uniform(15, 35), 1),
        }
    else:  # infrastructure
        meta = {
            "rainfall_mm": round(random.uniform(0, 30), 1),
            "water_level_m": round(random.uniform(0, 1.0), 1),
            "wind_speed_kmh": round(random.uniform(5, 30), 1),
            "temperature_c": round(random.uniform(20, 38), 1),
        }

    template = random.choice(DISASTER_DESCRIPTIONS[disaster])
    text = template.format(location=city["name"])

    return {
        "event_id": f"EVT-{uuid.uuid4().hex[:8].upper()}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "text": text,
        "location": city["name"],
        "disaster_type": disaster,
        "latitude": lat,
        "longitude": lon,
        "severity": severity,
        "source": "synthetic_generator",
        "metadata": {
            **meta,
            "is_synthetic": True,
            "location_accuracy": "Simulated coordinates near city center",
        },
    }


def main():
    parser = argparse.ArgumentParser(
        description="Cataclysm Synthetic Crisis Event Generator"
    )
    parser.add_argument(
        "--count", type=int, default=10, help="Number of disaster events to generate"
    )
    parser.add_argument(
        "--delay", type=float, default=0.5, help="Delay in seconds between events"
    )
    parser.add_argument(
        "--url", default="http://localhost:8000/events", help="FastAPI ingest endpoint"
    )
    args = parser.parse_args()

    print("===========================================================")
    print(f" Cataclysm Event Generator — Dispatching {args.count} Events")
    print(f" Target Endpoint: {args.url}")
    print("===========================================================\n")

    success = 0
    failed = 0

    for i in range(1, args.count + 1):
        event = generate_event()
        try:
            res = requests.post(args.url, json=event, timeout=4.0)
            if res.status_code == 200:
                data = res.json()
                print(
                    f"[{i}/{args.count}] ✅ Ingested {event['event_id']} | {event['disaster_type'].upper()} ({event['severity']}) in {event['location']} -> Kafka P{data.get('partition')}@O{data.get('offset')}"
                )
                success += 1
            else:
                print(
                    f"[{i}/{args.count}] ❌ Ingest failed: HTTP {res.status_code} - {res.text}"
                )
                failed += 1
        except Exception as e:
            print(f"[{i}/{args.count}] ⚠️ Request failed: {e}")
            failed += 1

        if i < args.count and args.delay > 0:
            time.sleep(args.delay)

    print(f"\nDispatch Summary: {success} Succeeded, {failed} Failed.")


if __name__ == "__main__":
    main()
