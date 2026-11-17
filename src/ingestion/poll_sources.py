"""Poll USGS/OpenWeather and submit normalized events through the API gateway."""

from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.common.config import config
from src.ingestion.clients import (
    fetch_gdacs,
    fetch_nasa_eonet,
    fetch_open_meteo,
    fetch_usgs,
    fetch_weather,
)

LOGGER = logging.getLogger(__name__)


def submit(session: requests.Session, events: Iterable[dict], endpoint: str) -> int:
    accepted = 0
    for event in events:
        response = session.post(
            endpoint, json=event, timeout=config.INGESTION_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        accepted += 1
    return accepted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest real public crisis-source observations."
    )
    parser.add_argument(
        "--usgs", action="store_true", help="Fetch USGS all-hour GeoJSON feed."
    )
    parser.add_argument(
        "--usgs-day",
        action="store_true",
        help="One-day public USGS backfill for historical analysis.",
    )
    parser.add_argument(
        "--nasa",
        action="store_true",
        help="Fetch NASA EONET active global natural events (wildfires, storms, volcanoes).",
    )
    parser.add_argument(
        "--gdacs",
        action="store_true",
        help="Fetch UN/EC GDACS active global disaster alerts (cyclones, floods, fires, volcanoes).",
    )
    parser.add_argument(
        "--weather",
        nargs=2,
        type=float,
        metavar=("LAT", "LON"),
        help="Fetch one OpenWeather current observation.",
    )
    parser.add_argument("--endpoint", default=config.INGESTION_API_URL)
    parser.add_argument(
        "--open-meteo",
        nargs=2,
        type=float,
        metavar=("LAT", "LON"),
        help="Keyless model-derived weather conditions; not disaster observations.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0,
        help="Seconds between polls; zero runs once.",
    )
    args = parser.parse_args()
    if (
        not args.usgs
        and not args.usgs_day
        and not args.nasa
        and not args.gdacs
        and not args.weather
        and not args.open_meteo
    ):
        parser.error(
            "select --usgs, --usgs-day, --nasa, --gdacs, --weather LAT LON, or --open-meteo LAT LON"
        )
    for coordinates in (args.weather, args.open_meteo):
        if coordinates and not (
            -90 <= coordinates[0] <= 90 and -180 <= coordinates[1] <= 180
        ):
            parser.error("latitude/longitude outside valid ranges")
    if args.weather and not config.OPENWEATHER_API_KEY:
        parser.error(
            "OPENWEATHER_API_KEY is missing; configure it privately or use --open-meteo"
        )
    if args.interval and args.interval < 600:
        parser.error("continuous polling interval must be at least 600 seconds")
    with requests.Session() as session:
        session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=3,
                    backoff_factor=0.5,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET"],
                )
            ),
        )
        while True:
            try:
                if args.open_meteo:
                    event = fetch_open_meteo(
                        session, *args.open_meteo, config.INGESTION_TIMEOUT_SECONDS
                    )
                    submit(session, [event], args.endpoint)
                    LOGGER.info(
                        "Submitted model-derived weather event %s", event["event_id"]
                    )
                if args.usgs or args.usgs_day:
                    count = submit(
                        session,
                        fetch_usgs(
                            session, config.INGESTION_TIMEOUT_SECONDS, day=args.usgs_day
                        ),
                        args.endpoint,
                    )
                    LOGGER.info("Submitted %d USGS events", count)
                if args.nasa:
                    count = submit(
                        session,
                        fetch_nasa_eonet(session, config.INGESTION_TIMEOUT_SECONDS),
                        args.endpoint,
                    )
                    LOGGER.info("Submitted %d NASA EONET events", count)
                if args.gdacs:
                    count = submit(
                        session,
                        fetch_gdacs(session, config.INGESTION_TIMEOUT_SECONDS),
                        args.endpoint,
                    )
                    LOGGER.info("Submitted %d GDACS events", count)
                if args.weather:
                    event = fetch_weather(
                        session,
                        config.OPENWEATHER_API_KEY,
                        args.weather[0],
                        args.weather[1],
                        config.INGESTION_TIMEOUT_SECONDS,
                    )
                    submit(session, [event], args.endpoint)
                    LOGGER.info("Submitted one OpenWeather observation")
            except (requests.RequestException, ValueError, KeyError, TypeError):
                # Do not log request URLs: weather query strings contain the key.
                LOGGER.error("Source request or ingestion failed")
                if not args.interval:
                    raise SystemExit(1)
            if not args.interval:
                break
            time.sleep(args.interval)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    main()
