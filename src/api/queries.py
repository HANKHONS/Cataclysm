"""Shared parameterized event filters for operational queries and aggregates."""

from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException

from src.common.db import get_pool


def event_filter(
    disaster_type: str | None = None,
    severity: str | None = None,
    source: str | None = None,
    q: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    bbox: str | None = None,
) -> tuple[str, list[Any]]:
    """Build an AND predicate; bbox is west,south,east,north in degrees."""
    conditions: list[str] = []
    params: list[Any] = []
    for column, value in (
        ("disaster_type", disaster_type),
        ("severity", severity),
        ("source", source),
    ):
        if value:
            conditions.append(f"e.{column} = %s")
            params.append(value)
    if q:
        conditions.append(
            "(e.text ILIKE %s OR e.location ILIKE %s OR e.event_id ILIKE %s)"
        )
        params.extend([f"%{q}%"] * 3)
    for column, operator, value in (
        ("timestamp", ">=", since),
        ("timestamp", "<", until),
    ):
        if value:
            conditions.append(f"e.{column} {operator} %s")
            params.append(
                value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
            )
    if (
        since
        and until
        and params
        and (since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since)
        >= (until.replace(tzinfo=timezone.utc) if until.tzinfo is None else until)
    ):
        raise HTTPException(422, "since must precede until")
    if bbox:
        try:
            west, south, east, north = map(float, bbox.split(","))
            if not (-180 <= west <= east <= 180 and -90 <= south <= north <= 90):
                raise ValueError
        except ValueError as exc:
            raise HTTPException(
                422,
                "bbox must be west,south,east,north; antimeridian boxes require two queries",
            ) from exc
        conditions.append(
            "e.longitude BETWEEN %s AND %s AND e.latitude BETWEEN %s AND %s"
        )
        params.extend([west, east, south, north])
    return (" WHERE " + " AND ".join(conditions) if conditions else ""), params


def read_rows(sql: str, params: list[Any] | tuple = ()) -> list[dict]:
    """Execute a read in a worker thread when called from a synchronous route."""
    try:
        with get_pool().connection() as conn:
            return conn.execute(sql, params).fetchall()
    except Exception as exc:
        raise HTTPException(503, "Event storage unavailable") from exc


def provenance(row: dict) -> dict:
    """Expose observation mode without relabeling unverified submissions live."""
    meta = row.get("metadata") or {}
    source = row["source"]
    if meta.get("is_replay"):
        mode = "replay"
    elif (
        meta.get("is_synthetic")
        or source.startswith("synthetic")
        or source in {"operator_manual_dispatch", "integration", "test"}
    ):
        mode = "synthetic"
    elif source == "open_meteo":
        mode = "weather_estimate"
    elif source in {"usgs_feed", "openweather", "nasa_eonet", "gdacs"}:
        mode = "live"
    else:
        mode = "unverified"
    row["data_mode"] = mode
    row["location_accuracy"] = (
        meta.get("location_accuracy") or "Source coordinates; accuracy not supplied"
    )
    for key in ("timestamp", "created_at"):
        if isinstance(row.get(key), datetime):
            row[key] = row[key].isoformat()
    return row
