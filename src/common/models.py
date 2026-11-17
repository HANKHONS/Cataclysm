from datetime import datetime, timezone
from typing import Literal, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator


class Metadata(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    magnitude: Optional[float] = None
    depth_km: Optional[float] = None
    provider_url: Optional[str] = None
    is_synthetic: bool = False
    is_replay: bool = False
    location_accuracy: Optional[str] = None
    severity_method: Optional[str] = None
    measurement_kind: Optional[str] = None
    precipitation_mm: Optional[float] = None
    precipitation_interval_seconds: Optional[int] = None
    attribution: Optional[str] = None
    rainfall_mm: Optional[float] = Field(
        default=None, description="Rainfall in millimeters"
    )
    water_level_m: Optional[float] = Field(
        default=None, description="Water level in meters"
    )
    wind_speed_kmh: Optional[float] = Field(
        default=None, description="Wind speed in km/h"
    )
    temperature_c: Optional[float] = Field(
        default=None, description="Temperature in Celsius"
    )


class CrisisEvent(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        """Legacy naive timestamps mean UTC; normalize aware inputs to UTC."""
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    event_id: str = Field(
        ...,
        min_length=1,
        max_length=50,
        pattern=r"^[A-Za-z0-9_.-]+$",
        description="Unique event identifier (UUID or hash)",
    )
    timestamp: datetime = Field(
        ..., description="Timestamp of the incident in ISO format"
    )
    text: str = Field(
        ...,
        min_length=1,
        max_length=10000,
        description="Descriptive narrative of the disaster event",
    )
    location: str = Field(
        ..., min_length=1, max_length=100, description="City or region name"
    )
    disaster_type: str = Field(
        ...,
        pattern=r"^[a-z_]+$",
        max_length=50,
        description="flood, earthquake, cyclone, fire, infrastructure, weather, or unknown",
    )
    latitude: float = Field(..., ge=-90.0, le=90.0, description="Geographic latitude")
    longitude: float = Field(
        ..., ge=-180.0, le=180.0, description="Geographic longitude"
    )
    severity: Literal["minor", "moderate", "severe", "critical", "unknown"] = Field(
        ...,
        min_length=1,
        max_length=20,
        description="minor, moderate, severe, or critical",
    )
    source: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="citizen_report, weather_station, usgs_feed, or iot_sensor",
    )
    metadata: Metadata = Field(
        default_factory=Metadata, description="Physical and weather telemetry"
    )


class EnrichedCrisisEvent(CrisisEvent):
    predicted_disaster_type: Optional[str] = Field(
        default=None, description="ML-predicted disaster classification"
    )
    predicted_severity: Optional[str] = Field(
        default=None, description="ML-predicted severity category"
    )
    prediction_confidence: Optional[float] = Field(
        default=None,
        description="Uncalibrated classifier score, not a probability of correctness",
    )


class DLQMessage(BaseModel):
    raw_payload: str
    error_type: str
    error_message: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_topic: str
    source_partition: Optional[int] = None
    source_offset: Optional[int] = None
