"""
domain/validators.py — Validación Pydantic del Contrato JSON

Modelo Pydantic que valida estrictamente los mensajes de telemetría
IoT contra el contrato definido en pubsub_schema.json.

Se usa dentro del ParseSensorReadingDoFn para rechazar mensajes
que no cumplan el esquema antes de que entren al pipeline.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class TelemetryEventValidator(BaseModel):
    """
    Validador Pydantic para los eventos de telemetría de Pub/Sub.

    Replica las restricciones del contrato pubsub_schema.json:
    - Tipos estrictos para cada campo
    - Rangos físicamente válidos (ej. GHI entre 0 y 1500 W/m²)
    - Prohibición de campos adicionales (extra='forbid')
    """

    model_config = {"extra": "forbid", "str_strip_whitespace": True}

    event_id: str = Field(..., min_length=1, description="UUID del evento")
    plant_id: str = Field(..., min_length=1, max_length=50)
    sensor_id: str = Field(..., min_length=1, max_length=100)
    sensor_type: str = Field(...)
    timestamp: str = Field(..., description="ISO 8601 datetime string")
    lmd_ghi: float = Field(..., ge=0, le=1500)
    power_mw: float = Field(..., ge=0, le=100)
    module_temp_c: float = Field(..., ge=-40, le=120)
    ambient_temp_c: float = Field(..., ge=-50, le=65)
    dc_voltage: float = Field(..., ge=0, le=1500)
    dc_current: float = Field(..., ge=0, le=100)
    wind_speed_ms: Optional[float] = Field(None, ge=0, le=100)
    humidity_pct: Optional[float] = Field(None, ge=0, le=100)

    @field_validator("sensor_type")
    @classmethod
    def validate_sensor_type(cls, v: str) -> str:
        allowed = {"irradiance", "inverter", "temperature", "weather"}
        if v not in allowed:
            raise ValueError(
                f"sensor_type '{v}' no es válido. Valores permitidos: {allowed}"
            )
        return v

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """Valida que el timestamp sea un ISO 8601 parseable."""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, AttributeError) as e:
            raise ValueError(f"timestamp '{v}' no es ISO 8601 válido: {e}")
        return v

    def to_timestamp(self) -> datetime:
        """Convierte el string timestamp a un objeto datetime."""
        return datetime.fromisoformat(
            self.timestamp.replace("Z", "+00:00")
        )
