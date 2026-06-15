"""
tests/test_validators.py — Pruebas unitarias para los validadores Pydantic.

Verifica que el contrato JSON estricto (pubsub_schema.json)
se cumpla correctamente para los mensajes entrantes.
"""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from domain.validators import TelemetryEventValidator


def test_valid_telemetry_event():
    """Un evento con todos los campos válidos debe parsearse correctamente."""
    payload = {
        "event_id": "123e4567-e89b-12d3-a456-426614174000",
        "plant_id": "plant-atacama-001",
        "sensor_id": "sensor-001",
        "sensor_type": "inverter",
        "timestamp": "2023-01-01T12:00:00Z",
        "lmd_ghi": 1050.5,
        "power_mw": 4.2,
        "module_temp_c": 45.5,
        "ambient_temp_c": 30.2,
        "dc_voltage": 600.0,
        "dc_current": 7.0,
        "wind_speed_ms": 5.0,
        "humidity_pct": 20.0,
    }

    validator = TelemetryEventValidator(**payload)
    
    assert validator.event_id == payload["event_id"]
    assert validator.sensor_type == "inverter"
    
    # Validar conversión de timestamp
    ts = validator.to_timestamp()
    assert ts == datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_invalid_sensor_type():
    """Un tipo de sensor no reconocido debe fallar."""
    payload = {
        "event_id": "123",
        "plant_id": "plant-1",
        "sensor_id": "sensor-1",
        "sensor_type": "unknown_type",  # Inválido
        "timestamp": "2023-01-01T12:00:00Z",
        "lmd_ghi": 1000.0,
        "power_mw": 1.0,
        "module_temp_c": 25.0,
        "ambient_temp_c": 20.0,
        "dc_voltage": 100.0,
        "dc_current": 10.0,
    }

    with pytest.raises(ValidationError) as exc:
        TelemetryEventValidator(**payload)
    
    assert "sensor_type" in str(exc.value)


def test_out_of_range_values():
    """Valores físicamente imposibles deben fallar la validación."""
    payload = {
        "event_id": "123",
        "plant_id": "plant-1",
        "sensor_id": "sensor-1",
        "sensor_type": "irradiance",
        "timestamp": "2023-01-01T12:00:00Z",
        "lmd_ghi": 2000.0,  # Demasiado alto (máx 1500)
        "power_mw": -1.0,   # Negativo
        "module_temp_c": 25.0,
        "ambient_temp_c": 20.0,
        "dc_voltage": 100.0,
        "dc_current": 10.0,
    }

    with pytest.raises(ValidationError) as exc:
        TelemetryEventValidator(**payload)
    
    errors = str(exc.value)
    assert "lmd_ghi" in errors
    assert "power_mw" in errors


def test_forbid_extra_fields():
    """Campos no definidos en el esquema deben rechazar el payload."""
    payload = {
        "event_id": "123",
        "plant_id": "plant-1",
        "sensor_id": "sensor-1",
        "sensor_type": "irradiance",
        "timestamp": "2023-01-01T12:00:00Z",
        "lmd_ghi": 1000.0,
        "power_mw": 1.0,
        "module_temp_c": 25.0,
        "ambient_temp_c": 20.0,
        "dc_voltage": 100.0,
        "dc_current": 10.0,
        "extra_field": "hacker_data",  # Campo extra prohibido
    }

    with pytest.raises(ValidationError) as exc:
        TelemetryEventValidator(**payload)
    
    assert "extra_field" in str(exc.value)
