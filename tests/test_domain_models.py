"""
tests/test_domain_models.py — Tests unitarios para las entidades del dominio.

Valida que las dataclasses sean inmutables y que los enums
contengan los valores esperados.
"""

from datetime import datetime, timezone

import pytest

from domain.models import (
    AggregatedMetrics,
    AnomalyType,
    DLQRecord,
    SensorReading,
    SensorType,
)


class TestSensorReading:
    """Tests para la entidad SensorReading."""

    def _make_reading(self, **overrides) -> SensorReading:
        defaults = {
            "plant_id": "plant-001",
            "sensor_id": "sensor-inv-01",
            "sensor_type": SensorType.INVERTER,
            "timestamp": datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
            "lmd_ghi": 850.5,
            "power_mw": 3.2,
            "module_temp_c": 45.0,
            "ambient_temp_c": 30.0,
            "dc_voltage": 600.0,
            "dc_current": 8.5,
        }
        defaults.update(overrides)
        return SensorReading(**defaults)

    def test_creation(self):
        reading = self._make_reading()
        assert reading.plant_id == "plant-001"
        assert reading.lmd_ghi == 850.5
        assert reading.power_mw == 3.2

    def test_immutability(self):
        reading = self._make_reading()
        with pytest.raises(AttributeError):
            reading.power_mw = 999.0  # type: ignore[misc]

    def test_auto_generated_event_id(self):
        r1 = self._make_reading()
        r2 = self._make_reading()
        assert r1.event_id != r2.event_id

    def test_optional_fields_default_none(self):
        reading = self._make_reading()
        assert reading.wind_speed_ms is None
        assert reading.humidity_pct is None

    def test_optional_fields_set(self):
        reading = self._make_reading(wind_speed_ms=5.2, humidity_pct=65.0)
        assert reading.wind_speed_ms == 5.2
        assert reading.humidity_pct == 65.0


class TestSensorType:
    """Tests para el enum SensorType."""

    def test_values(self):
        assert SensorType.IRRADIANCE.value == "irradiance"
        assert SensorType.INVERTER.value == "inverter"
        assert SensorType.TEMPERATURE.value == "temperature"
        assert SensorType.WEATHER.value == "weather"


class TestAnomalyType:
    """Tests para el enum AnomalyType."""

    def test_none_default(self):
        assert AnomalyType.NONE.value == "none"

    def test_all_types_exist(self):
        expected = {"thermal_anomaly", "irradiance_drop", "inverter_failure", "power_deviation", "none"}
        actual = {t.value for t in AnomalyType}
        assert actual == expected


class TestDLQRecord:
    """Tests para la entidad DLQRecord."""

    def test_creation(self):
        record = DLQRecord(
            original_payload='{"broken": true}',
            error_message="Schema validation failed",
            failure_timestamp=datetime.now(timezone.utc),
            failed_step="ParseJsonDoFn",
        )
        assert record.failed_step == "ParseJsonDoFn"
        assert record.event_id  # auto-generated
