"""
tests/test_strategies.py — Tests unitarios para las estrategias de anomalías.

Valida cada implementación del patrón Strategy con escenarios
de borde y casos nominales.
"""

from datetime import datetime, timezone

import pytest

from domain.models import SensorReading, SensorType
from domain.strategies import (
    InverterFailureStrategy,
    IrradianceDropStrategy,
    ThermalAnomalyStrategy,
)


def _make_reading(**overrides) -> SensorReading:
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


class TestThermalAnomalyStrategy:
    """Tests para ThermalAnomalyStrategy."""

    def test_normal_temperature_no_anomaly(self):
        strategy = ThermalAnomalyStrategy(temp_threshold_c=85.0)
        reading = _make_reading(module_temp_c=60.0)
        is_anomaly, score, _ = strategy.detect(reading)
        assert is_anomaly is False
        assert score == 0.0

    def test_high_temperature_detected(self):
        strategy = ThermalAnomalyStrategy(temp_threshold_c=85.0)
        reading = _make_reading(module_temp_c=95.0)
        is_anomaly, score, desc = strategy.detect(reading)
        assert is_anomaly is True
        assert score > 0.0
        assert "95.0°C" in desc

    def test_exact_threshold_no_anomaly(self):
        strategy = ThermalAnomalyStrategy(temp_threshold_c=85.0)
        reading = _make_reading(module_temp_c=85.0)
        is_anomaly, _, _ = strategy.detect(reading)
        assert is_anomaly is False

    def test_score_capped_at_one(self):
        strategy = ThermalAnomalyStrategy(temp_threshold_c=50.0)
        reading = _make_reading(module_temp_c=200.0)
        _, score, _ = strategy.detect(reading)
        assert score <= 1.0


class TestIrradianceDropStrategy:
    """Tests para IrradianceDropStrategy."""

    def test_normal_ghi_no_anomaly(self):
        strategy = IrradianceDropStrategy(ghi_min_threshold=50.0)
        reading = _make_reading(lmd_ghi=800.0, power_mw=3.0)
        is_anomaly, _, _ = strategy.detect(reading)
        assert is_anomaly is False

    def test_low_ghi_with_active_power_detected(self):
        strategy = IrradianceDropStrategy(ghi_min_threshold=50.0)
        reading = _make_reading(lmd_ghi=10.0, power_mw=2.0)
        is_anomaly, score, desc = strategy.detect(reading)
        assert is_anomaly is True
        assert score > 0.0
        assert "10.0 W/m²" in desc

    def test_low_ghi_zero_power_no_anomaly(self):
        strategy = IrradianceDropStrategy(ghi_min_threshold=50.0)
        reading = _make_reading(lmd_ghi=10.0, power_mw=0.0)
        is_anomaly, _, _ = strategy.detect(reading)
        assert is_anomaly is False


class TestInverterFailureStrategy:
    """Tests para InverterFailureStrategy."""

    def test_no_prediction_no_anomaly(self):
        strategy = InverterFailureStrategy(deviation_threshold_pct=0.25)
        reading = _make_reading(power_mw=3.0)
        is_anomaly, _, _ = strategy.detect(reading, predicted_power=None)
        assert is_anomaly is False

    def test_within_threshold_no_anomaly(self):
        strategy = InverterFailureStrategy(deviation_threshold_pct=0.25)
        reading = _make_reading(power_mw=3.0)
        is_anomaly, _, _ = strategy.detect(reading, predicted_power=3.1)
        assert is_anomaly is False

    def test_exceeds_threshold_detected(self):
        strategy = InverterFailureStrategy(deviation_threshold_pct=0.25)
        reading = _make_reading(power_mw=1.0)
        is_anomaly, score, desc = strategy.detect(reading, predicted_power=3.0)
        assert is_anomaly is True
        assert score > 0.0
        assert "1.0 MW" in desc
        assert "3.0 MW" in desc
