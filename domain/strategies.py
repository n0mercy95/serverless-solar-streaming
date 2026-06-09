"""
domain/strategies.py — Patrón Strategy (Detección de Anomalías)

Implementaciones concretas del AnomalyDetectionPort.
Cada estrategia encapsula un algoritmo aislado de detección
que puede intercambiarse dinámicamente en el pipeline.

IMPORTANTE: Este módulo es Python puro. No debe importar
ni Apache Beam ni ningún cliente de GCP.
"""

from __future__ import annotations

from typing import Optional

from domain.models import SensorReading
from domain.ports import AnomalyDetectionPort


class ThermalAnomalyStrategy(AnomalyDetectionPort):
    """
    Detecta anomalías térmicas en los módulos fotovoltaicos.

    Un módulo cuya temperatura excede el umbral operativo
    (típicamente >85°C) indica posible hotspot, degradación
    del encapsulante o fallo en el sistema de ventilación.
    """

    def __init__(self, temp_threshold_c: float = 85.0) -> None:
        self._threshold = temp_threshold_c

    def detect(
        self,
        reading: SensorReading,
        predicted_power: Optional[float] = None,
    ) -> tuple[bool, float, str]:
        if reading.module_temp_c > self._threshold:
            score = min(
                (reading.module_temp_c - self._threshold) / self._threshold,
                1.0,
            )
            return (
                True,
                score,
                f"Temperatura del módulo ({reading.module_temp_c}°C) "
                f"excede umbral de {self._threshold}°C",
            )
        return False, 0.0, ""


class IrradianceDropStrategy(AnomalyDetectionPort):
    """
    Detecta caídas abruptas de irradiancia (LMD_GHI).

    Una caída significativa de GHI con potencia estable puede
    indicar un sensor de irradiancia defectuoso. Si la potencia
    cae proporcionalmente, puede ser una nube pasajera legítima.
    """

    def __init__(
        self,
        ghi_min_threshold: float = 50.0,
        power_deviation_pct: float = 0.3,
    ) -> None:
        self._ghi_min = ghi_min_threshold
        self._deviation_pct = power_deviation_pct

    def detect(
        self,
        reading: SensorReading,
        predicted_power: Optional[float] = None,
    ) -> tuple[bool, float, str]:
        if reading.lmd_ghi < self._ghi_min and reading.power_mw > 0:
            score = 1.0 - (reading.lmd_ghi / max(self._ghi_min, 1.0))
            return (
                True,
                min(score, 1.0),
                f"GHI ({reading.lmd_ghi} W/m²) anormalmente bajo "
                f"con potencia activa ({reading.power_mw} MW)",
            )
        return False, 0.0, ""


class InverterFailureStrategy(AnomalyDetectionPort):
    """
    Detecta fallos en inversores comparando la potencia medida
    contra la predicción del modelo Transformer Bi-LSTM.

    Si la desviación entre la potencia real y la predicha excede
    un umbral porcentual, se genera una alerta de anomalía.
    """

    def __init__(self, deviation_threshold_pct: float = 0.25) -> None:
        self._threshold_pct = deviation_threshold_pct

    def detect(
        self,
        reading: SensorReading,
        predicted_power: Optional[float] = None,
    ) -> tuple[bool, float, str]:
        if predicted_power is None or predicted_power <= 0:
            return False, 0.0, ""

        deviation = abs(reading.power_mw - predicted_power) / predicted_power

        if deviation > self._threshold_pct:
            return (
                True,
                min(deviation, 1.0),
                f"Desviación de potencia ({deviation:.1%}) excede "
                f"umbral de {self._threshold_pct:.0%}. "
                f"Real: {reading.power_mw} MW, "
                f"Predicho: {predicted_power} MW",
            )
        return False, 0.0, ""
