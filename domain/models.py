"""
domain/models.py — Entidades del Dominio (Dataclasses Puras)

Contiene las representaciones inmutables de la telemetría IoT
fotovoltaica. Estas clases NO dependen de ninguna librería de
infraestructura (ni Beam, ni GCP).

Basado en el dataset PVOD (Photovoltaic Operations Dataset):
- LMD_GHI: Irradiancia Horizontal Global medida localmente (W/m²)
- Power: Potencia de salida del inversor (MW)
- Temperatura de módulo, voltajes DC, corrientes, etc.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import uuid4


class SensorType(str, Enum):
    """Tipos de sensores en una planta fotovoltaica."""
    IRRADIANCE = "irradiance"
    INVERTER = "inverter"
    TEMPERATURE = "temperature"
    WEATHER = "weather"


class AnomalyType(str, Enum):
    """Tipos de anomalías detectables por las estrategias del dominio."""
    THERMAL = "thermal_anomaly"
    IRRADIANCE_DROP = "irradiance_drop"
    INVERTER_FAILURE = "inverter_failure"
    POWER_DEVIATION = "power_deviation"
    NONE = "none"


@dataclass(frozen=True)
class SensorReading:
    """
    Lectura individual de un sensor fotovoltaico.

    Representa un punto de datos crudo enviado por un dispositivo IoT.
    Es la unidad atómica de ingesta desde Pub/Sub.
    """
    plant_id: str
    sensor_id: str
    sensor_type: SensorType
    timestamp: datetime
    lmd_ghi: float              # Irradiancia Horizontal Global (W/m²)
    power_mw: float             # Potencia de salida (MW)
    module_temp_c: float        # Temperatura del módulo (°C)
    ambient_temp_c: float       # Temperatura ambiente (°C)
    dc_voltage: float           # Voltaje DC (V)
    dc_current: float           # Corriente DC (A)
    wind_speed_ms: Optional[float] = None     # Velocidad del viento (m/s)
    humidity_pct: Optional[float] = None      # Humedad relativa (%)
    event_id: str = field(default_factory=lambda: str(uuid4()))


@dataclass(frozen=True)
class AggregatedMetrics:
    """
    Métricas agregadas por ventana temporal (15 min).

    Resultado del enventanamiento (windowing) en Apache Beam.
    Contiene los valores estadísticos que se comparan contra
    las predicciones del modelo Transformer Bi-LSTM.
    """
    plant_id: str
    window_start: datetime
    window_end: datetime
    avg_ghi: float              # Promedio de irradiancia en la ventana
    avg_power_mw: float         # Promedio de potencia en la ventana
    max_power_mw: float         # Pico de potencia en la ventana
    min_power_mw: float         # Mínimo de potencia en la ventana
    avg_module_temp_c: float    # Temperatura promedio del módulo
    reading_count: int          # Cantidad de lecturas en la ventana
    predicted_power_mw: Optional[float] = None  # Predicción del modelo ML
    anomaly_type: AnomalyType = AnomalyType.NONE
    anomaly_score: float = 0.0


@dataclass(frozen=True)
class DLQRecord:
    """
    Registro de la Dead Letter Queue (DLQ).

    Captura los mensajes que fallaron en cualquier etapa del pipeline
    para su posterior análisis y reprocesamiento.
    """
    original_payload: str       # JSON original tal como llegó
    error_message: str          # Descripción del error
    failure_timestamp: datetime # Momento exacto de la falla
    failed_step: str            # Nombre de la transformación que falló
    event_id: str = field(default_factory=lambda: str(uuid4()))
