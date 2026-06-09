"""
simulator/utils.py — Utilidades de Control de Flujo para el Simulador

Contiene la lógica de generación de datos sintéticos de telemetría
fotovoltaica, el control de tasa de publicación (rate limiting),
y la validación contra el contrato JSON del esquema.
"""

from __future__ import annotations

import json
import math
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from domain.models import SensorType


# ============================================================
# Generación de datos sintéticos
# ============================================================

# Constantes físicas realistas para una planta fotovoltaica
_PLANT_IDS = ["plant-atacama-001", "plant-atacama-002", "plant-sonora-001"]
_SENSORS_PER_PLANT = 8
_INVERTER_CAPACITY_MW = 5.0
_MAX_GHI_WM2 = 1200.0  # Irradiancia máxima típica en desierto


def generate_sensor_ids(plant_id: str) -> list[str]:
    """Genera IDs de sensores para una planta dada."""
    return [f"{plant_id}-sensor-{i:03d}" for i in range(_SENSORS_PER_PLANT)]


def _solar_position_factor(hour: float) -> float:
    """
    Simula la curva de irradiancia solar diurna.

    Usa una función coseno desplazada para modelar la campana
    gaussiana de producción solar (pico a las 12:00 UTC-4).
    """
    solar_noon = 16.0  # 12:00 hora local en UTC
    if hour < 10.0 or hour > 22.0:
        return 0.0
    return max(0.0, math.cos(math.pi * (hour - solar_noon) / 12.0))


def generate_telemetry_event(
    plant_id: str | None = None,
    sensor_id: str | None = None,
    timestamp: datetime | None = None,
    inject_anomaly: bool = False,
) -> dict[str, Any]:
    """
    Genera un evento de telemetría IoT sintético.

    Los valores simulan lecturas realistas de una planta fotovoltaica
    en el desierto de Atacama, respetando relaciones físicas:
    - Power ∝ GHI × efficiency × (1 - temp_coeff × ΔT)
    - Module temp > Ambient temp (calentamiento por radiación)

    Args:
        plant_id: ID de la planta. Si None, se elige aleatoriamente.
        sensor_id: ID del sensor. Si None, se genera.
        timestamp: Marca temporal. Si None, se usa datetime.now(UTC).
        inject_anomaly: Si True, introduce valores anómalos para testing.

    Returns:
        Diccionario que cumple el contrato pubsub_schema.json.
    """
    now = timestamp or datetime.now(timezone.utc)
    plant = plant_id or random.choice(_PLANT_IDS)
    sensor = sensor_id or random.choice(generate_sensor_ids(plant))

    hour = now.hour + now.minute / 60.0
    solar_factor = _solar_position_factor(hour)

    # Irradiancia con variabilidad por nubes (±15%)
    cloud_factor = random.uniform(0.85, 1.0)
    lmd_ghi = round(_MAX_GHI_WM2 * solar_factor * cloud_factor, 2)

    # Temperatura del módulo: función de la irradiancia y el ambiente
    ambient_temp = round(random.uniform(18.0, 38.0), 1)
    module_temp = round(ambient_temp + (lmd_ghi / 30.0) + random.uniform(-2, 5), 1)

    # Potencia: proporcional a GHI con degradación térmica
    temp_coeff = 0.004  # Pérdida de 0.4% por °C sobre 25°C
    temp_loss = max(0.0, (module_temp - 25.0) * temp_coeff)
    efficiency = random.uniform(0.16, 0.22)  # Eficiencia del panel
    power_mw = round(
        _INVERTER_CAPACITY_MW * solar_factor * efficiency * (1 - temp_loss),
        4,
    )
    power_mw = max(0.0, power_mw)

    # Voltaje y corriente DC coherentes con la potencia
    dc_voltage = round(random.uniform(550, 800) * max(solar_factor, 0.01), 1)
    dc_current = round((power_mw * 1e6) / max(dc_voltage, 1.0) / 1000, 2) if dc_voltage > 0 else 0.0

    # ---- Inyección de anomalías para testing ----
    if inject_anomaly:
        anomaly_type = random.choice(["thermal", "irradiance_drop", "inverter"])
        if anomaly_type == "thermal":
            module_temp = round(random.uniform(90.0, 115.0), 1)
        elif anomaly_type == "irradiance_drop":
            lmd_ghi = round(random.uniform(1.0, 20.0), 2)
        elif anomaly_type == "inverter":
            power_mw = round(power_mw * random.uniform(0.1, 0.4), 4)

    event: dict[str, Any] = {
        "event_id": str(uuid4()),
        "plant_id": plant,
        "sensor_id": sensor,
        "sensor_type": random.choice(list(SensorType)).value,
        "timestamp": now.isoformat(),
        "lmd_ghi": lmd_ghi,
        "power_mw": power_mw,
        "module_temp_c": module_temp,
        "ambient_temp_c": ambient_temp,
        "dc_voltage": dc_voltage,
        "dc_current": dc_current,
    }

    # Campos opcionales (70% de probabilidad de incluirse)
    if random.random() < 0.7:
        event["wind_speed_ms"] = round(random.uniform(0.5, 15.0), 1)
    if random.random() < 0.7:
        event["humidity_pct"] = round(random.uniform(10.0, 85.0), 1)

    return event


# ============================================================
# Validación contra el contrato JSON
# ============================================================

def load_schema(schema_path: Path | None = None) -> dict[str, Any]:
    """Carga el contrato JSON del esquema de entrada."""
    if schema_path is None:
        schema_path = (
            Path(__file__).resolve().parent.parent
            / "config"
            / "schemas"
            / "pubsub_schema.json"
        )
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_event_basic(event: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    """
    Validación ligera del evento contra el esquema (sin jsonschema).

    Verifica campos requeridos y tipos básicos. Para validación
    completa en producción se usará Pydantic.

    Returns:
        Lista de errores. Vacía si el evento es válido.
    """
    errors: list[str] = []
    required = schema.get("required", [])
    properties = schema.get("properties", {})

    for field_name in required:
        if field_name not in event:
            errors.append(f"Campo requerido faltante: '{field_name}'")

    for field_name, value in event.items():
        if field_name in properties:
            prop = properties[field_name]
            # Validar rangos numéricos
            if isinstance(value, (int, float)):
                if "minimum" in prop and value < prop["minimum"]:
                    errors.append(
                        f"'{field_name}' = {value} menor que mínimo {prop['minimum']}"
                    )
                if "maximum" in prop and value > prop["maximum"]:
                    errors.append(
                        f"'{field_name}' = {value} mayor que máximo {prop['maximum']}"
                    )

    return errors


# ============================================================
# Control de flujo (Rate Limiting)
# ============================================================

class RateLimiter:
    """
    Limitador de tasa simple basado en token bucket.

    Controla la velocidad de publicación de mensajes
    para no saturar el topic de Pub/Sub.
    """

    def __init__(self, max_messages_per_second: int = 100) -> None:
        self._max_rate = max_messages_per_second
        self._tokens = float(max_messages_per_second)
        self._last_refill = time.monotonic()

    def acquire(self) -> None:
        """Bloquea hasta que haya un token disponible."""
        while True:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                self._max_rate,
                self._tokens + elapsed * self._max_rate,
            )
            self._last_refill = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return

            sleep_time = (1.0 - self._tokens) / self._max_rate
            time.sleep(sleep_time)
