"""
application/transforms/aggregation.py — Agregación de Telemetría por Ventana

CombineFn que agrega las lecturas de sensores agrupadas por planta
dentro de cada ventana temporal de 15 minutos.

Produce un AggregatedMetrics por cada combinación de (plant_id, ventana).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import uuid4

import apache_beam as beam

from domain.models import AggregatedMetrics, AnomalyType, SensorReading

logger = logging.getLogger(__name__)


class AggregateTelemetryCombineFn(beam.CombineFn):
    """
    CombineFn que agrega lecturas de sensores en una ventana temporal.

    Calcula las métricas estadísticas (AVG, MAX, MIN) de la potencia,
    irradiancia y temperatura para cada planta dentro de la ventana.

    Funciona en 4 fases del modelo MapReduce de Beam:
    1. create_accumulator(): estado inicial
    2. add_input(): acumula cada lectura
    3. merge_accumulators(): combina resultados de distintos workers
    4. extract_output(): produce el resultado final
    """

    def create_accumulator(self) -> dict:
        """Crea el estado inicial del acumulador."""
        return {
            "sum_ghi": 0.0,
            "sum_power": 0.0,
            "max_power": float("-inf"),
            "min_power": float("inf"),
            "sum_module_temp": 0.0,
            "count": 0,
            "plant_id": None,
            "min_timestamp": None,
            "max_timestamp": None,
        }

    def add_input(self, accumulator: dict, reading: SensorReading) -> dict:
        """Acumula una lectura individual."""
        accumulator["sum_ghi"] += reading.lmd_ghi
        accumulator["sum_power"] += reading.power_mw
        accumulator["max_power"] = max(
            accumulator["max_power"], reading.power_mw
        )
        accumulator["min_power"] = min(
            accumulator["min_power"], reading.power_mw
        )
        accumulator["sum_module_temp"] += reading.module_temp_c
        accumulator["count"] += 1

        # Guardar el plant_id (todos los readings de este grupo
        # tienen el mismo plant_id por el GroupByKey previo)
        if accumulator["plant_id"] is None:
            accumulator["plant_id"] = reading.plant_id

        # Trackear el rango temporal de las lecturas
        ts = reading.timestamp
        if accumulator["min_timestamp"] is None or ts < accumulator["min_timestamp"]:
            accumulator["min_timestamp"] = ts
        if accumulator["max_timestamp"] is None or ts > accumulator["max_timestamp"]:
            accumulator["max_timestamp"] = ts

        return accumulator

    def merge_accumulators(self, accumulators: list[dict]) -> dict:
        """Combina acumuladores de distintos workers."""
        merged = self.create_accumulator()

        for acc in accumulators:
            if acc["count"] == 0:
                continue

            merged["sum_ghi"] += acc["sum_ghi"]
            merged["sum_power"] += acc["sum_power"]
            merged["sum_module_temp"] += acc["sum_module_temp"]
            merged["count"] += acc["count"]

            if acc["max_power"] > merged["max_power"]:
                merged["max_power"] = acc["max_power"]
            if acc["min_power"] < merged["min_power"]:
                merged["min_power"] = acc["min_power"]

            if merged["plant_id"] is None:
                merged["plant_id"] = acc["plant_id"]

            if acc["min_timestamp"] is not None:
                if (
                    merged["min_timestamp"] is None
                    or acc["min_timestamp"] < merged["min_timestamp"]
                ):
                    merged["min_timestamp"] = acc["min_timestamp"]
            if acc["max_timestamp"] is not None:
                if (
                    merged["max_timestamp"] is None
                    or acc["max_timestamp"] > merged["max_timestamp"]
                ):
                    merged["max_timestamp"] = acc["max_timestamp"]

        return merged

    def extract_output(self, accumulator: dict) -> AggregatedMetrics | None:
        """Produce el resultado final de la agregación."""
        count = accumulator["count"]
        if count == 0:
            return None

        plant_id = accumulator["plant_id"] or "unknown"

        # Usar los timestamps reales de las lecturas como ventana
        window_start = accumulator["min_timestamp"] or datetime.now(timezone.utc)
        window_end = accumulator["max_timestamp"] or datetime.now(timezone.utc)

        return AggregatedMetrics(
            plant_id=plant_id,
            window_start=window_start,
            window_end=window_end,
            avg_ghi=round(accumulator["sum_ghi"] / count, 2),
            avg_power_mw=round(accumulator["sum_power"] / count, 4),
            max_power_mw=round(accumulator["max_power"], 4),
            min_power_mw=round(accumulator["min_power"], 4),
            avg_module_temp_c=round(accumulator["sum_module_temp"] / count, 1),
            reading_count=count,
            # Fase 3: se integrará predicted_power_mw del modelo ML
            predicted_power_mw=None,
            anomaly_type=AnomalyType.NONE,
            anomaly_score=0.0,
        )


class KeyByPlantIdDoFn(beam.DoFn):
    """
    Extrae la clave de agrupación (plant_id) de un SensorReading.

    Produce tuplas (plant_id, reading) para que Beam pueda
    hacer GroupByKey y agregar por planta.
    """

    def process(self, reading: SensorReading):
        yield (reading.plant_id, reading)
