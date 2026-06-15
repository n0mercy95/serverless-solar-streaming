"""
adapters/input/mock_input.py — Fuente de Datos Mock para Testing

Adaptador "driving" que crea una fuente de datos estática
para probar el pipeline con DirectRunner sin necesidad de
un servicio de Pub/Sub activo.

Genera eventos de telemetría usando el simulador de la Fase 1
y los inyecta al pipeline como si vinieran de Pub/Sub.
"""

from __future__ import annotations

import json
import logging

import apache_beam as beam

from simulator.utils import generate_telemetry_event

logger = logging.getLogger(__name__)


def create_mock_source(
    num_events: int = 100,
    anomaly_rate: float = 0.05,
) -> beam.PTransform:
    """
    Factory que crea una fuente de datos mock.

    Genera eventos de telemetría sintéticos usando el generador
    de la Fase 1 y los devuelve como bytes JSON (idéntico formato
    al que produce Pub/Sub).

    Args:
        num_events: Número de eventos de prueba a generar.
        anomaly_rate: Porcentaje de eventos anómalos (0.0 a 1.0).

    Returns:
        PTransform que produce una colección finita de bytes JSON.
    """
    logger.info(
        "Generando %d eventos mock (anomaly_rate=%.1f%%)",
        num_events,
        anomaly_rate * 100,
    )

    events: list[bytes] = []
    anomaly_interval = int(1 / anomaly_rate) if anomaly_rate > 0 else 0

    for i in range(num_events):
        inject_anomaly = anomaly_interval > 0 and (i % anomaly_interval == 0) and i > 0
        event = generate_telemetry_event(inject_anomaly=inject_anomaly)
        events.append(json.dumps(event, ensure_ascii=False).encode("utf-8"))

    return beam.Create(events)
