"""
application/transforms/parsing.py — Parse y Validación de Mensajes JSON

DoFn que deserializa los mensajes de Pub/Sub, los valida contra
el contrato JSON estricto (vía Pydantic) y los convierte en
SensorReading del dominio.

Los mensajes que fallan la validación se desvían a un side output
etiquetado 'dlq' para no bloquear el pipeline principal.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import apache_beam as beam

from domain.models import DLQRecord, SensorReading, SensorType
from domain.validators import TelemetryEventValidator

logger = logging.getLogger(__name__)

# Tag para el side output de la Dead Letter Queue
DLQ_TAG = "dlq"


class ParseSensorReadingDoFn(beam.DoFn):
    """
    Parsea mensajes JSON crudos → SensorReading del dominio.

    Paso 1 del pipeline: recibe bytes de Pub/Sub, valida el schema,
    y produce SensorReading para el flujo principal o DLQRecord para
    la salida lateral en caso de error.

    Cumple el requisito del PRD: cada DoFn está blindado por
    bloques try-except con salida lateral etiquetada.
    """

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam):
        """
        Procesa un mensaje raw de Pub/Sub.

        Args:
            element: Bytes del mensaje JSON.
            timestamp: Timestamp del mensaje (asignado por Pub/Sub).

        Yields:
            SensorReading al flujo principal, o
            DLQRecord al side output 'dlq'.
        """
        try:
            # 1. Deserializar JSON
            raw_json = element.decode("utf-8")
            data = json.loads(raw_json)

            # 2. Validar contra el contrato Pydantic
            validated = TelemetryEventValidator(**data)

            # 3. Convertir a entidad del dominio
            reading = SensorReading(
                event_id=validated.event_id,
                plant_id=validated.plant_id,
                sensor_id=validated.sensor_id,
                sensor_type=SensorType(validated.sensor_type),
                timestamp=validated.to_timestamp(),
                lmd_ghi=validated.lmd_ghi,
                power_mw=validated.power_mw,
                module_temp_c=validated.module_temp_c,
                ambient_temp_c=validated.ambient_temp_c,
                dc_voltage=validated.dc_voltage,
                dc_current=validated.dc_current,
                wind_speed_ms=validated.wind_speed_ms,
                humidity_pct=validated.humidity_pct,
            )

            yield reading

        except Exception as e:
            # Desviar a la DLQ — nunca bloquear el pipeline
            logger.warning(
                "Mensaje fallido desviado a DLQ: %s", str(e)[:200]
            )
            raw_payload = (
                element.decode("utf-8", errors="replace")
                if isinstance(element, bytes)
                else str(element)
            )
            dlq_record = DLQRecord(
                original_payload=raw_payload,
                error_message=str(e)[:500],
                failure_timestamp=datetime.now(timezone.utc),
                failed_step="ParseSensorReadingDoFn",
            )
            yield beam.pvalue.TaggedOutput(DLQ_TAG, dlq_record)


class ExtractTimestampDoFn(beam.DoFn):
    """
    Extrae el timestamp del SensorReading para usarlo como event time.

    Esto permite que Apache Beam use el timestamp del dato original
    (cuando el sensor lo generó) en vez del processing time (cuando
    Beam lo recibió), lo cual es esencial para ventanas correctas.
    """

    def process(self, reading: SensorReading):
        """Emite el reading con su timestamp como event time de Beam."""
        yield beam.window.TimestampedValue(
            reading,
            reading.timestamp.timestamp(),
        )
