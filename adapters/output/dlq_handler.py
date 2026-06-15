"""
adapters/output/dlq_handler.py — Manejador de la Dead Letter Queue

Adaptador "driven" que escribe los registros fallidos del pipeline
a la tabla dead_letter_queue de BigQuery para su posterior análisis
y reprocesamiento.

Los registros llegan desde el side output etiquetado 'dlq' de las
transformaciones DoFn que fallan durante el procesamiento.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery

from config.settings import settings
from domain.models import DLQRecord

logger = logging.getLogger(__name__)

# Schema de BigQuery para la tabla dead_letter_queue
DLQ_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "original_payload", "type": "STRING", "mode": "REQUIRED"},
        {"name": "error_message", "type": "STRING", "mode": "REQUIRED"},
        {"name": "failure_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "failed_step", "type": "STRING", "mode": "REQUIRED"},
    ]
}


def format_dlq_row(record: DLQRecord) -> dict:
    """
    Convierte un DLQRecord del dominio a un dict compatible
    con la tabla dead_letter_queue de BigQuery.
    """
    return {
        "event_id": record.event_id,
        "original_payload": record.original_payload[:10000],  # Truncar payloads muy grandes
        "error_message": record.error_message[:2000],
        "failure_timestamp": record.failure_timestamp.isoformat(),
        "failed_step": record.failed_step,
    }


class FormatDLQDoFn(beam.DoFn):
    """DoFn que convierte DLQRecord a dicts para BigQuery."""

    def process(self, record: DLQRecord):
        yield format_dlq_row(record)


def create_dlq_sink() -> beam.PTransform:
    """
    Factory que crea el sumidero de escritura a BigQuery
    para la tabla dead_letter_queue.

    Returns:
        PTransform configurado con WriteToBigQuery en modo streaming.
    """
    project = settings.gcp.project_id
    dataset = settings.bigquery.dataset
    table = settings.bigquery.table_dlq

    table_ref = f"{project}:{dataset}.{table}"

    logger.info("Configurando sink DLQ de BigQuery — tabla: %s", table_ref)

    return WriteToBigQuery(
        table=table_ref,
        schema=DLQ_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
    )
