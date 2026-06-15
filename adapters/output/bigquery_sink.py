"""
adapters/output/bigquery_sink.py — Escritor hacia BigQuery

Adaptador "driven" que escribe las métricas agregadas y validadas
a la tabla telemetry_validated de Google BigQuery.

Usa streaming inserts de Apache Beam para escribir los datos
en tiempo real conforme las ventanas se cierran.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery

from config.settings import settings
from domain.models import AggregatedMetrics

logger = logging.getLogger(__name__)

# Schema de BigQuery para la tabla telemetry_validated
# (debe coincidir con bq_schema.json y el comando bq mk de setup-bigquery.md)
TELEMETRY_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "plant_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "avg_ghi", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "avg_power_mw", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "max_power_mw", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "min_power_mw", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "avg_module_temp_c", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "reading_count", "type": "INT64", "mode": "REQUIRED"},
        {"name": "predicted_power_mw", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "anomaly_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "anomaly_score", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


def format_metrics_row(metrics: AggregatedMetrics) -> dict:
    """
    Convierte un AggregatedMetrics del dominio a un dict compatible
    con la tabla de BigQuery.

    Los timestamps se formatean como strings ISO 8601 que BigQuery
    parsea automáticamente en campos TIMESTAMP.
    """
    from uuid import uuid4

    return {
        "event_id": str(uuid4()),
        "plant_id": metrics.plant_id,
        "window_start": metrics.window_start.isoformat(),
        "window_end": metrics.window_end.isoformat(),
        "avg_ghi": metrics.avg_ghi,
        "avg_power_mw": metrics.avg_power_mw,
        "max_power_mw": metrics.max_power_mw,
        "min_power_mw": metrics.min_power_mw,
        "avg_module_temp_c": metrics.avg_module_temp_c,
        "reading_count": metrics.reading_count,
        "predicted_power_mw": metrics.predicted_power_mw,
        "anomaly_type": metrics.anomaly_type.value,
        "anomaly_score": metrics.anomaly_score,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
    }


class FormatMetricsDoFn(beam.DoFn):
    """DoFn que convierte AggregatedMetrics a dicts para BigQuery."""

    def process(self, metrics: AggregatedMetrics):
        if metrics is not None:
            try:
                yield format_metrics_row(metrics)
            except Exception as e:
                logger.error("Error en FormatMetricsDoFn: %s", str(e))
                from application.transforms.parsing import DLQ_TAG
                from domain.models import DLQRecord
                dlq_record = DLQRecord(
                    original_payload=str(metrics),
                    error_message=str(e)[:500],
                    failure_timestamp=datetime.now(timezone.utc),
                    failed_step="FormatMetricsDoFn",
                )
                yield beam.pvalue.TaggedOutput(DLQ_TAG, dlq_record)


def create_telemetry_sink() -> beam.PTransform:
    """
    Factory que crea el sumidero de escritura a BigQuery
    para la tabla telemetry_validated.

    Returns:
        PTransform configurado con WriteToBigQuery en modo streaming.
    """
    project = settings.gcp.project_id
    dataset = settings.bigquery.dataset
    table = settings.bigquery.table_telemetry

    table_ref = f"{project}:{dataset}.{table}"

    logger.info("Configurando sink de BigQuery — tabla: %s", table_ref)

    return WriteToBigQuery(
        table=table_ref,
        schema=TELEMETRY_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
    )
