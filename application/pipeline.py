"""
application/pipeline.py — DAG de Apache Beam (Fase 2)

Define el grafo acíclico dirigido (DAG) del pipeline de streaming.
Orquesta el flujo completo: lectura → parsing → windowing →
agregación → escritura a BigQuery, con DLQ para errores.

Arquitectura Hexagonal:
  - Este módulo depende de Apache Beam (capa de aplicación)
  - NO depende directamente de Pub/Sub ni BigQuery
  - Los adaptadores se inyectan como PTransforms

Referencia PRD:
  "Construcción de la capa application/pipeline.py orquestando
   el DAG en modo streaming."
"""

from __future__ import annotations

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from adapters.output.bigquery_sink import FormatMetricsDoFn, create_telemetry_sink
from adapters.output.dlq_handler import FormatDLQDoFn, create_dlq_sink
from application.transforms.aggregation import (
    AggregateTelemetryCombineFn,
    KeyByPlantIdDoFn,
)
from application.transforms.parsing import DLQ_TAG, ParseSensorReadingDoFn, ExtractTimestampDoFn
from application.transforms.windowing import ApplyWindowing
from application.transforms.inference import AnomalyDetectionDoFn

logger = logging.getLogger(__name__)


def build_pipeline(
    pipeline: beam.Pipeline,
    source: beam.PTransform,
    use_bigquery: bool = True,
) -> beam.Pipeline:
    """
    Construye el DAG completo del pipeline de streaming.

    Args:
        pipeline: Instancia de beam.Pipeline ya configurada.
        source: PTransform de entrada (Pub/Sub real o mock).
        use_bigquery: Si True, escribe a BigQuery. Si False,
                      solo loguea los resultados (para testing local).

    Returns:
        El pipeline con todas las transformaciones aplicadas.

    Flujo del DAG:
        1. Source (Pub/Sub o Mock) → bytes JSON
        2. ParseSensorReadingDoFn → SensorReading + DLQ side output
        3. ExtractTimestampDoFn → SensorReading con event time
        4. ApplyWindowing → ventanas fijas de 15 min
        5. KeyByPlantIdDoFn → (plant_id, reading)
        6. CombinePerKey(AggregateTelemetryCombineFn) → AggregatedMetrics
        7. FormatMetricsDoFn → dict para BigQuery
        8. WriteToBigQuery → tabla telemetry_validated

    Side output (DLQ):
        2b. ParseSensorReadingDoFn[dlq] → DLQRecord
        2c. FormatDLQDoFn → dict para BigQuery
        2d. WriteToBigQuery → tabla dead_letter_queue
    """

    # ============================================================
    # Step 1: Lectura de mensajes
    # ============================================================
    raw_messages = pipeline | "ReadSource" >> source

    # ============================================================
    # Step 2: Parsing + Validación con DLQ
    # ============================================================
    parsed = raw_messages | "ParseAndValidate" >> beam.ParDo(
        ParseSensorReadingDoFn()
    ).with_outputs(DLQ_TAG, main="valid")

    valid_readings = parsed.valid
    dlq_records = parsed[DLQ_TAG]

    # ============================================================
    # Step 3: Extraer timestamp para event time
    # ============================================================
    timestamped = valid_readings | "ExtractTimestamp" >> beam.ParDo(
        ExtractTimestampDoFn()
    )

    # ============================================================
    # Step 4: Aplicar ventanas temporales de 15 minutos
    # ============================================================
    windowed = timestamped | "ApplyWindowing" >> ApplyWindowing()

    # ============================================================
    # Step 5: Agrupar por plant_id
    # ============================================================
    keyed = windowed | "KeyByPlantId" >> beam.ParDo(KeyByPlantIdDoFn())

    # ============================================================
    # Step 6: Agregar métricas por (plant_id, ventana)
    # ============================================================
    aggregated = keyed | "Aggregate" >> beam.CombinePerKey(
        AggregateTelemetryCombineFn()
    )

    # Extraer solo los valores (descartar la key plant_id)
    metrics = aggregated | "ExtractValues" >> beam.Values()

    # Filtrar Nones (ventanas vacías)
    valid_metrics = metrics | "FilterNone" >> beam.Filter(
        lambda m: m is not None
    )

    # ============================================================
    # Step 6.5: Detección de Anomalías e Inferencia ML
    # ============================================================
    anomalies_result = valid_metrics | "DetectAnomalies" >> beam.ParDo(
        AnomalyDetectionDoFn()
    ).with_outputs(DLQ_TAG, main="valid")

    enriched_metrics = anomalies_result.valid
    dlq_anomalies = anomalies_result[DLQ_TAG]

    # ============================================================
    # Step 7: Escribir resultados
    # ============================================================
    if use_bigquery:
        # Flujo principal → BigQuery telemetry_validated
        formatted_result = enriched_metrics | "FormatMetrics" >> beam.ParDo(
            FormatMetricsDoFn()
        ).with_outputs(DLQ_TAG, main="valid")

        formatted_result.valid | "WriteToBQ" >> create_telemetry_sink()

        # Consolidar todas las DLQs
        all_dlqs = (
            (dlq_records, dlq_anomalies, formatted_result[DLQ_TAG])
            | "FlattenDLQs" >> beam.Flatten()
        )

        # DLQ → BigQuery dead_letter_queue
        (
            all_dlqs
            | "FormatDLQ" >> beam.ParDo(FormatDLQDoFn())
            | "WriteDLQ" >> create_dlq_sink()
        )
    else:
        # Modo local: solo loguear resultados
        enriched_metrics | "LogMetrics" >> beam.Map(
            lambda m: logger.info(
                "Ventana cerrada: %s | %s | %d lecturas | avg_power=%.4f MW | anomalia=%s (%.2f)",
                m.window_start.isoformat(),
                m.plant_id,
                m.reading_count,
                m.avg_power_mw,
                m.anomaly_type.value,
                m.anomaly_score,
            )
        )

        all_dlqs_local = (
            (dlq_records, dlq_anomalies)
            | "FlattenLocalDLQs" >> beam.Flatten()
        )

        all_dlqs_local | "LogDLQ" >> beam.Map(
            lambda r: logger.warning(
                "DLQ: step=%s | error=%s",
                r.failed_step,
                r.error_message[:100],
            )
        )

    return pipeline
