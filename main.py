"""
main.py — ENTRYPOINT: Ensamblador de Inyección de Dependencias

Punto de entrada principal del pipeline de streaming.
Conecta los puertos (interfaces) del dominio con las
implementaciones concretas de los adaptadores siguiendo
el principio de Inversión de Dependencias.

Este archivo actúa como la "cáscara exterior" de la
Arquitectura Hexagonal, ensamblando todas las piezas.

Uso:
    # Desarrollo local con datos mock (sin infraestructura)
    $ python main.py --runner DirectRunner --mock

    # Desarrollo local con emulador de Pub/Sub
    $ python main.py --runner DirectRunner

    # Producción (DataflowRunner) — Fase 4
    $ python main.py --runner DataflowRunner
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
    WorkerOptions,
)

from application.pipeline import build_pipeline
from config.settings import settings


logger = logging.getLogger("main")


def _configure_json_logging(level: str = "INFO") -> None:
    """Configura JSON Structured Logging según el PRD."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            json.dumps({
                "severity": "%(levelname)s",
                "message": "%(message)s",
                "logger": "%(name)s",
                "timestamp": "%(asctime)s",
            })
        )
    )
    root = logging.getLogger()
    root.setLevel(getattr(logging, level))
    root.handlers.clear()
    root.addHandler(handler)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline de streaming para telemetría fotovoltaica"
    )
    parser.add_argument(
        "--runner",
        type=str,
        default="DirectRunner",
        choices=["DirectRunner", "DataflowRunner"],
        help="Runner de Apache Beam (default: DirectRunner)",
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Usa datos mock en vez de Pub/Sub (para testing local)",
    )
    parser.add_argument(
        "--mock-events",
        type=int,
        default=100,
        help="Número de eventos mock a generar (default: 100)",
    )
    return parser.parse_args()


def _build_pipeline_options(args: argparse.Namespace) -> PipelineOptions:
    """
    Construye las opciones del pipeline según el runner seleccionado.

    - DirectRunner: ejecución local, sin infraestructura
    - DataflowRunner: ejecución en GCP Dataflow (Fase 4)
    """
    options_dict = {
        "runner": args.runner,
        "project": settings.gcp.project_id,
        "region": settings.gcp.region,
    }

    if args.runner == "DataflowRunner":
        # Opciones específicas de Dataflow (Fase 4)
        options_dict.update({
            "temp_location": settings.dataflow.temp_location,
            "staging_location": settings.dataflow.staging_location,
            "job_name": settings.dataflow.job_name,
            "max_num_workers": settings.dataflow.max_workers,
            "machine_type": settings.dataflow.machine_type,
            "streaming": True,
            "save_main_session": True,
        })

    options = PipelineOptions(**options_dict)

    # Permitir serialización de las clases del proyecto
    options.view_as(SetupOptions).save_main_session = True

    # Configurar modo streaming si no es mock
    if not args.mock:
        options.view_as(StandardOptions).streaming = True

    return options


def main() -> None:
    """
    Ensamblador principal del pipeline.

    1. Configura logging JSON estructurado
    2. Parsea argumentos CLI
    3. Selecciona la fuente de datos (Pub/Sub real o mock)
    4. Construye el DAG de Apache Beam
    5. Ejecuta el pipeline
    """
    args = parse_args()
    _configure_json_logging(settings.log_level)

    logger.info("=" * 60)
    logger.info("Solar Streaming Pipeline — Inicialización")
    logger.info("=" * 60)
    logger.info("Proyecto GCP:  %s", settings.gcp.project_id)
    logger.info("Región:        %s", settings.gcp.region)
    logger.info("Runner:        %s", args.runner)
    logger.info("Modo mock:     %s", args.mock)
    logger.info("Topic:         %s", settings.pubsub.topic)
    logger.info("Suscripción:   %s", settings.pubsub.subscription)
    logger.info("Emulador:      %s", settings.pubsub.is_emulated)
    logger.info("=" * 60)

    # Seleccionar fuente de datos
    if args.mock:
        from adapters.input.mock_input import create_mock_source
        source = create_mock_source(num_events=args.mock_events)
        use_bigquery = False
        logger.info("Usando fuente MOCK con %d eventos", args.mock_events)
    else:
        from adapters.input.pubsub_input import create_pubsub_source
        source = create_pubsub_source()
        use_bigquery = True
        logger.info("Usando fuente PUB/SUB real")

    # Construir opciones del pipeline
    pipeline_options = _build_pipeline_options(args)

    # Construir y ejecutar el pipeline
    logger.info("Construyendo el DAG de Apache Beam...")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        build_pipeline(
            pipeline=pipeline,
            source=source,
            use_bigquery=use_bigquery,
        )
        logger.info("Pipeline construido. Ejecutando...")

    logger.info("Pipeline finalizado.")


if __name__ == "__main__":
    main()
