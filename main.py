"""
main.py — ENTRYPOINT: Ensamblador de Inyección de Dependencias

Punto de entrada principal del pipeline de streaming.
Conecta los puertos (interfaces) del dominio con las
implementaciones concretas de los adaptadores siguiendo
el principio de Inversión de Dependencias.

Este archivo actúa como la "cáscara exterior" de la
Arquitectura Hexagonal, ensamblando todas las piezas.

Uso:
    # Desarrollo local (DirectRunner + Pub/Sub emulador)
    $ python main.py --runner DirectRunner

    # Producción (DataflowRunner)
    $ python main.py --runner DataflowRunner
"""

from __future__ import annotations

import argparse
import logging
import sys

from config.settings import settings


logger = logging.getLogger("main")


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
    return parser.parse_args()


def main() -> None:
    """
    Ensamblador principal.

    Fase 2+ implementará:
      1. Instanciar el adaptador de entrada (PubSubInput o MockInput)
      2. Instanciar los adaptadores de salida (BigQuerySink, DLQHandler)
      3. Inyectar las estrategias de detección de anomalías
      4. Construir y ejecutar el DAG de Apache Beam
    """
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("=" * 60)
    logger.info("Solar Streaming Pipeline — Inicialización")
    logger.info("=" * 60)
    logger.info("Proyecto GCP:  %s", settings.gcp.project_id)
    logger.info("Región:        %s", settings.gcp.region)
    logger.info("Runner:        %s", args.runner)
    logger.info("Topic:         %s", settings.pubsub.topic)
    logger.info("Suscripción:   %s", settings.pubsub.subscription)
    logger.info("Emulador:      %s", settings.pubsub.is_emulated)
    logger.info("=" * 60)

    # TODO (Fase 2): Construir el pipeline de Apache Beam
    logger.warning(
        "El pipeline aún no está implementado (Fase 2). "
        "Usa 'python -m simulator.publisher' para la Fase 1."
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
