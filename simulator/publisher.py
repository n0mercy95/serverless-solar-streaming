"""
simulator/publisher.py — Publisher de Telemetría IoT a Pub/Sub (Fase 1)

Script principal del simulador. Genera eventos de telemetría
fotovoltaica y los publica al topic de Google Cloud Pub/Sub
usando el patrón Factory para instanciar el cliente correcto
según el entorno (emulador local vs. GCP real).

Uso:
    # Con el emulador de Pub/Sub (docker-compose)
    $ PUBSUB_EMULATOR_HOST=localhost:8085 python -m simulator.publisher

    # Con Pub/Sub real en GCP
    $ python -m simulator.publisher

    # Modo dry-run (solo imprime en stdout, sin publicar)
    $ python -m simulator.publisher --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Protocol

from google.cloud import pubsub_v1

from config.settings import settings
from simulator.utils import (
    RateLimiter,
    generate_telemetry_event,
    load_schema,
    validate_event_basic,
)

# ============================================================
# Logging estructurado (JSON Structured Logging)
# ============================================================

logger = logging.getLogger("simulator.publisher")


def _configure_logging(level: str = "INFO") -> None:
    """Configura logging JSON estructurado según el PRD."""
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
    logging.basicConfig(level=getattr(logging, level), handlers=[handler])


# ============================================================
# Patrón Factory — Creación de Publishers
# ============================================================

class PublisherPort(Protocol):
    """Interfaz del publisher (duck typing via Protocol)."""

    def publish(self, data: bytes) -> None: ...
    def shutdown(self) -> None: ...


class PubSubPublisher:
    """
    Publisher real hacia Google Cloud Pub/Sub.

    Se conecta al emulador si PUBSUB_EMULATOR_HOST está definida,
    o a la API de producción de GCP en caso contrario.
    """

    def __init__(self, project_id: str, topic_id: str) -> None:
        self._topic_path = f"projects/{project_id}/topics/{topic_id}"
        self._client = pubsub_v1.PublisherClient()
        self._futures: list = []

        # Crear el topic si se usa el emulador (en producción ya existe)
        if os.getenv("PUBSUB_EMULATOR_HOST"):
            self._ensure_topic_exists(project_id, topic_id)

        logger.info(
            "PubSubPublisher inicializado — topic: %s, emulador: %s",
            self._topic_path,
            bool(os.getenv("PUBSUB_EMULATOR_HOST")),
        )

    def _ensure_topic_exists(self, project_id: str, topic_id: str) -> None:
        """Crea el topic en el emulador si no existe."""
        try:
            self._client.create_topic(
                request={"name": self._topic_path}
            )
            logger.info("Topic creado en emulador: %s", self._topic_path)
        except Exception:
            # El topic ya existe, ignoramos
            logger.debug("Topic ya existe: %s", self._topic_path)

    def publish(self, data: bytes) -> None:
        """Publica un mensaje al topic de Pub/Sub."""
        future = self._client.publish(self._topic_path, data=data)
        self._futures.append(future)

        # Flush cada 100 mensajes para evitar backpressure
        if len(self._futures) >= 100:
            self._flush()

    def _flush(self) -> None:
        """Espera a que todos los futuros pendientes se resuelvan."""
        for future in self._futures:
            try:
                future.result(timeout=30)
            except Exception as e:
                logger.error("Error publicando mensaje: %s", str(e))
        self._futures.clear()

    def shutdown(self) -> None:
        """Cierra el publisher y espera mensajes pendientes."""
        self._flush()
        self._client.transport.close()
        logger.info("PubSubPublisher cerrado correctamente.")


class DryRunPublisher:
    """
    Publisher de prueba que imprime a stdout.

    Útil para verificar la generación de datos sin necesidad
    de un servicio de Pub/Sub activo.
    """

    def __init__(self) -> None:
        logger.info("DryRunPublisher inicializado — los mensajes se imprimirán en stdout.")

    def publish(self, data: bytes) -> None:
        event = json.loads(data.decode("utf-8"))
        print(json.dumps(event, indent=2, ensure_ascii=False))

    def shutdown(self) -> None:
        logger.info("DryRunPublisher cerrado.")


def create_publisher(dry_run: bool = False) -> PublisherPort:
    """
    Factory Method — Instancia el publisher correcto según el entorno.

    - dry_run=True       → DryRunPublisher (stdout)
    - PUBSUB_EMULATOR_HOST → PubSubPublisher (emulador)
    - Producción          → PubSubPublisher (API real de GCP)
    """
    if dry_run:
        return DryRunPublisher()

    return PubSubPublisher(
        project_id=settings.gcp.project_id,
        topic_id=settings.pubsub.topic,
    )


# ============================================================
# Bucle principal de publicación
# ============================================================

class SimulatorRunner:
    """Orquesta el ciclo de generación y publicación de eventos."""

    def __init__(
        self,
        publisher: PublisherPort,
        rate_limiter: RateLimiter,
        anomaly_rate: float = 0.05,
    ) -> None:
        self._publisher = publisher
        self._rate_limiter = rate_limiter
        self._anomaly_rate = anomaly_rate
        self._running = True
        self._published_count = 0
        self._error_count = 0
        self._schema = load_schema()

    def stop(self) -> None:
        """Señal de parada desde el manejador de señales."""
        self._running = False
        logger.info("Señal de parada recibida. Finalizando...")

    def run(self, max_events: int | None = None) -> None:
        """
        Ejecuta el bucle de publicación.

        Args:
            max_events: Número máximo de eventos a publicar.
                        None = infinito (hasta señal de parada).
        """
        logger.info(
            "Iniciando simulador — rate_limit: %d msg/s, anomaly_rate: %.1f%%",
            settings.simulator.rate_limit,
            self._anomaly_rate * 100,
        )

        start_time = time.monotonic()

        try:
            while self._running:
                if max_events and self._published_count >= max_events:
                    logger.info(
                        "Alcanzado límite de %d eventos. Finalizando.",
                        max_events,
                    )
                    break

                # Generar evento
                inject_anomaly = (
                    self._published_count > 0
                    and (self._published_count % int(1 / self._anomaly_rate)) == 0
                )
                event = generate_telemetry_event(inject_anomaly=inject_anomaly)

                # Validar contra el contrato
                errors = validate_event_basic(event, self._schema)
                if errors:
                    self._error_count += 1
                    logger.warning(
                        "Evento generado con errores de validación: %s",
                        "; ".join(errors),
                    )
                    continue

                # Serializar y publicar
                payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
                self._rate_limiter.acquire()
                self._publisher.publish(payload)
                self._published_count += 1

                # Log de progreso cada 500 eventos
                if self._published_count % 500 == 0:
                    elapsed = time.monotonic() - start_time
                    rate = self._published_count / max(elapsed, 0.001)
                    logger.info(
                        "Progreso: %d eventos publicados (%.1f msg/s, %d errores)",
                        self._published_count,
                        rate,
                        self._error_count,
                    )

                # Delay entre batches
                if self._published_count % settings.simulator.batch_size == 0:
                    time.sleep(settings.simulator.delay_ms / 1000.0)

        except KeyboardInterrupt:
            logger.info("Interrupción por teclado.")
        finally:
            elapsed = time.monotonic() - start_time
            logger.info(
                "Simulador finalizado — %d eventos en %.1f segundos (%.1f msg/s, %d errores)",
                self._published_count,
                elapsed,
                self._published_count / max(elapsed, 0.001),
                self._error_count,
            )
            self._publisher.shutdown()


# ============================================================
# Entrypoint
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simulador de telemetría IoT para plantas fotovoltaicas"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Imprime eventos a stdout sin publicar a Pub/Sub",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Número máximo de eventos a generar (infinito por defecto)",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.05,
        help="Porcentaje de eventos anómalos (default: 5%%)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _configure_logging(settings.log_level)

    publisher = create_publisher(dry_run=args.dry_run)
    rate_limiter = RateLimiter(max_messages_per_second=settings.simulator.rate_limit)

    runner = SimulatorRunner(
        publisher=publisher,
        rate_limiter=rate_limiter,
        anomaly_rate=args.anomaly_rate,
    )

    # Manejar señales para graceful shutdown
    signal.signal(signal.SIGTERM, lambda *_: runner.stop())
    signal.signal(signal.SIGINT, lambda *_: runner.stop())

    runner.run(max_events=args.max_events)


if __name__ == "__main__":
    main()
