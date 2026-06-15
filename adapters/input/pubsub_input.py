"""
adapters/input/pubsub_input.py — Lector de Google Cloud Pub/Sub

Adaptador "driving" que crea la fuente de lectura del pipeline
desde Google Cloud Pub/Sub.

Implementa el patrón Factory del GoF: la función create_pubsub_source()
decide en tiempo de ejecución si leer de un topic o de una suscripción
según la configuración del proyecto.
"""

from __future__ import annotations

import logging

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub

from config.settings import settings

logger = logging.getLogger(__name__)


def create_pubsub_source() -> beam.PTransform:
    """
    Factory que crea la fuente de lectura de Pub/Sub.

    Lee desde la suscripción configurada en settings.
    El timestamp del mensaje se usa como event time para
    que las ventanas temporales funcionen correctamente.

    Returns:
        PTransform que produce bytes de los mensajes JSON.
    """
    project = settings.gcp.project_id
    subscription = settings.pubsub.subscription

    subscription_path = f"projects/{project}/subscriptions/{subscription}"

    logger.info(
        "Configurando lectura de Pub/Sub — suscripción: %s",
        subscription_path,
    )

    return ReadFromPubSub(
        subscription=subscription_path,
        with_attributes=False,
        timestamp_attribute=None,  # Usa el publish time de Pub/Sub
    )
