"""
domain/ports.py — Interfaces Abstractas (Puertos del Hexágono)

Define los contratos que la infraestructura (adapters/) debe
implementar. Son las fronteras del dominio.

Convención de nombres:
  - InputPort:  adaptadores "driving" (inyectan datos al sistema)
  - OutputPort: adaptadores "driven" (reciben datos del sistema)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator, Optional

from domain.models import AggregatedMetrics, DLQRecord, SensorReading


class TelemetryInputPort(ABC):
    """
    Puerto de entrada para la ingesta de telemetría IoT.

    Implementaciones:
      - adapters/input/pubsub_input.py  → Google Cloud Pub/Sub
      - adapters/input/mock_input.py    → Archivos locales (testing)
    """

    @abstractmethod
    def read(self) -> Iterator[SensorReading]:
        """Lee un flujo de lecturas de sensores."""
        ...

    @abstractmethod
    def acknowledge(self, event_id: str) -> None:
        """Confirma el procesamiento exitoso de un mensaje."""
        ...


class TelemetrySinkPort(ABC):
    """
    Puerto de salida para el almacenamiento de telemetría validada.

    Implementaciones:
      - adapters/output/bigquery_sink.py → Google BigQuery
    """

    @abstractmethod
    def write_metrics(self, metrics: AggregatedMetrics) -> None:
        """Escribe métricas agregadas al almacén analítico."""
        ...

    @abstractmethod
    def write_dlq(self, record: DLQRecord) -> None:
        """Escribe un registro fallido a la Dead Letter Queue."""
        ...


class AnomalyDetectionPort(ABC):
    """
    Puerto para las estrategias de detección de anomalías.

    Implementa el Patrón Strategy del GoF.
    Cada estrategia concreta evalúa un tipo específico de anomalía.
    """

    @abstractmethod
    def detect(
        self,
        reading: SensorReading,
        predicted_power: Optional[float] = None,
    ) -> tuple[bool, float, str]:
        """
        Evalúa si una lectura contiene una anomalía.

        Returns:
            tuple[is_anomaly, anomaly_score, anomaly_description]
        """
        ...


class ModelInferencePort(ABC):
    """
    Puerto para la integración del modelo de predicción.

    Permite inyectar el modelo Transformer Bi-LSTM o cualquier
    otro modelo de inferencia sin acoplar al pipeline.
    """

    @abstractmethod
    def predict_power(self, reading: SensorReading) -> float:
        """Predice la potencia esperada dado el contexto del sensor."""
        ...
