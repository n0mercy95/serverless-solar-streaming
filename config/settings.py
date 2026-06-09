"""
config/settings.py — Configuración Centralizada del Proyecto

Carga las variables de entorno y expone la configuración
del pipeline, el simulador y los servicios de GCP.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# Cargar .env desde la raíz del proyecto
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class GCPConfig:
    """Configuración de Google Cloud Platform."""
    project_id: str = field(
        default_factory=lambda: os.getenv("GCP_PROJECT_ID", "solar-streaming-dev")
    )
    region: str = field(
        default_factory=lambda: os.getenv("GCP_REGION", "us-central1")
    )


@dataclass(frozen=True)
class PubSubConfig:
    """Configuración de Google Cloud Pub/Sub."""
    topic: str = field(
        default_factory=lambda: os.getenv("PUBSUB_TOPIC", "solar-telemetry-ingest")
    )
    subscription: str = field(
        default_factory=lambda: os.getenv("PUBSUB_SUBSCRIPTION", "solar-telemetry-sub")
    )
    dlq_topic: str = field(
        default_factory=lambda: os.getenv("PUBSUB_DLQ_TOPIC", "solar-telemetry-dlq")
    )
    emulator_host: str | None = field(
        default_factory=lambda: os.getenv("PUBSUB_EMULATOR_HOST")
    )

    @property
    def is_emulated(self) -> bool:
        """Indica si se está usando el emulador local."""
        return self.emulator_host is not None


@dataclass(frozen=True)
class BigQueryConfig:
    """Configuración de Google BigQuery."""
    dataset: str = field(
        default_factory=lambda: os.getenv("BQ_DATASET", "solar_streaming")
    )
    table_telemetry: str = field(
        default_factory=lambda: os.getenv("BQ_TABLE_TELEMETRY", "telemetry_validated")
    )
    table_dlq: str = field(
        default_factory=lambda: os.getenv("BQ_TABLE_DLQ", "dead_letter_queue")
    )


@dataclass(frozen=True)
class DataflowConfig:
    """Configuración del DataflowRunner de Apache Beam."""
    job_name: str = field(
        default_factory=lambda: os.getenv("DATAFLOW_JOB_NAME", "solar-streaming-pipeline")
    )
    temp_location: str = field(
        default_factory=lambda: os.getenv("DATAFLOW_TEMP_LOCATION", "")
    )
    staging_location: str = field(
        default_factory=lambda: os.getenv("DATAFLOW_STAGING_LOCATION", "")
    )
    max_workers: int = field(
        default_factory=lambda: int(os.getenv("DATAFLOW_MAX_WORKERS", "5"))
    )
    machine_type: str = field(
        default_factory=lambda: os.getenv("DATAFLOW_MACHINE_TYPE", "n1-standard-2")
    )


@dataclass(frozen=True)
class SimulatorConfig:
    """Configuración del simulador de telemetría IoT (Fase 1)."""
    rate_limit: int = field(
        default_factory=lambda: int(os.getenv("SIMULATOR_RATE_LIMIT", "100"))
    )
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("SIMULATOR_BATCH_SIZE", "50"))
    )
    delay_ms: int = field(
        default_factory=lambda: int(os.getenv("SIMULATOR_DELAY_MS", "200"))
    )


@dataclass(frozen=True)
class AppSettings:
    """Configuración raíz que agrupa todas las sub-configuraciones."""
    gcp: GCPConfig = field(default_factory=GCPConfig)
    pubsub: PubSubConfig = field(default_factory=PubSubConfig)
    bigquery: BigQueryConfig = field(default_factory=BigQueryConfig)
    dataflow: DataflowConfig = field(default_factory=DataflowConfig)
    simulator: SimulatorConfig = field(default_factory=SimulatorConfig)
    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO")
    )

    # Rutas del proyecto
    project_root: Path = _PROJECT_ROOT
    schemas_dir: Path = _PROJECT_ROOT / "config" / "schemas"


# Singleton de configuración
settings = AppSettings()
