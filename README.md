# ☀️ Serverless Solar Streaming Pipeline

Pipeline de procesamiento de telemetría IoT en tiempo real para plantas fotovoltaicas, construido sobre Google Cloud Platform con arquitectura hexagonal.

## 📋 Resumen

Este sistema ingesta datos masivos de sensores fotovoltaicos y procesa ventanas temporales en Google Cloud Dataflow para comparar continuamente los datos en vivo (LMD_GHI, Power MW) contra las predicciones del modelo Transformer Bi-LSTM, detectando fallas de hardware y anomalías operativas de manera proactiva.

## 🏗️ Arquitectura

```
Sensores IoT → [Pub/Sub] → [Dataflow / Apache Beam] → [BigQuery]
                               ↓
                        Modelo Bi-LSTM
                        (Inferencia)
                               ↓
                      Detección de Anomalías
                      (Patrón Strategy)
```

### Estructura de Carpetas (Arquitectura Hexagonal)

```
serverless-solar-streaming/
├── domain/                  # CORE: Centro del hexágono (Python puro)
│   ├── models.py            # Dataclasses (SensorReading, AggregatedMetrics)
│   ├── ports.py             # Interfaces abstractas (ABC)
│   └── strategies.py        # Patrón Strategy (anomalías)
│
├── application/             # CASOS DE USO: Orquestación
│   ├── pipeline.py          # DAG de Apache Beam
│   └── transforms/          # PTransforms y DoFns
│       ├── windowing.py     # Sliding/Tumbling windows
│       └── inference.py     # Modelo ML en caliente
│
├── adapters/                # INFRAESTRUCTURA: GCP
│   ├── input/               # Adaptadores "Driving"
│   │   ├── pubsub_input.py  # Lector de Pub/Sub
│   │   └── mock_input.py    # Lector local (testing)
│   └── output/              # Adaptadores "Driven"
│       ├── bigquery_sink.py # Escritor a BigQuery
│       └── dlq_handler.py   # Dead Letter Queue
│
├── config/                  # Configuraciones y esquemas
│   ├── schemas/             # Contratos JSON versionados
│   └── settings.py          # Variables de entorno
│
├── simulator/               # FASE 1: Inyector de datos
│   ├── publisher.py         # Publisher a Pub/Sub
│   └── utils.py             # Generación de datos y rate limiting
│
├── tests/                   # Pruebas unitarias e integración
├── main.py                  # Entrypoint (inyección de dependencias)
├── teardown.sh              # Kill switch
└── docker-compose.yml       # Entorno de desarrollo local
```

## 🚀 Inicio Rápido

### Prerrequisitos

- Docker y Docker Compose
- Python 3.11+
- Google Cloud SDK (para despliegue en GCP)

### 1. Clonar y configurar

```bash
git clone <repo-url>
cd serverless-solar-streaming
cp .env.example .env
# Editar .env con tus valores
```

### 2. Levantar el entorno local

```bash
# Inicia el emulador de Pub/Sub
docker compose up pubsub-emulator -d

# Ejecutar el simulador (Fase 1)
docker compose up simulator
```

### 3. Modo dry-run (sin Docker)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-simulator.txt

# Solo imprime eventos en stdout
python -m simulator.publisher --dry-run --max-events 10
```

### 4. Ejecutar tests

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=domain
```

## 📐 Patrones de Diseño

| Patrón | Tipo | Ubicación | Propósito |
|--------|------|-----------|-----------|
| **Factory** | Creacional | `simulator/publisher.py` | Instancia el publisher correcto según entorno |
| **Adapter** | Estructural | `adapters/` | Desacopla infraestructura GCP del dominio |
| **Strategy** | Comportamiento | `domain/strategies.py` | Algoritmos intercambiables de detección de anomalías |

## 📖 Documentación de Servicios GCP

Consultar la carpeta `docs/` para guías detalladas de configuración:

- [`setup-pubsub.md`](docs/configs/setup-pubsub.md) — Configuración de Google Cloud Pub/Sub
- [`setup-bigquery.md`](docs/configs/setup-bigquery.md) — Configuración de Google BigQuery
- [`setup-dataflow.md`](docs/configs/setup-dataflow.md) — Configuración de Google Cloud Dataflow

## 🔄 Plan de Ejecución (Fases)

| Fase | Descripción | Estado |
|------|-------------|--------|
| **1** | Simulador de Ingesta (Publisher → Pub/Sub) | ✅ Implementado |
| **2** | Core de Apache Beam y Enventanamiento | ⬜ Pendiente |
| **3** | Reglas de Dominio y Detección de Anomalías | ⬜ Pendiente |
| **4** | Despliegue, Adaptadores de Salida y DLQ | ⬜ Pendiente |

## 🛑 Teardown

```bash
# Solo local (Docker)
./teardown.sh

# Local + recursos GCP
./teardown.sh --gcp

# Sin confirmación interactiva
./teardown.sh --gcp --confirm
```
