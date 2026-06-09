# Fase 0 — Configuración Base del Proyecto

## Objetivo

Establecer los cimientos técnicos del proyecto antes de escribir una sola línea de lógica de negocio. Esta fase cubre todo lo necesario para que cualquier desarrollador del equipo pueda clonar el repositorio y levantar un entorno de desarrollo funcional en minutos, sin depender de una cuenta activa de Google Cloud Platform.

---

## ¿Qué se hizo?

### 1. Estructura de Carpetas (Arquitectura Hexagonal)

Se creó la organización completa de directorios siguiendo el patrón de **Arquitectura Hexagonal (Puertos y Adaptadores)**, tal como lo exige el PRD. La idea central es que el código de dominio (la lógica científica fotovoltaica) viva completamente aislado de la infraestructura de Google Cloud.

Se definieron cuatro capas claramente separadas:

- **`domain/`** — El centro del hexágono. Aquí vive Python puro: las entidades de datos (`models.py`), las interfaces abstractas (`ports.py`) y los algoritmos de detección de anomalías (`strategies.py`). Este código no importa ni Apache Beam ni ninguna librería de GCP.

- **`application/`** — La capa de casos de uso. Aquí vivirá el pipeline de Apache Beam y las transformaciones personalizadas. Depende de Beam pero no de servicios concretos como Pub/Sub o BigQuery.

- **`adapters/`** — La capa de infraestructura. Conecta el sistema con el mundo exterior a través de adaptadores de entrada (lectura de Pub/Sub, lectura de archivos locales) y adaptadores de salida (escritura a BigQuery, manejo de la Dead Letter Queue).

- **`config/`** — Configuraciones centralizadas, variables de entorno y los contratos JSON versionados que definen la forma exacta de los datos de entrada y salida.

### 2. Contenerización con Docker

Se crearon tres archivos de infraestructura Docker:

- **`Dockerfile.simulator`** — Imagen dedicada exclusivamente para el simulador de telemetría. Usa un build multi-etapa para producir una imagen ligera que solo incluye las capas `domain/`, `config/` y `simulator/`. Corre con un usuario no-root por seguridad.

- **`Dockerfile.pipeline`** — Imagen para el pipeline completo de Apache Beam. Incluye todas las capas de la arquitectura hexagonal ya que el pipeline necesita acceso a dominio, aplicación y adaptadores.

- **`docker-compose.yml`** — Orquesta tres servicios para desarrollo local:
  - Un **emulador de Google Cloud Pub/Sub** (usando la imagen oficial de Google), que permite trabajar sin credenciales reales de GCP.
  - El **simulador de telemetría**, que se conecta al emulador y publica mensajes.
  - El **pipeline de Beam** (bajo un perfil opcional llamado `full`), que consume del emulador usando DirectRunner.

### 3. Gestión de Dependencias

Se separaron las dependencias en dos archivos:

- **`requirements.txt`** — Dependencias completas del pipeline: Apache Beam con extensiones GCP, Pydantic, clientes de Pub/Sub y BigQuery, logging estructurado y herramientas de testing.

- **`requirements-simulator.txt`** — Subconjunto ligero solo con lo necesario para el simulador: cliente de Pub/Sub, Pydantic, PyArrow (para leer archivos Parquet del dataset PVOD) y utilidades de fecha.

### 4. Configuración de Entorno

- **`.env.example`** — Archivo plantilla con todas las variables de entorno que necesita el proyecto: credenciales de GCP, nombres de topics y suscripciones de Pub/Sub, configuración de BigQuery y Dataflow, parámetros del simulador y nivel de logging.

- **`config/settings.py`** — Módulo Python que carga las variables del `.env` y las expone como dataclasses inmutables (`frozen=True`), organizadas por servicio: `GCPConfig`, `PubSubConfig`, `BigQueryConfig`, `DataflowConfig` y `SimulatorConfig`.

### 5. Contratos JSON Estrictos

Se versionaron los esquemas de datos como archivos JSON estáticos dentro del repositorio:

- **`config/schemas/pubsub_schema.json`** — Contrato de entrada que define exactamente qué campos debe tener cada mensaje de telemetría, con validaciones de tipo, rangos permitidos (por ejemplo, GHI entre 0 y 1500 W/m², potencia entre 0 y 100 MW) y la prohibición de campos adicionales.

- **`config/schemas/bq_schema.json`** — Esquema de las tablas de destino en BigQuery: la tabla `telemetry_validated` (particionada por día en `window_start` y clusterizada por `plant_id` y `anomaly_type`) y la tabla `dead_letter_queue` con los cuatro campos obligatorios del PRD.

### 6. Control de Versiones

- **`.gitignore`** — Configurado para excluir credenciales, caches de Python, archivos de datos, artefactos de Beam y archivos de IDE. Incluye una excepción explícita para los archivos JSON dentro de `config/schemas/` ya que esos contratos sí deben versionarse.

### 7. Scripts Operacionales

- **`teardown.sh`** — Script de "kill switch" que detiene los contenedores Docker locales y, opcionalmente (con la flag `--gcp`), cancela jobs activos de Dataflow y elimina topics y suscripciones de Pub/Sub en GCP. Incluye confirmación interactiva para evitar borrados accidentales.

- **`pytest.ini`** — Configuración de pytest con markers para separar tests unitarios de tests de integración.

### 8. Documentación de Servicios GCP

Se crearon tres guías paso a paso en `docs/configs/` que explican cómo configurar desde cero cada servicio de Google Cloud que usa el proyecto:

- **Pub/Sub** — Creación de topics, suscripciones, Dead Letter Queue, permisos IAM y uso del emulador local.
- **BigQuery** — Creación del dataset, tablas con particionamiento y clustering, queries de validación.
- **Dataflow** — Bucket de staging, cuenta de servicio dedicada, parámetros del pipeline, monitoreo y alertas.

---

## ¿Por qué una "Fase 0"?

Aunque el PRD define cuatro fases comenzando por el simulador, en la práctica no se puede construir nada sin antes tener:

1. Un entorno de desarrollo reproducible (Docker).
2. Una estructura de código que refleje la arquitectura objetivo.
3. Contratos de datos que definan la forma de los mensajes.
4. Documentación que permita a otros miembros del equipo configurar los servicios.

Esta fase cero es el andamiaje sobre el cual se construyen las cuatro fases del PRD.
