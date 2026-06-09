# 📊 Configuración de Google BigQuery

Guía paso a paso para configurar el data warehouse columnar que sirve como sumidero definitivo para la telemetría validada y los eventos de error (DLQ).

---

## 1. Prerrequisitos

- Google Cloud SDK instalado y autenticado.
- Proyecto de GCP con facturación habilitada.
- API de BigQuery habilitada.

```bash
gcloud services enable bigquery.googleapis.com
```

## 2. Crear el Dataset

El dataset `solar_streaming` agrupa todas las tablas del pipeline.

```bash
bq mk \
    --dataset \
    --location=US \
    --default_table_expiration=0 \
    --description="Dataset para telemetría fotovoltaica en tiempo real" \
    TU_PROJECT_ID:solar_streaming
```

### Parámetros recomendados

| Parámetro | Valor | Justificación |
|-----------|-------|---------------|
| `location` | `US` (o tu región) | Debe coincidir con la región de Dataflow |
| `default_table_expiration` | `0` | Las tablas no expiran (datos permanentes) |

## 3. Crear la Tabla de Telemetría Validada

Esta tabla almacena las métricas agregadas por ventanas temporales de 15 minutos.

```bash
bq mk \
    --table \
    --time_partitioning_type=DAY \
    --time_partitioning_field=window_start \
    --clustering_fields=plant_id,anomaly_type \
    --description="Métricas de telemetría fotovoltaica agregadas por ventana temporal" \
    TU_PROJECT_ID:solar_streaming.telemetry_validated \
    event_id:STRING,plant_id:STRING,window_start:TIMESTAMP,window_end:TIMESTAMP,avg_ghi:FLOAT64,avg_power_mw:FLOAT64,max_power_mw:FLOAT64,min_power_mw:FLOAT64,avg_module_temp_c:FLOAT64,reading_count:INT64,predicted_power_mw:FLOAT64,anomaly_type:STRING,anomaly_score:FLOAT64,ingestion_timestamp:TIMESTAMP
```

### Esquema de campos

| Campo | Tipo | Modo | Descripción |
|-------|------|------|-------------|
| `event_id` | STRING | REQUIRED | UUID del evento agregado |
| `plant_id` | STRING | REQUIRED | Identificador de la planta |
| `window_start` | TIMESTAMP | REQUIRED | Inicio de la ventana temporal |
| `window_end` | TIMESTAMP | REQUIRED | Fin de la ventana temporal |
| `avg_ghi` | FLOAT64 | REQUIRED | Promedio de irradiancia GHI (W/m²) |
| `avg_power_mw` | FLOAT64 | REQUIRED | Potencia promedio (MW) |
| `max_power_mw` | FLOAT64 | REQUIRED | Potencia máxima (MW) |
| `min_power_mw` | FLOAT64 | REQUIRED | Potencia mínima (MW) |
| `avg_module_temp_c` | FLOAT64 | REQUIRED | Temperatura promedio del módulo (°C) |
| `reading_count` | INT64 | REQUIRED | Lecturas en la ventana |
| `predicted_power_mw` | FLOAT64 | NULLABLE | Predicción del modelo ML (MW) |
| `anomaly_type` | STRING | REQUIRED | Tipo de anomalía detectada |
| `anomaly_score` | FLOAT64 | REQUIRED | Confianza de la anomalía (0-1) |
| `ingestion_timestamp` | TIMESTAMP | REQUIRED | Timestamp de escritura a BQ |

### Optimizaciones de rendimiento

- **Particionamiento por día** en `window_start`: reduce los costos de escaneo al filtrar por fecha.
- **Clustering** por `plant_id` y `anomaly_type`: acelera las consultas filtrando por planta o tipo de anomalía.

## 4. Crear la Tabla de Dead Letter Queue (DLQ)

Almacena los mensajes que fallaron en cualquier etapa del pipeline.

```bash
bq mk \
    --table \
    --time_partitioning_type=DAY \
    --time_partitioning_field=failure_timestamp \
    --description="Dead Letter Queue — registros fallidos del pipeline de streaming" \
    TU_PROJECT_ID:solar_streaming.dead_letter_queue \
    event_id:STRING,original_payload:STRING,error_message:STRING,failure_timestamp:TIMESTAMP,failed_step:STRING
```

### Esquema de campos (DLQ)

| Campo | Tipo | Modo | Descripción |
|-------|------|------|-------------|
| `event_id` | STRING | REQUIRED | UUID del registro DLQ |
| `original_payload` | STRING | REQUIRED | JSON original tal como llegó |
| `error_message` | STRING | REQUIRED | Descripción del error |
| `failure_timestamp` | TIMESTAMP | REQUIRED | Momento de la falla |
| `failed_step` | STRING | REQUIRED | Nombre del DoFn que falló |

## 5. Permisos IAM Necesarios

```bash
# Rol para que el pipeline escriba a BigQuery
gcloud projects add-iam-policy-binding TU_PROJECT_ID \
    --member="serviceAccount:TU_SA@TU_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

# Rol para que el pipeline lea esquemas de tablas
gcloud projects add-iam-policy-binding TU_PROJECT_ID \
    --member="serviceAccount:TU_SA@TU_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"
```

| Rol | Quién lo necesita | Permisos |
|-----|-------------------|----------|
| `roles/bigquery.dataEditor` | Pipeline (Beam) | Insertar filas en las tablas |
| `roles/bigquery.jobUser` | Pipeline (Beam) | Ejecutar jobs de carga |
| `roles/bigquery.dataViewer` | Analistas / Looker | Leer datos para dashboards |

## 6. Consultas Útiles de Validación

### Verificar que llegan datos

```sql
SELECT
    plant_id,
    COUNT(*) AS total_records,
    MIN(window_start) AS first_window,
    MAX(window_start) AS last_window,
    AVG(avg_power_mw) AS avg_power
FROM `TU_PROJECT_ID.solar_streaming.telemetry_validated`
WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY plant_id
ORDER BY total_records DESC;
```

### Consultar anomalías detectadas

```sql
SELECT
    anomaly_type,
    COUNT(*) AS total,
    AVG(anomaly_score) AS avg_score,
    MAX(anomaly_score) AS max_score
FROM `TU_PROJECT_ID.solar_streaming.telemetry_validated`
WHERE anomaly_type != 'none'
  AND window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY anomaly_type
ORDER BY total DESC;
```

### Revisar la DLQ

```sql
SELECT
    failed_step,
    COUNT(*) AS errors,
    MIN(failure_timestamp) AS first_error,
    MAX(failure_timestamp) AS last_error,
    ARRAY_AGG(error_message LIMIT 3) AS sample_errors
FROM `TU_PROJECT_ID.solar_streaming.dead_letter_queue`
WHERE failure_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY failed_step
ORDER BY errors DESC;
```

## 7. Configuración de Expiración de Particiones (Opcional)

Para controlar costos, puedes configurar la expiración automática de particiones antiguas:

```bash
# Las particiones de más de 90 días se eliminan automáticamente
bq update \
    --time_partitioning_expiration=7776000 \
    TU_PROJECT_ID:solar_streaming.telemetry_validated

# La DLQ retiene solo 30 días
bq update \
    --time_partitioning_expiration=2592000 \
    TU_PROJECT_ID:solar_streaming.dead_letter_queue
```

## 8. Streaming Buffer

Cuando Apache Beam escribe en modo streaming a BigQuery, los datos pasan primero por un *streaming buffer* antes de ser consolidados en almacenamiento columnar. Ten en cuenta:

- Los datos en el buffer **no son particionados ni clusterizados** inmediatamente.
- El buffer se vacía automáticamente (típicamente en minutos).
- Las consultas sobre datos muy recientes pueden ser más lentas.

---

## Resumen de Recursos Creados

| Recurso | Nombre | Propósito |
|---------|--------|-----------|
| Dataset | `solar_streaming` | Contenedor de todas las tablas |
| Tabla | `telemetry_validated` | Métricas agregadas por ventana temporal |
| Tabla | `dead_letter_queue` | Registros que fallaron en el pipeline |
