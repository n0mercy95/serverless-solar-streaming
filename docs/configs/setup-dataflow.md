# ⚙️ Configuración de Google Cloud Dataflow

Guía paso a paso para configurar el motor de procesamiento distribuido que ejecuta el pipeline de Apache Beam en modo streaming.

---

## 1. Prerrequisitos

- Google Cloud SDK instalado y autenticado.
- Proyecto de GCP con facturación habilitada.
- APIs necesarias habilitadas.
- Pub/Sub y BigQuery ya configurados (ver guías correspondientes).

```bash
# Habilitar las APIs necesarias
gcloud services enable dataflow.googleapis.com
gcloud services enable compute.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
```

## 2. Crear un Bucket de GCS para Archivos Temporales

Dataflow necesita un bucket de Cloud Storage para archivos temporales (staging y temp).

```bash
# Crear el bucket (nombre debe ser globalmente único)
gsutil mb -l US -c STANDARD gs://TU_PROJECT_ID-dataflow-staging/

# Crear las carpetas de staging y temp
gsutil cp /dev/null gs://TU_PROJECT_ID-dataflow-staging/dataflow/temp/.keep
gsutil cp /dev/null gs://TU_PROJECT_ID-dataflow-staging/dataflow/staging/.keep
```

### Configurar lifecycle para limpieza automática

```bash
# Crear archivo de política
cat > /tmp/lifecycle.json << 'EOF'
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 7}
    }
  ]
}
EOF

# Aplicar al bucket
gsutil lifecycle set /tmp/lifecycle.json gs://TU_PROJECT_ID-dataflow-staging/
```

> **Nota:** Esto elimina automáticamente archivos temporales después de 7 días.

## 3. Crear la Cuenta de Servicio para Dataflow

Es una buena práctica usar una cuenta de servicio dedicada en lugar de la default.

```bash
# Crear la cuenta de servicio
gcloud iam service-accounts create solar-dataflow-sa \
    --display-name="Solar Streaming Dataflow SA" \
    --description="Cuenta de servicio para el pipeline de streaming fotovoltaico"

# Guardar el email para referencia
SA_EMAIL="solar-dataflow-sa@TU_PROJECT_ID.iam.gserviceaccount.com"
```

### Asignar roles IAM

```bash
# Rol de Worker de Dataflow
gcloud projects add-iam-policy-binding TU_PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/dataflow.worker"

# Leer/Escribir en GCS (staging y temp)
gcloud projects add-iam-policy-binding TU_PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.objectAdmin"

# Suscribirse a Pub/Sub
gcloud projects add-iam-policy-binding TU_PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/pubsub.subscriber"

# Escribir a BigQuery
gcloud projects add-iam-policy-binding TU_PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.dataEditor"

# Ejecutar jobs de BigQuery
gcloud projects add-iam-policy-binding TU_PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.jobUser"

# Logging
gcloud projects add-iam-policy-binding TU_PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/logging.logWriter"

# Monitoreo
gcloud projects add-iam-policy-binding TU_PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/monitoring.metricWriter"
```

### Resumen de roles

| Rol | Propósito |
|-----|-----------|
| `roles/dataflow.worker` | Ejecutar workers de Dataflow |
| `roles/storage.objectAdmin` | Leer/escribir archivos de staging |
| `roles/pubsub.subscriber` | Consumir mensajes del topic |
| `roles/bigquery.dataEditor` | Insertar filas en las tablas |
| `roles/bigquery.jobUser` | Ejecutar queries/jobs |
| `roles/logging.logWriter` | Emitir logs estructurados |
| `roles/monitoring.metricWriter` | Reportar métricas custom |

## 4. Configuración de Red (VPC)

Por defecto, Dataflow usa la red `default`. Para entornos de producción, configura una subred específica:

```bash
# Crear subred dedicada (opcional, para producción)
gcloud compute networks subnets create dataflow-subnet \
    --network=default \
    --region=us-central1 \
    --range=10.0.1.0/24 \
    --enable-private-ip-google-access
```

> **Importante:** `--enable-private-ip-google-access` permite que los workers accedan a APIs de Google sin IPs públicas (más seguro y más barato).

## 5. Parámetros del Pipeline (Apache Beam)

Estos son los parámetros que se pasan al ejecutar el pipeline con `DataflowRunner`:

```python
# En config/settings.py o como argumentos CLI
pipeline_options = {
    # Runner
    "--runner": "DataflowRunner",
    "--project": "TU_PROJECT_ID",
    "--region": "us-central1",

    # Staging
    "--temp_location": "gs://TU_PROJECT_ID-dataflow-staging/dataflow/temp",
    "--staging_location": "gs://TU_PROJECT_ID-dataflow-staging/dataflow/staging",

    # Workers
    "--max_num_workers": "5",
    "--machine_type": "n1-standard-2",
    "--disk_size_gb": "30",
    "--autoscaling_algorithm": "THROUGHPUT_BASED",

    # Streaming
    "--streaming": "true",
    "--enable_streaming_engine": "true",

    # Cuenta de servicio
    "--service_account_email": "solar-dataflow-sa@TU_PROJECT_ID.iam.gserviceaccount.com",

    # Red
    "--no_use_public_ips": "true",
    "--subnetwork": "regions/us-central1/subnetworks/dataflow-subnet",

    # Nombre del job
    "--job_name": "solar-streaming-pipeline",

    # Imagen Docker custom (para dependencias)
    "--sdk_container_image": "gcr.io/TU_PROJECT_ID/solar-pipeline:latest",
}
```

### Parámetros clave explicados

| Parámetro | Valor Recomendado | Justificación |
|-----------|-------------------|---------------|
| `max_num_workers` | `5` | Limita el costo durante desarrollo |
| `machine_type` | `n1-standard-2` | 2 vCPUs, 7.5 GB RAM — suficiente para el volumen |
| `enable_streaming_engine` | `true` | Ejecuta el shuffle en la infraestructura de Google (más eficiente) |
| `autoscaling_algorithm` | `THROUGHPUT_BASED` | Escala workers según el throughput real |
| `no_use_public_ips` | `true` | Ahorra costos y mejora la seguridad |

## 6. Desarrollo Local (DirectRunner)

Para desarrollo y testing, usa `DirectRunner` sin necesidad de infraestructura de GCP:

```bash
# Ejecutar el pipeline localmente
python main.py --runner DirectRunner
```

El `DirectRunner` ejecuta todo en un solo proceso. Es útil para:
- Depurar transformaciones
- Probar con datos de archivos locales (MockInput)
- Validar la lógica antes del despliegue

> **Limitaciones del DirectRunner:** No soporta auto-scaling, no escribe a BigQuery real, y el rendimiento no es representativo de producción.

## 7. Desplegar el Pipeline a Producción

### Opción A: Despliegue directo

```bash
python main.py \
    --runner DataflowRunner \
    --project TU_PROJECT_ID \
    --region us-central1 \
    --temp_location gs://TU_PROJECT_ID-dataflow-staging/dataflow/temp \
    --staging_location gs://TU_PROJECT_ID-dataflow-staging/dataflow/staging \
    --max_num_workers 5 \
    --streaming
```

### Opción B: Con imagen Docker custom

```bash
# Construir la imagen del pipeline
docker build -f Dockerfile.pipeline -t gcr.io/TU_PROJECT_ID/solar-pipeline:latest .

# Subir a Container Registry
docker push gcr.io/TU_PROJECT_ID/solar-pipeline:latest

# Ejecutar con la imagen custom
python main.py \
    --runner DataflowRunner \
    --sdk_container_image gcr.io/TU_PROJECT_ID/solar-pipeline:latest \
    --project TU_PROJECT_ID \
    --streaming
```

## 8. Monitoreo y Alertas

### Métricas clave a monitorear

| Métrica | Umbral de Alerta | Significado |
|---------|-------------------|-------------|
| **System Lag** | > 5 minutos | El pipeline está atrasado respecto a los datos |
| **Data Watermark** | > 10 minutos atrás | Los datos están llegando tarde |
| **Backlog (unacked messages)** | > 10,000 | Acumulación de mensajes sin procesar |
| **Worker CPU** | > 80% sostenido | Necesita más workers |
| **Elements Added (DLQ)** | > 100/min | Demasiados errores, investigar |

### Configurar alertas en Cloud Monitoring

```bash
# Alerta cuando el system lag supera los 5 minutos
gcloud monitoring policies create \
    --display-name="Dataflow High System Lag" \
    --condition-display-name="System Lag > 5min" \
    --condition-filter='resource.type="dataflow_job" AND metric.type="dataflow.googleapis.com/job/system_lag"' \
    --condition-threshold-value=300 \
    --condition-threshold-comparison=COMPARISON_GT \
    --combiner=OR \
    --notification-channels=TU_CHANNEL_ID
```

### Dashboards en Looker Studio

La consola de Dataflow en GCP proporciona un dashboard nativo con:
- Gráfico de throughput (elementos/segundo)
- Estado de los workers
- Latencia por step
- Watermark actual

Para dashboards custom, conecta BigQuery como fuente de datos en Looker Studio.

## 9. Actualización del Pipeline (In-place Update)

Para actualizar un pipeline en ejecución sin perder estado:

```bash
python main.py \
    --runner DataflowRunner \
    --project TU_PROJECT_ID \
    --update \
    --job_name solar-streaming-pipeline \
    --region us-central1 \
    --streaming
```

> **Importante:** Solo funciona si la topología del pipeline (nombres de steps) no cambia. Para cambios estructurales, usa `--transform_name_mapping`.

## 10. Detener el Pipeline

```bash
# Drain: procesa mensajes pendientes y luego se detiene
gcloud dataflow jobs drain JOB_ID \
    --project=TU_PROJECT_ID \
    --region=us-central1

# Cancel: detención inmediata (posible pérdida de datos en tránsito)
gcloud dataflow jobs cancel JOB_ID \
    --project=TU_PROJECT_ID \
    --region=us-central1
```

O usa el script de teardown:

```bash
./teardown.sh --gcp
```

---

## Resumen de Recursos Creados

| Recurso | Nombre | Propósito |
|---------|--------|-----------|
| Bucket GCS | `TU_PROJECT_ID-dataflow-staging` | Archivos temporales de staging |
| Service Account | `solar-dataflow-sa` | Identidad del pipeline |
| Subred (opcional) | `dataflow-subnet` | Red privada para workers |
| Job de Dataflow | `solar-streaming-pipeline` | Pipeline en ejecución |
