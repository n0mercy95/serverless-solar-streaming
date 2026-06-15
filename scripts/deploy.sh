#!/bin/bash
# scripts/deploy.sh — Despliegue del Pipeline a Google Cloud Dataflow

set -e

# Asegurar que el entorno virtual esté activo
if [ -z "$VIRTUAL_ENV" ]; then
    echo "⚠️  El entorno virtual no está activo. Activando .venv..."
    source .venv/bin/activate
fi

# Variables de configuración (pueden ser sobreescritas por variables de entorno)
PROJECT_ID=${GCP_PROJECT_ID:-serverless-solar-streaming}
REGION=${GCP_REGION:-us-central1}
STAGING_LOCATION=${GCP_STAGING_LOCATION:-gs://serverless-solar-streaming-dataflow-staging/staging}
TEMP_LOCATION=${GCP_TEMP_LOCATION:-gs://serverless-solar-streaming-dataflow-staging/temp}
SERVICE_ACCOUNT=${GCP_SA_EMAIL:-solar-streaming-dataflow-sa@serverless-solar-streaming.iam.gserviceaccount.com}
JOB_NAME="solar-streaming-pipeline-$(date +%Y%m%d-%H%M%S)"

echo "🚀 Iniciando despliegue hacia Google Cloud Dataflow..."
echo "Proyecto: $PROJECT_ID"
echo "Región: $REGION"
echo "Job Name: $JOB_NAME"

python main.py \
    --runner=DataflowRunner \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --job_name="$JOB_NAME" \
    --staging_location="$STAGING_LOCATION" \
    --temp_location="$TEMP_LOCATION" \
    --service_account_email="$SERVICE_ACCOUNT" \
    --streaming \
    --setup_file=./setup.py \
    --save_main_session

echo "✅ Despliegue enviado a Dataflow. Revisa la consola de GCP para monitorear el Job."
