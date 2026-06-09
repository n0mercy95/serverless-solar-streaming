#!/usr/bin/env bash
# ============================================================
# teardown.sh — "Kill Switch" para apagar la infraestructura
# ============================================================
# Detiene el pipeline de Dataflow, elimina suscripciones y topics
# de Pub/Sub, y limpia los recursos temporales.
#
# Uso:
#   ./teardown.sh                    # Solo local (docker-compose)
#   ./teardown.sh --gcp              # Local + recursos de GCP
#   ./teardown.sh --gcp --confirm    # Sin confirmación interactiva
# ============================================================

set -euo pipefail

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Cargar variables de entorno
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

PROJECT_ID="${GCP_PROJECT_ID:-solar-streaming-dev}"
REGION="${GCP_REGION:-us-central1}"
TOPIC="${PUBSUB_TOPIC:-solar-telemetry-ingest}"
SUBSCRIPTION="${PUBSUB_SUBSCRIPTION:-solar-telemetry-sub}"
DLQ_TOPIC="${PUBSUB_DLQ_TOPIC:-solar-telemetry-dlq}"
JOB_NAME="${DATAFLOW_JOB_NAME:-solar-streaming-pipeline}"

GCP_MODE=false
AUTO_CONFIRM=false

for arg in "$@"; do
    case $arg in
        --gcp) GCP_MODE=true ;;
        --confirm) AUTO_CONFIRM=true ;;
        *) echo -e "${RED}Argumento desconocido: $arg${NC}"; exit 1 ;;
    esac
done

confirm() {
    if [ "$AUTO_CONFIRM" = true ]; then
        return 0
    fi
    echo -e "${YELLOW}¿Continuar? (y/N)${NC}"
    read -r response
    case "$response" in
        [yY][eE][sS]|[yY]) return 0 ;;
        *) return 1 ;;
    esac
}

echo -e "${YELLOW}============================================${NC}"
echo -e "${YELLOW}  TEARDOWN — Solar Streaming Pipeline       ${NC}"
echo -e "${YELLOW}============================================${NC}"
echo ""

# ---- 1. Detener Docker Compose ----
echo -e "${GREEN}[1/4] Deteniendo contenedores Docker...${NC}"
if command -v docker &> /dev/null; then
    docker compose down --remove-orphans 2>/dev/null || true
    docker compose --profile full down --remove-orphans 2>/dev/null || true
    echo "  ✓ Contenedores detenidos."
else
    echo "  ⊘ Docker no encontrado, saltando."
fi

# ---- 2. Limpiar volúmenes y redes ----
echo -e "${GREEN}[2/4] Limpiando volúmenes y redes Docker...${NC}"
if command -v docker &> /dev/null; then
    docker network rm solar-streaming-net 2>/dev/null || true
    echo "  ✓ Red solar-streaming-net eliminada."
else
    echo "  ⊘ Docker no encontrado, saltando."
fi

if [ "$GCP_MODE" = true ]; then
    echo ""
    echo -e "${RED}⚠  Modo GCP activado — Se eliminarán recursos en el proyecto: ${PROJECT_ID}${NC}"
    if ! confirm; then
        echo "Cancelado."
        exit 0
    fi

    # ---- 3. Cancelar jobs de Dataflow ----
    echo -e "${GREEN}[3/4] Cancelando jobs de Dataflow...${NC}"
    ACTIVE_JOBS=$(gcloud dataflow jobs list \
        --project="$PROJECT_ID" \
        --region="$REGION" \
        --filter="name=$JOB_NAME AND state=Running" \
        --format="value(id)" 2>/dev/null || true)

    if [ -n "$ACTIVE_JOBS" ]; then
        for job_id in $ACTIVE_JOBS; do
            echo "  Cancelando job: $job_id"
            gcloud dataflow jobs cancel "$job_id" \
                --project="$PROJECT_ID" \
                --region="$REGION" 2>/dev/null || true
        done
        echo "  ✓ Jobs cancelados."
    else
        echo "  ⊘ No hay jobs activos."
    fi

    # ---- 4. Eliminar recursos de Pub/Sub ----
    echo -e "${GREEN}[4/4] Eliminando recursos de Pub/Sub...${NC}"

    # Suscripciones
    gcloud pubsub subscriptions delete "$SUBSCRIPTION" \
        --project="$PROJECT_ID" --quiet 2>/dev/null || true
    echo "  ✓ Suscripción '$SUBSCRIPTION' eliminada."

    # Topics
    for topic in "$TOPIC" "$DLQ_TOPIC"; do
        gcloud pubsub topics delete "$topic" \
            --project="$PROJECT_ID" --quiet 2>/dev/null || true
        echo "  ✓ Topic '$topic' eliminado."
    done

else
    echo -e "${GREEN}[3/4] Saltando limpieza GCP (usa --gcp para activar)${NC}"
    echo -e "${GREEN}[4/4] Saltando limpieza GCP (usa --gcp para activar)${NC}"
fi

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  ✓ Teardown completado.                    ${NC}"
echo -e "${GREEN}============================================${NC}"
