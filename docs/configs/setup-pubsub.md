# 📨 Configuración de Google Cloud Pub/Sub

Guía paso a paso para configurar el servicio de mensajería que actúa como bus principal de ingesta de telemetría IoT.

---

## 1. Prerrequisitos

- Una cuenta de Google Cloud con facturación habilitada.
- [Google Cloud SDK (gcloud)](https://cloud.google.com/sdk/docs/install) instalado y autenticado.
- Un proyecto de GCP creado.

```bash
# Autenticarse
gcloud auth login

# Establecer el proyecto activo
gcloud config set project TU_PROJECT_ID
```

## 2. Habilitar la API de Pub/Sub

```bash
gcloud services enable pubsub.googleapis.com
```

Verifica que la API esté habilitada:

```bash
gcloud services list --enabled --filter="name:pubsub"
```

## 3. Crear el Topic Principal (Ingesta de Telemetría)

El topic `solar-telemetry-ingest` es el punto de entrada para toda la telemetría IoT.

```bash
# Crear el topic principal
gcloud pubsub topics create solar-telemetry-ingest \
    --project=TU_PROJECT_ID \
    --message-retention-duration=7d

# Verificar creación
gcloud pubsub topics list --project=TU_PROJECT_ID
```

### Configuraciones recomendadas

| Parámetro | Valor | Justificación |
|-----------|-------|---------------|
| `message-retention-duration` | `7d` | Permite reprocesar mensajes de la última semana |
| `message-encoding` | `JSON` | Formato estándar del contrato `pubsub_schema.json` |

## 4. Crear la Suscripción (Para el Pipeline de Beam)

La suscripción `solar-telemetry-sub` será consumida por Apache Beam/Dataflow.

```bash
gcloud pubsub subscriptions create solar-telemetry-sub \
    --topic=solar-telemetry-ingest \
    --project=TU_PROJECT_ID \
    --ack-deadline=60 \
    --message-retention-duration=7d \
    --expiration-period=never \
    --enable-exactly-once-delivery
```

### Parámetros clave

| Parámetro | Valor | Justificación |
|-----------|-------|---------------|
| `ack-deadline` | `60s` | Tiempo para que Beam confirme el procesamiento |
| `message-retention-duration` | `7d` | Consistente con el topic |
| `expiration-period` | `never` | La suscripción nunca expira automáticamente |
| `enable-exactly-once-delivery` | `true` | Garantía de entrega exactly-once (reduce duplicados) |

## 5. Crear el Topic de Dead Letter Queue (DLQ)

Los mensajes que fallen repetidamente se redirigen aquí.

```bash
# Topic DLQ
gcloud pubsub topics create solar-telemetry-dlq \
    --project=TU_PROJECT_ID

# Suscripción DLQ (para inspección manual)
gcloud pubsub subscriptions create solar-telemetry-dlq-sub \
    --topic=solar-telemetry-dlq \
    --project=TU_PROJECT_ID \
    --ack-deadline=120 \
    --message-retention-duration=14d
```

## 6. Configurar la Política de Dead Letter en la Suscripción Principal

```bash
gcloud pubsub subscriptions update solar-telemetry-sub \
    --project=TU_PROJECT_ID \
    --dead-letter-topic=projects/TU_PROJECT_ID/topics/solar-telemetry-dlq \
    --max-delivery-attempts=5
```

> **Nota:** Después de 5 intentos fallidos de entrega, el mensaje se redirige automáticamente al topic DLQ.

## 7. Permisos IAM Necesarios

La cuenta de servicio que ejecuta el pipeline necesita estos roles:

```bash
# Para el Publisher (simulador)
gcloud projects add-iam-policy-binding TU_PROJECT_ID \
    --member="serviceAccount:TU_SA@TU_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher"

# Para el Consumer (pipeline de Beam)
gcloud projects add-iam-policy-binding TU_PROJECT_ID \
    --member="serviceAccount:TU_SA@TU_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/pubsub.subscriber"
```

| Rol | Quién lo necesita | Permisos |
|-----|-------------------|----------|
| `roles/pubsub.publisher` | Simulador | Publicar mensajes al topic |
| `roles/pubsub.subscriber` | Pipeline (Beam) | Leer y confirmar mensajes |
| `roles/pubsub.editor` | DLQ handler | Crear/administrar topics DLQ |

## 8. Desarrollo Local con el Emulador

Para desarrollar sin una cuenta GCP activa, usamos el emulador oficial:

```bash
# Opción A: Con Docker Compose (recomendado)
docker compose up pubsub-emulator -d

# Opción B: Instalación local
gcloud components install pubsub-emulator
gcloud beta emulators pubsub start --project=solar-streaming-dev
```

### Configurar la variable de entorno

```bash
# En tu .env o terminal
export PUBSUB_EMULATOR_HOST=localhost:8085
```

> **Importante:** Cuando `PUBSUB_EMULATOR_HOST` está definida, la librería `google-cloud-pubsub` automáticamente se conecta al emulador en lugar de la API de GCP. No se necesitan credenciales.

## 9. Validar la Configuración

```bash
# Publicar un mensaje de prueba
gcloud pubsub topics publish solar-telemetry-ingest \
    --project=TU_PROJECT_ID \
    --message='{"event_id":"test-001","plant_id":"plant-test","sensor_id":"sensor-001","sensor_type":"inverter","timestamp":"2025-06-15T12:00:00Z","lmd_ghi":850.0,"power_mw":3.2,"module_temp_c":45.0,"ambient_temp_c":30.0,"dc_voltage":600.0,"dc_current":8.5}'

# Leer el mensaje de la suscripción
gcloud pubsub subscriptions pull solar-telemetry-sub \
    --project=TU_PROJECT_ID \
    --auto-ack \
    --limit=1
```

## 10. Monitoreo

En la consola de GCP, navega a **Pub/Sub > Topics** para ver:

- **Tasa de publicación** (messages/sec)
- **Edad del mensaje más antiguo no confirmado** (indica backlog)
- **Tasa de confirmación** del subscriber

Configura alertas en Cloud Monitoring si la edad del mensaje supera los 5 minutos.

---

## Resumen de Recursos Creados

| Recurso | Nombre | Propósito |
|---------|--------|-----------|
| Topic | `solar-telemetry-ingest` | Ingesta principal de telemetría |
| Suscripción | `solar-telemetry-sub` | Consumida por el pipeline de Beam |
| Topic DLQ | `solar-telemetry-dlq` | Mensajes fallidos |
| Suscripción DLQ | `solar-telemetry-dlq-sub` | Inspección manual de errores |
