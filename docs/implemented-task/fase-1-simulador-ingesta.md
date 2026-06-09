# Fase 1 — Simulador de Ingesta (Publisher Python a Pub/Sub)

## Objetivo

Construir un generador de telemetría IoT fotovoltaica que publique datos sintéticos realistas al servicio de mensajería **Google Cloud Pub/Sub**. Este simulador reemplaza temporalmente a los sensores físicos reales de las plantas solares, permitiendo desarrollar y probar el pipeline de datos sin necesidad de hardware ni infraestructura de producción.

---

## Servicio de Google Cloud utilizado: Pub/Sub

El servicio central de esta fase es **Google Cloud Pub/Sub**, la capa de mensajería serverless de GCP. Pub/Sub actúa como un bus de eventos desacoplado que recibe los mensajes del simulador y los retiene hasta que el pipeline de Apache Beam (en fases posteriores) los consuma.

En desarrollo local, no se requiere una cuenta de GCP activa: el proyecto utiliza el **emulador oficial de Pub/Sub** que corre como un contenedor Docker. La librería `google-cloud-pubsub` de Python detecta automáticamente la variable de entorno `PUBSUB_EMULATOR_HOST` y redirige todas las operaciones al emulador en lugar de a la API real de Google.

---

## ¿Qué se implementó?

### 1. Generador de datos sintéticos (`simulator/utils.py`)

Se desarrolló un motor de generación de telemetría que simula lecturas realistas de una planta fotovoltaica en el desierto de Atacama. No se trata de números aleatorios arbitrarios: los valores respetan relaciones físicas reales entre las variables.

**Curva solar diurna:** La irradiancia (LMD_GHI) sigue una función coseno que modela la campana gaussiana de producción solar, con pico al mediodía y valores cero durante la noche. Esto significa que si ejecutas el simulador a las 3 AM UTC-4, los datos reflejarán irradiancia nula y potencia cero, exactamente como ocurriría en una planta real.

**Relaciones físicas coherentes:**
- La potencia de salida es proporcional a la irradiancia, modulada por la eficiencia del panel (16%-22%) y un coeficiente de degradación térmica (0.4% de pérdida por cada grado por encima de 25°C).
- La temperatura del módulo siempre es mayor que la temperatura ambiente, incrementada por la radiación solar incidente.
- El voltaje y la corriente DC son coherentes con la potencia generada.
- Se incluye variabilidad por nubes (factor aleatorio del 85%-100%) para simular condiciones meteorológicas cambiantes.

**Inyección controlada de anomalías:** El simulador puede introducir deliberadamente lecturas anómalas (temperaturas excesivas, caídas bruscas de irradiancia, fallos de inversor) con una tasa configurable (por defecto 5%). Esto permite testear los algoritmos de detección de anomalías definidos en el dominio.

**Tres plantas simuladas:** Los datos se generan para tres plantas ficticias (`plant-atacama-001`, `plant-atacama-002`, `plant-sonora-001`), cada una con 8 sensores, para simular un escenario multi-planta realista.

### 2. Publisher con Patrón Factory (`simulator/publisher.py`)

El componente central de la Fase 1 es el script publicador, que implementa el **Patrón Creacional Factory** del Gang of Four (GoF).

**¿Cómo funciona el Factory?** Una función llamada `create_publisher()` decide en tiempo de ejecución qué tipo de publicador instanciar, según el entorno de ejecución:

- **`PubSubPublisher`** — Se conecta al servicio real de Google Cloud Pub/Sub (o al emulador local si la variable `PUBSUB_EMULATOR_HOST` está definida). Publica mensajes de forma asíncrona, gestiona un buffer de 100 futuros pendientes y los drena periódicamente para evitar backpressure.

- **`DryRunPublisher`** — Simplemente imprime los eventos en la terminal (stdout) sin publicar a ningún servicio. Útil para verificar rápidamente que la generación de datos funciona correctamente sin levantar ninguna infraestructura.

Ambos publicadores cumplen la misma interfaz (`PublisherPort`), lo que significa que el resto del código del simulador no sabe ni le importa si los datos van a Pub/Sub real, al emulador o a la pantalla.

### 3. Control de flujo (Rate Limiting)

Se implementó un **limitador de tasa tipo Token Bucket** en `simulator/utils.py`. Este mecanismo controla la velocidad de publicación de mensajes para no saturar el topic de Pub/Sub. La tasa es configurable a través de la variable de entorno `SIMULATOR_RATE_LIMIT` (por defecto 100 mensajes por segundo).

### 4. Validación contra el contrato JSON

Antes de publicar cada mensaje, el simulador valida el evento generado contra el contrato definido en `config/schemas/pubsub_schema.json`. Se verifica que todos los campos requeridos estén presentes y que los valores numéricos estén dentro de los rangos físicamente válidos. Los mensajes que no cumplan el contrato son descartados y contabilizados como errores.

### 5. Logging estructurado en JSON

Siguiendo el requisito del PRD que prohíbe los logs en texto plano, el simulador emite todos sus registros en formato JSON estructurado con los campos `severity`, `message`, `logger` y `timestamp`. Esto prepara el terreno para la integración con Cloud Logging y Looker Studio en fases posteriores.

### 6. Graceful Shutdown

El simulador maneja las señales `SIGTERM` y `SIGINT` para detenerse de forma ordenada: deja de generar eventos, espera a que los mensajes pendientes se confirmen (flush), cierra la conexión con Pub/Sub y reporta estadísticas finales (total de eventos publicados, tasa promedio, errores).

### 7. Modelos del dominio (`domain/models.py`)

Se definieron las entidades de datos como dataclasses inmutables (`frozen=True`):

- **`SensorReading`** — Representa una lectura individual de un sensor: planta, sensor, tipo, timestamp, irradiancia, potencia, temperaturas, voltaje y corriente DC.
- **`AggregatedMetrics`** — Resultado de la agregación por ventana temporal (se usará en la Fase 2).
- **`DLQRecord`** — Registro de la Dead Letter Queue con los cuatro campos obligatorios del PRD.

### 8. Estrategias de detección de anomalías (`domain/strategies.py`)

Se implementaron tres estrategias concretas del **Patrón de Comportamiento Strategy**:

- **`ThermalAnomalyStrategy`** — Detecta cuando la temperatura del módulo excede un umbral configurable (por defecto 85°C).
- **`IrradianceDropStrategy`** — Detecta caídas abruptas de irradiancia con potencia activa, lo que indicaría un sensor defectuoso.
- **`InverterFailureStrategy`** — Compara la potencia real contra la predicción del modelo ML y genera alerta si la desviación excede un porcentaje (por defecto 25%).

### 9. Tests unitarios

Se crearon tests para validar:
- La inmutabilidad de las dataclasses (no se pueden modificar después de crear).
- La generación automática de UUIDs en los `event_id`.
- Los valores de los enums `SensorType` y `AnomalyType`.
- El comportamiento de cada estrategia de anomalías con escenarios normales, de borde y anómalos.

---

## ¿Cómo se ejecuta?

```bash
# Opción 1: Con Docker Compose (emulador de Pub/Sub)
docker compose up pubsub-emulator simulator

# Opción 2: Modo dry-run (sin infraestructura, solo stdout)
python -m simulator.publisher --dry-run --max-events 10

# Opción 3: Con límite de eventos
python -m simulator.publisher --max-events 1000 --anomaly-rate 0.10
```

---

## Resultado

Al finalizar esta fase, el proyecto cuenta con un generador de datos completamente funcional que produce telemetría fotovoltaica realista y la inyecta al bus de mensajería de Pub/Sub. Este componente es independiente del pipeline ETL y servirá como fuente de datos para las fases siguientes, donde Apache Beam consumirá estos mensajes, aplicará ventanas temporales y ejecutará la detección de anomalías en tiempo real.
