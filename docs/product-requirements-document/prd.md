PRODUCT REQUIREMENTS DOCUMENT (PRD)

Proyecto: Pipeline de Streaming y Telemetría en Tiempo Real para Plantas Fotovoltaicas Rol: Staff Data Engineer & Tech Lead

1. Resumen Ejecutivo y Conexión con la Investigación (Paper)
El objetivo de este proyecto es evolucionar nuestra arquitectura hacia un ecosistema de procesamiento de telemetría IoT en tiempo real. Este sistema materializa la aplicación práctica de la investigación científica sobre la predicción fotovoltaica a corto plazo.
El sistema debe ingestar datos masivos de sensores fotovoltaicos e instanciar ventanas temporales en Google Cloud Dataflow para alinear y comparar continuamente los datos en vivo (como LMD_GHI y la salida de Power en MW) contra las predicciones en caliente de nuestro modelo Transformer Bi-LSTM. Al confrontar en tiempo real la producción medida frente a la inferencia del modelo físico profundo, el pipeline logrará detectar fallas de hardware y anomalías operativas de manera proactiva, mitigando los tiempos de inactividad operativos y reduciendo los costos de mantenimiento de las plantas solares.

2. Arquitectura del Sistema (Pub/Sub -> Dataflow -> BigQuery)
El diseño del pipeline de streaming se estructura sobre tres pilares serverless en Google Cloud Platform, garantizando alta escalabilidad y baja latencia:
Ingesta (Pub/Sub): Capa de mensajería desacoplada con garantías de entrega at-least-once. Actuará como el bus principal para toda la telemetría IoT generada por los inversores y sensores de irradiancia.
Procesamiento (Dataflow / Apache Beam): Motor de cálculo distribuido operando en modo streaming. Implementará un enventanamiento temporal (Windowing) mediante ventanas deslizantes (sliding windows) y fijas (tumbling windows) de 15 minutos para la agregación continua de la potencia y la comparación con las marcas de agua (watermarks) del modelo ML.
Almacenamiento y Analítica (BigQuery): Data warehouse columnar que servirá como sumidero (sink) definitivo para la telemetría validada y los eventos de error.
Contratos JSON Estrictos: Es imperativo que las estructuras de los mensajes no fluyan de forma arbitraria. Los esquemas de entrada (Pub/Sub) y de salida (BigQuery) deben estar versionados en archivos estáticos .json dentro del repositorio. Cualquier mensaje que viole el contrato de entrada será rechazado en la capa de adaptación.

3. Diseño de Software (Arquitectura Hexagonal y Patrones GoF)
Para asegurar el desacoplamiento estricto entre nuestra lógica científica fotovoltaica y la infraestructura de GCP, el código base en Python debe estructurarse obligatoriamente bajo los principios de la Arquitectura Hexagonal (Puertos y Adaptadores) y Diseño Guiado por el Dominio (DDD).
La estructura de directorios debe definirse de la siguiente forma:
domain/models.py: Entidades puras y dataclasses (ej. representaciones de LMD_GHI y mediciones de paneles).
domain/ports.py: Interfaces abstractas (abc.ABC).
adapters/: Implementaciones de infraestructura para entrada y salida.
application/pipeline.py: El grafo acíclico dirigido (DAG) de Apache Beam.
Implementación de Patrones GoF:
Patrón Creacional - Factory: Se utilizará para instanciar de forma dinámica los adaptadores de infraestructura, como la inicialización del cliente de lectura de Pub/Sub dependiendo del entorno (ej. emulador local vs. beam.io.ReadFromPubSub en producción).
Patrón Estructural - Adapter: Actuará como puente semántico envolviendo las escrituras nativas hacia el Data Warehouse (ej. BigQuerySinkAdapter). Desacoplará la lógica de limpieza de ruido IoT de las firmas de entrada requeridas por el pipeline y el sumidero de datos.
Patrón de Comportamiento - Strategy: Se implementará para inyectar dinámicamente y aislar los algoritmos de detección de anomalías (ej. ThermalAnomalyStrategy o IrradianceDropStrategy). Al integrarse con la transformación RunInference de Beam, permitirá cambiar de reglas lógicas o modelos predictivos sobre la marcha sin alterar el DAG principal.

4. Manejo de Errores, DLQ y Observabilidad
La resiliencia ante un flujo incesante de datos corruptos debe garantizarse a nivel de worker para evitar el colapso del sistema.
Dead Letter Queue (DLQ) vía Try-Except en DoFn: Es obligatoria la implementación del patrón de salida lateral etiquetada (Tagged Side Outputs). Cada transformación en beam.DoFn debe estar blindada por bloques try-except. Ante un error transitorio o permanente (ej. violación del esquema JSON o lecturas fotovoltaicas negativas), la excepción debe capturarse y el registro original enviarse a un canal lateral etiquetado como DLQ para no bloquear el hilo de procesamiento.
Estructura de la DLQ: Los eventos de la DLQ se enviarán a una tabla de BigQuery con campos estandarizados obligatorios: original_payload, error_message, failure_timestamp, y failed_step.
JSON Structured Logging: Se prohíbe la emisión de logs en texto plano. Todos los registros emitidos desde las clases DoFn deben usar el formato JSON Structured Logging (mediante CloudLoggingHandler), incluyendo las claves reservadas de GCP como severity, message, logging.googleapis.com/trace, y atributos ricos para habilitar métricas basadas en logs y tableros operativos en tiempo real en Looker Studio.

5. Plan de Ejecución en 4 Fases (Milestones)
Se abstraera la implementacion de este proyecto de streaming de datos solares en 4 fases:

Fase 1: Simulador de Ingesta (Publisher Python a Pub/Sub)
Definición de los contratos de esquema mediante archivos estáticos .json para la entrada y salida de datos.
Desarrollo de un script publicador en Python (basado en el patrón Factory) que emule la generación masiva de telemetría IoT (LMD_GHI, voltajes, Power MW) e inyecte los eventos al topic de Pub/Sub respetando el contrato JSON.

Fase 2: Core de Apache Beam y Enventanamiento
Construcción de la capa application/pipeline.py orquestando el DAG en modo streaming.
Configuración de ventanas temporales (fijas y deslizantes de 15 minutos).
Configuración estricta del manejo del tiempo: implementación de watermarks, definición de la latencia permitida (allowed lateness) y políticas de disparadores (triggers) para el control de la recolección de basura del estado (obsolescence) y datos tardíos.

Fase 3: Reglas de Dominio y Detección de Anomalías (Patrón Strategy)
Implementación de la capa domain/ sin dependencias de Apache Beam.
Integración de las predicciones del modelo Transformer Bi-LSTM.
Aplicación del Patrón Strategy en el ModelHandler o bloque de inferencia para aislar el cálculo de discrepancias entre los datos reales (LMD_GHI, Power MW) y los valores inferidos en tiempo real para alertar sobre fallas físicas.

Fase 4: Despliegue, Adaptadores de Salida y DLQ
Desarrollo de los Adaptadores de salida (BigQuery).
Implementación rigurosa de los bloques try-except en las clases DoFn para desviar los fallos a la tabla DLQ en BigQuery mediante salidas laterales etiquetadas.
Habilitación del JSON Structured Logging, configuración de alertas operacionales (Watermark Lag, Trigger Frequency) y despliegue automatizado hacia el clúster de producción de Dataflow.

Hint: sigue este ordenamiento de carpetas hexagonal para este servicio de streaming:

serverless-solar-streaming/
├── domain/                  # CORE: El centro del hexágono (Python puro, sin dependencias de GCP o Beam)
│   ├── __init__.py
│   ├── models.py            # Dataclasses para la telemetría (ej. SensorData, PVODMetrics)
│   ├── ports.py             # Interfaces abstractas (Clases base ABC para Entrada/Salida)
│   └── strategies.py        # Patrón Strategy (ThermalAnomalyStrategy, InverterFailureStrategy)
│
├── application/             # CASOS DE USO: Orquestación (Depende de Beam, pero no de Pub/Sub o BQ)
│   ├── __init__.py
│   ├── pipeline.py          # Definición del DAG de Apache Beam
│   └── transforms/          # Transformaciones personalizadas (PTransforms y DoFns)
│       ├── __init__.py
│       ├── windowing.py     # Lógica de Sliding/Tumbling windows y watermarks
│       └── inference.py     # Integración del modelo Transformer Bi-LSTM en caliente
│
├── adapters/                # INFRAESTRUCTURA: Conexión con el mundo exterior (GCP)
│   ├── __init__.py
│   ├── input/               # Adaptadores "Driving" (Los que inyectan datos al pipeline)
│   │   ├── __init__.py
│   │   ├── pubsub_input.py  # Lector de Google Cloud Pub/Sub
│   │   └── mock_input.py    # Lector de archivos locales (Para el Dry Run / Testing)
│   └── output/              # Adaptadores "Driven" (Los sumideros donde se escribe)
│       ├── __init__.py
│       ├── bigquery_sink.py # Escritor hacia BigQuery
│       └── dlq_handler.py   # Manejador de la Dead Letter Queue (DLQ) para JSON corruptos
│
├── config/                  # Configuraciones, Variables y Esquemas
│   ├── schemas/
│   │   ├── pubsub_schema.json # Contrato estricto de entrada (El JSON que enviará el simulador)
│   │   └── bq_schema.json     # Esquema exacto de la tabla de destino
│   └── settings.py          # Variables de entorno y configuración del DataflowRunner
│
├── simulator/               # FASE 1: El inyector de datos (Totalmente aislado del ETL)
│   ├── publisher.py         # Script Python que lee el PVOD Parquet y stremea a Pub/Sub
│   └── utils.py             # Lógica de control de flujo (límites de mensajes, delay)
│
├── tests/                   # Pruebas unitarias y de integración
│
├── main.py                  # ENTRYPOINT: El "ensamblador" de inyección de dependencias
├── requirements.txt         # apache-beam[gcp], pydantic, etc.
├── teardown.sh              # "Kill Switch" para apagar Dataflow y borrar suscripciones
└── README.md

