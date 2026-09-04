# CLAUDE.md — Contexto del proyecto uea_tfm

Este archivo da contexto persistente a Claude Code para el desarrollo del pipeline de este trabajo. Léelo por completo antes de empezar a trabajar en cualquier tarea de código.

Este proyecto se desarrolla íntegramente en español, no obstante, las búsquedas de citaciones, marco teórico, datos adicionales, pueden hacerse en inglés y posteriormente traducirlos si serán incorporados al trabajo.

## Qué es este proyecto

TFM (Trabajo Fin de Máster) del Máster Universitario en Análisis de Grandes Volúmenes de Datos (Big Data), Universidad Europea de Andalucía (UEA). Título: "Desarrollo de un sistema escalable de microservicios para el procesamiento y análisis de datos IoT" (Boris añadió "procesamiento" el 21 de agosto de 2026, para equilibrar el peso real del trabajo). Entrega: septiembre de 2026.

El objetivo es implementar un pipeline IoT completo, containerizado y basado exclusivamente en herramientas open-source, siguiendo una **arquitectura Kappa** (flujo único reproducible, sin capa de lotes separada), aplicado a telemetría de consumo energético de edificios.

La documentación LaTeX del trabajo vive en `docs/` (capítulos en `docs/capitulos/`) y **no debe modificarse** desde Claude Code — esa parte se gestiona aparte, en Claude.ai. El foco de Claude Code es exclusivamente el **desarrollo del pipeline/código**, en `pipeline/`.

Dentro de `references/` hay una serie de trabajos finales TFM que pueden ser usados como referencias de estilo (no se versiona: son trabajos ajenos).

## Stack tecnológico (todo open-source, containerizado)

Definido en `pipeline/docker-compose.yml`, con versiones fijadas:

| Servicio | Imagen | Puerto host |
|---|---|---|
| Mosquitto | `eclipse-mosquitto:2.0.22` | 1883, 9001 |
| Kafka (KRaft, sin Zookeeper) | `apache/kafka:4.3.1` | 29092 |
| Apicurio Schema Registry | `apicurio/apicurio-registry:3.3.1` | 8080 |
| Apicurio UI (contenedor aparte en 3.x) | `apicurio/apicurio-registry-ui:3.3.1` | 8888 |
| TimescaleDB | `timescale/timescaledb:2.29.1-pg17-oss` | 5432 |
| PostgreSQL | `postgres:17-alpine` | 5433 |
| Grafana | `grafana/grafana:13.0.6` | 3000 |

- **Mosquitto** — *no usar NanoMQ*: se evaluó porque supuestamente tenía puente nativo a Kafka, pero se verificó que esa función es exclusiva de EMQX Enterprise (de pago). Mosquitto + bridge propio es la decisión final.
- **TimescaleDB variante `-oss`** — la imagen por defecto incluye funciones bajo Timescale License, que es source-available pero **no** open source según la OSI, y el trabajo afirma que el stack es íntegramente open-source. La edición Apache-2 conserva `create_hypertable` y `time_bucket` (ambos verificados funcionando); renuncia a continuous aggregates y compresión, irrelevantes aquí porque la agregación la hace Spark.
- **Spark Structured Streaming** — se ejecuta desde el venv del host, **no containerizado**, contra el listener externo de Kafka. Por eso Kafka publica dos listeners (`kafka:9092` interno, `localhost:29092` externo). API: DataFrame API con ventanas y watermarking, no RDD ni DataStream.
- **Databricks está excluido** del pipeline. Se usó puntualmente para el análisis exploratorio del dataset; no forma parte de la arquitectura ni de la preparación de datos reproducible.

## Decisiones de arquitectura y su razonamiento

- **Kafka + Spark Structured Streaming, no Kafka + Flink**: por análisis de mercado laboral español (Spark/Databricks es la habilidad dominante y de default-hire; Flink aparece casi siempre como "nice-to-have" secundario) y por las características de los datos: son **lecturas horarias de contadores**, de modo que la ventaja de latencia sub-segundo de un motor de streaming nativo no aporta ningún valor frente a micro-batch. El razonamiento completo está en `docs/capitulos/EstadoArte.tex`.
  - **AVISO**: ese capítulo justifica el pivote con "cadencia ~30s por sensor sin huecos >60s", cifras del dataset anterior que además resultaron falsas incluso allí. Con ASHRAE la cadencia real es **una lectura por hora y contador**, lo que refuerza el argumento pero exige actualizar el texto.
- **Arquitectura Kappa, no Lambda**: un único camino de procesamiento sobre un log reproducible (Kafka), sin capa de lotes separada.
- **Gobernanza de esquemas con Avro + Apicurio**: aporte diferenciador frente a las implementaciones Kappa-IoT de referencia.
- **Doble sumidero**: separar el consumo operacional casi en tiempo real (TimescaleDB → Grafana) del analítico de negocio (PostgreSQL → Power BI). Son **dos consultas de streaming independientes** con checkpoint propio, no una con dos escrituras: si un sumidero cae, el otro sigue.

## Dataset: ASHRAE Great Energy Predictor III

Consumo energético horario **real** de 1.449 edificios en 16 emplazamientos durante 2016 (competición de Kaggle, subconjunto del proyecto Building Data Genome 2). Medidas de contadores con su error de medición; no hay datos sintéticos.

**Se usa un subconjunto: emplazamientos 2, 3 y 5.** 652 sensores, 5.682.185 eventos, 46 combinaciones de agregación, ~128 min de reproducción a 740 ev/s. El razonamiento completo está en `pipeline/data/README.md`.

**El parque real genera 0,1797 ev/s.** Verificado sobre el Parquet: cada contador mide una vez por hora —mediana y p95 del intervalo son exactamente 3.600 s, sin dispersión— y los 652 contadores tienen datos en el 99,2% de las 8.784 horas de 2016. El caso de uso completo son **menos de dos décimas de evento por segundo**, y de ahí que el simulador acelere el reloj: `tasa = n_sensores × acelerar / 3600`. A `--acelerar 2000` cada sensor mide cada 1,8 s y el parque entero da 359 ev/s.

Hallazgos medidos que condicionan el diseño:

- **Un sensor es el par (edificio, tipo de contador)**: 498 edificios → 652 sensores.
- **La unidad depende de `meter_type`**: electricidad, agua fría, vapor y agua caliente no son comparables ni sumables (medianas de 43,6 / 146,0 / 8,8). Toda agregación debe incluir `meter_type` en la clave.
- **Estructura temporal real**: ciclo diario (valle nocturno, pico a primera hora de la tarde) y ciclo anual (el agua fría pasa de 152 en enero a 528 en agosto).
- **El emplazamiento 14 está excluido**: sus marcas de tiempo van 5 horas por delante del resto (correlación de forma 0,99). Los otros 15 están en hora local y son comparables sin conversión.
- **Todas las marcas se tratan como hora local**, sin dimensión de timezone. Consecuencia: dos eventos de emplazamientos distintos con la misma marca no son el mismo instante físico.
- Sin etiquetas de anomalía, pero con problemas de calidad reales y documentables (4,6% de lecturas a cero en el subconjunto).

**Dataset anterior descartado**: Kaggle "Power Telemetry" (khalilaraoui). Era sintético y no servía para la mitad analítica: `device_state` no influía en el consumo (57,9 W encendido frente a 57,8 W en reposo), `cpu_load` y `user_count` no correlacionaban con la potencia (r = 0,0004 y −0,0007), y cada máquina reportaba una vez cada ~29 horas. No hay proceso físico detrás de esos datos.

## Contrato de datos

**Tópico MQTT**: `iot/{building_id}/{meter_type}/telemetry` (ejemplo: `iot/156/electricity/telemetry`). Sin nivel de emplazamiento: `site_id` es derivable vía la dimensión y nadie lo consumía del tópico.

**Evento Avro** (`pipeline/schemas/telemetry_event_v1.avsc`, 23 B por evento):

```
building_id | meter_type | timestamp | meter_reading | sim_publish_ts
```

Reproduce **solo lo que emitiría el contador**. Sin `event_id` ni `sensor_id` (eran concatenaciones calculadas de datos ya presentes) y sin atributos del edificio, que viven en la tabla de dimensión de 498 filas e incorpora Spark con un broadcast join. `sim_publish_ts` es el único campo que no emite el contador: instrumentación para el KPI de latencia.

**`building_id` es TEXTO en todo el pipeline**, desde el Parquet hasta las tablas finales: nadie suma identificadores. Declararlo `int` abría una vía de corrupción silenciosa, comprobada: Avro no valida, convierte. Un `156.9` —normal desde JavaScript o numpy, donde no hay enteros de verdad— se truncaba a `156` y el evento quedaba **atribuido a otro edificio**, sin error ni rastro; un booleano se volvía el edificio `1` y un valor fuera del rango de int32 se escribía igual. Como texto, fastavro **rechaza** cualquier no-cadena (`TypeError: must be string on field building_id`) y el evento acaba en la DLQ. `site_id` sigue siendo entero: no viaja en el evento, lo añade Spark con el broadcast join, así que ningún productor puede equivocarse con él.

**Clave natural**: `(building_id, meter_type, timestamp)`. Verificada única (5.682.185 grupos para 5.682.185 filas). Es lo que hace idempotente el reprocesamiento del log de Kafka. `(building_id, timestamp)` **no basta**: colapsaría 1.345.428 eventos, porque un edificio tiene varios contadores midiendo a la misma hora.

**Clave del mensaje en Kafka**: solo `(building_id, meter_type)` — el sensor, 652 valores, serializado como `156:electricity`. Mantiene en orden y en la misma partición las lecturas de cada contador, que es lo que necesitan las ventanas de Spark. No confundirla con la anterior. Verificado sobre el sistema en agosto de 2026: el bridge conservaba `machine_id` del dataset anterior y la clave era **None en todos los mensajes**, con reparto round-robin y sin ningún error en el log.

**Formato de cable**: byte mágico `0x00` + id de esquema de 4 bytes big-endian + payload Avro schemaless — el formato de facto del ecosistema Kafka. Lo escribe el **`AvroSerializer` de `confluent-kafka` (Apache 2.0)** en el bridge; Spark lo desmonta a mano con `substring`/`conv` (sin librería JVM). El tamaño de la cabecera y la resolución del esquema se comparten en `pipeline/common/apicurio.py`. **El byte mágico se reintrodujo el 25 de agosto de 2026** al adoptar el SerDe de Confluent: se había retirado el 23 de agosto cuando el bridge serializaba a mano con `fastavro`, pero al pasar al SerDe estándar la cabecera vuelve a coincidir con la del ecosistema Kafka, y eso hace los eventos legibles por cualquier consumidor compatible con un registro de esquemas.

**Gobernanza por la API compatible con Confluent (ccompat), no la nativa de Apicurio.** El `AvroSerializer` resuelve el esquema por el protocolo de Confluent: un **subject** plano `iot.telemetry.raw-value` (convención `{topic}-value`), sin el concepto de «grupo» propio de Apicurio. `register_schema.py` registra por ese endpoint (`POST /subjects`) y fija la regla de compatibilidad por `PUT /config`. **El id que viaja en el cable es el de ccompat, no el `globalId` nativo**: son espacios de identificadores distintos, y la única registración gobernada es la de ccompat. Se descartó la API nativa (grupo `iot`) porque el SerDe no la ve —auto-registraría un duplicado sin gobernar— y porque «grupo» no existe en el estándar de Confluent. La barrera para no usar el SerDe antes era falsa: `confluent-kafka` es Apache 2.0 y su cliente Python habla con el ccompat de Apicurio; lo que faltaba no era la librería sino registrar por ese endpoint. Verificado el 25 de agosto de 2026 de extremo a extremo: la validación de dominio (incluido el rechazo de `building_id=156.9`), la DLQ, `check_enum_order` y el rechazo de cambios incompatibles se conservan.

## KPIs objetivo (de `docs/capitulos/Objetivos.tex`)

1. **Ingesta**: latencia extremo a extremo < 2s (p95); pérdida de mensajes < 0.1%.
2. **Gobernanza de esquema**: 100% de eventos validados contra el esquema Avro registrado; cero fallos de deserialización ante evolución compatible.
3. **Procesamiento**: latencia de micro-lote < 3s tras cierre de ventana; throughput sostenido ≥ 50 eventos/segundo.
4. **Visualización**: refresco de Grafana < 5s; **≥ 1 dashboard en Power BI** que cubra consumo energético, eficiencia operativa y detección de anomalías. Rebajado desde "≥ 3 reportes" el 21 de agosto de 2026: el criterio de Boris es no prometer de más y entregar lo adicional como extra.
5. **Escalabilidad y resiliencia**: ≥ 500 sensores concurrentes con degradación de throughput < 20% respecto a una carga base de 100; recuperación ante fallo de un servicio < 60s sin pérdida de datos.

### Cifras medidas sobre ASHRAE (estado limpio, 50.000 eventos a 400 ev/s)

| Métrica | Resultado | Objetivo |
|---|---|---|
| Pérdida de mensajes | **0,0000%** | < 0,1% ✓ |
| Latencia de ingesta (evento → PostgreSQL) p50 / p95 | **0,766 / 1,277 s** | < 2 s ✓ |
| Disponibilidad del agregado (→ TimescaleDB) p50 / p95 | 5,660 / 6,916 s | ver abajo |
| Duración de micro-lote, mediana / p95 | **572 / 776 ms** | < 3 s ✓ |
| Throughput sostenido | **1.284 ev/s** | ≥ 50 ✓ |
| Degradación 100 → 652 sensores | **0,6%** | < 20% ✓ |
| Consultas de los paneles de Grafana | 1,9 – 15,4 ms | < 5 s ✓ |

**El KPI 1 mezcla dos magnitudes.** La latencia de ingesta a grano de evento cumple el objetivo; la disponibilidad del agregado no, y no puede cumplirlo por diseño: un agregado horario no existe antes de que cierre su ventana, y con contadores que miden en punto eso obliga a esperar la lectura de la hora siguiente. Verificado con una predicción: al bajar la tasa de 262 a 110 ev/s la latencia pasó de 5,66 a 10,89 s, ajustándose a `1,5 × (652/tasa) + 1,9 s`. **Está acotada por la cadencia del sensor, no por el pipeline.**

**El panel «Latencia extremo a extremo (p95)» de Grafana mostraba la magnitud equivocada.** Consultaba `telemetry_metrics` (la disponibilidad del agregado, ~2,8 s, no cumple <2s) en vez de `telemetry_events` (la latencia de ingesta, ~1,3 s, sí cumple). Corregido el 26 de agosto de 2026: se añadió un segundo datasource de Grafana a PostgreSQL (`pipeline/docker/grafana/provisioning/datasources/postgres.yml`) y los dos paneles de latencia del dashboard 1 pasaron a consultar `telemetry_events`. Decisión deliberada: el dashboard **solo muestra la magnitud que está en los objetivos** (latencia de ingesta); la disponibilidad del agregado no se enseña ni se menciona en la demo, para no complicar la exposición con un número que no corresponde a ningún KPI prometido. Añadir el datasource no compromete la independencia de los sumideros: es una lectura de Grafana, no acopla las dos consultas de streaming.

### Cifras del 18 de agosto de 2026, ya con las herramientas de medición

Obtenidas con `tools/kpi_report.py` sobre 20.000 eventos a 400 ev/s, y reproducibles repitiendo el ciclo de `tools/README.md`.

| Métrica | Resultado | Objetivo |
|---|---|---|
| Latencia de ingesta p50 / p95 | 0,828 / **1,373** s | < 2 s ✓ |
| Disponibilidad del agregado p50 / p95 | 2,848 / 4,004 s | acotada por la cadencia |
| Eventos validados contra el esquema | **100,0000%** (0 en DLQ) | 100% ✓ |
| Micro-lote `eventos-postgresql` p50 / p95 | 380 / **709** ms | < 3 s ✓ |
| Micro-lote `metricas-timescaledb` p50 / p95 | 648 / **961** ms | < 3 s ✓ |
| Paneles de Grafana (20, vía API) | 6,4 – 51,2 ms | < 5 s ✓ |
| **Recuperación tras matar Mosquitto** | **16,1 s** (31,5 s desde el fallo) | < 60 s ✓ |
| Eventos perdidos con el broker caído 15 s | **0 de 60.000** | sin pérdida ✓ |
| Confirmaciones de Mosquitto, ventana 100 y bridge suscrito | **7.324 – 8.539 ev/s** | — |
| Lo mismo con ventana 1 (equivale al simulador) | 1.971 ev/s | — |

### Re-medición del 25 de agosto de 2026, ya con el formato de cable de Confluent

Tras migrar la serialización al `AvroSerializer` de `confluent-kafka` (ccompat) y reintroducir el byte mágico, se repitió el ciclo de `tools/kpi_report.py` sobre **50.000 eventos a 358 ev/s** (`--acelerar 2000`), estado limpio y `swap` nulo verificado. **La migración no degrada ningún KPI**: las cifras son comparables o algo mejores que las del formato anterior, y el byte extra de la cabecera (30 vs 29 B) es irrelevante.

| Métrica | Resultado | Objetivo |
|---|---|---|
| Pérdida de mensajes | **0,0000%** (50.000/50.000) | < 0,1% ✓ |
| Latencia de ingesta p50 / p95 | **0,729 / 1,182** s | < 2 s ✓ |
| Disponibilidad del agregado p50 / p95 | 2,959 / 3,983 s | acotada por la cadencia |
| Eventos validados contra el esquema | **100,0000%** (0 en DLQ) | 100% ✓ |
| Micro-lote `eventos-postgresql` p50 / p95 | 263 / **562** ms | < 3 s ✓ |
| Micro-lote `metricas-timescaledb` p50 / p95 | 528 / **821** ms | < 3 s ✓ |
| Panel de Grafana más lento (de 20, vía API) | **104,9** ms | < 5 s ✓ |

Comparación directa con el formato anterior (18 de agosto): ingesta p95 1,373 → **1,182 s**; micro-lote `eventos` p95 709 → **562 ms**; micro-lote `metricas` p95 961 → **821 ms**. Las pruebas de resiliencia (recuperación ante fallo, escalera de saturación y envenenamiento del watermark) se re-corrieron el 26 de agosto de 2026 (ver más abajo): recuperación y watermark salieron coherentes con lo histórico; la rampa de saturación dio un patrón anómalo en el primer intento, repetido con muestreo de swap y descartado como causa — fue ruido de medición de un solo tiro.

### Punto de saturacion y resiliencia (19 de agosto de 2026)

Rampa de ritmo creciente con los 652 sensores, medida en el consumo:

| Ritmo pedido | Publicado real | Persistido | Latencia p95 | Lote p95 |
|---|---|---|---|---|
| 906 ev/s | 896,5 (99%) | 40.000 | 1,28 s | 629 ms |
| 1.811 ev/s | **1.798,6** (99,3%) | 40.000 | 1,97 s | 906 ms |
| 3.622 ev/s | 2.550,5 (70%) | 40.000 | 3,02 s | 1.108 ms |
| 7.244 ev/s | 2.297,0 (32%) | 39.316 | 6,44 s | 1.192 ms |

**El pipeline sostiene 1.800 ev/s con todo dentro de objetivo: 10.000 veces el caso de uso real de 0,1797 ev/s.** El techo de ~2.900 ev/s es del SIMULADOR —por encima publica menos cuanto mas se le pide— y el pipeline persistio el 100% de lo que le llego en todos los peldanos.

Recuperacion ante fallo, tumbando cada servicio 15 s:

| Servicio | Flujo restablecido | Nota |
|---|---|---|
| Mosquitto | 16,1 s | 1 evento perdido, el que estaba en vuelo |
| Kafka | 15,2 s | sin intervencion |
| TimescaleDB | **4,1 s** | la otra consulta siguio con 21 micro-lotes |
| PostgreSQL | **5,1 s** | sin muerte silenciosa |

**El KPI 3 no discrimina, y conviene decirlo en el texto.** El caso de uso real exige **0,18 ev/s** (652 contadores × 1 lectura/hora), así que el umbral de 50 ev/s ya está 276× por encima de la necesidad: procede de la literatura, no del problema. Subirlo sería inventar un número; lo que aporta valor es la caracterización. Y ahí falta lo importante: **los 1.284 ev/s son el techo del SIMULADOR**, que confirma cada publicación con QoS 1 antes de emitir la siguiente.

Medido con el generador asíncrono el 18 de agosto de 2026, con el bridge suscrito y la ventana de mensajes en vuelo como única variable (40.000 eventos, 652 sensores):

| Ventana | Confirmados por Mosquitto | Confirmación p50 | Latencia MQTT→Kafka p50 |
|---|---|---|---|
| 1 (equivale al simulador) | 1.971 ev/s | 0,3 ms | ~2 ms |
| 100 | **7.324 – 8.539 ev/s** | 9–11 ms | 683 ms |
| 500 | 6.596 ev/s (baja) | 63,5 ms | — |

**La asincronía multiplica por ~4 lo que el broker confirma**, no por 8,6 como sugería una comparación anterior sin el bridge conectado. Cero pérdidas en las tres: 40.000 de 40.000 en Kafka.

**Pero el PUBACK de Mosquitto significa "aceptado", no "entregado al pipeline".** Con ventana 100, Mosquitto confirmaba a 7.324 ev/s mientras el bridge llevaba consumidos 8.230: el resto estaba encolado en el broker, y eso es lo que dispara la latencia MQTT→Kafka de 2 ms a 683 ms. El drenaje sostenido del bridge fue del orden de **4.000 ev/s**.

**Con ventana 500 el throughput BAJA y la latencia se multiplica por siete**: pasado el punto de saturación, ampliar la ventana solo añade cola. Es el codo que se buscaba. **Sigue faltando** situarlo con precisión y con Spark corriendo a la vez.

### Re-medición del 26 de agosto de 2026, ya con el formato de cable de Confluent

Las tres pruebas de resiliencia que quedaron pendientes tras la migración a ccompat (ver arriba) se re-corrieron con el pipeline tal como está hoy. **Recuperación ante fallo y envenenamiento del watermark: coherentes con lo histórico.** La rampa de saturación dio un patrón anómalo en el primer intento; repetida con muestreo de swap, se descarta el swap como causa (ver detalle abajo).

**Recuperación ante fallo**, tumbando cada servicio 15 s:

| Servicio | Flujo restablecido | Filas nuevas |
|---|---|---|
| Mosquitto | 2,0 s | 2.241 |
| Kafka | 15,3 s | 662 |
| TimescaleDB | 4,0 s | 322 |
| PostgreSQL | 2,0 s | 3.469 |

Prácticamente idénticas a las del 18 de agosto (16,1 / 15,2 / 4,1 / 5,1 s) — el formato de cable no afecta a esto, como se esperaba.

**Envenenamiento del watermark**, dos pasadas:

| | Sin guarda | Con guarda |
|---|---|---|
| `telemetry_metrics` | **+0** (222→222) | **+1.150** (503→1.653) |
| `telemetry_events` | +16.128 | +16.127 |
| Throughput del simulador | 357,1 ev/s | 357,6 ev/s |
| Veredicto | **ENVENENADO** | **PROTEGIDO** |

Cifras casi idénticas a las del 20 de agosto (0 / 1.149 filas). El throughput del simulador cayó justo en el valor teórico esperado (~359 ev/s a `--acelerar 2000`) en ambas pasadas.

**Rampa de saturación — patrón anómalo en el primer intento, descartado el swap como causa:**

| Ritmo pedido | Publicado (intento 1) | Publicado (repetición) |
|---|---|---|
| x5.000 (906 ev/s) | 900,5 | 904,3 |
| x10.000 (1.811 ev/s) | 1.792,1 | 1.800,7 |
| x20.000 (3.622 ev/s) | 2.913,8 ⚠️ techo | 3.401,9 ⚠️ techo |
| x40.000 (7.244 ev/s) | **4.060,9** (subió) | **3.244,6** (bajó, como se esperaba) |
| x5.000-caliente | 899,3 | 897,5 |

El primer intento mostró el peldaño x40.000 publicando más rápido que el x20.000, rompiendo la curva monotónicamente decreciente del 19 de agosto. Se sospechó del swap (807 MiB, no cero, al terminar). **Se repitió la rampa el 26 de agosto de 2026 con un muestreador de swap cada 2 s durante toda la ejecución (`/proc/meminfo`, no una sola lectura al final)**: el swap se mantuvo en **exactamente 805 MiB, sin variar un solo MiB, en las ~126 muestras de los 248 s que duró la rampa completa**. Con el swap probadamente estático, la repetición dio una curva monotónicamente decreciente, sin el pico anómalo.

**Conclusión: el swap no fue la causa** — estaba ya en 805 MiB antes de arrancar la rampa (con solo el stack, el bridge y Spark en marcha, sin carga) y no se movió durante la prueba. La anomalía del primer intento fue ruido de una medición de un solo tiro por peldaño (sin repetición ni promedio), en la línea de la trampa ya documentada del 18 de agosto ("una escalera de carga en orden creciente miente por calentamiento"): un GC de la JVM, una rotación de segmento de Kafka o un checkpoint de TimescaleDB coincidiendo con el peldaño x20.000 explican el bache sin necesidad de invocar el swap. Los 805 MiB de swap parecen ser un residuo permanente de este WSL con `swap=4GB`, no algo que entre en juego bajo carga — coherente con que la cifra fuera *idéntica* antes y después de 248 s de saturación.

## Entorno de desarrollo

- **venv + pip** (no conda). Dependencias en `requirements.txt` (raíz del repo, un único fichero para el pipeline y la preparación del dataset — se consolidó el 26 de agosto de 2026, antes vivían separados en `pipeline/requirements.txt` y `pipeline/data/requirements.txt`). `aiomqtt` es la que permite al simulador mantener una conexión por sensor: con paho serían 652 hilos, con aiomqtt son 652 corrutinas en uno solo. `confluent-kafka[avro]` aporta el `AvroSerializer` del bridge; arrastra `cryptography`, `httpx`, `authlib` y `cffi` (dependencia notablemente más pesada que `fastavro` solo, que se conserva porque lo usan `register_schema.py` y la prueba del watermark, y porque el propio SerDe serializa con él por debajo).
- **Java 21 LTS gestionado por SDKMAN** (ver `.sdkmanrc`) — única fuente de JDK. PySpark 4.x requiere Java 17+.
- Setup: `bash setup.sh` (raíz del repo). Comprueba Docker y Java (guía, no instala), crea el venv, instala dependencias y prepara el dataset ASHRAE si hay credenciales de Kaggle. Reemplaza a `pipeline/setup_env.sh`, retirado el 26 de agosto de 2026.
- Los conectores JVM de Spark (Kafka, Avro, JDBC PostgreSQL) se resuelven de Maven Central en el primer arranque y quedan en **`~/.ivy2.5.2`** (Spark 4.x usa directorio versionado, no `~/.ivy2`). No borrar esa caché.
- VS Code + WSL2/Ubuntu 24.04. `.vscode/settings.json` **sí se versiona**: apunta al intérprete de `.venv` para que Pylance resuelva los imports.

## Convenciones y aprendizajes validados

- **Verificar antes de comprometerse**, contra la documentación o el sistema real, no por marketing ni de memoria. Ha evitado errores reales (NanoMQ) y ha destapado otros (ver abajo).
- **Gestión de dependencias**: versiones exactas para dependencias directas. `pandas` y `pyarrow` **se declaran explícitamente**: se comprobó que `pip install pyspark` NO los instala (solo llegan con extras).
- **Fallar ruidosamente, nunca descartar en silencio.** Con 7 días de retención en Kafka, detenerse es recuperable y descartar es irreversible.
- **Comprobar las invariantes en cada ejecución** en lugar de asumirlas (`prepare_ashrae.py` verifica la unicidad de la clave natural; `register_schema.py` verifica el orden de los enums).
- **Los scripts del pipeline solo contienen lo que hace falta para mover datos.** Lo que se usa únicamente al preparar o medir una prueba vive en `tools/`, y lo que usan dos piezas vive en `common/`, nunca duplicado. Criterio de Boris, agosto de 2026.
- **Archivos binarios nunca se suben vía herramientas MCP** (se corrompen al tratarse como texto UTF-8).

### Trampas encontradas midiendo (material para el capítulo de desarrollo)

- **Apicurio NO protege el orden de los símbolos de un enum.** Ni con `FULL_TRANSITIVE`. Avro codifica el enum como índice, así que reordenar corrompe en silencio los datos ya escritos: verificado, `electricity` se lee como `chilledwater` sin ninguna excepción. La regla la implementa `register_schema.py`, no el registro.
- **`spark.sql.session.timeZone=UTC` no gobierna la conversión a objetos Python.** `collect()` devuelve `datetime` naive en la zona del sistema del driver. Con la máquina en Europe/Madrid, cada evento se escribía con una hora de más, y de forma variable según el horario de verano. Se fuerza `TZ=UTC` en el proceso antes de crear la sesión.
- **El UPSERT exige deduplicar dentro del micro-lote**: PostgreSQL aborta con `CardinalityViolation` ante una clave repetida en la misma sentencia, y las repeticiones son normales con garantía at-least-once.
- **Un evento con fecha futura envenena el watermark**, que es **uno solo para toda la consulta**: no hay watermark por clave ni por partición. Un único dispositivo mal configurado detiene la agregación de los 652 sensores, no solo la suya. Medido con `tools/watermark_poison_test.py` el 20 de agosto de 2026: con la guarda desactivada, `telemetry_metrics` escribió **0 filas en 45 s** mientras `telemetry_events` recibía 16.126, **sin un solo error**; con ella, 1.149 y 16.488. Se comprueba en dos sitios —bridge y Spark, antes del `withWatermark`— porque la guarda del bridge no alcanza a lo que ya está en Kafka.
- **`ingested_at` debe refrescarse en el `ON CONFLICT DO UPDATE`.** Si no, al reescribir una fila se actualiza `sim_publish_ts` pero no el instante de ingesta, y la latencia calculada sale **negativa**: la fila parece escrita antes de publicarse. Se manifiesta siempre que se reprocesa el log de Kafka. Corregirlo bajó la medición del agregado de 10,25 a 6,92 s; buena parte de aquella cifra era medición contaminada.
- **Una escalera de carga en orden creciente miente por calentamiento.** La primera pasada sugería que el throughput *mejora* al añadir sensores (1.062 → 1.332 ev/s). Repitiendo el primer peldaño al final, en caliente, la degradación real resultó ser del 0,6%. Hay que controlar el orden o repetir el peldaño base.
- **Una clave de Kafka a `None` no da ningún error.** El bridge seguía usando `evento.get("machine_id")`, campo que el contrato de ASHRAE no tiene, así que todos los mensajes salían sin clave y el productor los repartía en round-robin: el orden por sensor que la documentación daba por garantizado no existía. `.get()` sobre un campo obligatorio convierte un incumplimiento del contrato en un valor nulo silencioso; con acceso directo, el evento habría acabado en la DLQ el primer día.
- **El productor de una prueba de resiliencia también es parte del experimento.** La primera prueba de recuperación ante fallo concluyó "el flujo no se restableció". Falso: el bridge reconectó solo en 17 s. Lo que había muerto era el simulador, seis segundos después de tumbar el broker, al propagarse la excepción de `wait_for_publish`. La prueba medía si el simulador sobrevive, no si el sistema se recupera.
- **El orden de arranque contamina la medición tanto como un error de código.** El mismo sistema con la misma carga dio una latencia de ingesta p95 de **1,38 s o de 92,53 s** según si Spark estaba en marcha antes de publicar o se arrancaba después: en el segundo caso los eventos esperan en Kafka y esa espera cuenta, porque el reloj arranca en el `sim_publish_ts` del productor. El micro-lote p95 pasó igualmente de 692 a 2.745 ms, porque el primer lote absorbe de golpe todo lo acumulado. Ninguna de las dos cifras delata el problema por sí sola.
- **La validación Avro comprueba la forma, no el significado.** Pasan sin objeción un `meter_reading` de 1e308, uno negativo, un `timestamp` del año 2099 y hasta un número enviado como cadena. El peor es la fecha futura: envenena el watermark y hace que Spark descarte como tardío todo el tráfico legítimo posterior, sin un solo error. El bridge lo rechaza ahora con dos guardas de dominio.
- **Un `building_id` fuera de la dimensión no lo rechaza el bridge**: cumple el esquema Avro, así que entra y llega al join, que es LEFT y lo deja con `site_id` NULL. Como esa columna es NOT NULL en la clave de `telemetry_metrics`, la escritura fallaba con `NotNullViolation`, el supervisor relanzaba y volvía a fallar con el mismo lote, y **el evento se quedaba en Kafka envenenando cualquier arranque posterior**. Verificado con `building_id=99999`. Ahora la agregación los aparta, el dato sigue en `telemetry_events` y `kpi_report.py` avisa de cuántos hay.
- **`aiomqtt` no reconecta solo, y el sintoma no es un fallo sino un simulador inútil.** Cada `publish` sobre el cliente muerto lanzaba `MqttError`, se contaba como fallo y se seguía con el evento siguiente: el simulador quemaba su agenda entera a velocidad de CPU —12.814 publicados frente a 35.597 fallidos— sin publicar nada más y sin recuperarse aunque el broker volviera.
- **La pérdida tras una caída la causaba el consumidor tardón, no el productor rápido.** Al volver el broker, los 652 sensores publicaban con normalidad mientras el bridge tardaba **17 s más** en resuscribirse, porque paho aumenta su espera de reconexión hasta 120 s. En esa ventana Mosquitto encolaba para su sesión persistente hasta llenar `max_queued_messages` y descartaba 6.595 mensajes. Se probó antes un volcado acotado en el productor: no cambió nada y se retiró. Acotando el backoff del bridge a 5 s, la pérdida pasa a **cero**.
- **Mosquitto descarta en silencio pasado su punto de saturacion.** Empujando por encima del techo, el productor registro 40.000 publicados y 0 fallidos —recibio su PUBACK de cada uno— pero al bridge llegaron 38.221. El broker tiro 1.779 al llenarse `max_queued_messages`. No hay error en el productor, ni en el bridge, ni en la DLQ, ni en los logs del broker: **4,4% de perdida invisible con todos los indicadores en verde**. Solo lo delata `$SYS/broker/publish/messages/dropped`, que ahora lee `kpi_report.py`.
- **`for q in consultas: q.awaitTermination()` vigila UNA sola consulta.** Espera secuencialmente: si moria la primera, su excepcion terminaba el proceso entero y arrastraba a la otra; si moria la segunda, nadie la esperaba y el job seguia vivo escribiendo la mitad de los datos **sin un solo aviso**. Se sustituyo por un supervisor que comprueba `isActive` de todas y relanza la caida desde su checkpoint.
- **Reproducir el dataset sin limpiar el checkpoint inutiliza la agregacion por ventana.** El watermark es monotono: si ya esta avanzado de una pasada anterior, cada ejecucion nueva publica eventos que cubren horas ANTERIORES a ese instante y Spark los descarta como tardios. `telemetry_metrics` se queda a cero mientras la ruta de eventos, que no tiene watermark, sigue llenandose con normalidad. Por eso las pruebas parten de `reset_state.py`. (Antes de bakear las fechas, el mismo efecto lo disparaba reanclar las marcas al presente en cada ejecucion.)
- **El envenenamiento del watermark empieza por una RÁFAGA, no por un silencio.** Al saltar el watermark, todas las ventanas abiertas quedan por debajo y Spark las cierra y emite de golpe: 46 filas, 117 dos segundos después de la inyección, y ninguna en los 44 siguientes. La primera versión de la prueba contaba filas entre el principio y el final, vio ese pico como crecimiento y dictaminó **PROTEGIDO sobre un sistema envenenado**. De ahí los dos tramos: asentamiento y régimen, y el veredicto sale del segundo.
- **`sink_num_output_rows` vale -1 con `foreachBatch`**: Spark no sabe cuantas filas escribio un sumidero propio. No sirve para comprobar si un lote escribio algo.
- **Una marca de tiempo del reloj local no filtra una columna que escribe el servidor.** Los contenedores corren en UTC y los scripts en hora local: `WHERE ingested_at >= '12:37:14'` se interpretaba como UTC, dos horas en el futuro, y devolvia percentiles a NULL, que parece "no hubo trafico". La marca se pide con `SELECT now()` a la propia base.
- **`EXTRACT(EPOCH ...)` devuelve NUMERIC y psycopg2 lo convierte en Decimal**, que `json.dumps` no serializa. El fallo aparece al final, con toda la medicion hecha y perdida.
- **Los fallos silenciosos son ausencias, y un log de actividad no las ve.** De los cinco encontrados midiendo, el log solo delató uno (el simulador con `fallidos` disparados); los otros cuatro se detectaron por casualidad: una tabla vacía, otra congelada, un recuento que no cuadraba. La respuesta no es escribir más líneas sino **comprobar invariantes**: `monitoring.py` vigila ahora los eventos descartados por watermark y el desfase de Kafka con cero consumo, y `prepare_ashrae.py` verifica la unicidad de la clave natural en cada ejecución.
- **Los logs del job de Spark van en UTC** y los del resto en hora local, porque el job fuerza `TZ=UTC` en su proceso. Al comparar `spark_job.log` con `bridge.log` hay dos horas de desfase que no son un fallo.
- **`lastProgress` no sirve para un KPI.** Sondeando cada 10 s con un trigger de 1 s se pierden nueve de cada diez lotes, y la mediana de una muestra con huecos no es la mediana de los lotes. `recentProgress` conserva los últimos 100 informes y permite deduplicar por `batchId`.
- **`with psycopg2.connect(...)` hace commit pero NO cierra la conexión.** En un hilo que escribe cada 10 segundos, eso es una fuga de sockets que tarda horas en manifestarse.
- **Compose hace word-splitting** cuando `command` es un string multilínea y se come las continuaciones `\`.
- **La API ccompat de Apicurio devuelve las violaciones de regla como HTTP 409**, con el detalle en el campo `message` (`RuleViolationException`). No confundir con la API nativa v3, que las devolvía como HTTP 400 con el campo `name`; `register_schema.py` usa ccompat desde el 25 de agosto de 2026 y detecta por `message`.
- **Grafana 13 rompe el datasource de PostgreSQL provisionado a la vieja usanza.** El aprovisionamiento declaraba `type: postgres` y `database:` a nivel superior, y con Grafana 13.0.6 eso da **todos los paneles en rojo** con «you do not currently have a default database configured». En las versiones modernas el tipo del plugin es `grafana-postgresql-datasource` (lo que ya declaraban los dashboards) y la base de datos va en **`jsonData.database`**, no en el nivel superior, que se ignora. Corregido el 26 de agosto de 2026 en `pipeline/docker/grafana/provisioning/datasources/timescaledb.yml`; verificado con `GET /api/datasources/uid/timescaledb/health` («Database Connection OK») y una consulta real por `/api/ds/query`. Este fallo hacía que los dashboards salieran vacíos aunque `telemetry_metrics` tuviera datos, porque los tres leen de TimescaleDB (el sumidero operacional); PostgreSQL alimenta Power BI, no Grafana.
- **Reiniciar el contenedor de Grafana invalida la sesión de quien lo tenga abierto.** Aunque `grafana.db` persiste en el volumen `grafana-data`, un `docker compose down`/`up` del contenedor (lo que hace `demo.py --stop` seguido de un arranque nuevo) rompe la sesión activa del navegador y fuerza a reintroducir `admin`/`admin`. Para una demo o defensa, donde el stack puede reiniciarse varias veces, eso es un riesgo innecesario. Resuelto habilitando **acceso anónimo de solo lectura** (`GF_AUTH_ANONYMOUS_ENABLED` + rol `Viewer`) en `pipeline/docker-compose.yml`: sin sesión que perder, un reinicio del contenedor no expulsa a quien está mirando el dashboard. El login con `admin`/`admin` sigue disponible para editar.

## Informe de tolerancia a fallos

**`pipeline/FAULT_HANDLING.md` reúne todo el comportamiento del sistema ante fallos**, medido y reproducible: qué se rechaza a la DLQ y con qué motivo, qué se aparta sin romper nada, cuánto tarda en recuperarse cada servicio al tumbarlo, las invariantes que vigilan los fallos silenciosos, el punto de saturación y **lo que sigue sin estar cubierto**. Es la referencia a consultar antes de tocar nada relacionado con validación, resiliencia o pérdida de datos, y el material de partida para esa parte de la memoria.

## Estado actual del pipeline

**El pipeline corre de extremo a extremo sobre ASHRAE.** Simulador → Mosquitto → bridge → Kafka → Spark → doble sumidero → Grafana, todo verificado y medido.

**Arranque (desde el 3 de septiembre de 2026, "Opción 2" del refactor del bridge):** `docker compose up -d` levanta el stack **incluido el registro del esquema y el bridge**. `register-schema` es un contenedor de un solo uso que ejecuta `register_schema.py` (reusa la imagen del bridge con otro entrypoint) y termina; el `bridge` declara `depends_on: register-schema` con `condition: service_completed_successfully`, así que **no puede arrancar sin el esquema registrado** — la regla dejó de ser procedimiento y pasó a estar garantizada por Compose. Se quitó `profiles: ["bridge"]`. La URL del registro sale de `APICURIO_URL` (`common/apicurio.py`; el compose pasa `http://apicurio:8080` al contenedor). **Solo Spark y el simulador quedan como procesos del host.** `reset_state.py` para y reanuda el contenedor del bridge por su cuenta al recrear los tópicos; `demo.py` ya no lanza esquema ni bridge; `failover_test.py` acepta `--target bridge`.

**Escalado horizontal del bridge (4 de septiembre de 2026, commit `96f1312`).** El bridge es el único componente stateless containerizado pensado para `--scale`. `docker compose -f pipeline/docker-compose.yml up -d --scale bridge=N` levanta N réplicas que se **reparten** el flujo MQTT en vez de duplicarlo, gracias a dos cosas: (1) `--shared-group=bridge-pool` en el `command` del bridge en `docker-compose.yml` → suscripción compartida `$share/bridge-pool/iot/#`; con una sola instancia es inerte (equivale a una suscripción normal). (2) `--client-id` por defecto se deriva del hostname del contenedor (`tfm-bridge-<hostname>`) → único por réplica y estable entre reinicios de esa réplica; antes era el literal fijo `tfm-bridge`, con el que N instancias colisionaban en la sesión persistente (`clean_session=False` → el broker desconecta la más antigua). El `--client-id` explícito se sigue respetando (para fijar `tfm-bridge` en las corridas de instancia única). Verificado e2e el 4-sep con `--scale bridge=3` y 15.000 eventos a 358 ev/s: reparto **5000 / 5000 / 5000**, 15.000 en `iot.telemetry.raw` (sin duplicación), 0 en la DLQ, 0 perdidos; `reset_state.py` paró y reanudó las 3 réplicas sin tocarlo. **No wired:** `failover_test.py --target bridge` sigue asumiendo una sola instancia; `demo.py` levanta 1. El escalado horizontal de los servicios con estado (Kafka multi-broker, réplicas de BD) es otro eje y queda para `TrabajoFuturo.tex` (k8s); se descartó montar un `docker-compose.cluster.yml` de 3 brokers KRaft por el coste (re-medir KPIs, reescribir capítulos) frente al valor en una sola máquina.

Funcionando: stack completo en Docker; preparación de datos (tres Parquet: hechos, dimensión de 498 edificios y línea base de 652 sensores), con **reubicación temporal al presente vía `prepare_ashrae.py --fecha-final`**; esquema Avro v1 registrado con sus reglas; simulador con selección de cola para la demo (`--ultimas-semanas`), sin ningún desplazamiento temporal en ejecución (las marcas vienen ya datadas del Parquet); bridge con validación y DLQ; job de Spark con broadcast join contra las dos tablas de referencia, agregación por ventana y escritura idempotente en ambos sumideros; tres dashboards de Grafana aprovisionados declarativamente.

**El simulador modela el parque, no un banco de pruebas.** Tres grupos de opciones y ninguna artificial: selección de datos, ritmo (`--acelerar`, factor de compresión global del reloj) y conexiones (`--clients`, por defecto **una conexión MQTT por sensor**; 646 simultáneas verificadas sin un fallo). Corre sobre asyncio con `aiomqtt`. Si no sostiene el ritmo pedido, marca la ejecución como no válida en lugar de recuperar el tiempo publicando a ráfagas.

**No se reproducen las ráfagas horarias, y el motivo es cuantitativo**: con un drenaje del bridge de ~4.000 ev/s, el replay fiel se sostiene hasta ×22.000; por encima, la cola de Mosquitto (10.000 mensajes) se llena en menos de tres segundos y el broker descarta en silencio. Los sensores se escalonan de forma determinista dentro del intervalo, que equivale a suponer relojes no sincronizados al milisegundo.

**Medición de KPIs, en `pipeline/tools/`** (agosto de 2026). Cuatro scripts que NO forman parte del pipeline: `reset_state.py` (estado limpio reproducible), `kpi_report.py` (cuadro completo en Markdown), `failover_test.py` (recuperación ante fallo) y `watermark_poison_test.py` (demuestra que un evento con fecha futura detiene la agregación de todos los sensores). El ciclo de medición está en `pipeline/tools/README.md`. **`load_ladder.py` se retiró el 31 de agosto de 2026** al redefinirse el Objetivo 5 (commit `6da4c19`): ya no exige "≥ 500 sensores concurrentes con degradación < 20%" sino solo recuperación ante fallo de un servicio en < 60 s sin pérdida, que cubre `failover_test.py`. La caracterización de saturación que hacía `load_ladder.py` queda como material histórico en `FAULT_HANDLING.md` §5 y en este fichero, no como prueba reproducible.

**Demostración en un comando, `pipeline/tools/demo.py`.** Orquesta el arranque completo para ver los dashboards de Grafana en vivo (levanta el stack, limpia las bases, registra el esquema, arranca bridge + Spark esperando a las consultas de streaming, y lanza el simulador). `--semanas N` publica la **cola** de las últimas N semanas del histórico (pasa `--ultimas-semanas N`); como el Parquet ya viene reubicado al presente por `--fecha-final`, esa cola termina en fecha reciente sin ningún desplazamiento en tiempo de ejecución. Por defecto 6 semanas y `--acelerar 8000`. `demo.py --stop` cierra los procesos y baja el stack. Es para demostrar, no para medir: los KPIs se toman con el ciclo de medición. Lanza los subprocesos con `sys.executable` (el python del venv) para no caer al python base sin `confluent_kafka`.

**`load_generator.py` se retiró y no debe volver.** Alcanzaba tasas altas con una ventana de mensajes en vuelo: N publicaciones sin confirmar desde un único cliente. Era un artificio para que un cliente hiciera el trabajo de 652, y `--clients` consigue lo mismo sin inventar nada. Se comprobó además que con ventana 1 se comportaba exactamente igual que el simulador: eran el mismo programa con dos nombres.

**Spark corre en `local[*]` por defecto, y es una limitación de recursos, no de diseño.** El job acepta `--master`, así que el mismo código corre distribuido sin tocar una línea: verificado el 21 de agosto de 2026 con `pipeline/tools/cluster.sh`, que levanta un master y N workers con `spark-class` (la distribución de PySpark por pip **no trae** `start-master.sh` en `sbin/`). Con la misma carga —40.000 eventos a 358 ev/s— y el mismo estado limpio:

| Modo | Latencia ingesta p95 | Lote `metricas` p95 | ¿El simulador sostuvo el ritmo? |
|---|---|---|---|
| `local[*]`, 12 núcleos | **1,190 s** ✓ | 891 ms | Sí |
| standalone, 4 núcleos | 14,302 s ✗ | 1.554 ms | Sí |
| standalone, 10 núcleos | 34,274 s ✗ | **3.194 ms** ✗ | **No: 9,5 s de retraso** |

**Cuanta más CPU se da al clúster, peor va todo**, porque el simulador con sus 648 conexiones, el bridge, el driver, los executors y siete contenedores comparten **una sola máquina de 12 núcleos**: con 10 para el clúster la carga llegó a 19,76 y la medición pasó a medir la máquina en vez del pipeline. En la nube esto no ocurriría —productor y clúster en máquinas distintas—, y la vía natural sería `--master k8s://` sobre Kubernetes, o EMR Serverless / EMR on EKS en AWS (Fargate encaja mal con el modelo driver/executor). Las mediciones de KPI se toman en `local[*]`; standalone se reserva para demostrar.

**El fallo de un executor sí está medido**: matando un worker con `kill -9`, la aplicación siguió `RUNNING` bajando de 4 a 2 núcleos, con **0 intervenciones del supervisor**, **~9 s** de interrupción y recuperación automática de los 4 núcleos al reponerlo. Es la única prueba de fallo que alcanza al motor de procesamiento, porque `failover_test.py` solo tumba contenedores.

**El job de Spark son tres módulos**, cada uno con una responsabilidad: `stream_processing.py` (qué se calcula), `database_writers.py` (cómo se persiste, con el UPSERT y sus reintentos) y `monitoring.py` (progreso de micro-lote y vigilancia de las consultas). Antes era un fichero de 871 líneas donde más de la mitad no era lógica de streaming.

**Código compartido, en `pipeline/common/`**: `logging_setup.py`, `connection_args.py`, `apicurio.py` (resolución del esquema por ccompat con el cliente de `confluent-kafka` y formato de cable de Confluent) y `stop_event.py`. La interpretación del dataset vive en `simulator/telemetry_dataset.py`, junto a su único consumidor. El criterio es que nada que usen dos piezas viva duplicado en ambas.

**Registro de actividad**: todos los procesos escriben en `pipeline/logs/<nombre>.log` además de por consola, con la orden completa en la cabecera de cada arranque. Rotan a 3 MB × 3. No se versionan.

**Reparto de cálculos, aplicado a propósito**: `prepare_ashrae.py` calcula la línea base una vez; `enrich` solo produce lo que consume la agregación de Grafana; PostgreSQL guarda lecturas crudas más las dos tablas de referencia; **Power BI deriva lo suyo con un join**. El criterio es: si el consumidor puede derivarlo con lo que ya tiene, que lo derive él. Por eso la tabla de eventos tiene seis columnas y ningún campo calculado.

**Código muerto**: `pipeline/data/convert_to_parquet.py` (dataset anterior), `pipeline/data/power_measurements_parquet/`.

## Evolución de esquema: resolución por mensaje (cadena `when`)

**Cambiado el 31 de agosto de 2026.** El job resolvía todo el flujo con un único esquema y la evolución se hacía con un procedimiento de "drenar y conmutar" (parar simulador → drenar → registrar → reiniciar bridge y job → reanudar). Ahora el job **decodifica cada mensaje con el esquema de su `schema_id`**, que es el comportamiento del deserializador estándar de Confluent y encaja con la fidelidad al patrón industrial que guía el trabajo (fuente: [blog de Confluent, "Best Practices for Confluent Schema Registry"](https://www.confluent.io/blog/best-practices-for-confluent-schema-registry/)).

- `common/apicurio.py::all_schemas()` resuelve `{schema_id: schema_str}` de **todas** las versiones registradas del subject al arrancar el job.
- `stream_processing.py::decode_events()` construye una **cadena `F.when()` en runtime**, una rama por `schema_id`: cada rama hace `from_avro` con su propio esquema (el del escritor, así que enums y campos casan exactos) y proyecta a los cinco campos del contrato comunes a toda versión (`_proyeccion_contrato`). Spark evalúa el valor de una rama `when` solo si su condición se cumple (corto-circuito), así que un mensaje v1 nunca se intenta decodificar con otro esquema.
- **Un `schema_id` no registrado DETIENE el job** (`F.raise_error` sobre `meter_reading`, columna que el optimizador no puede eliminar). El evento sigue en Kafka; se registra el esquema, se reinicia y se reanuda desde el checkpoint sin pérdida. Sustituye a la antigua función `guard_schema_version`.
- **Se descartó ABRiS** (`za.co.absa:abris`): no tiene release para Spark 4.x (verificado el 31 de agosto de 2026; la rama 6.x soporta Spark 3.2–3.5). La cadena `when` se queda en la API declarativa, sin dependencia JVM nueva.

**Limitación consciente:** el conjunto de esquemas se fija al arrancar; una versión registrada *después* no se reconoce hasta reiniciar el job. No es el acoplamiento de drenar-y-conmutar: es un reinicio de **un** servicio, sin drenar. El productor (bridge) también resuelve su esquema al arrancar con `latest_schema`, así que **desplegar el consumidor antes que el productor** garantiza que nunca haya en Kafka bytes de una versión que el job no conozca. v1, v2, v3… conviven en el tópico indefinidamente.

**Pendiente de validar de extremo a extremo:** el camino multi-versión solo se ha ejercitado con v1 (única versión registrada; la cadena `when` queda con una sola rama). No hay ningún `.avsc` de evolución en el repo —el antiguo `telemetry_event_v2.avsc` se borró el 31 de agosto de 2026 porque declaraba `building_id` como `int`, incompatible con la v1 actual—. Cuando se redacte la v2 para la demo del Objetivo 2 (v1 + un campo opcional con `default: null`), hay que hacer una pasada e2e con v1 y v2 conviviendo en el tópico.

## Pendiente de implementar

- **Los informes de Power BI los hace Boris**, y van despues de cerrar las pruebas del pipeline: necesita el esquema estable antes de empezar. Mi parte es que `telemetry_events`, `buildings` y `sensor_baseline` no se muevan y avisar si algun cambio las altera.
- Redactar `Desarrollo.tex` y `Resumen.tex`, que siguen siendo plantilla. El material de las trampas medidas de arriba va ahí.
- Retirar el código muerto cuando Boris lo confirme.

## Estilo de comunicación

Responde en español. Boris tiene nivel intermedio de Python/PySpark — explica conceptos avanzados cuando aparezcan, sin asumir experiencia previa con Spark en producción. Prefiere lenguaje preciso y verificable; evita afirmaciones sin respaldo o sobreclaims sobre rendimiento o capacidades sin haberlas probado. Si una premisa suya es incorrecta, díselo con los datos delante.
