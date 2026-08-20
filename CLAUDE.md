# CLAUDE.md — Contexto del proyecto uea_tfm

Este archivo da contexto persistente a Claude Code para el desarrollo del pipeline de este trabajo. Léelo por completo antes de empezar a trabajar en cualquier tarea de código.

Este proyecto se desarrolla íntegramente en español, no obstante, las búsquedas de citaciones, marco teórico, datos adicionales, pueden hacerse en inglés y posteriormente traducirlos si serán incorporados al trabajo.

## Qué es este proyecto

TFM (Trabajo Fin de Máster) del Máster Universitario en Análisis de Grandes Volúmenes de Datos (Big Data), Universidad Europea de Andalucía (UEA). Título: "Desarrollo de un sistema escalable de microservicios para el análisis de datos IoT". Entrega: septiembre de 2026.

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

**El parque real genera 0,1797 ev/s.** Verificado sobre el Parquet: cada contador mide una vez por hora —mediana y p95 del intervalo son exactamente 3.600 s, sin dispersión— y los 652 contadores tienen datos en el 99,2% de las 8.784 horas de 2016. El caso de uso completo son **menos de dos décimas de evento por segundo**, y de ahí que el simulador acelere el reloj: `tasa = n_sensores × speedup / 3600`. A `--speedup 2000` cada sensor mide cada 1,8 s y el parque entero da 359 ev/s.

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

**Formato de cable**: byte mágico `0x00` + `globalId` de 4 bytes big-endian + payload Avro schemaless. Definido en `pipeline/common/schema_registry.py` y compartido por productor y consumidor.

## KPIs objetivo (de `docs/capitulos/Objetivos.tex`)

1. **Ingesta**: latencia extremo a extremo < 2s (p95); pérdida de mensajes < 0.1%.
2. **Gobernanza de esquema**: 100% de eventos validados contra el esquema Avro registrado; cero fallos de deserialización ante evolución compatible.
3. **Procesamiento**: latencia de micro-lote < 3s tras cierre de ventana; throughput sostenido ≥ 50 eventos/segundo.
4. **Visualización**: refresco de dashboards Grafana < 5s; ≥ 3 reportes en Power BI (consumo energético, eficiencia operativa, detección de anomalías).
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

## Entorno de desarrollo

- **venv + pip** (no conda). Dependencias en `pipeline/requirements.txt`. `aiomqtt` es la que permite al simulador mantener una conexión por sensor: con paho serían 652 hilos, con aiomqtt son 652 corrutinas en uno solo.
- **Java 21 LTS gestionado por SDKMAN** (ver `.sdkmanrc`) — única fuente de JDK. PySpark 4.x requiere Java 17+.
- Setup: `bash pipeline/setup_env.sh`.
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
- **Un evento con fecha futura envenena el watermark** y hace que los eventos ordenados posteriores se descarten en silencio como tardíos.
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
- **Un `--rebase-end now` repetido inutiliza la agregacion por ventana.** El watermark es monotono: una vez en "ahora", cada ejecucion nueva publica eventos que cubren las horas ANTERIORES a ese instante y Spark los descarta como tardios. `telemetry_metrics` se queda a cero mientras la ruta de eventos, que no tiene watermark, sigue llenandose con normalidad.
- **`sink_num_output_rows` vale -1 con `foreachBatch`**: Spark no sabe cuantas filas escribio un sumidero propio. No sirve para comprobar si un lote escribio algo.
- **Una marca de tiempo del reloj local no filtra una columna que escribe el servidor.** Los contenedores corren en UTC y los scripts en hora local: `WHERE ingested_at >= '12:37:14'` se interpretaba como UTC, dos horas en el futuro, y devolvia percentiles a NULL, que parece "no hubo trafico". La marca se pide con `SELECT now()` a la propia base.
- **`EXTRACT(EPOCH ...)` devuelve NUMERIC y psycopg2 lo convierte en Decimal**, que `json.dumps` no serializa. El fallo aparece al final, con toda la medicion hecha y perdida.
- **Los fallos silenciosos son ausencias, y un log de actividad no las ve.** De los cinco encontrados midiendo, el log solo delató uno (el simulador con `fallidos` disparados); los otros cuatro se detectaron por casualidad: una tabla vacía, otra congelada, un recuento que no cuadraba. La respuesta no es escribir más líneas sino **comprobar invariantes**: `monitoring.py` vigila ahora los eventos descartados por watermark y el desfase de Kafka con cero consumo, y `prepare_ashrae.py` verifica la unicidad de la clave natural en cada ejecución.
- **Los logs del job de Spark van en UTC** y los del resto en hora local, porque el job fuerza `TZ=UTC` en su proceso. Al comparar `spark_job.log` con `bridge.log` hay dos horas de desfase que no son un fallo.
- **`lastProgress` no sirve para un KPI.** Sondeando cada 10 s con un trigger de 1 s se pierden nueve de cada diez lotes, y la mediana de una muestra con huecos no es la mediana de los lotes. `recentProgress` conserva los últimos 100 informes y permite deduplicar por `batchId`.
- **`with psycopg2.connect(...)` hace commit pero NO cierra la conexión.** En un hilo que escribe cada 10 segundos, eso es una fuga de sockets que tarda horas en manifestarse.
- **Compose hace word-splitting** cuando `command` es un string multilínea y se come las continuaciones `\`.
- **Apicurio devuelve las violaciones de regla como HTTP 400**, no 409; detectarlas por el campo `name` del cuerpo.

## Informe de tolerancia a fallos

**`pipeline/FAULT_HANDLING.md` reúne todo el comportamiento del sistema ante fallos**, medido y reproducible: qué se rechaza a la DLQ y con qué motivo, qué se aparta sin romper nada, cuánto tarda en recuperarse cada servicio al tumbarlo, las invariantes que vigilan los fallos silenciosos, el punto de saturación y **lo que sigue sin estar cubierto**. Es la referencia a consultar antes de tocar nada relacionado con validación, resiliencia o pérdida de datos, y el material de partida para esa parte de la memoria.

## Estado actual del pipeline

**El pipeline corre de extremo a extremo sobre ASHRAE.** Simulador → Mosquitto → bridge → Kafka → Spark → doble sumidero → Grafana, todo verificado y medido.

Funcionando: stack completo en Docker; preparación de datos (tres Parquet: hechos, dimensión de 498 edificios y línea base de 652 sensores); esquema Avro v1 registrado con sus reglas; simulador con escalera de carga (`--max-sensors`) y rebase temporal (`--rebase-end`); bridge con validación y DLQ; job de Spark con broadcast join contra las dos tablas de referencia, agregación por ventana y escritura idempotente en ambos sumideros; tres dashboards de Grafana aprovisionados declarativamente.

**El simulador modela el parque, no un banco de pruebas.** Tres grupos de opciones y ninguna artificial: selección de datos, ritmo (`--speedup`, factor POR SENSOR) y conexiones (`--clients`, por defecto **una conexión MQTT por sensor**; 646 simultáneas verificadas sin un fallo). Corre sobre asyncio con `aiomqtt`. Si no sostiene el ritmo pedido, marca la ejecución como no válida en lugar de recuperar el tiempo publicando a ráfagas.

**No se reproducen las ráfagas horarias, y el motivo es cuantitativo**: con un drenaje del bridge de ~4.000 ev/s, el replay fiel se sostiene hasta ×22.000; por encima, la cola de Mosquitto (10.000 mensajes) se llena en menos de tres segundos y el broker descarta en silencio. Los sensores se escalonan de forma determinista dentro del intervalo, que equivale a suponer relojes no sincronizados al milisegundo.

**Medición de KPIs, en `pipeline/tools/`** (agosto de 2026). Cuatro scripts que NO forman parte del pipeline: `reset_state.py` (estado limpio reproducible), `load_ladder.py` (escalera del Objetivo 5, midiendo en el consumo), `kpi_report.py` (cuadro completo en Markdown) y `failover_test.py` (recuperación ante fallo). El ciclo de medición está en `pipeline/tools/README.md`.

**`load_generator.py` se retiró y no debe volver.** Alcanzaba tasas altas con una ventana de mensajes en vuelo: N publicaciones sin confirmar desde un único cliente. Era un artificio para que un cliente hiciera el trabajo de 652, y `--clients` consigue lo mismo sin inventar nada. Se comprobó además que con ventana 1 se comportaba exactamente igual que el simulador: eran el mismo programa con dos nombres.

**El job de Spark son tres módulos**, cada uno con una responsabilidad: `stream_processing.py` (qué se calcula), `database_writers.py` (cómo se persiste, con el UPSERT y sus reintentos) y `monitoring.py` (progreso de micro-lote y vigilancia de las consultas). Antes era un fichero de 871 líneas donde más de la mitad no era lógica de streaming.

**Código compartido, en `pipeline/common/`**: `logging_setup.py`, `connection_args.py`, `apicurio.py` y `stop_event.py`. La interpretación del dataset vive en `simulator/telemetry_dataset.py`, junto a su único consumidor. El criterio es que nada que usen dos piezas viva duplicado en ambas.

**Registro de actividad**: todos los procesos escriben en `pipeline/logs/<nombre>.log` además de por consola, con la orden completa en la cabecera de cada arranque. Rotan a 3 MB × 3. No se versionan.

**Reparto de cálculos, aplicado a propósito**: `prepare_ashrae.py` calcula la línea base una vez; `enrich` solo produce lo que consume la agregación de Grafana; PostgreSQL guarda lecturas crudas más las dos tablas de referencia; **Power BI deriva lo suyo con un join**. El criterio es: si el consumidor puede derivarlo con lo que ya tiene, que lo derive él. Por eso la tabla de eventos tiene seis columnas y ningún campo calculado.

**Código muerto**: `pipeline/data/convert_to_parquet.py` (dataset anterior), `pipeline/data/power_measurements_parquet/`.

## Evolución de esquema: drenar y conmutar

El job deserializa todo el flujo con **un único esquema**. En lugar de resolución por mensaje, se adopta un procedimiento que evita la coexistencia: parar el simulador → esperar a que bridge y job alcancen el final → registrar la versión nueva → reiniciar bridge → reiniciar job → reanudar. Segundos de interrupción y cero pérdida. Si el procedimiento se ejecuta mal, el job **se detiene** en lugar de descartar.

## Pendiente de implementar

- **Los informes de Power BI los hace Boris**, y van despues de cerrar las pruebas del pipeline: necesita el esquema estable antes de empezar. Mi parte es que `telemetry_events`, `buildings` y `sensor_baseline` no se muevan y avisar si algun cambio las altera.
- Redactar `Desarrollo.tex` y `Resumen.tex`, que siguen siendo plantilla. El material de las trampas medidas de arriba va ahí.
- Retirar el código muerto cuando Boris lo confirme.

## Estilo de comunicación

Responde en español. Boris tiene nivel intermedio de Python/PySpark — explica conceptos avanzados cuando aparezcan, sin asumir experiencia previa con Spark en producción. Prefiere lenguaje preciso y verificable; evita afirmaciones sin respaldo o sobreclaims sobre rendimiento o capacidades sin haberlas probado. Si una premisa suya es incorrecta, díselo con los datos delante.
