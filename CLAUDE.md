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

**Clave natural**: `(building_id, meter_type, timestamp)`. Verificada única (5.682.185 grupos para 5.682.185 filas). Es lo que hace idempotente el reprocesamiento del log de Kafka. `(building_id, timestamp)` **no basta**: colapsaría 1.345.428 eventos, porque un edificio tiene varios contadores midiendo a la misma hora.

**Clave del mensaje en Kafka**: solo `(building_id, meter_type)` — el sensor, 652 valores. Mantiene en orden y en la misma partición las lecturas de cada contador, que es lo que necesitan las ventanas de Spark. No confundirla con la anterior.

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

**El KPI 3 no discrimina, y conviene decirlo en el texto.** El caso de uso real exige **0,18 ev/s** (652 contadores × 1 lectura/hora), así que el umbral de 50 ev/s ya está 276× por encima de la necesidad: procede de la literatura, no del problema. Subirlo sería inventar un número; lo que aporta valor es la caracterización. Y ahí falta lo importante: **los 1.284 ev/s son el techo del SIMULADOR**, que confirma cada publicación con QoS 1 antes de emitir la siguiente. El bridge procesaba a 605 ev/s recibiendo 269 y Spark nunca acumuló retraso: **el punto de saturación del pipeline no se ha determinado.**

## Entorno de desarrollo

- **venv + pip** (no conda). Dependencias en `pipeline/requirements.txt`.
- **Java 21 LTS gestionado por SDKMAN** (ver `.sdkmanrc`) — única fuente de JDK. PySpark 4.x requiere Java 17+.
- Setup: `bash pipeline/setup_env.sh`.
- Los conectores JVM de Spark (Kafka, Avro, JDBC PostgreSQL) se resuelven de Maven Central en el primer arranque y quedan en **`~/.ivy2.5.2`** (Spark 4.x usa directorio versionado, no `~/.ivy2`). No borrar esa caché.
- VS Code + WSL2/Ubuntu 24.04. `.vscode/settings.json` **sí se versiona**: apunta al intérprete de `.venv` para que Pylance resuelva los imports.

## Convenciones y aprendizajes validados

- **Verificar antes de comprometerse**, contra la documentación o el sistema real, no por marketing ni de memoria. Ha evitado errores reales (NanoMQ) y ha destapado otros (ver abajo).
- **Gestión de dependencias**: versiones exactas para dependencias directas. `pandas` y `pyarrow` **se declaran explícitamente**: se comprobó que `pip install pyspark` NO los instala (solo llegan con extras).
- **Fallar ruidosamente, nunca descartar en silencio.** Con 7 días de retención en Kafka, detenerse es recuperable y descartar es irreversible.
- **Comprobar las invariantes en cada ejecución** en lugar de asumirlas (`prepare_ashrae.py` verifica la unicidad de la clave natural; `register_schema.py` verifica el orden de los enums).
- **Archivos binarios nunca se suben vía herramientas MCP** (se corrompen al tratarse como texto UTF-8).

### Trampas encontradas midiendo (material para el capítulo de desarrollo)

- **Apicurio NO protege el orden de los símbolos de un enum.** Ni con `FULL_TRANSITIVE`. Avro codifica el enum como índice, así que reordenar corrompe en silencio los datos ya escritos: verificado, `electricity` se lee como `chilledwater` sin ninguna excepción. La regla la implementa `register_schema.py`, no el registro.
- **`spark.sql.session.timeZone=UTC` no gobierna la conversión a objetos Python.** `collect()` devuelve `datetime` naive en la zona del sistema del driver. Con la máquina en Europe/Madrid, cada evento se escribía con una hora de más, y de forma variable según el horario de verano. Se fuerza `TZ=UTC` en el proceso antes de crear la sesión.
- **El UPSERT exige deduplicar dentro del micro-lote**: PostgreSQL aborta con `CardinalityViolation` ante una clave repetida en la misma sentencia, y las repeticiones son normales con garantía at-least-once.
- **Un evento con fecha futura envenena el watermark** y hace que los eventos ordenados posteriores se descarten en silencio como tardíos.
- **`ingested_at` debe refrescarse en el `ON CONFLICT DO UPDATE`.** Si no, al reescribir una fila se actualiza `sim_publish_ts` pero no el instante de ingesta, y la latencia calculada sale **negativa**: la fila parece escrita antes de publicarse. Se manifiesta siempre que se reprocesa el log de Kafka. Corregirlo bajó la medición del agregado de 10,25 a 6,92 s; buena parte de aquella cifra era medición contaminada.
- **Una escalera de carga en orden creciente miente por calentamiento.** La primera pasada sugería que el throughput *mejora* al añadir sensores (1.062 → 1.332 ev/s). Repitiendo el primer peldaño al final, en caliente, la degradación real resultó ser del 0,6%. Hay que controlar el orden o repetir el peldaño base.
- **Compose hace word-splitting** cuando `command` es un string multilínea y se come las continuaciones `\`.
- **Apicurio devuelve las violaciones de regla como HTTP 400**, no 409; detectarlas por el campo `name` del cuerpo.

## Estado actual del pipeline

**El pipeline corre de extremo a extremo sobre ASHRAE.** Simulador → Mosquitto → bridge → Kafka → Spark → doble sumidero → Grafana, todo verificado y medido.

Funcionando: stack completo en Docker; preparación de datos (tres Parquet: hechos, dimensión de 498 edificios y línea base de 652 sensores); esquema Avro v1 registrado con sus reglas; simulador con escalera de carga (`--max-sensors`) y rebase temporal (`--rebase-end`); bridge con validación y DLQ; job de Spark con broadcast join contra las dos tablas de referencia, agregación por ventana y escritura idempotente en ambos sumideros; tres dashboards de Grafana aprovisionados declarativamente.

**Reparto de cálculos, aplicado a propósito**: `prepare_ashrae.py` calcula la línea base una vez; `enrich` solo produce lo que consume la agregación de Grafana; PostgreSQL guarda lecturas crudas más las dos tablas de referencia; **Power BI deriva lo suyo con un join**. El criterio es: si el consumidor puede derivarlo con lo que ya tiene, que lo derive él. Por eso la tabla de eventos tiene seis columnas y ningún campo calculado.

**Código muerto**: `pipeline/data/convert_to_parquet.py` (dataset anterior), `pipeline/data/power_measurements_parquet/`.

## Evolución de esquema: drenar y conmutar

El job deserializa todo el flujo con **un único esquema**. En lugar de resolución por mensaje, se adopta un procedimiento que evita la coexistencia: parar el simulador → esperar a que bridge y job alcancen el final → registrar la versión nueva → reiniciar bridge → reiniciar job → reanudar. Segundos de interrupción y cero pérdida. Si el procedimiento se ejecuta mal, el job **se detiene** en lugar de descartar.

## Pendiente de implementar

- **Informes de Power BI** (Objetivo 4): tiene en PostgreSQL las lecturas crudas, `buildings` y `sensor_baseline`, con eso puede calcular intensidad energética, atípicos y umbrales ajustables sin tocar el pipeline.
- **Prueba formal de recuperación ante fallo** (Objetivo 5): verificada por separado para el bridge (sesión MQTT persistente) y para Spark (checkpoint), pero no como una prueba única y reproducible con su tiempo de recuperación medido.
- **Determinar el punto de saturación real** con un generador de carga asíncrono, que no bloquee esperando confirmación en cada mensaje.
- Redactar `Desarrollo.tex` y `Resumen.tex`, que siguen siendo plantilla. El material de las trampas medidas de arriba va ahí.
- Retirar el código muerto cuando Boris lo confirme.

## Estilo de comunicación

Responde en español. Boris tiene nivel intermedio de Python/PySpark — explica conceptos avanzados cuando aparezcan, sin asumir experiencia previa con Spark en producción. Prefiere lenguaje preciso y verificable; evita afirmaciones sin respaldo o sobreclaims sobre rendimiento o capacidades sin haberlas probado. Si una premisa suya es incorrecta, díselo con los datos delante.
