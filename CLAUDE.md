# CLAUDE.md — Contexto del proyecto uea_tfm

Este archivo da contexto persistente a Claude Code para el desarrollo del pipeline de este trabajo. Léelo por completo antes de empezar a trabajar en cualquier tarea de código.

Este proyecto se desarrolla íntegramente en español, no obstante, la buśqueda de citaciones, marco teórico, datos adicionales puede hacerse en inglés y posteriormente traducirlo si serán incorporados al trabajo.

## Qué es este proyecto

TFM (Trabajo Fin de Máster) del Máster Universitario en Análisis de Grandes Volúmenes de Datos (Big Data), Universidad Europea de Madrid. Título: "Desarrollo de un sistema escalable de microservicios para el análisis de datos IoT". Entrega: septiembre de 2026.

El objetivo es implementar un pipeline IoT completo, containerizado y basado exclusivamente en herramientas open-source, siguiendo una **arquitectura Kappa** (flujo único reproducible, sin capa de lotes separada), aplicado a telemetría de consumo eléctrico industrial.

La documentación LaTeX del trabajo vive en `docs/` (capítulos en `docs/capitulos/`) y **no debe modificarse** desde Claude Code — esa parte se gestiona aparte, en Claude.ai. El foco de Claude Code es exclusivamente el **desarrollo del pipeline/código** (previsiblemente en una carpeta `pipeline/` a crear).

## Stack tecnológico definitivo (todo open-source, containerizado vía Docker)

- **Mosquitto** — broker MQTT (implementación de referencia de Eclipse Foundation). *No usar NanoMQ*: se evaluó porque supuestamente tenía puente nativo a Kafka, pero se verificó que esa función es exclusiva de EMQX Enterprise (de pago). Mosquitto + bridge propio es la decisión final.
- **Microservicio de bridge MQTT→Kafka** — propio, a implementar.
- **Apache Kafka** — en modo KRaft (sin Zookeeper).
- **Apicurio Schema Registry** — gobernanza de esquemas con serialización Avro.
- **Apache Spark Structured Streaming** — motor de procesamiento (micro-batch). **No usar Flink/PyFlink** — hubo un pivote de arquitectura documentado y justificado en el trabajo (ver "Decisiones de arquitectura" abajo). API a usar: DataFrame API con ventanas de agregación temporal y watermarking, no RDD ni DataStream API.
- **TimescaleDB** — métricas agregadas por ventana temporal → consumidas por Grafana.
- **PostgreSQL** — eventos individuales enriquecidos → consumidos por Power BI.

**Databricks está excluido** del pipeline (aunque haya probabilidad de ser usado en el análisis inicial de los datos).

## Decisiones de arquitectura y su razonamiento

- **Kafka + Spark Structured Streaming, no Kafka + Flink**: decisión tomada tras análisis de mercado laboral español (Spark/Databricks es la habilidad dominante y de default-hire; Flink aparece casi siempre como "nice-to-have" secundario) y de las características de los datos reales (telemetría con cadencia ~30s por sensor, sin huecos >60s → la ventaja de latencia sub-segundo de un motor de streaming nativo no aporta valor perceptible frente a micro-batch, dado que el propio proceso físico que genera los datos ya opera en la escala de decenas de segundos). El razonamiento completo, con las citas académicas que lo sostienen, está en `docs/capitulos/EstadoArte.tex` (sección "Criterios de selección de herramientas de procesamiento streaming").
- **Arquitectura Kappa, no Lambda**: un único camino de procesamiento sobre un log reproducible (Kafka), sin capa de lotes separada. El reprocesamiento histórico se hace reproduciendo el propio flujo.
- **Gobernanza de esquemas con Avro + Apicurio**: es uno de los aportes diferenciadores del trabajo frente a las implementaciones Kappa-IoT de referencia existentes (que no la incluyen). Debe soportar evolución de esquema compatible hacia adelante/atrás (ver Objetivo 2 abajo).
- **Doble sumidero (dual sink)**: separar el consumo operacional en tiempo casi real (TimescaleDB → Grafana) del consumo analítico de negocio (PostgreSQL → Power BI). También es un aporte diferenciador documentado en el trabajo.

## Estructura de tópicos MQTT (ya definida y validada)

```
iot/{company_id}/{site_id}/{machine_id}/telemetry
```

El simulador (`pipeline/simulator/mqtt_simulator.py`, si ya existe, revisar antes de tocar) incluye `sim_publish_ts` en el payload (epoch millis) para medir latencia extremo a extremo.

## Dataset de referencia

Kaggle "Power Telemetry" (khalilaraoui): ~1M filas, 20 columnas, telemetría de consumo eléctrico por machine_id/company/site/department. Hallazgos del EDA relevantes para el diseño (no deben aparecer citados en el trabajo, son solo contexto técnico): sin nulos, `event_id` 100% único, `timestamp` e `ingest_ts` son idénticos en cada fila (redundante), `voltage` constante en 230V, `power_watts` muy sesgado, cadencia de eventos ~30s por sensor sin huecos >60s, sin etiquetas de anomalía explícitas.

## KPIs objetivo (de `docs/capitulos/Objetivos.tex` — el código debe poder validarlos)

1. **Ingesta**: latencia extremo a extremo (publicación MQTT → disponible en TimescaleDB) < 2s (percentil 95); pérdida de mensajes < 0.1%.
2. **Gobernanza de esquema**: 100% de eventos validados contra el esquema Avro registrado; cero fallos de deserialización ante evolución de esquema compatible.
3. **Procesamiento**: latencia de micro-lote en Spark Structured Streaming < 3s tras cierre de ventana; throughput sostenido ≥ 50 eventos/segundo.
4. **Visualización**: refresco de dashboards Grafana < 5s; ≥ 3 reportes en Power BI (consumo energético, eficiencia operativa, detección de anomalías).
5. **Escalabilidad y resiliencia**: soporta ≥ 500 sensores simulados concurrentes con degradación de throughput < 20% respecto a una carga base de 100 sensores; recuperación ante fallo de un servicio individual < 60s sin pérdida de datos (gracias a la retención de Kafka).

## Entorno de desarrollo

- **venv + pip** (no conda). Dependencias en `pipeline/requirements.txt`; snapshot reproducible en `pipeline/requirements.lock.txt` (generar con `pip freeze`).
- **Java 21 LTS gestionado por SDKMAN** (ver `.sdkmanrc` en la raíz del repo) — única fuente de JDK del proyecto. PySpark 4.x requiere Java 17 o superior; no usar Java 11 (eso era para PyFlink, ya no aplica).
- Se migró de conda a venv precisamente para eliminar el conflicto de `PATH` entre el JDK de conda y el de SDKMAN que existía antes del pivote a Spark — con venv, SDKMAN es la única fuente de Java y no hay nada con quien competir.
- Setup: `bash pipeline/setup_env.sh` (crea `.venv/` e instala dependencias; verifica que Java esté disponible antes de continuar).
- VS Code + WSL2/Ubuntu 24.04.

## Convenciones y aprendizajes ya validados

- **Gestión de dependencias**: en `requirements.txt` se fijan versiones exactas para las dependencias directas; las transitivas (ej. `pyarrow`, que llega con `pyspark`) no se declaran a mano, se dejan resolver por pip.
- **Verificar capacidades técnicas antes de comprometerse a ellas** en el diseño (ej. el caso de NanoMQ/Kafka bridge): confirmar contra la documentación o el repositorio real, no asumir por marketing o menciones de terceros.
- **Archivos binarios (imágenes, PDFs) nunca se suben vía herramientas MCP** (se corrompen al tratarse como texto UTF-8) — si hace falta subir algo así a GitHub, indicárselo a Boris para que lo haga manualmente vía la interfaz web.

## Estilo de comunicación

Responde en español. Boris tiene nivel intermedio de Python/PySpark — explica conceptos avanzados cuando aparezcan, sin asumir experiencia previa con Spark en producción. Prefiere lenguaje preciso y verificable; evita afirmaciones sin respaldo o sobreclaims sobre rendimiento/capacidades sin haberlas probado.

## Pendiente de implementar

- `docker-compose.yml` con el stack completo (Mosquitto, Kafka KRaft, Apicurio, Spark, TimescaleDB, PostgreSQL) — no existe todavía.
- Microservicio de bridge MQTT→Kafka.
- Job de Spark Structured Streaming (procesamiento + ventanas + watermarking).
- Esquema Avro inicial de telemetría + configuración de compatibilidad en Apicurio.
- Dashboards de Grafana y reportes de Power BI.
- Suite de pruebas de carga/estrés y de tolerancia a fallos para validar el Objetivo 5.
