# TFM -- Sistema escalable de microservicios para el procesamiento de datos IoT

Trabajo de Fin de Master, Universidad Europea de Andalucia (UEA), Master
Universitario en Analisis de Grandes Volumenes de Datos (Big Data). Autor:
Boris Renee Paternina Perez. Director: Victor Gomez Guirado. Curso 2025-2026.

Implementa una arquitectura Kappa para IoT, integramente open-source y
containerizada, usando como caso de uso telemetria de consumo electrico en
equipos de oficina e industriales:

    Simulador MQTT -> Mosquitto -> microservicio bridge MQTT-Kafka -> Kafka (KRaft) -> Apicurio (Avro) -> Spark Structured Streaming -> TimescaleDB / PostgreSQL -> Grafana / Power BI

Nota: Mosquitto no trae bridge nativo a Kafka (esa capacidad, en el
ecosistema EMQ, solo existe en EMQX Enterprise, de pago), por eso el puente
MQTT->Kafka es un microservicio propio dentro del pipeline.

El motor de procesamiento es **Spark Structured Streaming** (DataFrame API,
micro-batch), no Flink: la justificacion completa esta en la memoria
(`docs/capitulos/EstadoArte.tex`, "Criterios de seleccion de herramientas de
procesamiento streaming").

## Estado actual

- [x] Simulador MQTT basico (sincrono), validado contra el broker Mosquitto del stack
- [x] Docker Compose con Mosquitto + Kafka (KRaft) + Apicurio Schema Registry
- [ ] Microservicio bridge MQTT-Kafka
- [ ] Esquema Avro v1 registrado en Apicurio (el registro ya levanta; falta el esquema)
- [ ] Job de Spark Structured Streaming minimo end-to-end
- [ ] Sinks: TimescaleDB (metricas por ventana) y PostgreSQL (eventos enriquecidos)
- [ ] Dashboards Grafana / Power BI
- [ ] Pruebas de carga y de evolucion de esquema

## Requisitos del sistema

Desarrollado y probado en Ubuntu 24.04 (WSL2). Deberia funcionar igual en
Linux nativo o macOS; en Windows nativo (fuera de WSL) no esta probado.

| Herramienta | Para que se usa | Notas |
|---|---|---|
| Docker + Docker Compose v2 | Levantar Mosquitto, Kafka, Apicurio y (mas adelante) TimescaleDB, PostgreSQL, Grafana | `docker compose version` >= 2.20 recomendado |
| Python 3.11 + venv | Entorno del simulador y de los jobs de Spark | Se crea con `bash pipeline/setup_env.sh`; dependencias en `pipeline/requirements.txt` |
| JDK 21 LTS (Temurin) | Requerido por PySpark 4.x (Java 17 o superior) | Gestionado por SDKMAN, ver `.sdkmanrc`; es la unica fuente de JDK de este entorno de desarrollo |
| Git | Clonar el repo | -- |
| `mosquitto-clients` (`mosquitto_sub` / `mosquitto_pub`) | Inspeccionar manualmente los mensajes MQTT durante desarrollo/depuracion | Opcional: el contenedor `tfm-mosquitto` ya los trae (`docker exec tfm-mosquitto mosquitto_sub ...`). Para tenerlos en el host: `sudo apt install mosquitto-clients` |
| Cuenta de Kaggle (gratuita) | Solo una vez, para descargar el dataset | Ver `pipeline/data/README.md` |

El proyecto usa **venv + pip**, no conda. La migracion se hizo al pivotar a
Spark: con venv, SDKMAN es la unica fuente de Java y desaparece el conflicto
de `PATH` que provocaba el JDK que instalaba conda.

### Si no quieres instalar SDKMAN

SDKMAN no es un requisito tecnico del pipeline: `pipeline/setup_env.sh` solo
comprueba que el comando `java` resuelva en el `PATH` a una version 17 o
superior, sin importar como haya llegado ahi. `.sdkmanrc` simplemente deja de
tener efecto si no usas SDKMAN. Alternativas equivalentes, verificadas para
macOS (Apple Silicon e Intel) y Linux:

- **Homebrew** (macOS): `brew install openjdk@21` — hay *bottle* precompilado
  para ambas arquitecturas, no compila nada. Tras instalar, enlaza el JDK para
  que el sistema lo encuentre: `sudo ln -sfn $HOMEBREW_PREFIX/opt/openjdk@21/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-21.jdk`
- **Instalador `.pkg` de [Adoptium](https://adoptium.net)**: el mismo binario
  Temurin 21 que instala SDKMAN, pero con instalador grafico, sin terminal.
- **conda**: `conda install -c conda-forge openjdk=21`

Cualquier distribucion de JDK 17+ (Temurin, Corretto, Zulu, la de Homebrew...)
funciona igual: PySpark no exige una distribucion concreta.

**No hace falta cuenta ni acceso a Databricks para ejecutar el pipeline.**
Databricks se uso unicamente durante el desarrollo como almacen de
referencia puntual para inspeccionar el dataset; no forma parte de la
arquitectura final ni de estos requisitos. El dataset se obtiene
directamente de Kaggle (ver `pipeline/data/README.md`).

## Puesta en marcha

1. Clonar el repo:

       git clone https://github.com/paterninausa/uea_tfm.git
       cd uea_tfm

2. Preparar Java 21 (si usas SDKMAN; alternativas sin SDKMAN mas arriba) y
   crear el entorno virtual. El script no instala Java: solo verifica que
   este disponible antes de continuar.

       sdk env install     # instala Java 21.0.9-tem si no la tienes
       sdk env             # la activa para este repo
       bash pipeline/setup_env.sh
       source .venv/bin/activate

3. Obtener el dataset (una sola vez -- detalle completo en
   `pipeline/data/README.md`):

       cd pipeline/data
       pip install -r requirements.txt
       kaggle datasets download -d khalilaraoui/power-telemetry -f Power_measurements.xlsx -p ./raw
       python convert_to_parquet.py --input ./raw/Power_measurements.xlsx --output ./power_measurements.parquet
       cd ../..

4. Levantar el stack de ingesta (detalle de servicios, puertos y
   comprobaciones en `pipeline/README.md`):

       docker compose -f pipeline/docker-compose.yml up -d
       docker compose -f pipeline/docker-compose.yml ps -a

5. Probar el simulador contra el broker del stack (detalle de las 2
   terminales en `pipeline/simulator/README.md`):

       docker exec tfm-mosquitto mosquitto_sub -h localhost -t 'iot/#' -v   # terminal 1
       cd pipeline/simulator                                                 # terminal 2
       python mqtt_simulator.py \
           --parquet-path ../data/power_measurements.parquet \
           --broker-host localhost --broker-port 1883 \
           --rate 20 --limit 5000

## Estructura del repo

    docs/                    Memoria del TFM (LaTeX)
    references/              TFM de ejemplo usados como referencia de estilo
    pipeline/
      README.md              Stack containerizado: servicios, puertos, comprobaciones
      docker-compose.yml     Mosquitto + Kafka (KRaft) + Apicurio
      docker/                Configuracion montada en los contenedores (mosquitto.conf)
      requirements.txt       Dependencias Python del pipeline (venv + pip)
      setup_env.sh           Crea el .venv e instala dependencias (verifica Java)
      data/                  Preparacion del dataset (Kaggle -> Parquet)
      simulator/             Simulador MQTT de telemetria
