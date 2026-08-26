# TFM -- Sistema escalable de microservicios para el procesamiento y analisis de datos IoT

Trabajo de Fin de Master, Universidad Europea de Andalucia (UEA), Master
Universitario en Analisis de Grandes Volumenes de Datos (Big Data). Autor:
Boris Renee Paternina Perez. Director: Victor Gomez Guirado. Curso 2025-2026.

Implementa una arquitectura Kappa para IoT, integramente open-source y
containerizada, usando como caso de uso telemetria horaria **real** de
consumo energetico en edificios (electricidad, agua fria, vapor y agua
caliente), del dataset de la competicion Kaggle **ASHRAE Great Energy
Predictor III**:

    Simulador MQTT -> Mosquitto -> microservicio bridge MQTT-Kafka -> Kafka (KRaft) -> Apicurio (Avro) -> Spark Structured Streaming -> TimescaleDB / PostgreSQL -> Grafana / Power BI

Nota: Mosquitto no trae bridge nativo a Kafka (esa capacidad, en el
ecosistema EMQ, solo existe en EMQX Enterprise, de pago), por eso el puente
MQTT->Kafka es un microservicio propio dentro del pipeline.

El motor de procesamiento es **Spark Structured Streaming** (DataFrame API,
micro-batch), no Flink: la justificacion completa esta en la memoria
(`docs/capitulos/EstadoArte.tex`, "Criterios de seleccion de herramientas de
procesamiento streaming").

## Estado actual

El pipeline corre de extremo a extremo sobre el dataset real de ASHRAE:

- [x] Simulador MQTT (asyncio, una conexion por sensor) con escalera de carga y rebase temporal
- [x] Stack completo en Docker: Mosquitto, Kafka (KRaft), Apicurio, TimescaleDB, PostgreSQL, Grafana
- [x] Microservicio bridge MQTT-Kafka, con validacion de dominio y cola de mensajes muertos (DLQ)
- [x] Esquema Avro v1 registrado y gobernado (validez, y compatibilidad hacia atras y hacia adelante)
- [x] Job de Spark Structured Streaming: broadcast join, agregacion por ventana, escritura idempotente
- [x] Doble sumidero: TimescaleDB (metricas por ventana, casi en tiempo real) y PostgreSQL (eventos, para analitica)
- [x] Tres dashboards de Grafana aprovisionados de forma declarativa
- [x] Herramientas de medicion de KPIs y de resiliencia ante fallos (`pipeline/tools/`)
- [ ] Informes de Power BI (Objetivo 4) -- pendiente, va despues de cerrar las pruebas del pipeline

Que se rechaza y por que, tiempos de recuperacion medidos, y lo que sigue sin
cubrir: [`pipeline/FAULT_HANDLING.md`](pipeline/FAULT_HANDLING.md).

## Requisitos del sistema

Desarrollado y probado en Ubuntu 24.04 (WSL2). Deberia funcionar igual en
Linux nativo o macOS; en Windows nativo (fuera de WSL) no esta probado.

| Herramienta | Para que se usa | Notas |
|---|---|---|
| Docker + Docker Compose v2 | Levantar Mosquitto, Kafka, Apicurio, TimescaleDB, PostgreSQL y Grafana | `docker compose version` >= 2.20 recomendado |
| Python 3.11 + venv | Entorno del simulador y de los jobs de Spark | Se crea con `bash setup.sh`; dependencias en `pipeline/requirements.txt` |
| JDK 21 LTS (Temurin) | Requerido por PySpark 4.x (Java 17 o superior) | Gestionado por SDKMAN, ver `.sdkmanrc`; es la unica fuente de JDK de este entorno de desarrollo |
| Git | Clonar el repo | -- |
| `mosquitto-clients` (`mosquitto_sub` / `mosquitto_pub`) | Inspeccionar manualmente los mensajes MQTT durante desarrollo/depuracion | Opcional: el contenedor `tfm-mosquitto` ya los trae (`docker exec tfm-mosquitto mosquitto_sub ...`). Para tenerlos en el host: `sudo apt install mosquitto-clients` |
| Cuenta de Kaggle (gratuita) | Solo una vez, para descargar el dataset | Ver `pipeline/data/README.md` |

El proyecto usa **venv + pip**, no conda. La migracion se hizo al pivotar a
Spark: con venv, SDKMAN es la unica fuente de Java y desaparece el conflicto
de `PATH` que provocaba el JDK que instalaba conda.

### Si no quieres instalar SDKMAN

SDKMAN no es un requisito tecnico del pipeline: `setup.sh` solo
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

2. Ejecutar el script de preparacion del entorno:

       bash setup.sh

   Es idempotente y portable (Linux/WSL y macOS, Apple Silicon e Intel):
   comprueba Docker y Java -- **no los instala**, son herramientas de sistema
   y se guia en vez de automatizar, con las alternativas sin SDKMAN descritas
   mas arriba --, crea el entorno virtual e instala las dependencias Python
   (reutiliza lo que ya exista en vez de rehacerlo), y si encuentra
   credenciales de Kaggle en `~/.kaggle/kaggle.json` descarga y prepara el
   dataset ASHRAE automaticamente. Si no las encuentra, imprime como
   obtenerlas y deja el resto listo para repetir el script despues.

       source .venv/bin/activate

3. Levantar el stack completo -- Mosquitto, Kafka, Apicurio, TimescaleDB,
   PostgreSQL, Grafana (detalle de servicios, puertos y comprobaciones en
   `pipeline/README.md`):

       docker compose -f pipeline/docker-compose.yml up -d
       docker compose -f pipeline/docker-compose.yml ps -a

4. **Camino rapido -- ver los dashboards funcionando:**

       python pipeline/tools/demo.py --semanas 6

   Deja las bases limpias, registra el esquema, arranca el bridge y el job de
   Spark, y lanza el simulador con datos reales de ASHRAE traidos al presente
   (las ultimas 6 semanas terminan en "ahora"). Al acabar imprime la URL de
   Grafana -- sin necesidad de credenciales, acceso anonimo de solo lectura --
   y el rango de tiempo que poner en cada dashboard. Para cerrarlo todo:

       python pipeline/tools/demo.py --stop

   Detalle completo en [`pipeline/tools/README.md`](pipeline/tools/README.md).

5. **Camino manual -- para desarrollo o para medir los KPIs**, con el stack
   arriba y el esquema registrado, cada pieza en su propia terminal:

       python pipeline/schemas/register_schema.py
       python pipeline/bridge/mqtt_kafka_bridge.py
       python pipeline/spark/stream_processing.py --trigger "1 second"
       python pipeline/simulator/mqtt_simulator.py --acelerar 2000 --limite 50000 --traer-a now

   El ciclo completo de medicion de KPIs (estado limpio, carga, informe) esta
   en [`pipeline/tools/README.md`](pipeline/tools/README.md).

## Estructura del repo

    setup.sh                 Prepara el entorno: Docker/Java (comprueba y guia),
                              venv + dependencias, dataset ASHRAE
    docs/                    Memoria del TFM (LaTeX)
    references/              TFM de ejemplo usados como referencia de estilo
    pipeline/
      README.md              Stack containerizado: servicios, puertos, comprobaciones
      FAULT_HANDLING.md      Comportamiento del sistema ante fallos, medido
      docker-compose.yml     Mosquitto, Kafka (KRaft), Apicurio, TimescaleDB, PostgreSQL, Grafana
      docker/                Configuracion montada en los contenedores (mosquitto.conf, provisioning de Grafana)
      requirements.txt       Dependencias Python del pipeline (venv + pip)
      data/                  Preparacion del dataset (Kaggle -> Parquet)
      schemas/               Esquema Avro y su registro gobernado en Apicurio
      simulator/             Simulador MQTT de telemetria
      bridge/                Microservicio MQTT -> Kafka, con validacion de dominio y DLQ
      common/                Codigo compartido entre el bridge, Spark y las herramientas
      spark/                 Job de Spark Structured Streaming: procesamiento y doble sumidero
      tools/                 Medicion de KPIs, pruebas de resiliencia y demo.py
