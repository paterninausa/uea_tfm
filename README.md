# TFM -- Sistema escalable de microservicios para el procesamiento de datos IoT

Trabajo de Fin de Master, Universidad Europea de Andalucia (UEM), Master en
Analisis de Grandes Volumenes de Datos. Autor: Boris Renee Paternina Perez.
Director: Victor Gomez Guirado. Curso 2025-2026.

Implementa una arquitectura Kappa para IoT, integramente open-source y
containerizada, usando como caso de uso telemetria de consumo electrico en
equipos de oficina e industriales:

    Simulador MQTT -> NanoMQ -> Kafka (KRaft) -> Apicurio (Avro) -> PyFlink -> TimescaleDB / PostgreSQL -> Grafana / Power BI

## Estado actual

- [x] Simulador MQTT basico (sincrono), validado contra un broker Mosquitto local
- [ ] Docker Compose con NanoMQ + Kafka (bridge MQTT -> Kafka)
- [ ] Esquema Avro v1 + Apicurio Schema Registry
- [ ] Job PyFlink (DataStream API) minimo end-to-end
- [ ] Dashboards Grafana / Power BI
- [ ] Pruebas de carga y de evolucion de esquema

## Requisitos del sistema

Desarrollado y probado en Ubuntu 24.04 (WSL2). Deberia funcionar igual en
Linux nativo o macOS; en Windows nativo (fuera de WSL) no esta probado.

| Herramienta | Para que se usa | Notas |
|---|---|---|
| Docker + Docker Compose v2 | Levantar NanoMQ, Kafka, Apicurio, PyFlink, TimescaleDB, PostgreSQL, Grafana | `docker compose version` >= 2.20 recomendado |
| Conda / Miniforge | Entorno Python del simulador y de los jobs PyFlink | Ver `pipeline/environment.yml` y `pipeline/setup_env.sh` |
| Git | Clonar el repo | -- |
| `mosquitto-clients` (`mosquitto_sub` / `mosquitto_pub`) | Solo para pruebas manuales del simulador antes de tener NanoMQ levantado | `sudo apt install mosquitto-clients` (Ubuntu/Debian) |
| Cuenta de Kaggle (gratuita) | Solo una vez, para descargar el dataset | Ver `pipeline/data/README.md` |
| JDK 11 | Requerido por PyFlink | Gestionado automaticamente por conda (`openjdk=11`); no instalar aparte |

**No hace falta cuenta ni acceso a Databricks para ejecutar el pipeline.**
Databricks se uso unicamente durante el desarrollo como almacen de
referencia puntual para inspeccionar el dataset; no forma parte de la
arquitectura final ni de estos requisitos. El dataset se obtiene
directamente de Kaggle (ver `pipeline/data/README.md`).

## Puesta en marcha

1. Clonar el repo:

       git clone https://github.com/paterninausa/uea_tfm.git
       cd uea_tfm

2. Crear el entorno conda (incluye un fix de PATH necesario si tienes
   SDKMAN u otro gestor de JDK -- ver comentarios en el propio script):

       bash pipeline/setup_env.sh
       conda activate tfm

3. Obtener el dataset (una sola vez -- detalle completo en
   `pipeline/data/README.md`):

       cd pipeline/data
       pip install -r requirements.txt
       kaggle datasets download -d khalilaraoui/power-telemetry -f Power_measurements.xlsx -p ./raw
       python convert_to_parquet.py --input ./raw/Power_measurements.xlsx --output ./power_measurements.parquet
       cd ../..

4. Probar el simulador contra un broker Mosquitto temporal (detalle de las
   3 terminales en `pipeline/simulator/README.md`):

       docker run -it --rm -p 1883:1883 eclipse-mosquitto      # terminal 1
       mosquitto_sub -h localhost -t 'iot/#' -v                 # terminal 2
       cd pipeline/simulator                                    # terminal 3
       python mqtt_simulator.py \
           --parquet-path ../data/power_measurements.parquet \
           --broker-host localhost --broker-port 1883 \
           --rate 20 --limit 5000

5. *(Pendiente)* Levantar el stack completo con Docker Compose -- se
   documentara aqui en cuanto este disponible.

## Estructura del repo

    docs/                    Memoria del TFM (LaTeX)
    pipeline/
      environment.yml        Entorno conda del pipeline (simulador, PyFlink)
      environment.lock.yml   Snapshot de reproducibilidad (documentacion)
      setup_env.sh           Crea el entorno conda + fix de PATH para Java
      data/                  Preparacion del dataset (Kaggle -> Parquet)
      simulator/             Simulador MQTT de telemetria
