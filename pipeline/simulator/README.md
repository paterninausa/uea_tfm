# Simulador MQTT de telemetria IoT

Fase 1 del pipeline (Objetivo 1 / tarea "Disenar e implementar un simulador de
comunicacion IoT"). Publica eventos de consumo electrico leidos del historico
en Parquet hacia un broker MQTT, replicando la topologia de topicos definitiva
del TFM.

## Topico

    iot/{company_id}/{site_id}/{machine_id}/telemetry

`sensor_id` no identifica un dispositivo estable (rota entre maquinas en el
dataset: ~2000 sensor_id distintos frente a ~5000 machine_id y 951459 pares
machine-sensor unicos sobre ~1M filas), por lo que la identidad del "sensor
simulado" es `machine_id`, no `sensor_id`. `sensor_id` viaja como campo del
payload, no como nivel del topico.

`country_code` y `energy_type` son constantes en el dataset actual (FR /
ELECTRICITY) y por eso no se usan como niveles del topico; viajan tambien en
el payload.

## Obtener el dataset localmente

Los datos viven en Databricks (`/Volumes/tfm/data/data/power_measurements_parquet/`),
usado solo como almacen de referencia -- Databricks no forma parte del
pipeline final (ver restricciones del proyecto). Descarga el directorio
Parquet a tu maquina con la CLI de Databricks:

    databricks fs cp -r "dbfs:/Volumes/tfm/data/data/power_measurements_parquet" ./pipeline/data/power_measurements_parquet

Si tu version de la CLI no soporta rutas de Volumenes vía `dbfs:/Volumes/...`,
descarga los ficheros individuales desde Catalog Explorer > Volumes en la UI
de Databricks.

## Uso

Activa el entorno conda `tfm` (ver `../environment.yml`, incluye `pyarrow`
para leer Parquet) y ejecuta:

    python mqtt_simulator.py \
        --parquet-path ../data/power_measurements_parquet \
        --broker-host localhost --broker-port 1883 \
        --rate 20 --limit 5000

Parametros:
- `--rate`: eventos/segundo (0 = sin limite, para pruebas de carga)
- `--limit`: numero de filas a publicar (omitir para publicar el dataset completo)
- `--qos`: nivel de QoS MQTT (por defecto 1, ver justificacion en Objetivo 1)

## Prueba rapida sin NanoMQ

Hasta que NanoMQ este levantado (siguiente paso del plan), puedes validar el
simulador contra un broker Mosquitto temporal:

    docker run -it --rm -p 1883:1883 eclipse-mosquitto

y en otra terminal, para ver los mensajes llegar:

    mosquitto_sub -h localhost -t 'iot/#' -v

## Medicion de latencia extremo a extremo (Objetivo 1)

El payload incluye `sim_publish_ts` (epoch millis, generado en el instante del
`publish()`). Los campos `timestamp` / `ingest_ts` del dataset original son
historicos y no deben usarse para calcular la latencia del pipeline: el KPI
(percentil 95 < 2s) se calcula como `hora_de_persistencia_en_TimescaleDB -
sim_publish_ts`.

## Pendiente (fases siguientes)

- Serializacion Avro vía Apicurio (sustituye el JSON actual)
- Modo asincrono con pool de publishers para llegar a >=500 sensores
  concurrentes (Objetivo 5)
- Modo de replay historico con factor de aceleracion temporal, preservando
  los deltas relativos entre eventos consecutivos de una misma maquina
- Campo opcional reservado (p. ej. `firmware_version`) para la prueba de
  evolucion de esquema compatible (Objetivo 2)
