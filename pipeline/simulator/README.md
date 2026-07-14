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

Si tu version de la CLI no soporta rutas de Volumenes via `dbfs:/Volumes/...`,
descarga los ficheros individuales desde Catalog Explorer > Volumes en la UI
de Databricks.

## Uso

Activa el entorno conda `tfm` (ver `../environment.yml`). `pyarrow` (necesario
para leer Parquet con pandas) no se declara ahi explicitamente: llega como
dependencia transitiva de `apache-flink` -- ver `../environment.lock.yml`
para el detalle de por que se resolvio asi.

    python mqtt_simulator.py \
        --parquet-path ../data/power_measurements_parquet \
        --broker-host localhost --broker-port 1883 \
        --rate 20 --limit 5000

Parametros:
- `--rate`: eventos/segundo (0 = sin limite, para pruebas de carga)
- `--limit`: numero de filas a publicar (omitir para publicar el dataset completo)
- `--qos`: nivel de QoS MQTT (por defecto 1, ver justificacion en Objetivo 1)

## Prueba rapida sin NanoMQ (smoke test)

Hasta que NanoMQ este levantado (siguiente paso del plan), se valida el
simulador contra un broker Mosquitto temporal. Requiere 3 terminales, **en
este orden**:

**Terminal 1 -- levantar el broker** (dejarla corriendo en primer plano):

    docker run -it --rm -p 1883:1883 eclipse-mosquitto

**Terminal 2 -- suscribirse para ver llegar los mensajes:**

    mosquitto_sub -h localhost -t 'iot/#' -v

**Terminal 3 -- ejecutar el simulador** (entorno conda `tfm` activado):

    cd pipeline/simulator
    python mqtt_simulator.py \
        --parquet-path ../data/power_measurements_parquet \
        --broker-host localhost --broker-port 1883 \
        --rate 20 --limit 5000

Si el simulador se ejecuta antes de que el broker de la Terminal 1 este
escuchando, falla con `ConnectionRefusedError: [Errno 111] Connection
refused` al intentar `client.connect()`. Es el error esperado en ese caso
(no un bug del script) -- confirma que el broker no esta arriba todavia y
hay que levantarlo primero (Terminal 1) antes de lanzar la Terminal 3.

Con el broker arriba, en la Terminal 2 deberian verse los topicos
`iot/{company_id}/{site_id}/{machine_id}/telemetry` con el payload JSON
llegando al ritmo indicado por `--rate`, y al finalizar la Terminal 3
imprime el resumen de publicados/fallidos y la tasa de perdida.

## Medicion de latencia extremo a extremo (Objetivo 1)

El payload incluye `sim_publish_ts` (epoch millis, generado en el instante del
`publish()`). Los campos `timestamp` / `ingest_ts` del dataset original son
historicos y no deben usarse para calcular la latencia del pipeline: el KPI
(percentil 95 < 2s) se calcula como `hora_de_persistencia_en_TimescaleDB -
sim_publish_ts`.

## Pendiente (fases siguientes)

- Serializacion Avro via Apicurio (sustituye el JSON actual)
- Modo asincrono con pool de publishers para llegar a >=500 sensores
  concurrentes (Objetivo 5)
- Modo de replay historico con factor de aceleracion temporal, preservando
  los deltas relativos entre eventos consecutivos de una misma maquina
- Campo opcional reservado (p. ej. `firmware_version`) para la prueba de
  evolucion de esquema compatible (Objetivo 2)
