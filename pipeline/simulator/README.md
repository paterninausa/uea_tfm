# Simulador MQTT de telemetria IoT

Fase 1 del pipeline (Objetivo 1 / tarea "Disenar e implementar un simulador de
comunicacion IoT"). Publica eventos de consumo electrico leidos del historico
en Parquet hacia un broker MQTT, replicando la topologia de topicos definitiva
del TFM.

## Topico

    iot/{company_id}/{site_id}/{machine_id}/telemetry

`sensor_id` no identifica un dispositivo estable (rota entre maquinas en el
dataset original de Kaggle: ~2000 sensor_id distintos frente a ~5000
machine_id y 951459 pares machine-sensor unicos sobre ~1M filas), por lo que
la identidad del "sensor simulado" es `machine_id`, no `sensor_id`.
`sensor_id` viaja como campo del payload, no como nivel del topico.

`country_code` y `energy_type` son constantes en el dataset actual (FR /
ELECTRICITY) y por eso no se usan como niveles del topico; viajan tambien en
el payload.

## Obtener el dataset

Ver `../data/README.md`. En resumen: el dataset viene del Kaggle publico
"Power Telemetry" (no depende de ninguna cuenta o recurso privado), y el
script `../data/convert_to_parquet.py` lo convierte a
`../data/power_measurements.parquet`, que es la forma canonica que espera este
simulador.

Si en tu copia de trabajo lo que hay es el directorio
`../data/power_measurements_parquet/` (export anterior generado con Spark),
tambien sirve: `--parquet-path` acepta tanto un fichero Parquet como un
directorio, porque `pandas.read_parquet` resuelve ambos casos. Basta con
apuntar `--parquet-path` a lo que tengas en disco.

## Uso

Activa el entorno virtual del proyecto (venv + pip; el proyecto ya no usa
conda):

    bash pipeline/setup_env.sh      # solo la primera vez
    source .venv/bin/activate

`pyarrow` (necesario para leer Parquet con pandas) se declara
**explicitamente** en `../requirements.txt` junto con `pandas`: se comprobo
que `pip install pyspark==4.2.0` no los instala (solo llegan con extras como
`pyspark[sql]`), asi que no puede darse por supuesto que lleguen como
dependencia transitiva.

    python mqtt_simulator.py \
        --parquet-path ../data/power_measurements.parquet \
        --broker-host localhost --broker-port 1883 \
        --rate 20 --limit 5000

Parametros:
- `--rate`: eventos/segundo (0 = sin limite, para pruebas de carga)
- `--limit`: numero de filas a publicar (omitir para publicar el dataset completo)
- `--qos`: nivel de QoS MQTT (por defecto 1, ver justificacion en Objetivo 1)

## Prueba rapida contra el broker del stack (smoke test)

El broker es **Mosquitto** y forma parte del stack containerizado
(`pipeline/docker-compose.yml`, ver `../README.md`): no se levanta con un
`docker run` suelto. Requiere 2 terminales, **en este orden**:

**Terminal 1 -- levantar el broker** (desde la raiz del repo; basta el
servicio `mosquitto`, no hace falta el stack completo para esta prueba):

```bash
docker compose -f pipeline/docker-compose.yml up -d mosquitto
```

Y suscribirse para ver llegar los mensajes (dejar la terminal corriendo):

```bash
docker exec tfm-mosquitto mosquitto_sub -h localhost -t 'iot/#' -v
```

**Terminal 2 -- ejecutar el simulador** (con el venv activado):

```bash
cd pipeline/simulator
python mqtt_simulator.py \
    --parquet-path ../data/power_measurements.parquet \
    --broker-host localhost --broker-port 1883 \
    --rate 20 --limit 5000
```

Si el simulador se ejecuta antes de que el broker este escuchando, falla con
`ConnectionRefusedError: [Errno 111] Connection refused` al intentar
`client.connect()`. Es el error esperado en ese caso (no un bug del script) --
confirma que el broker no esta arriba todavia y hay que levantarlo primero
(Terminal 1) antes de lanzar la Terminal 2.

Con el broker arriba, en la Terminal 1 deberian verse los topicos
`iot/{company_id}/{site_id}/{machine_id}/telemetry` con el payload JSON
llegando al ritmo indicado por `--rate`, y al finalizar la Terminal 2
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
