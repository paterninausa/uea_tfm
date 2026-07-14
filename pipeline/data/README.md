# Preparacion del dataset (Kaggle -> Parquet)

El dataset de telemetria electrica usado en el TFM proviene de Kaggle:

    https://www.kaggle.com/datasets/khalilaraoui/power-telemetry
    Fichero: Power_measurements.xlsx

Las columnas y valores -- incluida la rotacion de `sensor_id` entre
`machine_id` distintos -- vienen tal cual del dataset original; no se
modifican ni enriquecen en ningun paso posterior.

Este es un paso de preparacion de datos de un solo uso, independiente del
pipeline en ejecucion. Su unico proposito es que cualquiera que clone el
repo pueda obtener el mismo dataset sin depender de ninguna cuenta o
recurso privado (en particular, sin necesitar acceso a Databricks, que se
uso solo puntualmente durante el desarrollo como almacen de referencia y no
forma parte de la arquitectura final).

## 1. Credenciales de Kaggle

Necesitas una cuenta de Kaggle (gratuita):

1. Entra en https://www.kaggle.com/settings/api y pulsa "Create New Token"
2. Descarga `kaggle.json` y colocalo en `~/.kaggle/kaggle.json`
3. Ajusta permisos: `chmod 600 ~/.kaggle/kaggle.json`

## 2. Instalar las dependencias de este paso

Estas dependencias (CLI de Kaggle + openpyxl) son solo para esta
preparacion puntual de datos; no se anaden al `environment.yml` principal
del pipeline para no cargarlo con herramientas que no se usan en el dia a
dia de la simulacion/procesamiento.

    pip install -r requirements.txt

(Puedes instalarlas en el propio entorno conda `tfm` activado, o en un
venv aparte -- es indiferente, es un paso desacoplado del resto.)

## 3. Descargar el dataset

    kaggle datasets download -d khalilaraoui/power-telemetry -f Power_measurements.xlsx -p ./raw

Esto descarga `./raw/Power_measurements.xlsx` (~136 MB). El directorio
`raw/` esta en `.gitignore`: no se versiona.

## 4. Convertir a Parquet

    python convert_to_parquet.py \
        --input ./raw/Power_measurements.xlsx \
        --output ./power_measurements.parquet

El resultado (`power_measurements.parquet`, unos 15-30 MB comprimido, un
solo fichero) tampoco se versiona (tambien en `.gitignore`) y es el que
espera `pipeline/simulator/mqtt_simulator.py` via `--parquet-path`.

## Nota sobre la rotacion de `sensor_id`

El dataset original ya trae `sensor_id` rotando entre distintos
`machine_id` (~2000 valores de `sensor_id` frente a ~5000 `machine_id`, con
~951000 pares machine-sensor unicos sobre ~1M filas). No es un error
introducido en la conversion ni en ningun paso posterior: es asi en el
fichero original de Kaggle. Por eso el simulador identifica al "sensor
simulado" por `machine_id`, no por `sensor_id` (ver
`pipeline/simulator/README.md`).
