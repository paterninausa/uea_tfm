# Preparacion del dataset (ASHRAE GEPIII -> Parquet)

El pipeline se alimenta del dataset de la competicion **ASHRAE Great Energy
Predictor III**: consumo energetico horario real de 1.449 edificios en 16
emplazamientos, medido durante 2016.

    https://www.kaggle.com/competitions/ashrae-energy-prediction

Es un subconjunto del proyecto Building Data Genome 2 (BDG2). Son medidas
reales de sistemas de contadores, con su error de medicion y sus problemas de
calidad; no hay ningun dato sintetico.

Este es un paso de preparacion de UN SOLO USO, independiente del pipeline en
ejecucion, y no depende de ninguna cuenta ni recurso privado: partiendo de los
ficheros publicos de Kaggle, cualquiera que clone el repositorio obtiene
exactamente el mismo Parquet. (Databricks se uso puntualmente durante el
analisis inicial como almacen de referencia; no forma parte de la arquitectura
ni de este procedimiento.)

## 1. Credenciales de Kaggle

Necesitas una cuenta de Kaggle (gratuita) y aceptar las condiciones de la
competicion desde su pagina web:

1. Entra en https://www.kaggle.com/settings/api y pulsa "Create New Token"
2. Descarga `kaggle.json` y colocalo en `~/.kaggle/kaggle.json`
3. Ajusta permisos: `chmod 600 ~/.kaggle/kaggle.json`

## 2. Obtener los ficheros originales

Hacen falta dos ficheros de la competicion:

| Fichero | Contenido |
|---|---|
| `train.csv` | Lecturas horarias: `building_id`, `meter`, `timestamp`, `meter_reading` |
| `building_metadata.csv` | Ficha del edificio: `site_id`, `primary_use`, `square_feet`, `year_built`, `floor_count` |

```bash
kaggle competitions download -c ashrae-energy-prediction -f train.csv -p ./raw
```

```bash
kaggle competitions download -c ashrae-energy-prediction -f building_metadata.csv -p ./raw
```

El script acepta tanto CSV como Parquet: detecta el formato por la extension.
El directorio `raw/` esta en `.gitignore` y no se versiona.

## 3. Instalar las dependencias de este paso

```bash
pip install -r requirements.txt
```

Este `requirements.txt` es autosuficiente: declara `pandas` y `pyarrow` ademas
de la CLI de Kaggle, de modo que funciona tambien en un venv aparte del
pipeline. Las versiones estan alineadas con `pipeline/requirements.txt` para
que instalarlo encima del venv principal no reinstale nada.

## 4. Generar el subconjunto

```bash
python prepare_ashrae.py
```

Produce `ashrae_telemetry.parquet` (~50 MB, 5.682.185 eventos), que es lo que
espera el simulador MQTT. Tampoco se versiona.

## El subconjunto: emplazamientos 2, 3 y 5

De los 16 emplazamientos se usan tres. La eleccion salio de medir el dataset
completo, no de la conveniencia:

| | Subconjunto | Dataset completo |
|---|---|---|
| Sensores (edificio × contador) | **652** | 2.380 |
| Eventos | **5.682.185** | 20.216.100 |
| Combinaciones de agregacion | **46** | 193 |
| Reproduccion a 740 ev/s | **128 min** | 6,6 h |

- **652 sensores** deja margen sobre los 500 concurrentes que exige el Objetivo
  5, y permite muestrear la carga base de 100 para medir la degradacion de
  throughput.
- **Emplazamiento 3**: 274 sensores con la mejor calidad del dataset, apenas un
  0,1% de lecturas a cero.
- **Emplazamiento 2**: aporta la variedad de contadores (electricidad, agua
  fria y agua caliente) y un ciclo estacional de refrigeracion muy marcado —el
  consumo medio de agua fria pasa de 152 en enero a 528 en agosto—.
- **Emplazamiento 5**: completitud del 100%, sirve de referencia limpia.

Se descarto anadir un emplazamiento con contador de vapor: en los candidatos
mas limpios su perfil anual resulto anomalo (en el emplazamiento 13 se dispara
de marzo a junio y cae casi a cero de julio a octubre, un factor 50x que no
corresponde a ninguna demanda de calefaccion), y cada uno anadia entre 15 y 60
minutos de reproduccion. El esquema Avro declara igualmente los cuatro simbolos
de contador: describe el dominio, no la muestra concreta.

## El emplazamiento 14 esta excluido

Se comprobo por correlacion cruzada de los perfiles diarios de consumo que sus
marcas de tiempo van **5 horas por delante** del resto: hace pico a las 18h
frente a las 13-14h de los demas, con una correlacion de forma de 0,99 —misma
curva, desplazada en bloque—. Es coherente con datos registrados en UTC cuando
el resto esta en hora local.

Los otros 15 emplazamientos tienen desfases de entre -1 y +2 horas, que es
variacion normal de comportamiento y no de huso horario. Por eso **todas las
marcas de tiempo se tratan como hora local, sin conversion ni dimension de
timezone**. La contrapartida, que conviene tener presente: dos eventos de
emplazamientos distintos marcados a las 14:00 no son el mismo instante fisico.
Es irrelevante para los perfiles por hora del dia y las comparativas entre
tipos de edificio, y solo importaria al cruzar con datos meteorologicos.

## Decisiones de modelado

**`event_id` derivado y legible** (`B{edificio}-M{contador}-{epoch}`). ASHRAE no
trae identificador de evento y el pipeline lo necesita como clave primaria. Se
deriva de (edificio, contador, instante) —unico, porque cada contador tiene como
mucho una lectura por hora— en lugar de generar un UUID: con un UUID,
reprocesar el log de Kafka duplicaria filas, porque cada reproceso inventaria
identificadores nuevos.

Se prefirio la forma legible a un hash truncado por dos razones: un
identificador que aparece en el log de la DLQ dice de que sensor e instante
procede sin cruzarlo con nada, y ademas **ocupa tres veces menos en disco** (50
MB frente a 159 MB), porque los prefijos compartidos y los epochs casi
secuenciales se comprimen muy bien y un hash, por diseno, no.

**Un sensor es el par edificio-contador.** Un mismo edificio con contador de
electricidad y de agua fria son dos sensores con series independientes. De ahi
la topologia de topicos:

    iot/{site_id}/{building_id}/{meter_type}/telemetry

**Las lecturas a cero no se eliminan** (4,6% del subconjunto). Son parte del
dato real y son material para el informe de deteccion de anomalias.

**Los nulos se conservan**: `year_built` falta en el 32,2% de los eventos y
`floor_count` en el 86,2%. Se declaran como uniones con `null` en el esquema
Avro, lo que da un uso legitimo a la nulabilidad del contrato — algo que el
dataset anterior no permitia, porque no tenia ni un solo nulo.
