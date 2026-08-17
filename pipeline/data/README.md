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
exactamente el mismo Parquet.

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

Este `requirements.txt` declara `pandas` y `pyarrow` ademas
de la CLI de Kaggle, de modo que funciona tambien en un venv aparte del
pipeline. Las versiones estan alineadas con `pipeline/requirements.txt` para
que instalarlo encima del venv principal no reinstale nada.

## 4. Generar el subconjunto de datos

```bash
python prepare_ashrae.py
```

Produce tres ficheros, que no se versionan por estar en `.gitignore`:

| Fichero | Contenido | Tamano |
|---|---|---|
| `ashrae_telemetry.parquet` | Tabla de hechos: 5.682.185 lecturas | ~18 MB |
| `ashrae_buildings.parquet` | Dimension: 498 edificios | ~11 KB |
| `ashrae_sensor_baseline.parquet` | Cuartiles del historico de cada contador | ~23 KB |

La linea base la usan Spark, para marcar picos atipicos contra el historial del
propio contador, y Power BI, que la recibe cargada en PostgreSQL y puede ajustar
el umbral sin tocar el pipeline. Se calcula aqui, una sola vez, porque exige
recorrer el historico completo.

Los nombres de salida **no son configurables**, y es deliberado: el simulador y
el job de Spark ya los tienen cableados como valores por defecto, asi que un
nombre distinto no lo seguiria ningun consumidor. Seria una opcion que solo
sirve para romper el pipeline.

Ademas elimina por construccion un riesgo que llego a aparecer dos veces: cuando
la ruta de una salida se deducia de la de otra, ambas podian coincidir y la
segunda escritura destruia la primera sin dar ningun error. Con tres nombres
fijos e independientes la colision es imposible, y sobra la comprobacion que
antes hacia falta.

Los unicos argumentos son `--train`, `--metadata` y `--sites`, que si varian de
verdad: los dos primeros permiten partir del CSV de Kaggle o de un Parquet, y el
tercero cambia el subconjunto de emplazamientos.

## El subconjunto elegido: emplazamientos 2, 3 y 5

De los 16 emplazamientos de la tabla original se usan tres:

| | Subconjunto elegido | Dataset completo |
|---|---|---|
| Sensores (edificio × contador) | **652** | 2.380 |
| Eventos | **5.682.185** | 20.216.100 |
| Combinaciones de agregación | **46** | 193 |
| Reproducción a 740 eventos/s | **128 min** | 6,6 h |

- **652 sensores** deja margen sobre los 500 concurrentes que exige el Objetivo
  5, y permite muestrear la carga base de 100 para medir la degradacion de
  throughput.
- El **emplazamiento 3** con 274 sensores tiene la mejor calidad del dataset, apenas un
  0,1% de lecturas a cero.
- El **emplazamiento 2** aporta la variedad de contadores (electricidad, agua
  fria y agua caliente) y un ciclo estacional de refrigeracion muy marcado -el
  consumo medio de agua fria pasa de 152 en enero a 528 en agosto-.
- El **emplazamiento 5** tiene completitud del 100%, sirve de referencia limpia.

Se descartó anadir un emplazamiento con contador de vapor: en los candidatos
mas limpios su perfil anual resultó anómalo (en el emplazamiento 13 se dispara
de marzo a junio y cae casi a cero de julio a octubre, un factor 50x que no
corresponde a ninguna demanda de calefaccion), y cada uno añadía entre 15 y 60
minutos de reproducción. No obstante esquema Avro declara los cuatro simbolos
de contador describiendo el dominio, no la muestra concreta.

## Emplazamiento 14 excluido de todo análisis

Se comprobó por correlacion cruzada de los perfiles diarios de consumo que sus
marcas de tiempo van **5 horas por delante** del resto: hace pico a las 18h
frente a las 13-14h de los demas, con una correlacion de forma de 0,99 —misma
curva, desplazada en bloque—. Es coherente con datos registrados en UTC cuando
el resto está en hora local.

Los otros 15 emplazamientos tienen desfases de entre -1 y +2 horas, que es
variacion normal de comportamiento y no de huso horario. Por eso **todas las
marcas de tiempo se tratan como hora local, sin conversion ni dimension de
timezone**. La contrapartida, que conviene tener presente: dos eventos de
emplazamientos distintos marcados a las 14:00 no son el mismo instante fisico.
Es irrelevante para los perfiles por hora del dia y las comparativas entre
tipos de edificio, y solo importaria al cruzar con datos meteorológicos.

## Decisiones de modelado

### La tabla de hechos reproduce lo que emite el contador

    building_id | meter_type | timestamp | meter_reading

Cuatro columnas y nada más. El dataset original se publicó para entrenar
modelos de prediccion, asi que las caracteristicas del edificio estan ahi como
variables de entrada del modelo. Pero un contador real no envia estos datos 
con cada lectura: envia quien es y cuanto midio.
Cualquier columna adicional la anadiriamos nosotros y dejaria de reproducir el
comportamiento del sensor.

Por eso **no hay `event_id` ni `sensor_id`**: ambos eran concatenaciones
calculadas por el propio script, es decir, informacion que ya estaba en el
dato. Y **las caracteristicas del edificio salen a una tabla de dimension**.

El efecto sobre el tamano del mensaje es grande: medido con el esquema Avro
real, el evento pasa de 59,4 a **23,0 bytes**, un 61,3% menos, lo que se
traduce en 131 MB en Kafka en lugar de 338 MB.

`meter_type` es la unica concesion, y no es un campo derivado sino una
**decodificacion**: la correspondencia `0 -> electricity ... 3 -> hotwater`
viene de la documentacion de la competicion, es biyectiva y no anade ni pierde
informacion. Se prefiere al codigo crudo porque permite declararlo como enum en
Avro, que se serializa exactamente igual —un indice varint— pero deja el
dominio explicito en el contrato.

### Clave natural: (building_id, meter_type, timestamp)

Es la clave primaria y la clave de deduplicacion, y lo que garantiza que el
reprocesamiento de la arquitectura Kappa sea idempotente. Cumple las cuatro
propiedades necesarias:

- **Determinista**: los tres campos vienen del sensor. No depende del instante
  de recepcion, del offset de Kafka ni de ningun generador aleatorio.
- **Unica**: verificado sobre el subconjunto, 5.682.185 grupos para 5.682.185
  filas, sin un solo duplicado en el origen. El script lo comprueba en cada
  ejecucion en lugar de darlo por supuesto.
- **Estable**: no cambia si se recrea el topico, se reordenan las particiones o
  se reprocesa el historico.
- **Disponible en todo el recorrido**: bridge, Spark y ambas bases de datos la
  tienen sin necesidad de ningun join.

Un UUID generado en la ingesta falla en la primera propiedad —cada reproceso
inventaria identificadores nuevos y duplicaria filas— y el offset de Kafka
falla en la tercera.

**`(building_id, timestamp)` no basta**: un mismo edificio tiene contadores de
electricidad, agua fria y agua caliente midiendo a la misma hora. Sin
`meter_type` en la clave se colapsarian 1.345.428 eventos.

**No confundir con la clave del mensaje en Kafka**, que es solo
`(building_id, meter_type)` —el sensor, 652 valores distintos—. Es lo que
mantiene en la misma particion y en orden todas las lecturas de un contador,
que es lo que necesitan las ventanas de Spark. Usar la clave del evento como
clave de Kafka repartiria cada lectura a una particion al azar.

### Tabla de dimension `ashrae_buildings.parquet`

    building_id | site_id | primary_use | square_feet | year_built | floor_count

498 filas. Spark la incorpora con un broadcast join para agregar por tipo de
edificio, y Power BI la consume como dimension de un esquema en estrella, que
es el modelo que esa herramienta espera; desnormalizar estos atributos en cada
una de los 5,68 M de filas es justo el antipatron.

Ahi viven tambien los nulos reales del dataset: `year_built` falta en 184 de
los 498 edificios (36,9%) y `floor_count` en 409 (82,1%).

Separarlos tiene ademas una ventaja operativa: si se corrige el año de
construccion de un edificio, se actualiza una fila; desnormalizado, habria que
reprocesar el historico entero.

### Otras decisiones

**Un sensor es el par edificio-contador.** Un mismo edificio con contador de
electricidad y de agua fria son dos sensores con series independientes. De ahi
la topologia de topicos, que el simulador construye cruzando con la dimension:

    iot/{site_id}/{building_id}/{meter_type}/telemetry

**Las lecturas a cero no se eliminan** (4,6% del subconjunto). Son parte del
dato real y son material para el informe de deteccion de anomalias.
