# Job de Spark Structured Streaming — doble sumidero


Lee los eventos Avro de `iot.telemetry.raw` y los escribe en dos destinos con
proposito distinto.

```
                          +--> agregacion por ventana --> TimescaleDB --> Grafana
iot.telemetry.raw --------+
   (Avro + cabecera)      +--> enriquecimiento --------> PostgreSQL --> Power BI
```

## Tres modulos, tres responsabilidades

El job estaba en un solo fichero de 871 lineas, y mas de la mitad no era logica
de streaming sino fontaneria. Se partio en:

| Modulo | Lineas | De que responde |
|---|---|---|
| `telemetry_streaming.py` | 510 | QUE se calcula: leer Kafka, decodificar Avro, enriquecer, agregar por ventana y orquestar |
| `database_writers.py` | 183 | COMO se persiste: el UPSERT idempotente con sus reintentos y la carga de las tablas de referencia. No sabe de Spark mas alla de recibir un DataFrame resuelto |
| `monitoring.py` | 220 | Lo que el job hace sobre SI MISMO: registrar el progreso de cada micro-lote y vigilar que sus consultas sigan vivas |

La frontera es util al leer un fallo: los mensajes salen etiquetados con su
modulo (`spark.database_writers`, `spark.monitoring`), asi que se ve de un vistazo si
el problema esta en el calculo, en la base de datos o en la vigilancia.

## Por que dos consultas y no una

Son dos consultas de streaming **independientes** sobre el mismo topico, cada
una con su propio checkpoint. Cuesta leer Kafka dos veces, pero a cambio si
PostgreSQL se cae los dashboards operativos siguen actualizandose, y al
reanudar cada consulta retoma su offset sin arrastrar a la otra. En una
arquitectura Kappa, donde el log de Kafka es la fuente de verdad reproducible,
esa independencia vale mas que ahorrar una lectura.

## Union flujo-estatico: las dos tablas de referencia

El evento solo trae `building_id`, `meter_type`, `timestamp`, `meter_reading` y
`sim_publish_ts`. Todo lo demas entra por **broadcast join** contra dos tablas
pequenas que se cargan al arrancar:

| Tabla | Filas | Aporta |
|---|---|---|
| `ashrae_buildings.parquet` | 498 | `site_id`, `primary_use`, `square_feet` |
| `ashrae_sensor_baseline.parquet` | 652 | `baseline_p50`, `baseline_p75`, `baseline_iqr` |

Al difundirlas a los ejecutores, cada micro-lote las cruza en memoria sin
shuffle. Es el patron estandar de union flujo-estatico de Structured Streaming.

Que estos atributos vivan fuera del evento es deliberado: son del edificio, no
del contador. Corregir el ano de construccion de un edificio es actualizar una
fila; desnormalizado en cada evento, obligaria a reprocesar el historico entero.

## Por que se agrupa por site + uso + tipo de contador

`meter_type` es **obligatorio** en la clave de agrupacion, y no por comodidad:
la unidad de `meter_reading` depende del medio, asi que promediar a traves de
tipos de contador da una cifra sin significado fisico. Se ve en las medianas del
subconjunto: 43,6 en electricidad, 146,0 en agua fria y 8,8 en agua caliente.

Junto a `site_id` y `primary_use` dan **46 combinaciones**. Como las lecturas
son horarias, cada ventana de una hora agrega una lectura por cada sensor del
grupo.

Con el replay acelerado, el tiempo de evento avanza mucho mas rapido que el
reloj, de modo que las ventanas se cierran continuamente y el flujo se comporta
como un sistema en regimen.

## Reparto de calculos: quien calcula que

El criterio es uno solo: **si el consumidor puede derivarlo con lo que ya tiene,
que lo derive el**. Si necesita datos que no le llegan, lo calcula el pipeline.

| Quien | Que calcula | Por que ahi |
|---|---|---|
| `prepare_ashrae.py` | Linea base por contador | Una vez, sobre el historico completo |
| `enrich` en Spark | Solo lo que consume la agregacion | Las claves y banderas que Grafana necesita por ventana |
| PostgreSQL | Nada: guarda lecturas crudas y las dos tablas de referencia | — |
| Power BI | Intensidad, atipicos, ceros, umbrales | Un join le basta, y es potente |

Por eso **la tabla de eventos no tiene campos derivados ni copias de la
dimension**: seis columnas, la clave natural mas la medida y la instrumentacion.
Persistir intensidad o banderas seria almacenar 5,68 millones de valores
derivables, y ademas congelar en el pipeline unos umbrales que el analista
deberia poder mover.

Comprobado que Power BI se basta con lo que recibe:

| Consulta sobre las tablas crudas | Resultado | Tiempo |
|---|---|---|
| Intensidad energetica por uso de edificio | Utility 0,0107, Manufacturing 0,0066... | 15,3 ms |
| Atipicos con umbral `p75 + 5 x IQR` | 0,36% | 8,5 ms |
| **El analista cambia el umbral a `3 x IQR`** | **0,96%, sin tocar el pipeline** | 5,4 ms |
| Lecturas a cero | 4,12% | 1,7 ms |

La tercera fila es la que justifica la decision: mover un umbral pasa de exigir
un cambio de codigo, un reinicio y un reproceso, a ser una consulta distinta.

Las mismas cifras **si** se agregan en TimescaleDB, porque alli no son
reconstruibles: `avg_energy_intensity` es un cociente de sumas —no la media de
cocientes— y `zero_count` y `anomaly_count` resumen una ventana entera que ya no
esta disponible fila a fila.

## Deteccion de anomalias: contra la linea base del propio sensor

El umbral se eligio **midiendo la tasa de disparo** sobre los 5,68 M de eventos,
no por intuicion:

| Umbral | Dispara en |
|---|---|
| `p75 + 3 x IQR` | 0,95% |
| **`p75 + 5 x IQR`** | **0,34%** (elegido) |
| `p75 + 10 x IQR` | 0,04% |

La comparacion es contra la linea base **del propio contador**, no contra un
umbral global: los edificios van de 801 a 850.354 pies cuadrados y cada medio
tiene su unidad, asi que un contador solo es comparable consigo mismo. Los 17
sensores con IQR = 0 quedan exentos: sin dispersion historica no hay forma de
definir que es atipico para ellos.

**Se descarto la regla de lectura negativa**: es fisicamente imposible en un
contador y se comprobo que no ocurre ni una vez en el subconjunto. Una regla que
nunca dispara es indistinguible de una regla rota.

**Las lecturas a cero NO se marcan como anomalia**, aunque sean el 4,64%. Una
lectura a cero aislada puede ser legitima —un contador parado unas horas—, asi
que se registran como `is_zero_reading`, un indicador de calidad. El patron real
emerge en la agregacion: se midio que las rachas de ceros llegan a durar **8.051
horas seguidas, 335 dias**, y eso ya no es una medida sino un contador muerto.
Detectar la racha evento a evento exigiria procesamiento con estado; a nivel de
ventana, un `zero_count` alto y sostenido la delata igual.

## Tres problemas encontrados al probarlo

**1. Desplazamiento horario silencioso.** `spark.sql.session.timeZone=UTC`
gobierna como Spark interpreta los timestamps, pero **no** la conversion a
objetos de Python: `collect()` devuelve `datetime` *naive* convertido a la zona
del sistema del driver. Con la maquina en `Europe/Madrid`, un evento de las
00:00 UTC se escribia como 01:00 UTC. Sin error, y con desplazamiento variable
(+1 h en invierno, +2 h en verano). Se corrige forzando `TZ=UTC` en el proceso
antes de crear la sesion, y marcando ademas los datetime como UTC explicito
antes del INSERT para que la base de datos no pueda reinterpretarlos.

**2. El UPSERT exige deduplicar dentro del lote.** PostgreSQL aborta con
`CardinalityViolation` si la misma clave aparece dos veces en la misma
sentencia `ON CONFLICT`. Y las claves repetidas son normales, no una anomalia:
la cadena MQTT QoS 1 + reintentos del productor da garantia *at-least-once*.

**3. Un evento con fecha futura envenena el watermark.** Al inyectar eventos de
prueba tomados de cualquier punto del dataset de 350 dias, unos pocos con
tiempo muy posterior empujaron el watermark hacia adelante; a partir de ahi
todos los eventos ordenados que llegaron despues quedaron por detras y se
descartaron **en silencio** como tardios. No es un fallo del codigo sino
semantica de watermark, pero es un riesgo real: una sola fecha mal puesta
silencia datos validos posteriores sin dar ningun error.

## Evolucion de esquema: drenar y conmutar

El job deserializa todo el flujo con **un unico esquema**, el vigente al
arrancar, porque `from_avro` recibe uno solo. No sabe consumir dos versiones a
la vez. En lugar de anadir resolucion de esquema por mensaje, se adopta un
procedimiento operativo que hace que **la coexistencia no llegue a ocurrir**:

1. **Parar el simulador.** Deja de entrar telemetria nueva.
2. **Esperar unos segundos.** El bridge vacia su buffer y el job alcanza el
   final del topico.
3. **Registrar la version nueva** con `register_schema.py`.
4. **Reiniciar el bridge.** Pasa a producir con el esquema nuevo.
5. **Reiniciar el job.** Pasa a leer con el esquema nuevo.
6. **Reanudar el simulador.**

La interrupcion son segundos y **no se pierde ningun evento**: Kafka retiene 7
dias, el job reanuda desde su checkpoint y el bridge desde su sesion MQTT
persistente. Ambas recuperaciones estan verificadas por separado.

Es la opcion mas sencilla de explicar y de defender, y no requiere codigo
adicional. La alternativa —resolver el esquema de cada mensaje por su
`globalId` contra el registro— se descarto a proposito: `ApicurioClient` ya
tiene el metodo y la cache necesarios, pero anade complejidad que este trabajo
no necesita.

### Si el procedimiento se ejecuta mal, el job se detiene

`guard_schema_version` comprueba el `globalId` de cada evento y **detiene el
job** si no es el esperado. Antes lo *filtraba*, y esa era la ultima via de
perdida silenciosa del pipeline: los eventos de una version inesperada
desaparecian sin contador ni traza, justo en el momento en que mas importa.

Detenerse es preferible a descartar, y no por purismo: con 7 dias de retencion
en Kafka, **parar es recuperable** —se corrige y se reanuda desde el checkpoint
sin perder un evento— mientras que descartar es irreversible.

La comprobacion se aplica sobre la columna `meter_reading` y no como columna
aparte, para que el optimizador no pueda eliminarla. Verificado: inyectando un
evento con `globalId=99` en un flujo donde `meter_reading` solo se usa dentro
de una agregacion, el job aborta con el mensaje completo, y la traza muestra
que la comprobacion se evaluo dentro del propio `hashAgg`.

## Resiliencia: el proceso ya no es un punto unico de fallo

Hasta agosto de 2026 el job hacia esto al final:

```python
for q in consultas:
    q.awaitTermination()
```

Espera SECUENCIALMENTE, de modo que solo vigilaba la primera consulta. Las
consecuencias se midieron tumbando cada base de datos por separado:

| Base tumbada | Que ocurria |
|---|---|
| TimescaleDB | Moria `metricas-timescaledb`, la primera de la lista, y su excepcion terminaba **el proceso entero**, arrastrando a la consulta de PostgreSQL que no dependia de ella |
| PostgreSQL | Moria `eventos-postgresql`, la segunda, y como nadie la esperaba **el proceso seguia vivo** escribiendo metricas con normalidad: media arquitectura parada, sin un solo aviso, con la tabla de eventos congelada |

Las dos formas contradecian la promesa del doble sumidero. Hoy hay tres capas:

1. **Reintentos en la escritura** (`--db-retries`, `--db-retry-wait`): una caida
   corta se absorbe sin que muera nada. Solo se reintenta `OperationalError`
   —"no pude hablar con el servidor"—; un error de datos no se reintenta, porque
   eso es un incumplimiento del contrato y tiene que salir a la luz.
2. **Supervision de todas las consultas** (`--supervision-interval`): se
   comprueba `isActive` de cada una, no se espera a ninguna en particular.
3. **Relanzado automatico** (`--max-reinicios`): la consulta caida se vuelve a
   arrancar desde su propio checkpoint, que es lo que hace que reanudar no
   pierda ni duplique nada. Agotados los reinicios, se abandona esa consulta y
   se dice por que.

### Medido con las tres capas

| Base tumbada 15 s | Flujo restablecido | La otra consulta |
|---|---|---|
| TimescaleDB | **4,1 s** tras levantar el servicio | siguio con 21 micro-lotes durante la caida |
| PostgreSQL | **5,1 s** | sin interrupcion |

El job permanecio vivo en ambos casos. Objetivo 5: recuperacion < 60 s.

## Uso

Requiere el stack levantado, el esquema registrado y el bridge en marcha:

```bash
docker compose -f pipeline/docker-compose.yml up -d
```

```bash
python pipeline/spark/telemetry_streaming.py --sink both
```

Opciones: `--sink metrics|events|both` (aislar un sumidero es lo que necesita la
prueba de recuperacion ante fallo), `--window`, `--watermark`, `--trigger`,
`--starting-offsets`, `--progress-interval`. Ver `--help`.

### Progreso de micro-lote

El job vuelca cada `--progress-interval` segundos (10 por defecto) el progreso de
cada micro-lote a la tabla `streaming_progress` de TimescaleDB. **Es la fuente
del KPI de latencia de lote del Objetivo 3**, y de ahi lo lee
`herramientas/kpi_report.py`.

Se leen los informes de `recentProgress` y no `lastProgress`, que es lo que hacia
la version anterior de esta instrumentacion: con un trigger de 1 s y un sondeo de
10 s, `lastProgress` deja fuera nueve de cada diez lotes, y la mediana de una
muestra con huecos no es la mediana de los lotes. Cada ejecucion del job se
identifica con un `run_id` propio, porque al borrar los checkpoints —lo primero
que hace `reset_state.py`— el `batch_id` vuelve a empezar en cero.

El primer arranque descarga de Maven Central los conectores de Kafka, Avro y el
driver JDBC; quedan cacheados en `~/.ivy2` y los siguientes no necesitan red.
Los checkpoints viven en `pipeline/spark/checkpoints/` y no se versionan:
borrarlos hace que el job reprocese el topico desde el principio.

## Resultados medidos

Sobre el stack completo, estado limpio (topico recreado, tablas vacias,
checkpoints borrados) y 50.000 eventos a 400 ev/s:

| Metrica | Resultado | Objetivo |
|---|---|---|
| Perdida de mensajes | **0,0000%** | < 0,1% ✓ |
| Latencia de ingesta (evento -> PostgreSQL) p50 / p95 | **0,766 / 1,277 s** | < 2 s ✓ |
| Disponibilidad del agregado (-> TimescaleDB) p50 / p95 | 5,660 / 6,916 s | — |

### Pruebas de carga (Objetivo 5)

Escalera de sensores concurrentes, 20.000 eventos por peldano sin limite de
tasa:

| Sensores | Throughput | Perdida |
|---|---|---|
| 100 (primera ejecucion, en frio) | 1.062,6 ev/s | 0,0000% |
| 250 | 1.226,0 ev/s | 0,0000% |
| 500 | 1.270,8 ev/s | 0,0000% |
| 652 | 1.331,9 ev/s | 0,0000% |

El throughput parecia **subir** con mas sensores, lo que no tenia sentido. Se
sospecho de un sesgo de calentamiento —la escalera se ejecuto en orden
creciente, asi que JIT y conexiones favorecian a los ultimos peldanos— y se
controlo repitiendo el primero al final:

| Comparacion en igualdad de condiciones | Throughput |
|---|---|
| 100 sensores, en caliente | **1.291,2 ev/s** |
| 652 sensores, en caliente | **1.284,0 ev/s** |

**Degradacion real: 0,6%**, muy por debajo del 20% que admite el Objetivo 5, y
con 0,0000% de perdida en todos los peldanos. El bridge proceso 320.000 eventos
sin perder ninguno.

La leccion metodologica vale para el trabajo: la escalera en orden creciente
habria producido la conclusion falsa de que el sistema mejora al cargarlo.

## Por que el agregado tarda ~6 s: diagnostico cerrado

Se instrumento el cierre de ventana anadiendo el watermark y el maximo tiempo de
evento al registro de progreso (hoy `--progress-interval`). El resultado explica la cifra
por completo, y la causa no es una ineficiencia del pipeline.

### Lo que se ve en el log

```
lote=2  evento_max=01:00  watermark=2015-12-31T23:58
lote=3  evento_max=01:00  watermark=00:58
lote=4  evento_max=01:00  watermark=00:58
lote=5  evento_max=02:00  watermark=00:58
lote=6  evento_max=02:00  watermark=01:58
```

Dos hechos:

1. **El watermark va un lote por detras.** En el lote N vale (maximo tiempo de
   evento del lote N-1) menos el margen. Es comportamiento documentado de Spark
   y explica una parte pequena.

2. **El tiempo de evento avanza a saltos de una hora exacta.** ASHRAE mide en
   punto: no existen instantes intermedios. Para cerrar la ventana 00:00-01:00
   hace falta un watermark mayor que 01:00, o sea tiempo de evento mayor que
   01:02; y como el siguiente valor posible es **02:00**, hay que esperar a que
   llegue la hora siguiente entera.

**El watermark de 2 minutos es por tanto irrelevante aqui**: cualquier valor
entre 0 y 1 hora produce exactamente la misma espera. Lo que fija la latencia es
cuanto se tarda en publicar una hora de datos, es decir 652 lecturas —una por
sensor— divididas por la tasa de publicacion.

### La prediccion y su comprobacion

Si la causa es esa, publicar mas despacio debe alargar la latencia
proporcionalmente. Medido:

| Tasa | Publicar 1 hora de evento | Latencia p50 |
|---|---|---|
| 262 ev/s | 2,49 s | 5,66 s |
| 110 ev/s | 5,93 s | 10,89 s |

Ajustando ambos puntos: **latencia ≈ 1,5 × (652 / tasa) + 1,9 s**. El factor 1,5
corresponde a esperar el resto de la hora propia mas parte de la siguiente; el
termino constante de 1,9 s es el micro-lote mas el retraso del watermark.

### Que significa esto para el trabajo

**La latencia del agregado esta acotada por abajo por el intervalo de muestreo
de los sensores.** Un agregado horario no puede existir antes de que termine la
hora que resume, y con contadores que miden una vez por hora eso significa
esperar a la lectura siguiente. En un despliegue real esa espera es de una hora
de reloj; aqui se comprime porque el replay va acelerado.

Comparar esa cifra con un KPI de 2 segundos es un error de categoria: no mide la
velocidad del pipeline, mide la cadencia del sensor. La latencia que si mide el
pipeline es la de ingesta a grano de evento, **1,277 s en p95**, por debajo del
objetivo.

Las tres hipotesis previas quedan descartadas con medidas: el job no va
retrasado (procesa a 605 ev/s frente a 269 de entrada), la granularidad de lote
apenas influye (trigger de 500 ms deja la latencia en 6,65 s frente a 6,92) y
`addBatch` domina el lote con el 84% de 572 ms, pero 572 ms no explican 6,9 s.
