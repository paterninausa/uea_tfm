# Job de Spark Structured Streaming — doble sumidero


Lee los eventos Avro de `iot.telemetry.raw` y los escribe en dos destinos con
proposito distinto.

```
                          +--> agregacion por ventana --> TimescaleDB --> Grafana
iot.telemetry.raw --------+
   (Avro + cabecera)      +--> enriquecimiento --------> PostgreSQL --> Power BI
```

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

## Uso

Requiere el stack levantado, el esquema registrado y el bridge en marcha:

```bash
docker compose -f pipeline/docker-compose.yml up -d
```

```bash
python pipeline/spark/telemetry_streaming.py --sink both
```

Opciones: `--sink metrics|events|both`, `--console` (depuracion sin bases de
datos), `--window`, `--watermark`, `--trigger`, `--starting-offsets`. Ver
`--help`.

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

## Diagnostico de la latencia del agregado: parcial

Se encontro y corrigio **un bug real de la instrumentacion**. `ingested_at` no
se refrescaba en el `ON CONFLICT DO UPDATE`: al reescribir una fila existente se
actualizaba `sim_publish_ts` al instante nuevo pero `ingested_at` conservaba el
de la primera escritura. La latencia calculada salia entonces **negativa** —la
fila parecia escrita antes de publicarse— y ocurre siempre que se reprocesa el
log de Kafka, que es la operacion basica de una arquitectura Kappa.

Con el bug corregido y midiendo sobre estado limpio, la latencia del agregado
bajo de 10,25 s a **6,92 s** en p95. Buena parte de aquella cifra era medicion
contaminada, no latencia.

**Donde se va el tiempo de cada micro-lote** (opcion `--log-progress`):

| Fase | Consulta de metricas |
|---|---|
| `addBatch` (escritura al sumidero) | **480 ms — el 84%** |
| `queryPlanning` | 15-24 ms |
| `latestOffset` | 6-12 ms |
| `walCommit` | 24-59 ms |
| **`triggerExecution` total** | **572 ms** |

**Tres hipotesis descartadas con medidas**:

1. *El job va retrasado*: no. Procesa a 605 ev/s frente a 269 ev/s de entrada.
2. *La granularidad de lote*: no. Con `--trigger 500ms` y
   `--max-offsets-per-trigger 2000` la latencia paso de 6,92 a 6,65 s, un 4%.
3. *La escritura domina la latencia extremo a extremo*: domina el LOTE (84%),
   pero 572 ms de lote no explican 6,9 s de latencia.

**Queda sin explicar** el grueso de esos ~6 s. La sospecha restante es la
propagacion del watermark entre lotes: Spark lo calcula a partir del maximo
tiempo de evento del lote ANTERIOR, de modo que cerrar una ventana cuesta
varios lotes aunque los datos ya hayan llegado. Comprobarlo exige instrumentar
el instante de cierre de cada ventana, que es el siguiente paso.

## Sobre el KPI de latencia del Objetivo 1

El objetivo pide "publicacion MQTT -> disponible en TimescaleDB < 2 s (p95)".
**Medido: 3,55 s. No se cumple tal como esta redactado**, y conviene entender
por que antes de intentar optimizarlo.

TimescaleDB guarda **agregados por ventana**, y un agregado no puede existir
antes de que su ventana cierre: hay que esperar a que el watermark garantice
que no llegaran mas eventos de esa ventana. Ese tiempo de espera es parte de la
definicion de la metrica, no una ineficiencia del pipeline.

Lo que si mide latencia de ingesta pura es el camino de eventos individuales,
que no espera a ninguna ventana: **1,26 s en p95, por debajo del objetivo**.

Es decir, el pipeline ingiere por debajo de 2 s; lo que tarda mas es la
disponibilidad del agregado, que es otra cosa. Merece la pena reformular el KPI
separando ambas cifras — latencia de ingesta y latencia de disponibilidad del
agregado — porque ahora mismo mezcla dos magnitudes distintas en un solo
numero. Es una decision que afecta al texto del trabajo, no al codigo.
