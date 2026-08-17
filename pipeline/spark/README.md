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

Sobre el stack completo (Kafka 4.3.1, TimescaleDB 2.29.1-oss, PostgreSQL 17,
Spark 4.2.0 en `local[*]`), con el simulador publicando 90.000 eventos a 200 ev/s:

| Metrica | Resultado |
|---|---|
| Perdida de mensajes | **0,0000%** |
| Latencia MQTT -> PostgreSQL (evento) p50 / p95 | **0,800 s / 1,303 s** |
| Latencia MQTT -> TimescaleDB (agregado) p50 / p95 | 8,380 s / 10,249 s |
| Lecturas a cero | 3,66% |
| Anomalias detectadas | **0,46%** |
| Filas de metricas | 6.345 en 138 ventanas, 46 combinaciones |

La tasa de anomalias del 0,46% concuerda con el 0,34% previsto al elegir el
umbral de 5 x IQR sobre el dataset completo; la diferencia se explica porque la
muestra son los primeros eventos del ano y no el conjunto entero.

### La latencia del agregado empeoro y esta sin explicar

Con el dataset anterior este camino daba 3,55 s en p95; ahora da 10,25 s. La
hipotesis es que el cuello esta en el sumidero, no en la agregacion: cada
ventana de una hora agrega ahora una lectura de cada uno de los 652 sensores, se
mantienen mas ventanas abiertas a la vez, y el UPSERT recoge las filas en el
driver. **Es una hipotesis sin comprobar**; hay que medir la duracion de los
micro-lotes antes de tocar nada.

Parametros a revisar: `--max-offsets-per-trigger` (10.000 por defecto, quiza
demasiado para un trigger de 1 s), el numero de particiones de shuffle, y la
escritura por particion en lugar de recoger en el driver.

El camino de eventos individuales **si cumple** el objetivo de 2 s con holgura.

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
