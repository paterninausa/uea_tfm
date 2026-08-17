# Job de Spark Structured Streaming — doble sumidero

> **ESTADO: parcialmente desactualizado.** La lectura de Kafka, la
> deserializacion Avro y la guarda de version de esquema ya trabajan con el
> contrato de ASHRAE (`building_id`, `meter_type`, `timestamp`,
> `meter_reading`, `sim_publish_ts`). El **enriquecimiento, las reglas de
> anomalia y la agregacion siguen escritos para el dataset anterior** y
> referencian columnas que ya no existen (`power_watts`, `company_id`,
> `department`, `cpu_load`...). El job **no se ejecuta de extremo a extremo**
> hasta adaptarlos, junto con el DDL de las dos tablas y el broadcast join
> contra la tabla de dimension. Las cifras de la seccion "Resultados medidos"
> corresponden al dataset anterior y habra que rehacerlas.

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

## Por que se agrupa por planta y no por maquina

Se midio la cadencia real del dataset y **contradice el supuesto de partida**
del proyecto:

| Medicion | Resultado |
|---|---|
| Delta entre eventos consecutivos del flujo | 30 s (90,4% exactos) |
| Timestamps unicos | 999.966 sobre 999.966 filas |
| **Delta por maquina (mediana)** | **~104.000 s (~29 horas)** |
| Huecos > 60 s por maquina | 99,97% |

Los 30 s son la cadencia **global**: el dataset visita una maquina distinta
cada 30 s, y cada maquina concreta reporta una vez cada ~29 h. Una ventana de
1 minuto agrupada por `machine_id` contendria exactamente un evento y la
agregacion seria un paso a traves. Por eso se agrupa por
`company_id + site_id + department` con ventana de 1 hora.

Con el replay acelerado del simulador esto encaja bien: a 100 ev/s el tiempo de
evento avanza ~50 minutos por segundo de reloj, de modo que las ventanas de 1
hora se cierran continuamente y el flujo se comporta como un sistema en
regimen.

## Reglas de anomalia: las que se descartaron y por que

Las reglas se eligieron **midiendo su tasa de disparo** sobre el millon de
filas, no por intuicion. Estas tres parecen razonables y se descartaron:

| Regla candidata | Dispara en |
|---|---|
| `sensor_status = WARN` | 25,04% de los eventos |
| `measurement_quality = ESTIMATED` | 33,25% |
| `power_factor < 0,75` | 15,00% |
| **Union de las tres** | **57,49%** |

Un informe que senala a la mayoria de los eventos no detecta anomalias:
renombra la normalidad. La causa es que el dataset es sintetico y esos campos
siguen distribuciones casi uniformes — `power_factor` es uniforme entre 0,70 y
1,00, asi que cualquier umbral fijo marca una fraccion fija sin ninguna senal.

Las reglas que si discriminan:

| Regla activa | Dispara en | Significado |
|---|---|---|
| `equipo_encendido_sin_consumo` | 0,03% | `device_state=ON` con corriente cero: contradiccion entre estado declarado y medida |
| `incoherencia_electrica` | 0,21% | `power_watts` se aparta > 50% de V·I·PF |

El **orden de evaluacion importa**: toda fila con "ON y corriente cero" tiene
tambien potencia teorica cero y por tanto un error relativo del 100%, asi que
la regla de incoherencia la absorbe si va primero. Se comprobo inyectando
filas reales: con el orden inverso, las 10 filas de "ON sin consumo" se
etiquetaban como `incoherencia_electrica` y el motivo mas accionable nunca
aparecia.

`sensor_status` y `measurement_quality` siguen como columnas en la tabla, asi
que Power BI puede filtrar por ellos; simplemente no son anomalias. Ademas van
agregados en TimescaleDB como `warn_count` y `estimated_count`.

Pendiente: una tercera regla por desviacion respecto al consumo tipico de cada
`machine_model`. Los modelos forman poblaciones muy separadas (mediana de 8 W
en un movil frente a 27.450 W en un chiller, un rango de 3.400x), asi que
requiere una tabla de referencia por modelo unida al flujo por broadcast join.

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
Spark 4.2.0 en `local[*]`), con el simulador publicando a 100 ev/s:

**Integridad**

| Metrica | Resultado |
|---|---|
| Eventos recibidos / publicados por el bridge | 32.000 / 32.000 |
| Perdida | **0,0000%** |
| Eventos unicos en PostgreSQL | 12.000 (la deduplicacion colapso 20.000 reenvios) |
| Ventanas en TimescaleDB | 100 ventanas, 8.915 filas |

**Latencia, segun configuracion**

| Configuracion | Metricas p50/p95 | Eventos p95 |
|---|---|---|
| trigger 2 s, watermark 10 min | 4,44 / 5,61 s | 2,22 s |
| **trigger 1 s, watermark 2 min** (por defecto) | **2,66 / 3,55 s** | **1,26 s** |

Bajar el watermark de 10 a 2 minutos no perdio ni una ventana (4.426 filas y 50
ventanas en ambos casos), porque el flujo llega ordenado por construccion.

**Throughput**

Publicando 8.000 eventos sin limite de tasa: **739,8 ev/s** de extremo a
extremo con 0% de perdida, y Spark siguiendo el ritmo sin acumular retraso.
Son ~15x el objetivo de >=50 ev/s.

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
