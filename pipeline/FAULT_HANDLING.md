# Tolerancia a fallos del pipeline

Que puede fallar, que hace el sistema cuando ocurre y donde se ve. Todo lo que
sigue esta **medido sobre el stack real**, no razonado: las cifras salen de
ejecuciones concretas de agosto de 2026, y cada apartado dice como reproducirlas.

El documento cubre dos familias que se comportan de forma muy distinta:

- **Fallos del dato** — un dispositivo envia algo que no deberia.
- **Fallos de componente** — una pieza del pipeline se cae.

Y una tercera que no es una familia sino un patron, y es la mas importante:
**los fallos silenciosos**, los que no producen ninguna excepcion.

---

## 1. Las capas de defensa

Cada dato atraviesa cuatro controles antes de contar como bueno:

| Capa | Donde | Que comprueba | Que hace si falla |
|---|---|---|---|
| Forma | Bridge, `schemaless_writer` | Campos presentes, tipos, simbolos del enum | DLQ con el motivo |
| Dominio | Bridge, `validar_dominio` | Fecha no futura, lectura finita y no negativa | DLQ con el motivo |
| Referencia | Spark, `aggregate_metrics` | Que el edificio exista en la dimension | Se aparta del agregado y se avisa |
| Identidad | Spark, `database_writers` | Que dos lecturas distintas no compartan clave | Se escribe una y se avisa como ERROR |

Ninguna de las cuatro detiene el pipeline. Todas dejan constancia.

---

## 2. Fallos del dato

### Lo que se rechaza a la DLQ

| Caso | Motivo que aparece en el log |
|---|---|
| JSON malformado | `JSONDecodeError` |
| Falta un campo obligatorio | `no value and no default for <campo>` |
| `meter_type` fuera del enum | `'gas' is not in list` |
| Fecha ISO no parseable | `ValueError` al convertir |
| **`building_id` que no es texto** | `must be string on field building_id` |
| **Fecha futura** | `timestamp en el futuro (...), lo que envenenaria el watermark` |
| **Lectura negativa, `inf` o `NaN`** | `meter_reading negativo (...)` / `no finito (...)` |

El bridge agrupa los rechazos **por clase de motivo** en su informe periodico,
quitando los valores concretos: cuatro fechas futuras distintas salen como una
sola linea con su recuento, no como cuatro motivos. El valor exacto se conserva
integro en el registro de la DLQ y en el aviso individual.

```
desglose de los 7 eventos rechazados:
      4 x ValueError: timestamp en el futuro ..., lo que envenenaria el watermark
      3 x ValueError: meter_reading negativo ..., imposible en un contador
```

La DLQ tiene **retencion infinita** (`retention.ms=-1`), tambien al recrearla con
`reset_state.py`: un rechazo del viernes seguia ahi el lunes siguiente, y con la
retencion por defecto de 7 dias no habria sobrevivido a una semana de vacaciones.

### Por que `building_id` es texto

Es el caso mas instructivo, porque el fallo no era un rechazo sino una
**corrupcion invisible**. Avro no valida los tipos: los convierte.

| Se envia | Con `int` (antes) | Con `string` (hoy) |
|---|---|---|
| `156.9` | **`156`** — atribuido a otro edificio | rechazado a la DLQ |
| `true` | **`1`** — el edificio 1 | rechazado a la DLQ |
| `2^40` | escrito, fuera del rango de int32 | rechazado a la DLQ |

Un `156.9` no es rebuscado: un productor en JavaScript no tiene enteros, y en
numpy pasa lo mismo si el tipo se queda en `float64`. El evento acababa archivado
en el sensor equivocado, con su agregado y su fila, **indistinguible de un dato
bueno**.

`site_id` sigue siendo entero a proposito: no viaja en el evento —lo anade Spark
con el broadcast join— asi que ningun productor puede equivocarse con el.

### Lo que no se rechaza pero se aparta

Un `building_id` bien formado que **no existe en la dimension** cumple el
contrato, asi que el bridge no tiene nada que objetar. Se persiste en
`telemetry_events` y se aparta de la agregacion, porque sin `site_id` ni
`primary_use` no hay por donde agruparlo.

**El dato no se pierde**: si manana se anade ese edificio a `buildings`, basta
reprocesar para que entre en los agregados.

Antes de apartarlo, uno solo bastaba para inutilizar la ruta operativa: la
escritura fallaba con `NotNullViolation`, el supervisor relanzaba la consulta y
volvia a fallar con el mismo lote, y **el evento se quedaba en Kafka
envenenando cualquier arranque posterior**. Verificado con `building_id=99999`.

Avisan dos sitios: `monitoring.py` mientras ocurre —cada cinco minutos, y solo
cuando la cifra crece— y `kpi_report.py` con el desglose por edificio al cerrar
una medicion.

### Duplicados y colisiones

La cadena MQTT QoS 1 mas los reintentos del productor da garantia
*at-least-once*, asi que el mismo evento puede llegar dos veces y caer en el
mismo micro-lote. Deduplicar es obligatorio: PostgreSQL aborta con
`CardinalityViolation` si la misma clave aparece dos veces en una sentencia.

Lo que se distingue ahora es **que clase de duplicado es**:

| Caso | Que es | Que se hace |
|---|---|---|
| Misma clave, misma medida | Reentrega del mismo evento | Se queda una. Sin aviso |
| Misma clave, **medida distinta** | Dos lecturas reales compitiendo | Se queda una y se avisa como ERROR |

La comparacion excluye las columnas de instrumentacion: desde que el simulador
reintenta lo no confirmado, un evento reenviado trae un `sim_publish_ts` nuevo
siendo el mismo evento, y comparar filas enteras habria marcado como colision
cada reentrega legitima.

---

## 3. Fallos de componente

### Recuperacion medida

Tumbando cada servicio 15 segundos con `docker kill`, que no avisa con un
SIGTERM ordenado:

| Servicio | Flujo restablecido | Mecanismo |
|---|---|---|
| **Mosquitto** | **16,1 s** | Sesion MQTT persistente del bridge y reconexion de los productores |
| **Kafka** | **15,2 s** | Reintentos del productor del bridge (`acks=all`, `retries=5`) |
| **TimescaleDB** | **4,1 s** | Reintentos de escritura y relanzado de la consulta |
| **PostgreSQL** | **5,1 s** | Igual |

Objetivo del trabajo: recuperacion < 60 s. Se reproduce con
`tools/failover_test.py --target <servicio>`.

### El proceso de Spark ya no es un punto unico de fallo

Hasta agosto de 2026 el job esperaba asi:

```python
for q in consultas:
    q.awaitTermination()
```

Espera **secuencialmente**, de modo que solo vigilaba la primera consulta. Las
consecuencias, medidas:

| Base tumbada | Que ocurria |
|---|---|
| TimescaleDB | Moria `metricas-timescaledb`, la primera de la lista, y su excepcion terminaba **el proceso entero**, arrastrando a la consulta de PostgreSQL que no dependia de ella |
| PostgreSQL | Moria `eventos-postgresql`, la segunda, y como nadie la esperaba **el proceso seguia vivo** escribiendo metricas: media arquitectura parada, sin un solo aviso |

Hoy hay tres capas:

1. **Reintentos en la escritura** (`--db-retries`, `--db-retry-wait`). Una caida
   corta se absorbe sin que muera nada. Solo se reintenta `OperationalError`;
   un error de datos no se reintenta, porque eso es un incumplimiento del
   contrato y tiene que salir a la luz.
2. **Supervision de todas las consultas** (`--supervision-interval`): se
   comprueba `isActive` de cada una, sin esperar a ninguna en particular.
3. **Relanzado automatico** (`--max-reinicios`): la consulta caida se rearranca
   desde su propio checkpoint. Agotados los reinicios, se abandona esa consulta
   y se dice por que.

Comprobado tumbando TimescaleDB: la consulta de PostgreSQL **siguio produciendo
21 micro-lotes** durante la caida.

### El productor sobrevive a la caida del broker

`aiomqtt` no reconecta por su cuenta, y el sintoma no era un fallo sino un
simulador inutil: cada `publish` sobre el cliente muerto lanzaba `MqttError`, se
contaba como fallo y se pasaba al evento siguiente. El simulador quemaba su
agenda entera a velocidad de CPU —**12.814 publicados frente a 35.597
fallidos**— sin publicar nada mas y sin recuperarse aunque el broker volviera.

Hoy cada conexion guarda el evento que no llego a confirmarse, reintenta la
conexion y al volver reenvia primero ese pendiente. Es lo que hace un medidor
real, que guarda el perfil de carga y lo vuelca al recuperar el enlace.

Con eso, y acotando el backoff de reconexion del bridge a 5 s, el resultado de
tumbar el broker 15 segundos es:

| Metrica | Resultado |
|---|---|
| Publicados por el simulador | **60.000 / 60.000**, 0 fallidos |
| Recibidos por el bridge | **60.000** |
| Descartados por Mosquitto | **0** |

**La perdida que quedaba no era del productor.** Al volver el broker, los 652
sensores publicaban con normalidad mientras el bridge tardaba **17 segundos
mas** en resuscribirse, porque paho aumenta su espera de reconexion hasta 120 s.
En esa ventana Mosquitto encolaba para la sesion persistente del bridge hasta
llenar `max_queued_messages` y descartaba 6.595 mensajes. El consumidor tardon
era la causa, no el productor rapido.

### Retencion de Kafka

`failOnDataLoss` esta **activo**: si la retencion de 7 dias borro offsets que el
job aun no habia leido, la consulta se detiene en vez de saltarselos. Estuvo en
`false` y era el fallo silencioso mas grave que quedaba, porque ninguna
comprobacion podia verlo: un offset saltado no cuenta como evento descartado ni
como atraso.

**Aviso operativo**: un `docker compose down -v` destruye los topicos, pero los
checkpoints de Spark viven en el sistema de ficheros del host y sobreviven. Al
arrancar quedan apuntando a offsets que ya no existen y el job falla —
correctamente. Hay que borrar `spark/checkpoints/` despues de un `down -v`.

---

## 4. Los fallos silenciosos, que son el patron importante

De los cinco encontrados midiendo, **el log de actividad solo delato uno**. Los
otros cuatro se detectaron por casualidad: una tabla vacia, otra congelada, un
recuento que no cuadraba.

| Fallo | Como se detecto entonces | Como se detecta hoy |
|---|---|---|
| Clave de Kafka a `None` | Leyendo el codigo | Acceso directo al campo: si falta, va a la DLQ |
| Simulador quemando su agenda | El log, con `fallidos` disparados | Igual, y ademas ya no ocurre |
| Consulta de Spark muerta | Una tabla congelada | El supervisor la relanza y lo dice |
| Descartes de Mosquitto | Comparando publicados con recibidos | `kpi_report` lee `$SYS/broker/publish/messages/dropped` |
| Watermark envenenado | `telemetry_metrics` aparecia vacia | Invariante `rows_dropped_by_watermark` |

La leccion metodologica: **un log que registra actividad sirve para reconstruir
que paso despues del incidente, pero no puede delatar lo que NO pasa**. Para eso
hacen falta invariantes — preguntarle al sistema si sigue cumpliendose lo que se
da por hecho.

Las que vigila el pipeline hoy:

| Invariante | Donde | Que caza |
|---|---|---|
| `rows_dropped_by_watermark > 0` | `monitoring.py` | Eventos descartados por tardios |
| Desfase de Kafka con cero filas de entrada | `monitoring.py` | Consulta que no consume |
| Eventos sin dimension | `monitoring.py` y `kpi_report.py` | Edificios desconocidos |
| Colision de clave natural | `database_writers.py` | Dos medidas distintas compitiendo |
| Descartes del broker | `kpi_report.py` | Perdida invisible por saturacion |
| Unicidad de la clave natural | `prepare_ashrae.py` | Un dataset que no cumple lo que se asume |

---

## 5. Punto de saturacion

Por encima de cierta carga aparece una perdida que **no delata ningun indicador
del pipeline**. Medido con los 652 sensores y ritmo creciente:

| Ritmo pedido | Publicado real | Persistido | Latencia p95 |
|---|---|---|---|
| 906 ev/s | 896,5 (99%) | 40.000 | 1,28 s |
| **1.811 ev/s** | **1.798,6 (99,3%)** | 40.000 | **1,97 s** |
| 3.622 ev/s | 2.550,5 (70%) | 40.000 | 3,02 s |
| 7.244 ev/s | 2.297,0 (32%) | 39.316 | 6,44 s |

**El pipeline sostiene 1.800 ev/s con todo dentro de objetivo**, que son 10.000
veces el caso de uso real de 0,1797 ev/s. El techo de ~2.900 ev/s es del
SIMULADOR: por encima publica menos cuanto mas se le pide.

Y pasado ese punto, **Mosquitto descarta en silencio**. En el peldano mas alto el
productor registro 40.000 publicados y 0 fallidos —recibio su PUBACK de cada
uno— pero al bridge llegaron 38.221: el broker tiro 1.779 al llenarse su cola de
salida. **Un 4,4% de perdida invisible con todos los indicadores en verde.**

---

## 6. Lo que NO esta cubierto

Por honestidad, y porque saber donde no llega el sistema vale tanto como saber
donde si:

**Un sensor que deja de emitir.** No hay deteccion de ausencias: si un contador
se apaga, sus eventos dejan de llegar y nadie lo echa de menos. Los que si
emiten siguen contando, las latencias siguen bien y los agregados siguen
escribiendose. Es decision consciente: se cubre con la vigilancia del dashboard
de Grafana, donde el panel de edificios reportando lo delata.

**Valores plausibles pero incorrectos.** Un `meter_reading` de `1e100` es
positivo y finito, asi que pasa las guardas y contamina la suma y la media de su
ventana. Se marcaria como anomalia solo si ese sensor tiene linea base. Lo mismo
con una lectura en la unidad equivocada: es indetectable sin contexto externo.

**Un `meter_type` que ese edificio no tiene.** El edificio existe, asi que el
join encuentra dimension y el evento entra en los agregados con normalidad. Nadie
comprueba que la combinacion edificio-contador sea real, aunque
`sensor_baseline` tiene las 652 combinaciones legitimas y permitiria hacerlo.

**Campos extra en el payload.** Se ignoran sin decir nada. Inofensivo, salvo que
alguien crea que esta enviando algo que no llega a ninguna parte.

---

## 7. Como reproducir todo esto

El ciclo completo esta en [tools/README.md](tools/README.md). En resumen:

```bash
python pipeline/tools/reset_state.py --yes
```

```bash
python pipeline/tools/failover_test.py --target mosquitto
```

```bash
python pipeline/tools/load_ladder.py --speedups 5000,10000,20000,40000
```

```bash
python pipeline/tools/kpi_report.py
```

**El orden importa**: los consumidores tienen que estar en marcha antes de
publicar. Se midio el mismo sistema con la misma carga en los dos ordenes y la
latencia de ingesta p95 paso de **1,38 s a 92,53 s**, porque arrancando Spark
despues los eventos esperan en Kafka y esa espera cuenta.
