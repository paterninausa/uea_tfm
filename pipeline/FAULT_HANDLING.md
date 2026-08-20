# Tolerancia a fallos del pipeline

Que puede fallar, que hace el sistema cuando ocurre y donde queda constancia.
Las cifras estan medidas sobre el stack completo y cada apartado indica como
reproducirlas.

Se distinguen dos familias, porque se comportan de forma distinta:

- **Fallos del dato** — un dispositivo envia algo que no cumple el contrato.
- **Fallos de componente** — una pieza del pipeline deja de responder.

Y una tercera categoria que las atraviesa: los fallos que **no producen ninguna
excepcion** y solo se detectan comprobando invariantes.

---

## 1. Capas de validacion

Cada evento atraviesa cuatro controles antes de contar como bueno:

| Capa | Donde | Que comprueba | Que ocurre si no pasa |
|---|---|---|---|
| Forma | Bridge, `schemaless_writer` | Campos presentes, tipos y simbolos del enum | Se desvia a la DLQ con el motivo |
| Dominio | Bridge, `validar_dominio` | Fecha no futura, lectura finita y no negativa | Se desvia a la DLQ con el motivo |
| Dominio (2.a linea) | Spark, `aggregate_metrics` | Fecha no futura, sobre lo que ya esta en Kafka | Se aparta del agregado, sigue en `telemetry_events` |
| Referencia | Spark, `aggregate_metrics` | Que el edificio exista en la dimension | Se aparta del agregado y se avisa |
| Identidad | Spark, `database_writers` | Que dos lecturas distintas no compartan clave | Se conserva una y se avisa |

Ninguna detiene el pipeline. Todas dejan constancia.

---

## 2. Fallos del dato

### Rechazo a la cola de mensajes muertos

| Caso | Motivo registrado |
|---|---|
| JSON malformado | `JSONDecodeError` |
| Falta un campo obligatorio | `no value and no default for <campo>` |
| `meter_type` fuera del enum | `'gas' is not in list` |
| Fecha ISO no parseable | `ValueError` en la conversion |
| `building_id` que no es texto | `must be string on field building_id` |
| Fecha futura | `timestamp en el futuro (...), lo que envenenaria el watermark` |
| Lectura negativa, `inf` o `NaN` | `meter_reading negativo (...)` / `no finito (...)` |

El evento rechazado se publica integro en `iot.telemetry.dlq` junto al motivo, el
topico de origen y el instante, de modo que puede reprocesarse una vez corregida
la causa. El topico tiene **retencion infinita** (`retention.ms=-1`), tambien al
recrearlo con `reset_state.py`: la evidencia de un rechazo no caduca.

El bridge agrupa los rechazos **por clase de motivo** en su informe periodico,
sustituyendo los valores concretos, de forma que varios eventos con el mismo
defecto se resumen en una linea con su recuento:

```
desglose de los 7 eventos rechazados:
      4 x ValueError: timestamp en el futuro ..., lo que envenenaria el watermark
      3 x ValueError: meter_reading negativo ..., imposible en un contador
```

El valor exacto se conserva en el registro de la DLQ y en el aviso individual de
cada rechazo.

### Por que el identificador del edificio es texto

`building_id` se declara `string` en el esquema Avro y se almacena como `TEXT` en
las tres tablas. La razon es que **Avro no valida los tipos numericos: los
convierte**, y esa conversion es silenciosa:

| Valor recibido | Si el campo fuera `int` | Siendo `string` |
|---|---|---|
| `156.9` | Se truncaria a `156`, atribuyendo el evento a **otro edificio** | Rechazado a la DLQ |
| `true` | Se convertiria en el edificio `1` | Rechazado a la DLQ |
| `2^40` | Se escribiria pese a exceder el rango de int32 | Rechazado a la DLQ |

El caso decimal no es rebuscado: un productor escrito en JavaScript no dispone de
enteros, y con numpy ocurre lo mismo si el tipo permanece en `float64`. Un evento
asi quedaria archivado en el sensor equivocado, con su agregado y su fila,
**indistinguible de un dato correcto**.

Como texto no existe conversion posible y el contrato se cumple o se rechaza.
Ademas, un identificador mal formado no coincide con ningun edificio, de modo que
el mecanismo de eventos sin dimension lo detecta.

`site_id` se mantiene como entero: no viaja en el evento —lo incorpora Spark
mediante el broadcast join contra la dimension— asi que ningun productor puede
enviarlo con un tipo incorrecto.

### Eventos sin dimension

Un `building_id` bien formado que no exista en `buildings` cumple el contrato,
por lo que la validacion de esquema no tiene nada que objetar. El evento:

- **Se persiste** en `telemetry_events`, que no necesita la dimension.
- **Se aparta** de la agregacion antes de la ventana temporal, porque sin
  `site_id` ni `primary_use` no existe criterio de agrupacion.

El dato no se pierde: incorporando ese edificio a la dimension y reprocesando, el
evento entra en los agregados con normalidad.

Se avisa en dos momentos: `monitoring.py` lo comprueba periodicamente contra la
dimension mientras el job corre —y solo emite el aviso cuando la cifra crece— y
`kpi_report.py` lo desglosa por edificio al cerrar una medicion.

### Duplicados y colisiones de clave

La cadena MQTT QoS 1 con los reintentos del productor de Kafka ofrece garantia
*at-least-once*, de modo que un mismo evento puede llegar mas de una vez y caer
en el mismo micro-lote. Deduplicar es obligatorio: PostgreSQL aborta con
`CardinalityViolation` si una clave aparece dos veces en la misma sentencia.

El escritor distingue dos situaciones muy distintas:

| Situacion | Interpretacion | Tratamiento |
|---|---|---|
| Misma clave, misma medida | Reentrega del mismo evento | Se conserva una, sin aviso |
| Misma clave, **medida distinta** | Dos lecturas reales en conflicto | Se conserva una y se registra un ERROR |

La comparacion excluye las columnas de instrumentacion. Un evento reenviado tras
una reconexion lleva un `sim_publish_ts` nuevo siendo el mismo evento, de manera
que comparar las filas completas marcaria como conflicto cada reentrega
legitima.

---

### La fecha futura y el watermark global

Una marca de tiempo adelantada es el evento mas danino que puede recibir el
sistema, y el que menos se parece a un fallo: cumple el esquema, tiene todos sus
campos y su lectura es perfectamente plausible.

El dano viene de como funciona el watermark de Spark. Es **uno solo para toda la
consulta** —no existe watermark por clave ni por particion— y vale el mayor
`timestamp` visto en cualquier evento menos el retraso configurado. Un evento
del ano 2036 lo deja ahi, y desde ese instante los eventos legitimos caen por
debajo y Spark los descarta como tardios. **Un solo dispositivo mal configurado
detiene la agregacion de los 652 sensores.**

El efecto se reparte de forma desigual entre las dos rutas, y eso lo hace mas
dificil de advertir: `telemetry_metrics` se detiene y Grafana se congela,
mientras `telemetry_events` —que no tiene watermark— sigue escribiendo con
normalidad.

Por eso la comprobacion existe **en dos sitios**. El bridge rechaza el evento a
la DLQ antes de que entre en Kafka; el job lo aparta antes del `withWatermark`,
que es la unica posicion util, ya que el watermark se calcula al entrar el
evento. La segunda linea cubre lo que la primera no alcanza: un evento que ya
esta en el log de Kafka, publicado sin pasar por el bridge o presente desde
antes, y que reaparece en cada reprocesamiento. Ninguna de las dos destruye el
dato: en ambos casos queda constancia, y en el segundo el evento se persiste
igual en `telemetry_events`.

Ambos margenes son de 300 segundos, muy por encima del desajuste esperable entre
relojes: solo disparan ante un problema estructural —zona horaria mal
interpretada en el origen, reloj del servidor atrasado, epoch en milisegundos
leido como segundos—.

Medido con `tools/watermark_poison_test.py`, inyectando **un** evento diez anos
adelantado directamente en Kafka mientras el simulador publica a ~357 ev/s:

| | Sin la segunda linea | Con ella |
|---|---|---|
| `telemetry_metrics`, 45 s en regimen | **0 filas** | 1.149 filas |
| `telemetry_events`, mismo intervalo | 16.126 filas | 16.488 filas |
| Errores registrados | **ninguno** | ninguno |

El envenenamiento se manifiesta primero como una **rafaga** y despues como
silencio: al saltar el watermark, todas las ventanas abiertas quedan por debajo
de el y se emiten de golpe —174 filas— antes de que no vuelva a escribirse
ninguna. Un recuento acumulado a traves de ese pico lo confunde con actividad
normal, de modo que la comprobacion util es la del tramo posterior.

---

## 3. Fallos de componente

### Recuperacion medida

Tumbando cada servicio durante 15 segundos con `docker kill`, que no concede el
cierre ordenado de un SIGTERM:

| Servicio | Flujo restablecido | Mecanismo |
|---|---|---|
| Mosquitto | **16,1 s** | Sesion MQTT persistente del bridge y reconexion de los productores |
| Kafka | **15,2 s** | Reintentos del productor del bridge (`acks=all`, `retries=5`) |
| TimescaleDB | **4,1 s** | Reintentos de escritura y relanzado de la consulta |
| PostgreSQL | **5,1 s** | Reintentos de escritura y relanzado de la consulta |

Objetivo del trabajo: recuperacion inferior a 60 s. Se reproduce con
`tools/failover_test.py --target <servicio>`.

### Independencia de los dos sumideros

El job mantiene dos consultas de streaming independientes, cada una con su
checkpoint. Tres mecanismos garantizan que el fallo de un sumidero no afecte al
otro:

1. **Reintentos en la escritura** (`--db-retries`, `--db-retry-wait`). Absorben
   una indisponibilidad breve sin que la consulta llegue a caer. Solo se
   reintenta `OperationalError` —imposibilidad de comunicarse con el servidor—;
   un error de datos no se reintenta, porque indica un incumplimiento del
   contrato que debe hacerse visible.
2. **Supervision por estado** (`--supervision-interval`). Se comprueba
   periodicamente `isActive` de cada consulta, sin esperar bloqueado a ninguna en
   particular.
3. **Relanzado automatico** (`--max-reinicios`). La consulta caida se rearranca
   desde su propio checkpoint, lo que garantiza que reanude sin perder ni
   duplicar. Agotados los reintentos, se abandona esa consulta y se registra el
   motivo.

Comprobado tumbando TimescaleDB: la consulta que escribe en PostgreSQL continuo
produciendo **21 micro-lotes** durante la caida.

### Comportamiento del productor ante la caida del broker

Cada conexion del simulador conserva el evento que no llego a confirmarse,
reintenta la conexion y, al restablecerse, reenvia primero ese pendiente. Es el
comportamiento de un medidor real, que almacena el perfil de carga y lo vuelca
cuando recupera el enlace.

El backoff de reconexion del bridge esta acotado a 5 segundos. Sin ese limite, un
consumidor que tarda mas en resuscribirse que los productores en reconectar
provoca que el broker acumule mensajes para su sesion persistente hasta agotar
`max_queued_messages` y descarte el excedente.

Resultado de tumbar el broker durante 15 segundos con 652 sensores publicando:

| Metrica | Resultado |
|---|---|
| Publicados por el simulador | **60.000 / 60.000**, 0 fallidos |
| Recibidos por el bridge | **60.000** |
| Descartados por Mosquitto | **0** |

### Retencion de Kafka

`failOnDataLoss` esta activo: si la retencion de 7 dias elimina offsets que el
job aun no ha leido, la consulta se detiene en lugar de omitirlos. Omitirlos
seria una perdida que ninguna otra comprobacion podria detectar, porque un offset
saltado no figura como evento descartado ni como retraso de consumo.

**Nota operativa**: `docker compose down -v` destruye los topicos, pero los
checkpoints de Spark residen en el sistema de ficheros del host y sobreviven. Al
arrancar apuntan a offsets inexistentes y el job se detiene, correctamente. Tras
un `down -v` hay que eliminar `spark/checkpoints/`.

---

## 4. Deteccion de fallos sin excepcion

Determinados fallos no producen ningun error: se manifiestan como **ausencias**
—datos que dejan de llegar, ventanas que no cierran, filas que no se escriben— y
un registro de actividad no puede delatarlos, porque solo anota lo que ocurre.

El pipeline comprueba invariantes para cubrirlos:

| Invariante | Donde | Que detecta |
|---|---|---|
| `rows_dropped_by_watermark > 0` | `monitoring.py` | Eventos descartados por llegar tarde |
| Desfase de Kafka con cero filas de entrada | `monitoring.py` | Consulta que ha dejado de consumir |
| Eventos sin correspondencia en la dimension | `monitoring.py`, `kpi_report.py` | Edificios desconocidos |
| Colision de clave natural | `database_writers.py` | Dos medidas distintas en conflicto |
| `$SYS/broker/publish/messages/dropped` | `kpi_report.py` | Mensajes que el broker acepto y no entrego |
| Unicidad de la clave natural | `prepare_ashrae.py` | Un dataset que no cumple lo que se asume |

Las dos primeras se obtienen del informe de progreso que Spark ya publica
—`stateOperators[].numRowsDroppedByWatermark` y
`sources[].metrics.maxOffsetsBehindLatest`— de modo que no suponen ninguna
consulta adicional sobre el flujo, y quedan ademas persistidas en
`streaming_progress` para su analisis posterior.

---

## 5. Punto de saturacion

Con los 652 sensores y ritmo creciente, medido en el consumo:

| Ritmo solicitado | Publicado real | Persistido | Latencia p95 |
|---|---|---|---|
| 906 ev/s | 896,5 (99%) | 40.000 | 1,28 s |
| **1.811 ev/s** | **1.798,6 (99,3%)** | 40.000 | **1,97 s** |
| 3.622 ev/s | 2.550,5 (70%) | 40.000 | 3,02 s |
| 7.244 ev/s | 2.297,0 (32%) | 39.316 | 6,44 s |

El pipeline sostiene **1.800 ev/s** con la latencia y el micro-lote dentro de
objetivo, cifra que equivale a **10.000 veces el caso de uso real** de 0,1797
ev/s. El techo de aproximadamente 2.900 ev/s corresponde al simulador, no al
pipeline: por encima de ese punto publica menos cuanto mayor es el ritmo
solicitado.

Superado el punto de saturacion aparece una perdida que **ningun indicador del
pipeline refleja**. En el escalon mas alto, el productor registro 40.000
publicados y 0 fallidos —recibio confirmacion de todos— mientras al bridge
llegaron 38.221: el broker descarto 1.779 al llenarse su cola de salida. Un
**4,4% de perdida** sin error en el productor, en el bridge, en la DLQ ni en los
registros del broker. El unico indicio es el contador `$SYS`, que `kpi_report.py`
consulta.

---

## 6. Limites conocidos

**Sensores que dejan de emitir.** No hay deteccion de ausencias: si un contador
se apaga, sus eventos dejan de llegar sin que nada lo senale. Los sensores
activos siguen contando, las latencias se mantienen y los agregados continuan
escribiendose. La cobertura de este caso se delega en la vigilancia del dashboard
de Grafana, donde el panel de edificios reportando lo evidencia.

**Valores plausibles pero incorrectos.** Una lectura de `1e100` es positiva y
finita, de modo que supera las guardas y contamina la suma y la media de su
ventana. Solo se marcaria como anomalia si ese sensor dispone de linea base. Lo
mismo ocurre con una medida expresada en la unidad equivocada: es indetectable
sin contexto externo.

**Combinaciones edificio-contador inexistentes.** Si el edificio existe, el join
resuelve la dimension y el evento entra en los agregados con normalidad. No se
comprueba que esa combinacion concreta sea real, aunque `sensor_baseline`
contiene las 652 legitimas y permitiria hacerlo.

**Campos adicionales en el payload.** Se ignoran sin dejar constancia. No afectan
al dato, pero un productor podria creer que envia informacion que no llega a
ningun destino.

---

## 7. Reproduccion

El ciclo completo esta descrito en [tools/README.md](tools/README.md):

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
python pipeline/tools/watermark_poison_test.py
```

```bash
python pipeline/tools/kpi_report.py
```

**El orden de arranque condiciona el resultado**: los consumidores deben estar en
marcha antes de generar carga. Con la misma carga y el mismo sistema, arrancar
Spark despues del productor eleva la latencia de ingesta p95 de **1,38 s a 92,53
s**, porque los eventos esperan en Kafka y esa espera se contabiliza desde el
instante de publicacion.
