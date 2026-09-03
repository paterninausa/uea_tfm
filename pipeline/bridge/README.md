# Bridge MQTT → Kafka

Microservicio que conecta el broker MQTT con el log de Kafka, validando cada
evento contra el esquema Avro registrado en Apicurio.

```
iot/{building_id}/{meter_type}/telemetry            iot.telemetry.raw
    (JSON, QoS 1)   -->   [ bridge ]   -->   Avro + cabecera de esquema
                                \
                                 -->   iot.telemetry.dlq  (JSON + motivo)
```

Existe porque Mosquitto no tiene puente nativo a Kafka. Se evaluo NanoMQ por
tenerlo, pero se verifico que esa funcion es exclusiva de EMQX Enterprise (de
pago), asi que el puente es codigo propio.

## Formato del mensaje en Kafka

El bridge serializa con el `AvroSerializer` de confluent-kafka, que escribe el
formato de cable de facto del ecosistema Kafka. La resolucion del esquema y el
tamano de la cabecera se comparten con el job de Spark en
[`../common/apicurio.py`](../common/apicurio.py), para que productor y
consumidor no puedan divergir:

```
[ 1 byte  ] byte magico 0x00
[ 4 bytes ] id del esquema en el registro (big-endian)
[ resto   ] payload Avro binario schemaless
```

La cabecera hace que **cada evento declare su version de esquema**, lo que
permite al consumidor resolverla sin acuerdos previos y que convivan dos
versiones en el mismo topico durante una evolucion (Objetivo 2). La alternativa
—Avro "pelado", sin cabecera— obligaria a asumir con que esquema se escribio
cada mensaje.

El id lo asigna la API compatible con Confluent (ccompat) del registro, no la
API nativa de Apicurio: son espacios de identificadores distintos, y la unica
registracion gobernada es la de ccompat (ver
[`../schemas/register_schema.py`](../schemas/register_schema.py)). Al respetar
este formato estandar, cualquier consumidor compatible con un registro de
esquemas puede leer los eventos sin adaptacion.

## Decisiones de diseño

**La clave de Kafka identifica al sensor**, es decir el par `(building_id,
meter_type)`, y se escribe como `156:electricity`. Garantiza que todas las
lecturas de un mismo medidor caen en la misma particion y se procesan en orden,
que es lo que necesitan las agregaciones por ventana de Spark.

No confundirla con la clave natural del evento, que ademas incluye `timestamp`:
esa identifica una lectura concreta, y usarla aqui repartiria al azar entre
particiones las lecturas de un mismo medidor.

Se construye con acceso directo a los dos campos, no con `.get()`, y el motivo es
un fallo real: el bridge conservaba `evento.get("machine_id")` del dataset
anterior, campo que el contrato de ASHRAE no tiene. La clave era **None en todos
los mensajes**, el productor los repartia en round-robin y el orden por sensor se
perdia sin un solo error en el log. Con acceso directo, un evento que no traiga
esos campos no cumple el contrato y acaba en la DLQ, que es donde debe acabar.

**Dos guardas de dominio, ademas de la validacion de esquema.** Avro comprueba la
FORMA —que los campos esten y sean del tipo declarado— pero no el significado. Se
verifico que pasan sin objecion un `meter_reading` de 1e308, uno negativo y una
marca de tiempo del ano 2099:

| Guarda | Por que |
|---|---|
| `timestamp` no futuro (margen de 5 min, `--margen-futuro`) | Es el caso que mas dano hace y no da ningun error: Spark adelanta su watermark a esa fecha y **descarta como tardio todo el trafico legitimo posterior**. El pipeline sigue vivo, los medidores dicen que todo va bien y los agregados dejan de escribirse |
| `meter_reading` finito y no negativo | Un valor negativo no existe en un medidor, y un infinito o un NaN arruinan la suma y la media de toda la ventana en la que caigan |

No hay limite por abajo en la fecha a proposito: el simulador reproduce el
historico de 2016 y sus marcas son legitimamente antiguas.

**El backoff de reconexion esta acotado a 5 segundos.** Por defecto paho lo
aumenta exponencialmente hasta 120 s, y eso costaba datos: tras una caida de 15 s
el broker volvia pero el bridge tardaba 17 s mas en resuscribirse, y en esa
ventana Mosquitto encolaba para su sesion persistente hasta llenar
`max_queued_messages` y descartar 6.595 mensajes en silencio. Con el tope, el
bridge vuelve en unos 2 s y la perdida desaparece.

**Los timestamps ISO se interpretan como UTC de forma explicita.** El dataset
los trae sin zona horaria (`2025-01-01T00:00:00`). Dejarlos a merced de la zona
local haria que el mismo evento cayera en una ventana temporal distinta segun
la maquina donde corriera el bridge.

**El bridge no corrige nada.** No rellena campos ausentes ni normaliza valores
fuera de dominio. Si el evento no cumple el contrato, falla y va a la DLQ. Ahi
esta la evidencia del "100% de eventos validados" del Objetivo 2: un bridge
indulgente la haria imposible de sostener.

**La DLQ guarda el payload original intacto**, en JSON y no en Avro (por
definicion el evento no cumple el esquema, luego no puede serializarse con el),
junto con el motivo del rechazo. Asi es reprocesable una vez corregida la causa.

**Lo desviado a la DLQ no cuenta como perdida** en las metricas: esta
persistido y es recuperable. Cuenta como evento invalido. La perdida real es lo
recibido de MQTT que no llego a ningun topico de Kafka.

**El esquema se resuelve una sola vez, al arrancar.** Si se registra una version
nueva, el bridge la adopta al reiniciarse, no en caliente: cambiar de esquema a
mitad de un flujo sin control explicito es justo lo que la gobernanza evita.

**Sin cola de trabajo intermedia.** El procesado ocurre en el hilo de red de
paho porque el trabajo por mensaje es corto y `producer.send` es asincrono.
Una cola intermedia anadiria un punto donde perder mensajes ante una caida. Si
las pruebas de carga del Objetivo 5 muestran que ese hilo se satura, el
siguiente paso seria un pool de trabajadores.

## Uso

Desde el 3 de septiembre de 2026 el bridge es un **servicio de Compose**: lo
levanta `docker compose up -d`, y no antes de que el contenedor `register-schema`
(que ejecuta `register_schema.py`) termine con exito. No hay que arrancarlo a
mano.

```bash
docker compose -f pipeline/docker-compose.yml up -d
```

```bash
docker compose -f pipeline/docker-compose.yml logs -f bridge
```

Generar trafico contra el (en otra terminal, con el simulador del host):

```bash
python pipeline/simulator/mqtt_simulator.py --acelerar 2000 --limite 300
```

Para iterar sobre el codigo del bridge sin reconstruir la imagen se puede
ejecutar el script directamente contra el stack, parando antes el contenedor
(`docker compose stop bridge`). Se queda en primer plano; `Ctrl+C` cierra
ordenadamente vaciando el buffer del productor:

```bash
python pipeline/bridge/mqtt_kafka_bridge.py --report-interval 5
```

Parametros utiles: `--bootstrap-servers`, `--broker-host`, `--qos`,
`--linger-ms`, `--registry-url`. Ver `--help`.

## Metricas

Cada `--report-interval` segundos se registra una linea con los medidores y
los percentiles de latencia MQTT→Kafka, y al cerrar se imprime el resumen final.
El codigo de salida es 0 si no hubo perdidas y 1 si las hubo.

Un informe intermedio puede mostrar `perdidos=1` de forma transitoria: es un
evento recibido cuyo callback de entrega de Kafka aun no ha llegado cuando se
toma la instantanea. Se resuelve en el informe siguiente; solo el resumen final,
tras el `flush()`, es concluyente.

## Estado verificado

Medido sobre el stack real (Mosquitto 2.0.22, Kafka 4.3.1 KRaft, Apicurio
3.3.1), con el simulador publicando a 60 ev/s:

**Camino nominal**

| Metrica | Resultado |
|---|---|
| Eventos recibidos / publicados | 300 / 300 |
| Perdida | **0,0000%** |
| Latencia MQTT→Kafka p50 / p95 / max | **1,5 / 2,2 / 6,0 ms** |
| Particiones utilizadas | 0, 1, 2 |

**Esta medicion es anterior al dataset de ASHRAE y a la correccion de la
clave**: se tomo cuando la clave seguia siendo `machine_id` y el payload era el
del dataset sintetico anterior, de modo que ni el tamano del mensaje ni el
reparto por particiones que refleja son los actuales. Sobre ASHRAE, con el
`AvroSerializer` de Confluent, el mensaje ronda los **30 B** (5 de cabecera de
Confluent -- byte magico + id de esquema de 4 bytes -- mas el payload Avro
schemaless, ~23 B segun la longitud del `building_id`) y las claves del tipo
`156:electricity` reparten de forma desigual por hash (6.144 / 7.468 / 6.388
sobre 20.000 eventos), no en round-robin como haria una clave nula.

**Camino de rechazo (DLQ)**

Sobre el contrato de ASHRAE (`building_id`, `meter_type`, `timestamp`,
`meter_reading`), cada caso se desvia con su motivo exacto y sin afectar a la
tasa de perdida (tabla completa en [../FAULT_HANDLING.md](../FAULT_HANDLING.md)
§2):

| Evento inyectado | Motivo registrado en la DLQ |
|---|---|
| `meter_type` fuera del enum (p. ej. `"gas"`) | `'gas' is not in list` |
| Falta un campo obligatorio | `no value and no default for <campo>` |
| `building_id` numerico (p. ej. `156.9`) | `must be string on field building_id` |
| `timestamp` en el futuro | `timestamp en el futuro (...), lo que envenenaria el watermark` |
| `meter_reading` negativo, `inf` o `NaN` | `meter_reading negativo (...)` / `no finito (...)` |
| JSON mal formado | `JSONDecodeError: Expecting property name...` |

**Recuperacion ante caida (Objetivo 5)**

Se detuvo el bridge, se publicaron 50 eventos con el caido y se reinicio: el
broker le entrego los 50 al reconectar gracias a la sesion persistente
(`clean_session=False` + QoS 1), **sin perdida**. La latencia p50 de esos
eventos fue de 10,8 s, que es exactamente el tiempo que pasaron encolados en el
broker.

Nota: la latencia medida aqui es solo el tramo MQTT→Kafka. La latencia extremo a
extremo hasta el sumidero (Objetivo 1) se mide con `tools/kpi_report.py`; las
cifras vigentes estan en la memoria y en `../FAULT_HANDLING.md`.
