# Bridge MQTT → Kafka

Microservicio que conecta el broker MQTT con el log de Kafka, validando cada
evento contra el esquema Avro registrado en Apicurio.

```
iot/{company}/{site}/{machine}/telemetry            iot.telemetry.raw
    (JSON, QoS 1)   -->   [ bridge ]   -->   Avro + cabecera de esquema
                                \
                                 -->   iot.telemetry.dlq  (JSON + motivo)
```

Existe porque Mosquitto no tiene puente nativo a Kafka. Se evaluo NanoMQ por
tenerlo, pero se verifico que esa funcion es exclusiva de EMQX Enterprise (de
pago), asi que el puente es codigo propio.

## Formato del mensaje en Kafka

Definido en [`../common/schema_registry.py`](../common/schema_registry.py) y
compartido con el job de Spark, para que productor y consumidor no puedan
divergir:

```
[ 1 byte  ] byte magico 0x00
[ 4 bytes ] globalId del esquema en Apicurio (big-endian)
[ resto   ] payload Avro binario schemaless
```

Es el formato de cable de Confluent. La alternativa —Avro "pelado", sin
cabecera— obligaria al consumidor a asumir con que esquema se escribio cada
mensaje. Con la cabecera, **cada evento declara su version de esquema**, que es
lo que permite que convivan dos versiones en el mismo topico durante una
evolucion (Objetivo 2).

## Decisiones de diseño

**La clave de Kafka identifica al sensor**, es decir el par `(building_id,
meter_type)`, y se escribe como `156:electricity`. Garantiza que todas las
lecturas de un mismo contador caen en la misma particion y se procesan en orden,
que es lo que necesitan las agregaciones por ventana de Spark.

No confundirla con la clave natural del evento, que ademas incluye `timestamp`:
esa identifica una lectura concreta, y usarla aqui repartiria al azar entre
particiones las lecturas de un mismo contador.

Se construye con acceso directo a los dos campos, no con `.get()`, y el motivo es
un fallo real: el bridge conservaba `evento.get("machine_id")` del dataset
anterior, campo que el contrato de ASHRAE no tiene. La clave era **None en todos
los mensajes**, el productor los repartia en round-robin y el orden por sensor se
perdia sin un solo error en el log. Con acceso directo, un evento que no traiga
esos campos no cumple el contrato y acaba en la DLQ, que es donde debe acabar.

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

Requiere el stack levantado y el esquema registrado:

```bash
docker compose -f pipeline/docker-compose.yml up -d
```

```bash
python pipeline/schemas/register_schema.py
```

Arrancar el bridge (se queda en primer plano; `Ctrl+C` cierra ordenadamente
vaciando el buffer del productor):

```bash
python pipeline/bridge/mqtt_kafka_bridge.py --report-interval 5
```

Y en otra terminal, generar trafico:

```bash
python pipeline/simulator/mqtt_simulator.py --speedup 500 --limit 300
```

Parametros utiles: `--bootstrap-servers`, `--broker-host`, `--qos`,
`--linger-ms`, `--registry-url`. Ver `--help`.

## Metricas

Cada `--report-interval` segundos se registra una linea con los contadores y
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
| Tamano del mensaje | 158 B (5 de cabecera + 153 de Avro) |
| Particiones utilizadas | 0, 1, 2 |

Los 300 mensajes se releyeron desde Kafka resolviendo el esquema por el
`globalId` de la cabecera. **Esta medicion es anterior al dataset de ASHRAE y a
la correccion de la clave**: se tomo cuando la clave seguia siendo `machine_id`,
de modo que el reparto por particiones que refleja no es el actual. Verificado
despues sobre ASHRAE: las claves son del tipo `156:electricity` y el reparto
entre las tres particiones es desigual (6.144 / 7.468 / 6.388 sobre 20.000
eventos), que es lo que produce el hash de una clave real y no el round-robin de
una clave nula.

**Camino de rechazo (DLQ)**

Los cuatro casos se desviaron con el motivo exacto y sin afectar a la tasa de
perdida:

| Evento inyectado | Motivo registrado en la DLQ |
|---|---|
| `device_state="OFF"` (fuera del enum) | `ValueError: 'OFF' is not in list` |
| Sin `power_watts` | `ValueError: no value and no default for power_watts` |
| `voltage="doscientos treinta"` | `TypeError: an integer is required on field voltage` |
| JSON mal formado | `JSONDecodeError: Expecting property name...` |

**Recuperacion ante caida (Objetivo 5)**

Se detuvo el bridge, se publicaron 50 eventos con el caido y se reinicio: el
broker le entrego los 50 al reconectar gracias a la sesion persistente
(`clean_session=False` + QoS 1). Kafka paso de 300 a 350 mensajes, **sin
perdida**. La latencia p50 de esos eventos fue de 10,8 s, que es exactamente el
tiempo que pasaron encolados en el broker.

Nota: la latencia medida aqui es solo el tramo MQTT→Kafka. El KPI de 2 s del
Objetivo 1 se mide extremo a extremo hasta TimescaleDB, y se completara cuando
exista el sumidero.
