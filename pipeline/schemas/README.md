# Esquemas Avro y gobernanza en Apicurio (Objetivo 2)

Contrato de datos del pipeline. El esquema Avro es lo que convierte al topico
de Kafka en un contrato explicito y verificable, en lugar de un flujo de JSON
cuyo formato solo existe en la cabeza de quien escribio el productor.

## Ficheros

| Fichero | Papel |
|---|---|
| `telemetry_event_v1.avsc` | Esquema en produccion. 21 campos: las 20 columnas del dataset mas `sim_publish_ts`. |
| `telemetry_event_v2.avsc` | Evolucion de prueba: anade `firmware_version` opcional. **No se registra en el uso normal**, existe para demostrar el Objetivo 2. |
| `register_schema.py` | Registra un `.avsc` en Apicurio y configura las reglas de gobernanza. |

## Identificacion en el registro

    grupo:      iot
    artefacto:  iot.telemetry.raw-value

El sufijo `-value` sigue la convencion habitual: el esquema describe el *valor*
de los mensajes del topico `iot.telemetry.raw` (la clave se serializa aparte,
como string con el `machine_id`). Mantener esta convencion permite que el
bridge y el job de Spark resuelvan el esquema a partir del nombre del topico,
sin configuracion adicional.

## Reglas de gobernanza activas

| Regla | Valor | Por que |
|---|---|---|
| `VALIDITY` | `FULL` | El registro rechaza contenido que no sea Avro valido, en vez de almacenarlo y fallar despues en los consumidores. |
| `COMPATIBILITY` | `FULL_TRANSITIVE` | Toda version nueva debe ser compatible hacia atras **y** hacia adelante, y no solo con la version anterior sino con **todas** las anteriores. |

`FULL_TRANSITIVE` es lo que sostiene literalmente el Objetivo 2 ("evolucion
compatible hacia adelante/atras"). Con `BACKWARD` a secas, un consumidor
antiguo podria romperse ante un evento producido con el esquema nuevo.

## Decisiones de modelado

**Enums con simbolo por defecto.** `sensor_status`, `measurement_quality` y
`device_state` son enums Avro con un simbolo `UNKNOWN` que no aparece en los
datos y un `default` que apunta a el. Esto da las dos mitades del Objetivo 2:

- *Validacion estricta al escribir*: si un sensor emitiera `device_state="OFF"`,
  la serializacion falla y el evento va a la DLQ, en vez de colarse un valor
  fuera del dominio. Es lo que hace medible el "100% de eventos validados".
- *Lectura tolerante*: un consumidor con v1 que reciba un evento escrito con un
  esquema futuro que anada simbolos nuevos lo lee como `UNKNOWN` en lugar de
  fallar la deserializacion ("cero fallos ante evolucion compatible").

**Lo que NO es enum.** `machine_model`, `department`, `country_code` y
`energy_type` se modelan como `string` aunque hoy tengan pocos valores
distintos (incluso uno solo, en los dos ultimos). Son catalogos de negocio:
incorporar un equipo o un pais nuevo es un cambio de *datos*, no de contrato.
Convertirlos en enum obligaria a una version de esquema por cada alta.

**`voltage` es `int`, no `long`.** Avro permite promocionar `int` a `long` en
una evolucion compatible, pero no al reves. Empezar con el tipo estrecho
conserva margen de cambio; empezar con `long` lo cierra para siempre.

**Timestamps como `long` con `logicalType: timestamp-millis`.** El simulador
publica `timestamp` e `ingest_ts` como cadenas ISO-8601: la conversion a epoch
en milisegundos es responsabilidad del bridge.

**Todos los campos de v1 son obligatorios.** Se verifico que el dataset no
tiene ni un solo nulo en ninguna de las 20 columnas, asi que un esquema
estricto describe la realidad. Un esquema laxo (todo nullable) haria pasar la
validacion a cualquier cosa y vaciaria de contenido el Objetivo 2.

## Uso

Registrar el esquema v1 (idempotente: repetirlo no crea versiones nuevas):

```bash
python pipeline/schemas/register_schema.py --schema pipeline/schemas/telemetry_event_v1.avsc
```

Ver lo que hay registrado:

```bash
python pipeline/schemas/register_schema.py --show
```

Comprobar si una evolucion seria aceptada, **sin registrarla**:

```bash
python pipeline/schemas/register_schema.py --schema pipeline/schemas/telemetry_event_v2.avsc --dry-run
```

El script devuelve codigo de salida 0 si el esquema es valido y aceptado, y 1
si es rechazado, si el `.avsc` esta mal formado o si el registro no responde;
es utilizable tal cual desde un script de automatizacion.

Tambien se puede inspeccionar todo desde la UI: <http://localhost:8888>

## Estado verificado

Comprobado contra Apicurio 3.3.1 con almacenamiento KafkaSQL:

**Esquema contra datos reales**

- 2000 filas del dataset serializadas y releidas con `fastavro` sin un solo error.
- Payload Avro de **153 B** frente a **513 B** del JSON equivalente que publica
  hoy el simulador: **70,2% menos** por evento.
- `device_state="OFF"` (fuera del dominio del enum) es rechazado al serializar
  -> el evento acabaria en la DLQ, que es el comportamiento buscado.
- Un evento escrito con un esquema que anade el simbolo `OFF` y leido con v1 se
  deserializa como `UNKNOWN`, sin fallar.

**Reglas del registro**

| Caso probado | Resultado | Codigo de salida |
|---|---|---|
| v1 ya registrado, se vuelve a ejecutar | Sin cambios, sigue en version 1 | 0 |
| v2: campo opcional con `default: null` | **Aceptado** | 0 |
| v2 sin `default` | **Rechazado** (`firmware_version` en `/fields/21`) | 1 |
| `voltage` de `int` a `string` | **Rechazado** (reader/writer incompatibles) | 1 |
| `.avsc` con un tipo inexistente | Rechazado en local, sin llegar al registro | 1 |
| Registro no accesible | Error explicito con la orden para levantar el stack | 1 |

El caso de v2 sin `default` es el mas instructivo: anadir un campo **no** es
compatible por si solo. Lo que da la compatibilidad hacia adelante es el
`default`, porque permite que un consumidor con el esquema nuevo lea eventos
antiguos a los que les falta el campo.

Ninguna de las comprobaciones `--dry-run` dejo rastro: el registro permanece en
la version 1.
