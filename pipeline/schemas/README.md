## Por que building_id es texto y no un entero

Cambiarlo de `int` a `string` fue una **correccion de contrato**, no una
evolucion, y se hizo antes de que existiera produccion. El motivo es que Avro no
valida los tipos: los convierte. Comprobado:

| Se envia | Se guardaba con `int` | Se guarda con `string` |
|---|---|---|
| `156.9` | **`156`** — atribuido a otro edificio | rechazado a la DLQ |
| `true` | **`1`** — el edificio 1 | rechazado a la DLQ |
| `2^40` | escrito, fuera del rango de int32 | rechazado a la DLQ |
| `156` (entero) | `156` | rechazado: el contrato exige texto |

`TypeError: must be string on field building_id` es hoy el motivo de rechazo, y
sale agrupado en el desglose de la DLQ del bridge.

**Este cambio es incompatible en Avro** —no hay promocion definida entre `int` y
`string`— asi que con la regla `COMPATIBILITY=FULL_TRANSITIVE` activa, Apicurio
lo habria rechazado. Aqui no hizo falta saltarsela: al no haber datos historicos
que proteger, se recreo el registro y el contrato nuevo quedo como version 1.

La leccion, que vale para la memoria: **una regla de compatibilidad no es
gratis, y el tipo de un identificador es de las decisiones que hay que acertar
antes de la primera version**, porque despues solo se puede cambiar rompiendo.

# Esquemas Avro y gobernanza en Apicurio (Objetivo 2)

Contrato de datos del pipeline. El esquema Avro convierte el topico de Kafka en
un contrato explicito y verificable, en lugar de un flujo cuyo formato solo
existe en la cabeza de quien escribio el productor.

## Ficheros

| Fichero | Papel |
|---|---|
| `telemetry_event_v1.avsc` | Esquema en produccion. 5 campos. |
| `telemetry_event_v2.avsc` | Evolucion de prueba: anade `reading_quality` opcional. **No se registra en el uso normal**, existe para demostrar el Objetivo 2. |
| `register_schema.py` | Registra un `.avsc` en Apicurio y aplica las reglas de gobernanza. |

## El evento

    building_id | meter_type | timestamp | meter_reading | sim_publish_ts

23 bytes por evento, medidos con este esquema sobre datos reales. Los atributos
del edificio no viajan aqui: viven en la tabla de dimension y Spark los
incorpora con un broadcast join.

`sim_publish_ts` es el unico campo que no emite el contador: es instrumentacion
para medir la latencia del Objetivo 1. Los otros cuatro reproducen lo que
enviaria el dispositivo.

## Identificacion en el registro

    grupo:      iot
    artefacto:  iot.telemetry.raw-value

El sufijo `-value` sigue la convencion **TopicNameStrategy**: un esquema por
topico, describiendo el *valor* de los mensajes de `iot.telemetry.raw`. Es la
convencion adecuada aqui porque los cuatro tipos de contador comparten forma
—identificador, instante y un numero—. Si el pipeline llegara a ingerir
sensores estructuralmente distintos, habria que replantearlo hacia topicos
separados por familia o hacia `RecordNameStrategy`.

## Reglas de gobernanza activas

| Regla | Valor | Quien la aplica |
|---|---|---|
| `VALIDITY` | `FULL` | Apicurio |
| `COMPATIBILITY` | `FULL_TRANSITIVE` | Apicurio |
| Orden de simbolos de enum | Solo se permite anadir al final | **`register_schema.py`** |

Las dos primeras son del registro. La tercera es nuestra, y hace falta.

## El hallazgo: Apicurio no protege el orden de los enums

Avro codifica un enum como el **indice del simbolo** dentro del array
`symbols`, no como su nombre. `electricity` viaja como `00` porque es el primer
simbolo; exactamente los mismos bytes que un `int` con valor 0.

Se comprobo que la regla `COMPATIBILITY`, incluso en `FULL_TRANSITIVE`,
**acepta** reordenar ese array e incluso insertar un simbolo en medio. Tiene su
logica desde la especificacion de Avro: la resolucion de esquemas empareja los
simbolos por nombre, siempre que el lector use el esquema del *escritor*.

Pero este pipeline no hace eso. El job de Spark deserializa todo el flujo con
un unico esquema, porque `from_avro` recibe uno solo. Y entonces el indice
manda. Comprobado, escribiendo con v1 y leyendo con el array reordenado
alfabeticamente:

```
Escrito con v1  : meter_type='electricity'
Leido reordenado: meter_type='chilledwater'
```

Sin excepcion, sin aviso, y con el resto de campos intactos. Corrupcion
silenciosa.

Por eso `register_schema.py` compara los enums de la version propuesta con los
de la registrada y **exige que los simbolos existentes conserven su indice**:
solo admite anadir al final. Cuando rechaza, dice que indice cambia y que
lectura se malinterpretaria.

## Decisiones de modelado

**`meter_type` como enum y no como entero crudo.** No es un campo derivado sino
una decodificacion biyectiva documentada por la competicion, y en el cable
ocupa lo mismo (`00`, `02`, `04`, `06`). A cambio da validacion de dominio
gratis: un codigo fuera del dominio falla al serializar y va a la DLQ, mientras
que un entero aceptaria un `7` sin rechistar. Es la misma estructura que usa el
precedente industrial del dominio —BACnet, ANSI/ASHRAE 135— con su tipo de
objeto y su catalogo normalizado: el codigo en el mensaje, el significado en el
registro.

**Los cuatro medios se declaran aunque solo haya tres en los datos.** El
subconjunto en uso no contiene `steam`. El contrato describe el dominio, no la
muestra.

**`unknown` como simbolo por defecto.** No aparece en los datos: existe para
que un consumidor con este esquema pueda leer eventos escritos con una version
futura que anada medios nuevos, en lugar de fallar la deserializacion.

**La unidad de `meter_reading` depende de `meter_type`** y queda advertido en el
`doc` del campo. Los cuatro medios no son comparables: la mediana es 43,6 en
electricidad, 146,0 en agua fria y 8,8 en agua caliente. Toda agregacion debe
incluir `meter_type` en la clave de agrupacion.

## Uso

Registrar el esquema (idempotente):

```bash
python pipeline/schemas/register_schema.py --schema pipeline/schemas/telemetry_event_v1.avsc
```

Probar una evolucion:

```bash
python pipeline/schemas/register_schema.py --schema pipeline/schemas/telemetry_event_v2.avsc
```

Si es aceptada queda registrada como version nueva. Para volver al estado
anterior hay que borrarla, algo que el compose habilita expresamente:

```bash
curl -X DELETE http://localhost:8080/apis/registry/v3/groups/iot/artifacts/iot.telemetry.raw-value/versions/2
```

Codigo de salida 0 si el esquema es valido y aceptado, 1 si es rechazado, si el
`.avsc` esta mal formado o si el registro no responde.

Para ver lo registrado, la UI en <http://localhost:8888> o:

```bash
curl -s http://localhost:8080/apis/registry/v3/groups/iot/artifacts/iot.telemetry.raw-value/versions
```

## Estado verificado

Contra Apicurio 3.3.1 con almacenamiento KafkaSQL:

- 5.000 lecturas reales serializadas y releidas con `fastavro` sin error, a
  **23 B por evento**.
- Enum y entero crudo producen **bytes identicos** (`00`, `02`, `04`, `06`); el
  enum ademas rechaza un simbolo fuera del dominio, el entero acepta un `7`.

| Caso probado | Resultado | Quien lo rechaza |
|---|---|---|
| v1 registrado de nuevo, sin cambios | Aceptado, sigue en version 1 | — |
| v2: campo opcional con `default: null` | **Aceptado** | — |
| Simbolo de enum anadido al final | **Aceptado** | — |
| Enum reordenado alfabeticamente | **Rechazado** | `register_schema.py` |
| Simbolo insertado en medio del enum | **Rechazado** | `register_schema.py` |
| v2 sin `default` | **Rechazado** | Apicurio |
| `.avsc` con un tipo inexistente | Rechazado en local, sin llegar al registro | `fastavro` |
| Registro no accesible | Error explicito con la orden para levantar el stack | — |

El registro permanece en la version 1: ninguna de las evoluciones probadas
llego a persistirse, porque todas fueron rechazadas antes de escribirse.

El grupo, el artefacto y las dos reglas son **constantes** del script, no
opciones: hay un unico contrato en el proyecto y una unica politica de
compatibilidad. Como opciones invitaban a registrar con una politica distinta y
dejar el registro incoherente sin darse cuenta.
