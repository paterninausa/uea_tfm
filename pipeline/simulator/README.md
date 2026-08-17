# Simulador MQTT de telemetria IoT

Reproduce el historico de ASHRAE sobre MQTT, como si 652 contadores estuvieran
emitiendo en directo. Es el punto de entrada del pipeline (Objetivo 1).

## Topologia de topicos

    iot/{building_id}/{meter_type}/telemetry

Ejemplo real: `iot/156/electricity/telemetry`

**Un sensor es el par (edificio, tipo de contador).** Un mismo edificio con
contador de electricidad y de agua fria son dos sensores con series
independientes: 498 edificios dan 652 sensores. El topico identifica
exactamente eso: al sensor.

**No hay nivel de emplazamiento.** `site_id` es derivable de `building_id` a
traves de la tabla de dimension, igual que lo eran `event_id` y `sensor_id`
antes de eliminarlos. Tenerlo tambien en el topico creaba una segunda fuente de
verdad —topico y dimension podrian discrepar si un edificio se reasignara— y
ademas nadie lo consumia: el bridge se suscribe a `iot/#` y nunca parte el
topico. El emplazamiento entra en el analisis donde importa, en el broadcast
join de Spark contra la dimension.

## Que va en el topico y que va en el payload

El payload lleva **solo lo que emitiria el contador**:

```json
{
  "building_id": 156,
  "meter_type": "electricity",
  "timestamp": "2016-01-01T00:00:00",
  "meter_reading": 114.71,
  "sim_publish_ts": 1786988417219
}
```

Los atributos del edificio —uso, superficie, ano de construccion— no
aparecen en ninguna parte del mensaje: viven en la tabla de dimension y es
Spark quien los incorpora con un broadcast join.

`sim_publish_ts` es el unico campo que no emite el contador: es instrumentacion
para medir la latencia extremo a extremo del Objetivo 1. `timestamp` viaja en
ISO-8601 y el bridge lo convierte a epoch en milisegundos, que es lo que declara
el esquema Avro; se mantiene en ISO aqui porque hace legible el trafico al
depurar con `mosquitto_sub`.

## Uso

Requiere los datos preparados (`python pipeline/data/prepare_ashrae.py`) y el
stack levantado.

```bash
python pipeline/simulator/mqtt_simulator.py --rate 100 --limit 5000
```

| Opcion | Para que |
|---|---|
| `--rate` | Eventos por segundo. `0` = sin limite, para pruebas de carga |
| `--limit` | Numero maximo de eventos |
| `--max-sensors` | Publica solo los primeros N sensores (Objetivo 5) |
| `--rebase-end` | Desplaza las marcas de tiempo (demostracion en vivo) |
| `--qos` | QoS MQTT, 1 por defecto |

### `--max-sensors`: la escalera de carga del Objetivo 5

El objetivo pide soportar 500 sensores concurrentes con una degradacion de
throughput inferior al 20% respecto a una carga base de 100. Esta opcion da los
cuatro peldanos:

```bash
python pipeline/simulator/mqtt_simulator.py --max-sensors 100 --rate 0
```

| Sensores | Eventos |
|---|---|
| 100 | 875.090 |
| 250 | 2.191.296 |
| 500 | 4.356.884 |
| 652 (todos) | 5.682.185 |

La seleccion es **determinista y anidada**: los 100 primeros sensores estan
contenidos en los 250, estos en los 500 y estos en los 652 —verificado—. Eso
importa para la medida: la degradacion se compara sobre los mismos sensores mas
otros, y no entre dos muestras aleatorias distintas que no serian comparables.

### `--rebase-end`: para la demostracion en vivo

Los datos son de 2016, asi que un panel de Grafana a "ultimas 6 horas" saldria
vacio. Esta opcion desplaza todas las marcas con un unico offset constante para
que la ultima caiga en el instante indicado:

```bash
python pipeline/simulator/mqtt_simulator.py --rebase-end now --rate 0
```

Al ser un offset constante, las distancias relativas entre eventos se conservan
y los ciclos diario y estacional quedan intactos. Se ancla la **ultima** marca y
no la primera para que todo el historico quede en el pasado: anclando la
primera, el replay acelerado generaria marcas en el futuro.

Tambien acepta un instante ISO concreto, util para preparar la vispera de una
defensa: `--rebase-end 2026-09-15T09:00:00`.

## Comprobar el trafico

En otra terminal, mientras corre el simulador:

```bash
docker exec tfm-mosquitto mosquitto_sub -h localhost -t 'iot/#' -v
```

Para ver solo un tipo de contador, aprovechando la jerarquia del topico:

```bash
docker exec tfm-mosquitto mosquitto_sub -h localhost -t 'iot/+/chilledwater/telemetry' -v
```

## Throughput y perdida

Cada `publish()` espera la confirmacion del broker (`wait_for_publish` con QoS
1). Eso hace fiable el contador de perdidas y a la vez fija el techo de
throughput del simulador: **~740 ev/s medidos**, muy por encima de los 50 ev/s
que exige el Objetivo 3.

Al terminar imprime publicados, fallidos, tasa de perdida, duracion y
throughput, y devuelve codigo de salida 1 si hubo algun fallo.

## Estado verificado

- Publicacion contra Mosquitto con la topologia correcta
  (`iot/156/electricity/telemetry`) y payload de cinco campos.
- Filtrado jerarquico por tipo de contador: una suscripcion a
  `iot/+/chilledwater/telemetry` recibe solo las lecturas de agua fria.
- Escalera de sensores anidada: 100 ⊂ 250 ⊂ 500 ⊂ 652.
- `--rebase-end now` desplaza el rango completo conservando las distancias
  relativas entre eventos.
