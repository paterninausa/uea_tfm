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

```bash
python pipeline/simulator/mqtt_simulator.py --speedup 2000
```

| Opcion | Para que |
|---|---|
| `--speedup` | Cuantas veces mas rapido avanza el reloj simulado |
| `--clients` | Conexiones MQTT simultaneas (0 = una por sensor) |
| `--max-sensors` | Publica solo los primeros N sensores (Objetivo 5) |
| `--limit` | Techo de eventos publicados |
| `--rebase-end` | Desplaza las marcas de tiempo (demostracion en vivo) |
| `--max-lag` | Retraso tolerado antes de invalidar la ejecucion |

Son tres grupos —seleccion de datos, ritmo y conexiones— y **ninguno es un
artificio de banco de pruebas**. Lo que mide y orquesta vive en `herramientas/`.

## El parque real genera 0,1797 ev/s

Verificado sobre el Parquet: cada contador mide una vez por hora —mediana y p95
del intervalo son exactamente 3.600 s, sin dispersion— y los 652 contadores
tienen datos en el 99,2% de las 8.784 horas de 2016. **El caso de uso completo
son menos de dos decimas de evento por segundo.**

Por eso el simulador acelera el reloj:

    tasa agregada = n_sensores x speedup / 3600

| `--speedup` | Cadencia por sensor | Con 652 sensores | Ano completo en |
|---|---|---|---|
| 1 | 1 h | 0,18 ev/s | 8.784 h |
| 2.000 | 1,8 s | 359 ev/s | 4,4 h |
| 7.145 | 0,5 s | 1.294 ev/s | 74 min |

**Es un factor POR SENSOR, no una tasa global**, y esa diferencia decide si la
escalera del Objetivo 5 significa algo: con una tasa global fija, pasar de 100 a
652 sensores reparte los mismos eventos entre mas identidades y la carga total no
cambia. Con `--speedup`, cada contador mantiene su cadencia y la carga crece con
el numero de contadores, que es lo que quiere decir "sensores concurrentes".

Si el simulador no consigue sostener el ritmo pedido, **la ejecucion se marca
como no valida** (codigo de salida 1) en lugar de recuperar el tiempo perdido
publicando a rafagas: sus cifras medirian entonces esta maquina y no el pipeline.

## Por que no se reproducen las rafagas horarias

Los contadores reales miden en la hora en punto, asi que un replay literal
publicaria los 652 de golpe y luego callaria durante todo el intervalo. No se
hace, y el motivo es cuantitativo: con un drenaje del bridge de unos 4.000 ev/s,
el replay fiel se sostiene hasta **x22.000** —de `652 / (3600/speedup) <= 4.000`—
y por encima la cola de Mosquitto, 10.000 mensajes segun `mosquitto.conf`, se
llena en menos de tres segundos y el broker **descarta en silencio**.

En su lugar los sensores se escalonan de forma determinista dentro de cada
intervalo, lo que equivale a suponer que sus relojes no estan sincronizados al
milisegundo: mas realista que la rafaga perfecta y sin perdida artificial.

## Una conexion por sensor

Por defecto (`--clients 0`) el simulador abre **una conexion MQTT por sensor**:
652 en el parque completo, verificadas simultaneas contra Mosquitto sin un solo
fallo. Es lo que hace que "652 sensores concurrentes" signifique 652 sesiones y
no 652 identidades compartiendo una conexion.

Con un valor menor, cada conexion agrupa varios contadores, y eso tambien modela
algo real: en un edificio los contadores hablan BACnet o Modbus con una pasarela,
y es la pasarela la que publica por todos. **El numero de conexiones MQTT de un
despliegue real no lo fija el numero de sensores, sino el de pasarelas.**

## Por que ya no hay un generador de carga aparte

Existio `herramientas/load_generator.py`, que alcanzaba tasas altas mediante una
VENTANA DE MENSAJES EN VUELO: publicaba N mensajes sin esperar confirmacion desde
un unico cliente. Se retiro en agosto de 2026 y **no debe volver**.

El motivo: esa ventana era un artificio para que un cliente hiciera el trabajo de
652, y `--clients` consigue lo mismo sin inventar nada, porque 652 conexiones con
un mensaje en vuelo cada una dan 652 mensajes en vuelo por la via realista. Se
comprobo ademas que el generador con ventana 1 se comportaba exactamente como el
simulador: eran el mismo programa con dos nombres.

## Ante una caida del broker

Cada conexion es independiente: si el broker cae, la conexion afectada se anota
como caida y deja de emitir, sin arrastrar a las demas ni terminar el proceso.

Antes no era asi, y se descubrio midiendo: en la primera prueba de recuperacion
ante fallo del Objetivo 5, la excepcion de `wait_for_publish` terminaba el
proceso seis segundos despues de tumbar el broker. La prueba concluyo "el flujo
no se restablecio" cuando el bridge se habia reconectado perfectamente; lo que no
sobrevivia era el productor. Un contador real no se apaga porque se reinicie el
broker.

## De donde sale lo que publica

La frontera con `pipeline/simulator/telemetry_dataset.py` es esta:

| Aqui, en el simulador | En `telemetry_dataset.py` |
|---|---|
| `build_topic()`, `build_payload()` — como se serializa un mensaje | `preparar()` — que filas se reproducen y con que marcas |
| `repartir()` — que sensores van en cada conexion | `filtrar_sensores()` — cuales entran, en orden determinista |
| `_programa()` — cuando publica cada sensor | `rebasar()` — a que instante se anclan las marcas |
| Los argumentos del broker: host, puerto, QoS | Los argumentos del dataset: fichero, limite, sensores |

Dicho corto: **`telemetry_dataset` es el guion —que se dice y en que orden— y el simulador son
los actores y el reloj —quien lo dice, por que canal y en que instante—**.

`build_payload` esta aqui y no alli por una razon concreta: sella
`sim_publish_ts` con el instante REAL de emision, que es el origen de tiempo del
KPI de latencia. Lo que hace no es leer una fila, es emitirla.

## Estado verificado

- Publicacion contra Mosquitto con la topologia correcta
  (`iot/156/electricity/telemetry`) y payload de cinco campos.
- Filtrado jerarquico por tipo de contador: una suscripcion a
  `iot/+/chilledwater/telemetry` recibe solo las lecturas de agua fria.
- Escalera de sensores anidada: 100 ⊂ 250 ⊂ 500 ⊂ 652.
- `--rebase-end now` desplaza el rango completo conservando las distancias
  relativas entre eventos.
- **646 conexiones MQTT simultaneas** publicando 20.000 eventos con 0 fallos y 0
  conexiones caidas, a 357,7 ev/s efectivos frente a los 358,9 teoricos de
  `--speedup 2000`, con un retraso maximo sobre la agenda de 0,47 s (18 de agosto
  de 2026, recorrido completo hasta ambos sumideros).
- Reparto por particiones de Kafka determinista y desigual —6.144 / 7.468 /
  6.388 sobre 20.000 eventos—, que es lo que produce el hash de la clave de
  sensor y no el round-robin de una clave nula.
