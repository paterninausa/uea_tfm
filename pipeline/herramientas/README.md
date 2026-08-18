# Herramientas de medicion

Scripts que **no forman parte del pipeline**: no procesan telemetria, la miden.
Viven aparte a proposito, para que el simulador, el bridge y el job de Spark
contengan solo lo que hace falta para mover datos de un extremo al otro y no
opciones que unicamente se usan al preparar una prueba.

| Script | Para que | Objetivo del TFM |
|---|---|---|
| `reset_state.py` | Dejar el sistema en estado limpio antes de medir | precondicion de todos |
| `load_ladder.py` | Escalera de sensores concurrentes | 5 |
| `kpi_report.py` | Emitir el cuadro de KPIs en Markdown | 1, 2, 3, 4 y 5 |
| `failover_test.py` | Tumbar un servicio y cronometrar la recuperacion | 5 |

## El ciclo de medicion completo

Las cifras solo son comparables entre si si se obtienen siempre igual. Este es
el orden, y cada paso existe por una razon.

**1. Estado limpio.** Sin esto se mide sobre los restos de la prueba anterior, y
el sintoma —unas latencias mejores de lo que tocaba— no se distingue de un buen
resultado.

```bash
python pipeline/herramientas/reset_state.py --yes
```

**2. Levantar el pipeline.** Cada pieza en su terminal, porque hay que poder
pararlas por separado.

```bash
docker compose -f pipeline/docker-compose.yml up -d
```

```bash
python pipeline/schemas/register_schema.py
```

```bash
python pipeline/bridge/mqtt_kafka_bridge.py
```

```bash
python pipeline/spark/telemetry_streaming.py --trigger "1 second"
```

Esperar a que el job escriba `Consultas en marcha` antes de generar carga: el
primer arranque resuelve los conectores de Maven y tarda bastante mas.

**3. Generar la carga.** Para una medicion de latencia, tasa fija; para buscar
el punto de saturacion, sin limite.

```bash
python pipeline/simulator/mqtt_simulator.py --speedup 2000 --limit 50000 --rebase-end now
```

**4. Emitir el cuadro.** Deja el informe en `pipeline/logs/informe_kpi.md`.

```bash
python pipeline/herramientas/kpi_report.py
```

### El orden importa, y mucho

**Los consumidores tienen que estar en marcha ANTES de publicar.** No es una
recomendacion de estilo: se midio el mismo sistema con la misma carga en los dos
ordenes y la latencia de ingesta p95 paso de **1,38 s a 92,53 s**. Arrancando
Spark despues del simulador, los 20.000 eventos esperaron en Kafka a que
apareciera alguien a consumirlos, y esa espera se contabiliza como latencia
porque el reloj arranca en el `sim_publish_ts` del productor. El micro-lote sufre
lo mismo: el primer lote proceso 9.998 filas de golpe en vez de las ~360 de
regimen, y el p95 subio de 692 a 2.745 ms.

Ninguna de esas cifras es un fallo del pipeline, pero las dos son igual de
publicables por error. De ahi que el paso 2 diga esperar a `Consultas en marcha`.

**Los logs del job de Spark van en UTC** y los del resto de procesos en hora
local: el job fuerza `TZ=UTC` en su proceso para que los timestamps no se
desplacen al convertirlos a objetos de Python. Al comparar `spark_job.log` con
`bridge.log` hay que contar el desfase de la zona.

## reset_state.py

Borra tres estados, y los tres hacen falta:

- **Checkpoints de Spark**: guardan hasta que offset se leyo. Si sobreviven, el
  job reanuda donde estaba y no vuelve a procesar nada.
- **Topicos de Kafka**: se recrean en vez de vaciarse, porque el log retiene 7
  dias y un consumidor que empiece por `earliest` leeria tambien lo de ayer. Se
  recrean con las particiones que tenian, leidas antes de borrar.
- **Tablas de ambos sumideros**: la escritura es idempotente, asi que reprocesar
  no duplica filas, pero si deja dos mediciones mezcladas en la misma tabla.

Se niega a correr si detecta el bridge, el job o un productor en marcha:
recrear un topico bajo los pies de un consumidor lo deja leyendo de algo que ya
no existe. Con `--all` trunca ademas `buildings` y `sensor_baseline`, que no
hace falta para aislar una medicion porque el job las recarga al arrancar.

## load_ladder.py

Ejecuta la escalera del Objetivo 5 invocando al simulador una vez por peldano,
con el MISMO `--speedup` y distinto `--max-sensors`.

```bash
python pipeline/herramientas/load_ladder.py --ladder 100,250,500,652 --speedup 2000
```

**Por que el speedup se mantiene y la tasa no.** Con una tasa global fija, 100 y
652 sensores publican los mismos eventos por segundo: se reparte la misma carga
entre mas identidades y no se escala nada —de ahi salia aquella degradacion del
0,6% que no significaba gran cosa—. `--speedup` fija la cadencia por sensor, asi
que la carga total crece con el numero de contadores, que es lo que dice el
objetivo.

**Por que se mide en el consumo y no en el productor.** El PUBACK de Mosquitto
significa "aceptado", no "entregado al pipeline": se midio al broker confirmando
a 7.324 ev/s mientras el bridge llevaba consumidos 8.230 de 40.000, con el resto
encolado y la latencia MQTT→Kafka disparada de 2 ms a 683 ms. Cada peldano se
mide por tanto con lo que llega al final del recorrido —mensajes en Kafka, filas
en PostgreSQL, duracion de micro-lote de Spark— y no con lo que el productor
consigue soltar.

Entre peldanos espera a que las filas dejen de crecer: si no, los mensajes
todavia en vuelo al terminar un peldano se contarian en el siguiente.

Si el simulador no sostuvo su propia agenda, el peldano se marca como no valido
en lugar de publicar una cifra que mide la maquina del productor.

**Se repite el primer peldano al final, en caliente**, y no es opcional: la
primera version de esta escalera, ejecutada solo en orden creciente, concluyo que
el throughput MEJORA al anadir sensores (1.062 → 1.332 ev/s). El sesgo era de
calentamiento y solo se ve repitiendo el peldano base cuando el sistema ya lleva
rato en marcha.

## kpi_report.py

Interroga las cuatro fuentes donde el pipeline deja constancia:

| Objetivo | Metrica | Fuente |
|---|---|---|
| 1 | latencia de ingesta p50/p95, perdida | PostgreSQL + offsets de Kafka |
| 2 | % validados, esquema vigente | Apicurio + topico DLQ |
| 3 | duracion de micro-lote, ritmo | `streaming_progress` de TimescaleDB |
| 4 | tiempo de respuesta de cada panel | API de consultas de Grafana |
| 5 | escalabilidad | `ultima_escalera.json` |

El Objetivo 4 se mide **a traves de Grafana** (`/api/ds/query`) y no lanzando el
SQL contra la base. Dos razones: los `rawSql` de los paneles llevan macros
(`$__timeFilter`, `$__timeGroupAlias`) que no son SQL valido y habria que
reimplementar su sustitucion, y el objetivo habla del refresco del dashboard,
que incluye el trayecto por el servidor de Grafana.

Avisa si encuentra filas con latencia negativa: es el sintoma de que el UPSERT
no esta refrescando `ingested_at`, y significa que las latencias del informe
estan contaminadas.

## failover_test.py

Lanza el simulador, mata un contenedor con `docker kill` —no `stop`: un fallo
real no avisa con un SIGTERM ordenado—, lo levanta y cronometra cuanto tarda el
flujo en restablecerse, contando filas nuevas en la base de datos. Se mide sobre
datos persistidos y no sobre el estado del contenedor porque que Docker diga
`healthy` solo significa que el proceso responde, no que el pipeline haya vuelto
a mover datos de un extremo al otro.

```bash
python pipeline/herramientas/failover_test.py --target mosquitto --downtime 15
```

Exige que el bridge y el job esten en marcha, y **informa de lo que pase,
incluido que no se recupere**: un servicio cuyo fallo detiene el pipeline es un
resultado publicable; afirmar una recuperacion que no se ha observado, no.

### Resultado medido (Mosquitto)

| Metrica | Resultado | Objetivo |
|---|---|---|
| Flujo restablecido tras levantar el servicio | **16,1 s** | < 60 s |
| Tiempo total desde el fallo (incluye 15 s caido a proposito) | 31,5 s | — |
| Eventos perdidos | **1** (el que estaba en vuelo al caer el broker) | sin perdida |

La primera ejecucion de esta prueba dio "no se recupero", y la causa no estaba
en el pipeline: **el simulador moria** seis segundos despues de tumbar el
broker, al propagarse la excepcion de `wait_for_publish`. Lo que se estaba
midiendo era si el simulador sobrevive, no si el sistema se recupera. Corregido
—un contador real no se apaga porque se reinicie el broker—, el bridge
demostro reconectar solo gracias a su sesion MQTT persistente.

Queda una decision de diseño abierta: ese unico evento se pierde porque el
simulador no reintenta lo que no se confirmo. Un contador real con QoS 1 y
memoria local si lo haria.

## Registro de actividad

Todos los procesos escriben a `pipeline/logs/<nombre>.log`, ademas de por
consola. Cada arranque deja una cabecera con **la orden completa y sus
argumentos**: sin ellos, una cifra de throughput no se puede atribuir a una
configuracion concreta ni reproducir. Los ficheros rotan a los 3 MB y se
conservan tres copias, de modo que una sesion de medicion entera cabe sin
vigilar el disco.

No se versionan. Los artefactos de datos —`informe_kpi.md`,
`ultima_escalera.json`, `ultimo_failover.json`— viven en el mismo directorio.
