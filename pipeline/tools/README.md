# Herramientas de medicion

Scripts que **no forman parte del pipeline**: no procesan telemetria, la miden
o preparan una demostracion. Viven aparte a proposito, para que el simulador, el
bridge y el job de Spark contengan solo lo que hace falta para mover datos de un
extremo al otro y no opciones que unicamente se usan al preparar una prueba.

| Script | Para que | Objetivo del TFM |
|---|---|---|
| `reset_state.py` | Dejar el sistema en estado limpio antes de medir | precondicion de todos |
| `load_ladder.py` | Escalera de sensores concurrentes | 5 |
| `kpi_report.py` | Emitir el cuadro de KPIs en Markdown | 1, 2, 3, 4 y 5 |
| `failover_test.py` | Tumbar un servicio y cronometrar la recuperacion | 5 |
| `watermark_poison_test.py` | Demostrar que un evento con fecha futura detiene la agregacion de todos los sensores | 1 y 5 |
| `cluster.sh` | Levantar un cluster Spark standalone para probar el fallo de un executor | 5 |
| `demo.py` | Preparar el terreno para ver los dashboards de Grafana en vivo | demostracion |

## demo.py — la demostracion en un comando

Orquesta todo el arranque para ver los dashboards, sin ejecutar los pasos a
mano: levanta el stack, deja las bases limpias, registra el esquema, arranca
bridge + Spark (esperando a que las consultas de streaming esten activas) y
lanza el simulador con datos recientes.

```bash
python pipeline/tools/demo.py                 # 6 semanas hacia atras, --acelerar 8000
python pipeline/tools/demo.py --semanas 10    # 10 semanas
python pipeline/tools/demo.py --stop          # cierra procesos y baja el stack
```

`--semanas N` publica **la cola de las ultimas N semanas del historico**: pasa
`--ultimas-semanas N` al simulador. Como el Parquet ya viene reubicado al presente
por `prepare_ashrae.py --fecha-final`, esa cola termina en fecha reciente sin
ningun desplazamiento en tiempo de ejecucion. El replay tarda en llenarse
`N x 604.800 / acelerar` segundos (a `--acelerar 8000`: ~7,5 min para 6 semanas),
rellenando el dashboard de izquierda a derecha. Al terminar, Grafana en
`http://localhost:3000` (admin/admin); en los dashboards 2 y 3 hay que poner el
rango a `now-Nw` para ver el bloque completo (asume que el Parquet se preparo con
`--fecha-final` reciente, idealmente el mismo dia).

Es para **demostrar**, no para medir: las cifras de KPI se toman con el ciclo de
abajo, no con este script.

## El ciclo de medicion completo

Las cifras solo son comparables entre si si se obtienen siempre igual. Este es
el orden, y cada paso existe por una razon.

**1. Estado limpio.** Sin esto se mide sobre los restos de la prueba anterior, y
el sintoma —unas latencias mejores de lo que tocaba— no se distingue de un buen
resultado.

```bash
python pipeline/tools/reset_state.py --yes
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
python pipeline/spark/stream_processing.py --trigger "1 second"
```

Esperar a que el job escriba `Consultas en marcha` antes de generar carga: el
primer arranque resuelve los conectores de Maven y tarda bastante mas.

**3. Generar la carga.** Para una medicion de latencia, tasa fija; para buscar
el punto de saturacion, sin limite.

```bash
python pipeline/simulator/mqtt_simulator.py --acelerar 2000 --limite 50000
```

**4. Emitir el cuadro.** Deja el informe en `pipeline/logs/informe_kpi.md`.

```bash
python pipeline/tools/kpi_report.py
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
con el MISMO `--acelerar` y distinto `--max-sensors`.

```bash
python pipeline/tools/load_ladder.py --ladder 100,250,500,652 --acelerar 2000
```

**Por que el speedup se mantiene y la tasa no.** Con una tasa global fija, 100 y
652 sensores publican los mismos eventos por segundo: se reparte la misma carga
entre mas identidades y no se escala nada —de ahi salia aquella degradacion del
0,6% que no significaba gran cosa—. `--acelerar` fija la cadencia por sensor, asi
que la carga total crece con el numero de medidores, que es lo que dice el
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

### La rampa de saturacion

`--aceleraciones` en lugar de `--ladder` mantiene los sensores fijos y sube el ritmo.
Responde a otra pregunta: no "cuanto se degrada al crecer", sino "hasta donde
aguanta".

```bash
python pipeline/tools/load_ladder.py --aceleraciones 5000,10000,20000,40000 --events-per-step 40000
```

Medido el 19 de agosto de 2026 con los 652 sensores:

| Ritmo pedido | Publicado real | Persistido | Latencia p95 | Lote p95 |
|---|---|---|---|---|
| 906 ev/s | 896,5 (99%) | 40.000 | 1,28 s | 629 ms |
| 1.811 ev/s | **1.798,6** (99,3%) | 40.000 | 1,97 s | 906 ms |
| 3.622 ev/s | 2.550,5 (70%) | 40.000 | 3,02 s | 1.108 ms |
| 7.244 ev/s | 2.297,0 (32%) | 39.316 | 6,44 s | 1.192 ms |

**El pipeline sostiene 1.800 ev/s con todo dentro de objetivo**, que son 10.000
veces el caso de uso real de 0,1797 ev/s.

**El techo de ~2.900 ev/s es del simulador, no del pipeline**: a partir de ahi
publica MENOS cuanto mas se le pide (2.920 -> 2.686 -> 2.421), la curva
descendente tipica de saturacion. El pipeline persistio el 100% de lo que le
llego en todos los peldanos.

**Y por encima de ese punto, Mosquitto descarta en silencio.** En el peldano mas
alto el simulador registro 40.000 publicados y 0 fallidos —recibio su PUBACK de
cada uno— pero al bridge solo llegaron 38.221. El broker habia tirado 1.779 al
llenarse su cola de salida. Nadie da error: ni el productor, ni el bridge, ni la
DLQ, ni los logs del broker. Es un 4,4% de perdida invisible con todos los
indicadores en verde, y por eso `kpi_report.py` lee ahora
`$SYS/broker/publish/messages/dropped`.

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
python pipeline/tools/failover_test.py --target mosquitto --downtime 15
```

Exige que el bridge y el job esten en marcha, y **informa de lo que pase,
incluido que no se recupere**: un servicio cuyo fallo detiene el pipeline es un
resultado publicable; afirmar una recuperacion que no se ha observado, no.

### Resultados medidos

| Servicio tumbado 15 s | Flujo restablecido | Nota |
|---|---|---|
| Mosquitto | **16,1 s** | El bridge reconecta solo por su sesion persistente. Se pierde 1 evento: el que el productor tenia en vuelo |
| Kafka | **15,2 s** | Reintentos del productor del bridge; sin intervencion |
| TimescaleDB | **4,1 s** | Reintentos de escritura y relanzado de la consulta; `eventos-postgresql` siguio con 21 micro-lotes durante la caida |
| PostgreSQL | **5,1 s** | Igual, sin muerte silenciosa |

Objetivo: recuperacion < 60 s. Al reiniciar, cada consulta reanuda desde su
checkpoint y **no se pierde ningun evento**: los publicados durante la caida
siguen en Kafka y la clave natural hace idempotente el reproceso.

### Tres trampas que costaron cuatro ejecuciones falsas

**El productor tambien es parte del experimento.** La primera prueba concluyo
"el flujo no se restablecio". Falso: el bridge habia reconectado en 17 s. Lo que
moria era el simulador, seis segundos despues de tumbar el broker, al propagarse
la excepcion de `wait_for_publish`.

**La tabla testigo tiene que ser la que corta el fallo.** Al tumbar TimescaleDB
se contaban filas en PostgreSQL, que no se habia caido: daba "recuperado en 1,0
s" sin que la parte afectada hubiera hecho nada.

**La referencia hay que tomarla con el servicio vivo.** Cuando el servicio
tumbado es la propia base testigo, `contar` devuelve -1 mientras esta muerta.
Tomar eso como referencia hacia que, al volver, sus filas ANTIGUAS ya superaran
el umbral y se declarara "flujo restablecido" sin que hubiera llegado nada nuevo.

## Registro de actividad

Todos los procesos escriben a `pipeline/logs/<nombre>.log`, ademas de por
consola. Cada arranque deja una cabecera con **la orden completa y sus
argumentos**: sin ellos, una cifra de throughput no se puede atribuir a una
configuracion concreta ni reproducir. Los ficheros rotan a los 3 MB y se
conservan tres copias, de modo que una sesion de medicion entera cabe sin
vigilar el disco.

No se versionan. Los artefactos de datos —`informe_kpi.md`,
`ultima_escalera.json`, `ultimo_failover.json`— viven en el mismo directorio.

## watermark_poison_test.py

Demuestra, con el sistema en marcha, que **un unico evento con fecha futura
detiene la agregacion por ventana de los 652 sensores**, no solo la del que lo
emitio, y que lo hace sin producir un error en ninguna parte.

El motivo es que el watermark de Spark es *uno solo para toda la consulta* —no
hay watermark por clave ni por particion— y vale (mayor `timestamp` visto en
cualquier evento) menos el retraso configurado. Un evento del ano 2036 lo deja
ahi, y desde ese instante los eventos legitimos de 2016 caen por debajo y se
descartan como tardios.

El evento se publica **directamente a Kafka**, saltandose el bridge a proposito:
la guarda del bridge ya rechaza estos eventos a la DLQ, asi que lo que se
reproduce aqui es el caso que esa guarda no cubre —un evento que ya esta en el
log, porque entro antes de existir la guarda, porque alguien publico al topico
sin pasar por el bridge, o porque se reprocesa el log desde el principio—.

Se ejecuta en dos pasadas, con `reset_state.py --yes` entre ellas porque **el
watermark envenenado sobrevive en el checkpoint**:

```bash
python pipeline/spark/stream_processing.py --trigger "1 second" --margen-futuro 999999999
python pipeline/tools/watermark_poison_test.py
```

```bash
python pipeline/spark/stream_processing.py --trigger "1 second"
python pipeline/tools/watermark_poison_test.py
```

### Resultados medidos (20 de agosto de 2026)

Un evento, 10 anos en el futuro, con el simulador publicando a ~357 ev/s:

| | Filtro desactivado | Filtro activo (300 s) |
|---|---|---|
| Tramo de asentamiento (15 s) | +174 filas | +366 filas |
| **`telemetry_metrics` en regimen (45 s)** | **+0 filas** | **+1.149 filas** |
| `telemetry_events` en regimen | +16.126 filas | +16.488 filas |
| Errores en los logs | **ninguno** | ninguno |
| Veredicto | `ENVENENADO` | `PROTEGIDO` |

La fila que importa es la tercera junto a la segunda: con el filtro desactivado
la ruta operativa se para en seco mientras la analitica sigue recibiendo mas de
dieciseis mil filas. **Medio pipeline detenido y ningun indicador en rojo.**

### La trampa: el envenenamiento empieza por una rafaga, no por un silencio

La primera version de esta prueba dio `PROTEGIDO` sobre un sistema que estaba
envenenado. Contaba filas entre el principio y el final, y vio crecimiento.

Mirando las escrituras segundo a segundo se ve por que: 46 filas, **117 de golpe
dos segundos despues de la inyeccion**, y ninguna en los 44 siguientes. Al saltar
el watermark, todas las ventanas abiertas quedan por debajo de el y Spark las
cierra y las emite a la vez. Ese pico terminal es lo que un medidor acumulado
interpreta como buena salud.

De ahi los dos tramos: uno de **asentamiento**, que absorbe la rafaga, y otro de
**regimen**, del que sale el veredicto. Es la misma leccion que el resto del
proyecto: un fallo silencioso es una **ausencia**, y para verla hay que mirar
donde deberia haber actividad en lugar de sumar totales.

## Dos modos de ejecucion de Spark, y para que sirve cada uno

El job acepta `--master`, de modo que el **mismo codigo, sin una sola linea de
cambio**, corre en modo local o contra un gestor de cluster. Se usan dos modos
con propositos distintos, y la eleccion no es de gusto sino de medicion.

**`local[*]` es el modo de MEDIR**, y es el valor por defecto. El `*` son hilos
dentro de la JVM del driver: no hay executors ni serializacion entre procesos, y
el job dispone de los 12 nucleos de la maquina.

**Standalone es el modo de DEMOSTRAR.** `pipeline/tools/cluster.sh` levanta un
master y N workers en la misma maquina; el job se lanza con
`--master spark://127.0.0.1:7077` y se ejecuta con executors en procesos
independientes.

```bash
bash pipeline/tools/cluster.sh start
```

```bash
python pipeline/spark/stream_processing.py --master spark://127.0.0.1:7077 --trigger "1 second"
```

```bash
bash pipeline/tools/cluster.sh status
```

### Por que las mediciones NO se toman en standalone

Con la misma carga (40.000 eventos a 358 ev/s), el mismo estado limpio y el
mismo orden de arranque, el 21 de agosto de 2026:

| Modo | Latencia ingesta p95 | Lote `eventos` p95 | Lote `metricas` p95 | ¿El simulador sostuvo el ritmo? |
|---|---|---|---|---|
| `local[*]`, 12 nucleos | **1,190 s** ✓ | 475 ms | 891 ms | Si |
| standalone, 4 nucleos | 14,302 s ✗ | 992 ms | 1.554 ms | Si |
| standalone, 10 nucleos | 34,274 s ✗ | 2.266 ms | **3.194 ms** ✗ | **No: 9,5 s de retraso** |

Cuanta mas CPU se asigna al cluster, PEOR va todo. No es un defecto del modo
distribuido: es que el generador de carga con sus 648 conexiones MQTT, el
bridge, el driver, los executors y siete contenedores comparten **una sola
maquina de 12 nucleos**. Con 10 nucleos para el cluster la carga del sistema
llego a 19,76 y el simulador se retraso respecto a su agenda, con lo que la
ejecucion dejo de medir el pipeline para medir la maquina.

**Es una limitacion de los recursos disponibles, no del diseño.** En un
despliegue real el productor y el cluster estarian en maquinas distintas, y el
job soporta ese escenario sin cambios: basta apuntar `--master` a un gestor de
cluster (Spark standalone, YARN o `k8s://`).

### Lo que si demuestra el modo standalone

Ejecutando el pipeline completo con 2 workers de 2 nucleos y matando uno con
`kill -9`:

| | Resultado |
|---|---|
| Estado del worker | `DEAD` |
| Estado de la aplicacion | `RUNNING`, de 4 a 2 nucleos |
| Intervenciones del supervisor del job | **0** — ninguna consulta se detuvo |
| Interrupcion de la escritura | **~9 s** |
| Al reponer el worker | La aplicacion recupera sus 4 nucleos sola |

Es la unica prueba de fallo que alcanza al motor de procesamiento:
`failover_test.py` tumba contenedores —broker, Kafka y los dos sumideros—, y en
`local[*]` no hay executors que matar.
