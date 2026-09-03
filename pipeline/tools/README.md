# Herramientas de medicion

Scripts que **no forman parte del pipeline**: no procesan telemetria, la miden
o preparan una demostracion. Viven aparte a proposito, para que el simulador, el
bridge y el job de Spark contengan solo lo que hace falta para mover datos de un
extremo al otro y no opciones que unicamente se usan al preparar una prueba.

| Script | Para que | Objetivo del TFM |
|---|---|---|
| `reset_state.py` | Dejar el sistema en estado limpio antes de medir | precondicion de todos |
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

**2. Levantar el pipeline.** El stack incluye ahora el registro del esquema
(`register-schema`, contenedor de un solo uso) y el `bridge`, encadenados por
`depends_on`: `docker compose up -d` no arranca el bridge hasta que el esquema
esta registrado. Solo el job de Spark queda como proceso del host.

```bash
docker compose -f pipeline/docker-compose.yml up -d
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

Se niega a correr si detecta el job de Spark o un productor del host en marcha:
recrear un topico bajo los pies de un consumidor lo deja leyendo de algo que ya
no existe. El bridge es un contenedor: no hace falta pararlo a mano, `reset_state`
lo detiene y lo reanuda solo mientras recrea los topicos. Con `--all` trunca
ademas `buildings` y `sensor_baseline`, que no hace falta para aislar una
medicion porque el job las recarga al arrancar.

## kpi_report.py

Interroga las fuentes donde el pipeline deja constancia:

| Objetivo | Indicador | Fuente |
|---|---|---|
| 1 | latencia de ingesta (mediana/p95), tasa de perdida | PostgreSQL + offsets de Kafka |
| 2 | % validados, esquema registrado | Apicurio + topico DLQ |
| 3 | duracion de micro-lote (mediana/p95/maxima) | `streaming_progress` de TimescaleDB |
| 4 | refresco por dashboard (panel mas lento) | API de consultas de Grafana |
| 5 | recuperacion ante fallo | `ultimo_failover.json` (de `failover_test.py`) |

La **tasa de perdida** del Objetivo 1 es lo publicado en el flujo (offsets del
topico de Kafka) menos lo persistido en PostgreSQL, sobre lo publicado. Los
mensajes que Mosquitto acepta y descarta por cola llena son una perdida
adicional, anterior a Kafka: se informan aparte cuando su contador `$SYS` no es
cero.

El Objetivo 4 se mide **a traves de Grafana** (`/api/ds/query`) y no lanzando el
SQL contra la base. Dos razones: los `rawSql` de los paneles llevan macros
(`$__timeFilter`, `$__timeGroupAlias`) que no son SQL valido y habria que
reimplementar su sustitucion, y el objetivo habla del refresco del dashboard,
que incluye el trayecto por el servidor de Grafana. Cada consulta se lanza
contra la fuente de datos que declara su panel (los paneles de latencia leen el
sumidero analitico; el resto, el operacional).

Avisa si encuentra eventos con latencia negativa (marca de persistencia
anterior a la de publicacion): la medicion no es fiable y hay que repetirla.

## failover_test.py

Lanza el simulador, provoca el fallo de un contenedor, y cronometra cuanto tarda
el flujo en restablecerse contando filas nuevas en la base de datos. Se mide
sobre datos persistidos y no sobre el estado del contenedor porque que Docker
diga `healthy` solo significa que el proceso responde, no que el pipeline haya
vuelto a mover datos de un extremo al otro.

```bash
python pipeline/tools/failover_test.py --target mosquitto --downtime 15
python pipeline/tools/failover_test.py --target postgres --fallo oom
```

`--target`: `mosquitto`, `kafka`, `timescaledb`, `postgres` o `bridge` (los
cuatro contenedores de infraestructura mas el bridge, contenedor desde que dejo
de correr en el host).

`--fallo` decide como cae el servicio:

- **`kill`** (por defecto): `docker kill` + `docker compose start` manual tras
  `--downtime` segundos. Es el **peor caso**: `docker kill` no dispara
  `restart: unless-stopped`, asi que alguien tiene que levantar el servicio. El
  cronometro arranca en la orden de reinicio, con el arranque del contenedor
  incluido. Es el modo cuyas cifras van a la memoria.
- **`oom`**: baja el limite de memoria hasta forzar un OOM-kill del proceso
  principal. Un OOM **si** es un fallo genuino, asi que `restart:
  unless-stopped` rearranca el contenedor **solo**, sin intervencion; el
  cronometro va desde el OOM. Deja el limite de memoria puesto, asi que al
  terminar recrea el contenedor. No sirve para `mosquitto` (usa ~3 MiB, por
  debajo del minimo de 6 MB de `docker update`).

Exige que el bridge y el job de Spark esten en marcha, y **informa de lo que
pase, incluido que no se recupere**: un servicio cuyo fallo detiene el pipeline
es un resultado publicable; afirmar una recuperacion que no se ha observado, no.

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

No se versionan. Los artefactos de datos —`informe_kpi.md` y
`ultimo_failover.json`— viven en el mismo directorio.

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
