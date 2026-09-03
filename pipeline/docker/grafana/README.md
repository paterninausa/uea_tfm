# Dashboards de Grafana (Objetivo 4)

Mitad **operacional** del doble sumidero: responde a "que esta pasando ahora".
Lee de TimescaleDB, que guarda agregados por ventana de 1 hora.

Acceso: <http://localhost:3000> (admin / admin), carpeta **TFM**.

## Aprovisionamiento declarativo

Ni la fuente de datos ni los dashboards se construyen pulsando en la interfaz:
se cargan de ficheros versionados en git al arrancar el contenedor.

```
provisioning/datasources/timescaledb.yml   -> conexion a TimescaleDB
provisioning/dashboards/dashboards.yml     -> declara de donde cargar
dashboards/*.json                          -> los tres dashboards
```

Mismo criterio que con el esquema Avro: **la fuente de verdad es el
repositorio** y el estado de Grafana es reconstruible. Se borro el volumen de
Grafana varias veces durante el desarrollo y los tres dashboards volvieron
solos.

`allowUiUpdates: true` permite retocarlos en la interfaz para explorar, pero al
reiniciar manda el fichero. Si un cambio merece quedarse, hay que exportarlo al
JSON y commitearlo.

## Los tres dashboards

### 1. Estado del pipeline

Salud del sistema en tiempo real, con refresco de 5 s. **El eje temporal es
`ingested_at`, el reloj de pared, no `window_start`**: aqui interesa cuando
llegaron los datos, no cuando se midieron.

Contiene la evidencia en vivo del KPI del Objetivo 1: un panel dibuja el
percentil 95 de `ingested_at - sim_publish_ts` sobre `telemetry_events`
(PostgreSQL, via su propio datasource), con la linea de referencia en 2 s.
Durante la defensa, ese panel *es* el KPI.

Tambien lleva la antiguedad del ultimo dato, que delata al instante si el
pipeline se ha detenido.

### 2. Consumo energetico

Analisis situacional siguiendo las metricas habituales del sector:

| Panel | Por que esta |
|---|---|
| **Demanda pico** | Las electricas facturan al cliente comercial por la potencia contratada en kW, no solo por la energia consumida |
| **Consumo base** | El diagnostico clasico de consumo fantasma: lo que gasta el edificio cuando no hay nadie |
| **Factor de carga** | Media entre pico. Mide si la potencia contratada se aprovecha o se paga por picos puntuales |
| **Perfil de carga por hora** | El grafico canonico de gestion energetica |

El selector `medio` filtra por tipo de medidor. **No es un adorno**: las
unidades de electricidad, agua fria y agua caliente no son comparables, asi que
casi todos los paneles tienen que fijar uno solo.

### 3. Calidad y anomalias

Salud de los medidores: porcentaje de lecturas a cero, de anomalias y de
lecturas validas, mas su evolucion temporal y las tablas de los grupos peores.

El panel de ceros en el tiempo es donde **emergen los medidores muertos**: una
lectura a cero aislada puede ser legitima, pero una banda continua hora tras
hora no lo es. Se midio que en el dataset las rachas llegan a durar 8.051 horas
seguidas, 335 dias.

## Lo que estos dashboards NO hacen

Comparacion entre edificios individuales, rankings de consumidores, drill-down a
la instalacion concreta, tendencias del ano completo. Eso es Power BI, que
conserva el grano de evento y las tablas de referencia.

No es una limitacion accidental: `building_id` no esta en la agregacion. Se
evaluo anadirlo y se descarto, porque con lecturas horarias agrupar por edificio
equivale al grano crudo y la tabla pasaria de 404.000 filas al ano a 5,7
millones. Catorce veces mas volumen para habilitar un panel, ademas de duplicar
en TimescaleDB lo que ya guarda PostgreSQL.

## Rendimiento

Todo panel lleva `$__timeFilter(window_start)` —que activa la exclusion de
chunks de la hypertable— y agrega por tiempo. Las consultas medidas van de 1,9 a
15,4 ms, muy por debajo del refresco de 5 s que exige el Objetivo 4.

Sin agregacion temporal, una consulta al ano completo devuelve 1,7 millones de
puntos: no mata a la base —tarda 824 ms— pero si al navegador. Por eso la regla
es que ningun panel devuelva series sin agrupar.

## Las fechas: `--fecha-final`

Los datos son de 2016, asi que los paneles con rangos relativos saldrian vacios.
La serie se reubica al presente **de una vez, al preparar los datos**, no en
tiempo de ejecucion:

```bash
python pipeline/data/prepare_ashrae.py --train ./raw/train.parquet --metadata ./raw/building_metadata.parquet --fecha-final 2026-08-30
```

Desplaza todas las marcas con un offset constante para que la ultima lectura
caiga en la fecha dada, dejando el Parquet datado en el presente. Al ser un
offset constante, la cadencia horaria y los ciclos diario y estacional se
conservan intactos. `setup.sh` pasa la fecha del dia automaticamente; para la
demo en vivo, `demo.py` reproduce la cola de N semanas con `--ultimas-semanas`.

Sin `--fecha-final` los datos quedan en 2016 y hay que fijar a mano el rango
temporal de los dashboards.

## Estado verificado

Los tres dashboards se aprovisionan solos en la carpeta TFM tras recrear el
contenedor. Ejecutando las consultas de los paneles contra la API de Grafana con
4 dias de datos rebasados:

| Panel | Resultado |
|---|---|
| Latencia p95 (KPI en vivo) | 4,60 s |
| Demanda pico / base / factor de carga | 70.225 / 44.209 / **0,722** |
| Perfil de carga por hora | 24 puntos |
| Grupos con mas ceros | 15 filas |

El factor de carga de 0,722 sobre cuatro dias es un perfil creible. Con muestras
mas cortas y agregando los tres emplazamientos salia 0,941, casi plano: la suma
de muchos edificios con horarios desplazados aplana la curva, que es
precisamente por lo que el analisis por edificio corresponde a Power BI.
