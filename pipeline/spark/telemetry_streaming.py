"""
Job de Spark Structured Streaming: Kafka -> doble sumidero (Objetivos 3 y 4).

Lee los eventos Avro de iot.telemetry.raw y los escribe en dos destinos con
proposito distinto, que es el "doble sumidero" del trabajo:

  * TimescaleDB <- metricas agregadas por ventana temporal, para los dashboards
    operativos de Grafana ("que esta pasando ahora en la planta").
  * PostgreSQL  <- eventos individuales enriquecidos, para los informes
    analiticos de Power BI ("como ha evolucionado el consumo este trimestre").

Son dos consultas de streaming independientes sobre el mismo topico, no una
sola con dos escrituras. Cuesta leer Kafka dos veces, pero a cambio cada
sumidero tiene su propio checkpoint y su propio ritmo: si PostgreSQL se cae,
los dashboards operativos siguen actualizandose, y al reanudar cada consulta
retoma su offset sin arrastrar a la otra. En una arquitectura Kappa, donde el
log de Kafka es la fuente de verdad reproducible, esa independencia vale mas
que el ahorro de una lectura.

Uso:
    python telemetry_streaming.py                      # ambos sumideros
    python telemetry_streaming.py --sink metrics       # solo TimescaleDB

El progreso de cada micro-lote se persiste en la tabla `streaming_progress` de
TimescaleDB: es la fuente del KPI de latencia de lote del Objetivo 3, y sin
persistirlo desaparece al terminar el job.
"""

import argparse
import logging
import os
import sys
import time as _time
from datetime import datetime, timezone
from pathlib import Path

# Se fuerza la zona horaria del PROCESO a UTC, y tiene que ocurrir antes de
# crear la SparkSession.
#
# Motivo, comprobado empiricamente: `spark.sql.session.timeZone=UTC` gobierna
# como Spark interpreta y muestra los timestamps, pero NO la conversion a
# objetos de Python. `collect()` devuelve datetime *naive* convertido a la zona
# del SISTEMA del driver y sin tzinfo. Con la maquina en Europe/Madrid, un
# evento de las 00:00 UTC llegaba a la base de datos como las 01:00 UTC: una
# hora de desplazamiento silencioso en todos los timestamps, y ademas variable
# (+1 h en invierno, +2 h en verano por el horario de verano), lo que
# corromperia cualquier analisis temporal sin dar ningun error.
os.environ["TZ"] = "UTC"
_time.tzset()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spark.database_writers import load_reference_tables, make_upsert_writer  # noqa: E402
from spark.monitoring import RegistroProgreso, asegurar_tabla_progreso, supervisar  # noqa: E402

from common.connection_args import (  # noqa: E402
    POSTGRES,
    TIMESCALE,
    anadir_argumentos_bd,
    props_bd,
)
from common.logging_setup import configurar_logging  # noqa: E402
from common.apicurio import (  # noqa: E402
    DEFAULT_ARTIFACT,
    DEFAULT_GROUP,
    DEFAULT_REGISTRY_URL,
    HEADER_SIZE,
    ApicurioClient,
    SchemaRegistryError,
)

logger = logging.getLogger("spark_job")

# Dependencias JVM que no vienen con PySpark. Se resuelven de Maven Central en
# el primer arranque y quedan en la cache local (~/.ivy2), asi que solo la
# primera ejecucion necesita red.
MAVEN_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    "org.apache.spark:spark-avro_2.13:4.2.0",
    "org.postgresql:postgresql:42.7.13",
]


def build_spark(args: argparse.Namespace) -> SparkSession:
    return (
        SparkSession.builder
        .appName("tfm-telemetry-streaming")
        .master(args.master)
        .config("spark.jars.packages", ",".join(MAVEN_PACKAGES))
        # Zona horaria fija a UTC. Sin esto, las ventanas temporales se
        # calcularian en la zona local de la maquina y el mismo evento caeria
        # en ventanas distintas segun donde se ejecute el job, lo que destruye
        # la reproducibilidad que exige la arquitectura Kappa.
        .config("spark.sql.session.timeZone", "UTC")
        # Con 3 particiones en el topico, las 200 particiones de shuffle por
        # defecto generarian 200 tareas casi vacias por micro-lote y anadirian
        # latencia sin ningun beneficio.
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        # failOnDataLoss=false evita que el job muera si la retencion de Kafka
        # (7 dias) descarta offsets que aun no se habian leido. En desarrollo
        # es lo practico; en produccion conviene lo contrario, para enterarse.
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", str(args.max_offsets_per_trigger))
        .load()
    )


def decode_events(raw: DataFrame, schema_json: str) -> DataFrame:
    """Quita la cabecera de 5 bytes y deserializa el payload Avro.

    La cabecera es [0x00][globalId de 4 bytes big-endian]; el globalId se
    extrae a una columna para poder comprobar que todos los mensajes se
    escribieron con el esquema que este job espera.

    LIMITACION CONOCIDA: se deserializa todo el flujo con un unico esquema, el
    vigente al arrancar. Avro es un formato posicional, de modo que leer bytes
    escritos con otra version usando este esquema produciria campos corruptos,
    no un error limpio. Por eso los mensajes con un globalId distinto se
    detienen el job (ver `guard_schema_version`) en lugar de deserializarlos a
    ciegas. Soportar varias versiones a la vez exigiria resolver el esquema por
    fila contra el registro; se descarto a proposito en favor del procedimiento
    de drenar y conmutar, que hace que la coexistencia no llegue a ocurrir.
    """
    con_cabecera = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("value").alias("raw_value"),
        # conv(hex(...), 16, 10) interpreta los 4 bytes como entero big-endian.
        F.conv(F.hex(F.substring(F.col("value"), 2, 4)), 16, 10)
            .cast("long").alias("schema_global_id"),
        F.expr(f"substring(value, {HEADER_SIZE + 1}, length(value) - {HEADER_SIZE})")
            .alias("avro_payload"),
    )
    return con_cabecera.withColumn(
        "evento",
        # mode=FAILFAST: un payload que no encaje debe romper el micro-lote en
        # lugar de propagar una fila de nulos silenciosa aguas abajo. El bridge
        # ya garantiza que todo lo que entra en el topico valida contra el
        # esquema, asi que un fallo aqui senala un problema real de formato.
        from_avro(F.col("avro_payload"), schema_json, {"mode": "FAILFAST"}),
    )


def guard_schema_version(df: DataFrame, expected_global_id: int) -> DataFrame:
    """Detiene el job si aparece un evento escrito con otro esquema.

    La version anterior FILTRABA esos eventos, y era la ultima via de perdida
    silenciosa que quedaba en el pipeline: los mensajes de una version de
    esquema inesperada desaparecian sin dejar rastro ni contador, lo que
    chocaria con el objetivo de perdida < 0,1% justo cuando mas importa.

    Detenerse es preferible a descartar, y no por purismo: en una arquitectura
    Kappa el log de Kafka conserva 7 dias, asi que parar es RECUPERABLE —se
    corrige la configuracion y se reanuda desde el checkpoint sin perder un
    evento—, mientras que descartar es irreversible.

    La comprobacion se aplica sobre `meter_reading`, no como columna aparte,
    para que el optimizador de Spark no pueda eliminarla: esa columna se usa
    siempre aguas abajo, de modo que la condicion se evalua necesariamente en
    cada fila.

    El procedimiento operativo que evita llegar aqui es el de "drenar y
    conmutar" descrito en el README: no se permite que dos versiones de
    esquema convivan en el topico.
    """
    lectura_validada = F.when(
        F.col("schema_global_id") == F.lit(expected_global_id),
        F.col("evento.meter_reading"),
    ).otherwise(
        F.raise_error(
            F.concat(
                F.lit("Evento con globalId de esquema inesperado: se esperaba "),
                F.lit(str(expected_global_id)),
                F.lit(" y llego "),
                F.col("schema_global_id").cast("string"),
                F.lit(". El job se detiene en lugar de descartarlo; el evento sigue en Kafka. "),
                F.lit("Revisa el procedimiento de drenar y conmutar del README."),
            )
        )
    )

    return df.select(
        "kafka_key", "kafka_partition", "kafka_offset", "schema_global_id",
        F.col("evento.building_id").alias("building_id"),
        F.col("evento.meter_type").alias("meter_type"),
        F.col("evento.timestamp").alias("timestamp"),
        lectura_validada.alias("meter_reading"),
        F.col("evento.sim_publish_ts").alias("sim_publish_ts"),
    )


# --------------------------------------------------------------------------
# Datos de referencia (broadcast join)
# --------------------------------------------------------------------------
def load_reference(spark: SparkSession, dim_path: Path, base_path: Path):
    """Carga las dos tablas de referencia estaticas que enriquecen el flujo.

    Son pequenas —498 edificios y 652 sensores— asi que se difunden a todos los
    ejecutores con broadcast: cada micro-lote las cruza en memoria, sin shuffle.
    Es el patron estandar de union flujo-estatico de Structured Streaming.

    Que estos atributos vivan aqui y no en el evento es deliberado: son del
    edificio, no del contador. Si manana se corrige el ano de construccion de un
    edificio se actualiza una fila, mientras que desnormalizados en cada evento
    habria que reprocesar el historico entero.
    """
    for p in (dim_path, base_path):
        if not p.exists():
            raise FileNotFoundError(
                f"No se encontro {p}. Genera los datos de referencia: "
                "python pipeline/data/prepare_ashrae.py")

    dimension = spark.read.parquet(str(dim_path)).select(
        "building_id", "site_id", "primary_use", "square_feet")
    linea_base = spark.read.parquet(str(base_path)).select(
        "building_id", "meter_type", "baseline_p75", "baseline_iqr")

    logger.info("Referencia cargada: %d edificios, %d sensores con linea base",
                dimension.count(), linea_base.count())
    return F.broadcast(dimension), F.broadcast(linea_base)


# --------------------------------------------------------------------------
# Enriquecimiento
# --------------------------------------------------------------------------
def enrich(df: DataFrame, dimension: DataFrame, linea_base: DataFrame) -> DataFrame:
    """Cruza con la referencia y calcula SOLO lo que necesita la agregacion.

    Los campos que produce no llegan a PostgreSQL: existen para que
    `aggregate_metrics` pueda agrupar por emplazamiento y uso de edificio, y
    contar ceros y anomalias por ventana. Todo lo que Power BI puede calcularse
    solo —intensidad energetica, motivo de la anomalia— se ha retirado: recibe
    las lecturas crudas y las dos tablas de referencia, y con eso se basta.

    El criterio es: si el consumidor puede derivarlo con lo que ya tiene, que lo
    derive el. Si necesita datos que no le llegan, lo calcula el pipeline.
    """
    df = df.join(dimension, on="building_id", how="left")
    df = df.join(linea_base, on=["building_id", "meter_type"], how="left")

    # Lectura exactamente cero: no se juzga como anomalia porque una sola no lo
    # es —un contador puede estar legitimamente parado unas horas—. Se marca
    # como indicador de calidad y son las AGREGACIONES las que revelan el
    # patron: se midio que las rachas de ceros llegan a durar 8.051 horas
    # seguidas, 335 dias, lo que ya no es una medida sino un contador muerto.
    # Detectar la racha evento a evento exigiria procesamiento con estado.
    lectura_cero = F.col("meter_reading") == 0

    # Regla de pico atipico, contra la linea base del PROPIO sensor. Un umbral
    # global no serviria: cada contador solo es comparable consigo mismo.
    #
    # El umbral se eligio midiendo la tasa de disparo sobre los 5,68 M de
    # eventos, no por intuicion: p75 + 3*IQR marca el 0,95%, 5*IQR el 0,34% y
    # 10*IQR el 0,04%. Se toma 5*IQR por dejar una tasa de anomalias creible.
    # Los 17 sensores con IQR = 0 quedan exentos: sin dispersion historica no
    # hay forma de definir que es atipico para ellos.
    pico_atipico = (
        F.col("baseline_iqr").isNotNull()
        & (F.col("baseline_iqr") > 0)
        & (F.col("meter_reading") > F.col("baseline_p75") + 5 * F.col("baseline_iqr"))
    )

    # Se descarto la regla de lectura negativa: es fisicamente imposible en un
    # contador y se comprobo que no ocurre ni una vez en el subconjunto. Una
    # regla que nunca dispara es indistinguible de una regla rota.
    return (
        df.withColumn("is_zero_reading", lectura_cero)
        .withColumn("is_anomaly", pico_atipico)
    )


# --------------------------------------------------------------------------
# Agregacion por ventana
# --------------------------------------------------------------------------
def aggregate_metrics(df: DataFrame, window_duration: str, watermark: str) -> DataFrame:
    """Agrega por ventana temporal sobre EVENT TIME (columna `timestamp`).

    El watermark le dice a Spark cuanto puede tardar un evento en llegar antes
    de considerarlo demasiado tardio para modificar una ventana ya emitida. Sin
    el, Spark mantendria el estado de todas las ventanas abiertas para siempre.

    SE AGRUPA POR meter_type OBLIGATORIAMENTE, y no por comodidad: la unidad de
    meter_reading depende del medio, asi que promediar a traves de tipos de
    contador produce una cifra sin significado fisico. Se ve en las medianas del
    subconjunto: 43,6 en electricidad, 146,0 en agua fria y 8,8 en agua
    caliente.

    Junto a site_id y primary_use dan 46 combinaciones, y con lecturas horarias
    cada ventana de una hora agrega una lectura por cada sensor del grupo.
    """
    return (
        df.withWatermark("timestamp", watermark)
        .groupBy(
            F.window(F.col("timestamp"), window_duration),
            F.col("site_id"), F.col("primary_use"), F.col("meter_type"),
        )
        .agg(
            F.count("*").alias("event_count"),
            # approx_count_distinct y no countDistinct: el conteo distinto
            # exacto no esta soportado en agregaciones de streaming, porque
            # exigiria guardar en el estado todos los valores vistos.
            F.approx_count_distinct("building_id").alias("distinct_buildings"),
            F.avg("meter_reading").alias("avg_reading"),
            F.max("meter_reading").alias("max_reading"),
            F.sum("meter_reading").alias("sum_reading"),
            # Intensidad agregada como cociente de sumas, no como media de
            # cocientes: asi los edificios grandes pesan lo que les corresponde.
            # Cada edificio aporta su superficie una vez por ventana, porque
            # emite una lectura por hora y contador.
            F.sum("square_feet").alias("sum_square_feet"),
            F.sum(F.when(F.col("is_zero_reading"), 1).otherwise(0)).alias("zero_count"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
            # Instrumentacion del KPI de latencia extremo a extremo: el instante
            # de publicacion MQTT mas reciente de la ventana.
            F.max("sim_publish_ts").alias("max_sim_publish_ts"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "site_id", "primary_use", "meter_type",
            "event_count", "distinct_buildings",
            "avg_reading", "max_reading", "sum_reading",
            # Se guarda sum_square_feet ademas del cociente para que Grafana
            # pueda agregar la intensidad entre grupos como cociente de sumas.
            # Con solo el cociente, un rollup por uso de edificio tendria que
            # promediar cocientes, que no es lo mismo cuando las superficies van
            # de 801 a 850.354 pies cuadrados.
            "sum_square_feet",
            F.when(F.col("sum_square_feet") > 0,
                   F.col("sum_reading") / F.col("sum_square_feet"))
             .alias("avg_energy_intensity"),
            "zero_count", "anomaly_count", "max_sim_publish_ts",
        )
    )


# --------------------------------------------------------------------------
# Escritura
# --------------------------------------------------------------------------
def run(args: argparse.Namespace) -> int:
    registry = ApicurioClient(args.registry_url)
    registry.check()
    global_id, schema = registry.latest(args.group, args.artifact)
    import json
    schema_json = json.dumps(schema)

    spark = build_spark(args)
    spark.sparkContext.setLogLevel(args.spark_log_level)
    logger.info("Spark %s | esquema globalId=%d | ventana=%s watermark=%s",
                spark.version, global_id, args.window, args.watermark)

    eventos = guard_schema_version(
        decode_events(read_kafka(spark, args), schema_json), global_id
    )

    # El enriquecimiento se aplica UNA vez y lo comparten los dos sumideros: el
    # de metricas necesita site_id y primary_use para agrupar, y el de eventos
    # necesita ademas los campos derivados.
    dimension, linea_base = load_reference(spark, Path(args.buildings), Path(args.baseline))
    enriquecidos = enrich(eventos, dimension, linea_base)

    # Se guardan ARRANCADORES, no consultas ya arrancadas: para poder relanzar
    # una que se caiga hay que saber construirla de nuevo. Cada una conserva su
    # checkpoint, asi que relanzarla reanuda donde estaba.
    arrancadores = {}
    checkpoint_raiz = Path(args.checkpoint_dir).resolve()

    if args.sink in ("metrics", "both"):
        metricas = aggregate_metrics(enriquecidos, args.window, args.watermark)
        arrancadores["metricas-timescaledb"] = lambda: (
            metricas.writeStream
            # outputMode append: emite cada ventana UNA vez, cuando el
            # watermark garantiza que ya no llegaran mas eventos suyos. Es
            # lo que hace medible el KPI "latencia de micro-lote < 3 s tras
            # el cierre de ventana": la fila aparece justo al cerrarse.
            .outputMode("append")
            .foreachBatch(make_upsert_writer(
                props_bd(args, TIMESCALE), "telemetry_metrics",
                ["window_start", "site_id", "primary_use", "meter_type"],
                args.db_retries, args.db_retry_wait))
            .option("checkpointLocation", str(checkpoint_raiz / "metrics"))
            .trigger(processingTime=args.trigger)
            .queryName("metricas-timescaledb")
            .start()
        )

    if args.sink in ("events", "both"):
        load_reference_tables(spark, Path(args.buildings), Path(args.baseline),
                              props_bd(args, POSTGRES))

        # PostgreSQL recibe SOLO el contrato mas la instrumentacion. Ni
        # copias de la dimension ni campos derivados: Power BI tiene las dos
        # tablas de referencia cargadas y calcula lo suyo con un join, que
        # es justo para lo que sirve. La clave primaria es la clave natural
        # del evento, lo que hace idempotente el reprocesamiento.
        eventos_bd = enriquecidos.select(
            "building_id", "meter_type",
            F.col("timestamp").alias("event_time"),
            "meter_reading", "sim_publish_ts",
        )
        arrancadores["eventos-postgresql"] = lambda: (
            eventos_bd.writeStream
            .outputMode("append")
            .foreachBatch(make_upsert_writer(
                props_bd(args, POSTGRES), "telemetry_events",
                ["building_id", "meter_type", "event_time"],
                args.db_retries, args.db_retry_wait))
            .option("checkpointLocation", str(checkpoint_raiz / "events"))
            .trigger(processingTime=args.trigger)
            .queryName("eventos-postgresql")
            .start()
        )

    # run_id: identifica esta ejecucion en streaming_progress. Hace falta porque
    # al dejar el sistema en estado limpio se borran los checkpoints y batch_id
    # vuelve a 0, de modo que sin el no se distinguirian dos mediciones.
    run_id = _time.strftime("%Y%m%dT%H%M%SZ", _time.gmtime())
    registro = None
    if args.progress_interval:
        props_progreso = props_bd(args, TIMESCALE)
        asegurar_tabla_progreso(props_progreso)
        registro = RegistroProgreso([], props_progreso, run_id)
        registro.arrancar(args.progress_interval)
        logger.info("Progreso de micro-lote -> TimescaleDB.streaming_progress (run_id=%s)", run_id)

    try:
        codigo = supervisar(arrancadores, args.supervision_interval,
                            args.max_reinicios, registro)
    except KeyboardInterrupt:
        logger.info("Parada solicitada")
        codigo = 0
    finally:
        # Volcado final: el hilo es daemon y muere con el proceso, asi que los
        # ultimos lotes —justo los del final de una prueba de carga— se
        # perderian sin esto.
        if registro:
            logger.info("Registrando el progreso de los ultimos lotes...")
            registro.volcar()
    return codigo


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Job de Spark Structured Streaming (TFM)")
    p.add_argument("--master", default="local[*]")
    p.add_argument("--bootstrap-servers", default="localhost:29092")
    p.add_argument("--topic", default="iot.telemetry.raw")
    p.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    p.add_argument("--max-offsets-per-trigger", type=int, default=10000,
                   help="Techo de eventos por micro-lote; acota la latencia del lote")

    p.add_argument("--window", default="1 hour",
                   help="Duracion de la ventana tumbling sobre event time")
    p.add_argument("--watermark", default="2 minutes",
                   help="Retraso maximo admitido antes de dar una ventana por cerrada")
    p.add_argument("--trigger", default="1 second",
                   help="Intervalo de micro-lote (KPI del Objetivo 3: < 3 s)")
    p.add_argument("--buildings", default=str(Path(__file__).parent / "../data/ashrae_buildings.parquet"),
                   help="Tabla de dimension de edificios (broadcast join)")
    p.add_argument("--baseline", default=str(Path(__file__).parent / "../data/ashrae_sensor_baseline.parquet"),
                   help="Linea base por sensor para la deteccion de picos (broadcast join)")
    p.add_argument("--shuffle-partitions", type=int, default=8)

    p.add_argument("--sink", default="both", choices=["both", "metrics", "events"],
                   help="Sumideros activos. Aislar uno solo sirve para la prueba de "
                        "recuperacion ante fallo del Objetivo 5")
    p.add_argument("--checkpoint-dir", default=str(Path(__file__).parent / "checkpoints"))

    anadir_argumentos_bd(p)

    p.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    p.add_argument("--group", default=DEFAULT_GROUP)
    p.add_argument("--artifact", default=DEFAULT_ARTIFACT)
    p.add_argument("--spark-log-level", default="WARN")
    p.add_argument("--db-retries", type=int, default=5,
                   help="Intentos de escritura antes de dar por perdida la base de datos. "
                        "Absorbe una caida corta sin que muera la consulta")
    p.add_argument("--db-retry-wait", type=float, default=3.0,
                   help="Segundos entre intentos de escritura")
    p.add_argument("--supervision-interval", type=float, default=5.0,
                   help="Segundos entre comprobaciones de que las consultas siguen vivas")
    p.add_argument("--max-reinicios", type=int, default=3,
                   help="Veces que se relanza una consulta caida antes de abandonarla")
    p.add_argument("--progress-interval", type=float, default=10.0,
                   help="Segundos entre volcados del progreso de micro-lote a "
                        "streaming_progress (0 = no registrar). Es la fuente del KPI de "
                        "latencia de lote del Objetivo 3")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("spark_job")
    try:
        sys.exit(run(parse_args()))
    except SchemaRegistryError as exc:
        logger.error("%s", exc)
        sys.exit(1)
