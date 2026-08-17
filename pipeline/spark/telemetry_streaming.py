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
    python telemetry_streaming.py --console            # depuracion por consola
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
from common.schema_registry import (  # noqa: E402
    DEFAULT_ARTIFACT,
    DEFAULT_GROUP,
    DEFAULT_REGISTRY_URL,
    HEADER_SIZE,
    ApicurioClient,
    SchemaRegistryError,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
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
# Enriquecimiento
# --------------------------------------------------------------------------
def enrich(df: DataFrame, nominal_interval_s: int) -> DataFrame:
    """Anade los campos derivados que consumira Power BI.

    Se calculan aqui, una sola vez en el pipeline, y no en cada consulta del
    informe: asi la logica de negocio vive en un unico sitio y dos informes no
    pueden acabar con definiciones distintas de la misma metrica.
    """
    # Potencia aparente (VA) = tension x intensidad. Junto al factor de
    # potencia permite razonar sobre la potencia reactiva, que es lo que
    # penalizan las companias electricas.
    potencia_aparente = F.col("voltage") * F.col("current_amp")

    # Energia NOMINAL, no real: asume que la potencia medida se mantiene
    # durante el intervalo de muestreo nominal del flujo (30 s). Se comprobo
    # que ese es el ritmo GLOBAL del dataset y que cada maquina concreta
    # reporta con mucha menos frecuencia, asi que esto NO es la integral de
    # energia de la maquina: es un indicador comparable entre eventos, y como
    # tal debe interpretarse en los informes.
    energia_wh = F.col("power_watts") * (nominal_interval_s / 3600.0)

    # Reglas de anomalia. El dataset no trae etiquetas, asi que no hay forma de
    # validar un detector aprendido: se usan condiciones de dominio explicitas
    # y auditables, cada una dejando constancia del motivo.
    #
    # Las reglas se eligieron midiendo su tasa de disparo sobre el millon de
    # filas, no por intuicion. Se DESCARTARON estas tres, que parecen
    # razonables pero no discriminan nada en estos datos:
    #
    #   sensor_status = WARN            -> dispara en el 25,04% de los eventos
    #   measurement_quality = ESTIMATED -> dispara en el 33,25%
    #   power_factor < 0,75             -> dispara en el 15,00%
    #
    # Su union marcaba el 57,49% del dataset: un informe que senala a la
    # mayoria de los eventos no detecta anomalias, renombra la normalidad. La
    # causa es que el dataset es sintetico y esos campos siguen distribuciones
    # casi uniformes (power_factor es uniforme entre 0,70 y 1,00, de modo que
    # cualquier umbral fijo marca una fraccion fija sin ninguna senal).
    #
    # sensor_status y measurement_quality siguen disponibles como columnas en
    # esta misma tabla, asi que Power BI puede filtrar por ellos cuando
    # interese; simplemente no son anomalias. Ademas ya van agregados en
    # TimescaleDB como warn_count y estimated_count.

    # Regla 1 - incoherencia electrica: la potencia declarada se aparta mas de
    # un 50% de la que implican tension, intensidad y factor de potencia. El
    # error relativo mediano en estos datos es del 15%, asi que el umbral del
    # 50% deja fuera el ruido de generacion y solo marca el 0,21% de los casos.
    potencia_teorica = F.col("voltage") * F.col("current_amp") * F.col("power_factor")
    error_relativo = F.abs(F.col("power_watts") - potencia_teorica) / F.greatest(
        F.col("power_watts"), F.lit(1e-9)
    )

    # Regla 2 - equipo encendido sin consumo: contradiccion entre el estado
    # declarado y la medida. Dispara en el 0,03% de los eventos.
    # El ORDEN importa: se evalua primero la regla especifica y despues la
    # general. Se comprobo que toda fila con "ON y corriente cero" tiene
    # tambien potencia teorica cero y, por tanto, un error relativo del 100%,
    # de modo que la regla de incoherencia la absorbe si va antes. Con el orden
    # inverso, esas filas se etiquetaban todas como "incoherencia_electrica" y
    # el motivo mas preciso —y mas accionable para mantenimiento— nunca
    # aparecia en el informe.
    motivo = (
        F.when((F.col("device_state") == "ON") & (F.col("current_amp") <= 0.0),
               F.lit("equipo_encendido_sin_consumo"))
        .when(error_relativo > 0.5, F.lit("incoherencia_electrica"))
        .otherwise(F.lit(None))
    )
    # Pendiente: una tercera regla por desviacion respecto al consumo tipico de
    # cada machine_model. Se midio que los modelos forman poblaciones muy
    # separadas (mediana de 8 W en un movil frente a 27.450 W en un chiller,
    # un rango de 3.400x), asi que un umbral global no sirve y hace falta una
    # tabla de referencia por modelo unida al flujo mediante broadcast join.

    return (
        df.withColumn("apparent_power_va", potencia_aparente)
        .withColumn("energy_wh_estimated", energia_wh)
        .withColumn("anomaly_reason", motivo)
        .withColumn("is_anomaly", F.col("anomaly_reason").isNotNull())
    )


# --------------------------------------------------------------------------
# Agregacion por ventana
# --------------------------------------------------------------------------
def aggregate_metrics(df: DataFrame, window_duration: str, watermark: str) -> DataFrame:
    """Agrega por ventana temporal sobre EVENT TIME (columna `timestamp`).

    El watermark le dice a Spark cuanto puede tardar un evento en llegar antes
    de considerarlo demasiado tardio para modificar una ventana ya emitida. Sin
    el, Spark tendria que mantener el estado de todas las ventanas abiertas
    para siempre y la memoria creceria sin limite.

    Se agrupa por company/site/department y no por machine_id porque se midio
    que cada maquina reporta una vez cada ~29 h de media: una ventana por
    maquina contendria un unico evento y la agregacion no aportaria nada.
    """
    return (
        df.withWatermark("timestamp", watermark)
        .groupBy(
            F.window(F.col("timestamp"), window_duration),
            F.col("company_id"), F.col("site_id"), F.col("department"),
        )
        .agg(
            F.count("*").alias("event_count"),
            # approx_count_distinct y no countDistinct: el conteo distinto
            # exacto no esta soportado en agregaciones de streaming, porque
            # exigiria guardar en el estado todos los valores vistos.
            F.approx_count_distinct("machine_id").alias("distinct_machines"),
            F.avg("power_watts").alias("avg_power_watts"),
            F.min("power_watts").alias("min_power_watts"),
            F.max("power_watts").alias("max_power_watts"),
            F.sum("power_watts").alias("sum_power_watts"),
            F.avg("power_factor").alias("avg_power_factor"),
            F.avg("current_amp").alias("avg_current_amp"),
            F.avg("cpu_load").alias("avg_cpu_load"),
            F.sum(F.when(F.col("sensor_status") == "WARN", 1).otherwise(0)).alias("warn_count"),
            F.sum(F.when(F.col("measurement_quality") == "ESTIMATED", 1).otherwise(0))
                .alias("estimated_count"),
            # Instrumentacion del KPI de latencia extremo a extremo: el
            # instante de publicacion MQTT mas reciente de la ventana.
            F.max("sim_publish_ts").alias("max_sim_publish_ts"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "company_id", "site_id", "department",
            "event_count", "distinct_machines",
            "avg_power_watts", "min_power_watts", "max_power_watts", "sum_power_watts",
            "avg_power_factor", "avg_current_amp", "avg_cpu_load",
            "warn_count", "estimated_count", "max_sim_publish_ts",
        )
    )


# --------------------------------------------------------------------------
# Escritura
# --------------------------------------------------------------------------
def make_upsert_writer(props: dict, table: str, conflict_cols: list[str]):
    """Devuelve una funcion foreachBatch que hace UPSERT en PostgreSQL.

    Por que no `batchDF.write.jdbc(mode="append")`: el escritor JDBC de Spark
    solo sabe insertar, y un reintento tras un fallo, o un reproceso del log de
    Kafka, reventaria contra la clave primaria. El UPSERT
    (INSERT ... ON CONFLICT DO UPDATE) es lo que hace idempotente la escritura,
    que es condicion necesaria para que el reprocesamiento de la arquitectura
    Kappa no duplique datos.

    Las filas se recogen en el driver para ejecutar el UPSERT con psycopg2. Es
    asumible con el volumen de este trabajo (a 60 ev/s, ~120 filas por
    micro-lote de 2 s) y mantiene el codigo legible. Si el volumen creciera,
    el paso siguiente seria escribir por particion con foreachPartition, o a
    una tabla de staging seguida de un MERGE en el servidor.
    """
    import psycopg2
    from psycopg2.extras import execute_values

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        # Deduplicar por la clave de conflicto ANTES del UPSERT es obligatorio,
        # no una optimizacion: PostgreSQL aborta con CardinalityViolation
        # ("ON CONFLICT DO UPDATE command cannot affect row a second time") si
        # la misma clave aparece dos veces en la misma sentencia.
        #
        # Y las claves repetidas son un caso normal, no una anomalia: la cadena
        # MQTT QoS 1 -> reintentos del productor de Kafka da garantia
        # at-least-once, asi que un mismo event_id puede llegar mas de una vez
        # y caer en el mismo micro-lote. Como los duplicados son reentregas del
        # mismo evento, quedarse con cualquiera de ellos es equivalente.
        deduplicado = batch_df.dropDuplicates(conflict_cols)
        filas = deduplicado.collect()
        if not filas:
            return
        columnas = deduplicado.columns

        def a_utc(valor):
            """Marca explicitamente como UTC los datetime sin zona.

            Segunda linea de defensa frente al desplazamiento horario: aunque
            el proceso ya corre en UTC, enviar el valor con tzinfo impide que
            la base de datos lo reinterprete segun SU propia zona al escribir
            en una columna TIMESTAMPTZ. Asi el instante queda determinado por
            el dato y no por la configuracion de dos maquinas.
            """
            if isinstance(valor, datetime) and valor.tzinfo is None:
                return valor.replace(tzinfo=timezone.utc)
            return valor

        valores = [tuple(a_utc(v) for v in fila) for fila in filas]

        actualizables = [c for c in columnas if c not in conflict_cols]
        sql = (
            f"INSERT INTO {table} ({', '.join(columnas)}) VALUES %s "
            f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET "
            + ", ".join(f"{c} = EXCLUDED.{c}" for c in actualizables)
        )

        conn = psycopg2.connect(
            host=props["host"], port=props["port"], dbname=props["dbname"],
            user=props["user"], password=props["password"],
        )
        try:
            with conn, conn.cursor() as cur:
                execute_values(cur, sql, valores, page_size=500)
            logger.info("[batch %d] %d filas -> %s", batch_id, len(filas), table)
        finally:
            conn.close()

    return write_batch


def db_props(args: argparse.Namespace, cual: str) -> dict:
    if cual == "metrics":
        return {"host": args.timescale_host, "port": args.timescale_port,
                "dbname": args.timescale_db, "user": args.db_user, "password": args.db_password}
    return {"host": args.postgres_host, "port": args.postgres_port,
            "dbname": args.postgres_db, "user": args.db_user, "password": args.db_password}


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

    consultas = []
    checkpoint_raiz = Path(args.checkpoint_dir).resolve()

    if args.console:
        consultas.append(
            eventos.writeStream.format("console")
            .option("truncate", "false").option("numRows", 5)
            .trigger(processingTime=args.trigger)
            .option("checkpointLocation", str(checkpoint_raiz / "console"))
            .start()
        )
    else:
        if args.sink in ("metrics", "both"):
            metricas = aggregate_metrics(eventos, args.window, args.watermark)
            consultas.append(
                metricas.writeStream
                # outputMode append: emite cada ventana UNA vez, cuando el
                # watermark garantiza que ya no llegaran mas eventos suyos. Es
                # lo que hace medible el KPI "latencia de micro-lote < 3 s tras
                # el cierre de ventana": la fila aparece justo al cerrarse.
                .outputMode("append")
                .foreachBatch(make_upsert_writer(
                    db_props(args, "metrics"), "telemetry_metrics",
                    ["window_start", "company_id", "site_id", "department"]))
                .option("checkpointLocation", str(checkpoint_raiz / "metrics"))
                .trigger(processingTime=args.trigger)
                .queryName("metricas-timescaledb")
                .start()
            )

        if args.sink in ("events", "both"):
            enriquecidos = enrich(eventos, args.nominal_interval_s).select(
                "event_id",
                F.col("timestamp").alias("event_time"),
                "company_id", "site_id", "machine_id", "machine_model", "department",
                "country_code", "energy_type",
                "power_watts", "voltage", "current_amp", "power_factor",
                "sensor_id", "sensor_status", "measurement_quality",
                "cpu_load", "device_state", "user_count",
                "apparent_power_va", "energy_wh_estimated", "is_anomaly", "anomaly_reason",
                "sim_publish_ts",
            )
            consultas.append(
                enriquecidos.writeStream
                .outputMode("append")
                .foreachBatch(make_upsert_writer(
                    db_props(args, "events"), "telemetry_events", ["event_id"]))
                .option("checkpointLocation", str(checkpoint_raiz / "events"))
                .trigger(processingTime=args.trigger)
                .queryName("eventos-postgresql")
                .start()
            )

    logger.info("Consultas en marcha: %s", [q.name for q in consultas])
    try:
        for q in consultas:
            q.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Parada solicitada; deteniendo consultas...")
        for q in consultas:
            q.stop()
    return 0


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
    p.add_argument("--nominal-interval-s", type=int, default=30,
                   help="Intervalo nominal de muestreo del flujo, para la energia estimada")
    p.add_argument("--shuffle-partitions", type=int, default=8)

    p.add_argument("--sink", default="both", choices=["both", "metrics", "events"])
    p.add_argument("--console", action="store_true", help="Escribe por consola en vez de a las BD")
    p.add_argument("--checkpoint-dir", default=str(Path(__file__).parent / "checkpoints"))

    p.add_argument("--timescale-host", default="localhost")
    p.add_argument("--timescale-port", type=int, default=5432)
    p.add_argument("--timescale-db", default="tfm_metrics")
    p.add_argument("--postgres-host", default="localhost")
    p.add_argument("--postgres-port", type=int, default=5433)
    p.add_argument("--postgres-db", default="tfm_analytics")
    p.add_argument("--db-user", default="tfm")
    p.add_argument("--db-password", default="tfm_dev_password")

    p.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    p.add_argument("--group", default=DEFAULT_GROUP)
    p.add_argument("--artifact", default=DEFAULT_ARTIFACT)
    p.add_argument("--spark-log-level", default="WARN")
    return p.parse_args()


if __name__ == "__main__":
    try:
        sys.exit(run(parse_args()))
    except SchemaRegistryError as exc:
        logger.error("%s", exc)
        sys.exit(1)
