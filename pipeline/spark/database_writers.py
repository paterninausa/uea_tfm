"""
Escritura idempotente en los dos sumideros.

Todo lo que este job habla en SQL vive aqui: el UPSERT que hace idempotente el
reprocesamiento del log de Kafka, sus reintentos ante una caida de la base, y la
carga de las tablas de referencia que consume Power BI.

Esta separado de `stream_processing.py` porque son responsabilidades
distintas: alli se decide QUE se calcula —ventanas, agregados, enriquecimiento—
y aqui COMO se persiste. La frontera se nota en que este modulo no sabe nada de
Spark salvo que recibe un DataFrame ya resuelto.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
import time as _time

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


# Columna de instrumentacion, no del dominio: la sella el productor en el momento
# de emitir, asi que un evento reenviado tras una reconexion trae una distinta
# siendo el mismo evento. Comparar filas enteras marcaria esas reentregas
# legitimas como colisiones.
COLUMNAS_INSTRUMENTACION = ("sim_publish_ts", "ingested_at", "max_sim_publish_ts")


def _deduplicar(filas: list, columnas: list[str], claves: list[str]) -> tuple[list, list]:
    """Quita las claves repetidas y delata las que no son reentregas.

    Deduplicar es OBLIGATORIO, no una optimizacion: PostgreSQL aborta con
    CardinalityViolation si la misma clave aparece dos veces en la misma
    sentencia, y las repeticiones son normales —la cadena MQTT QoS 1 mas los
    reintentos del productor de Kafka dan garantia at-least-once, asi que un
    mismo evento puede llegar dos veces y caer en el mismo micro-lote—.

    Lo que no es normal es que dos lecturas DISTINTAS compartan clave natural:
    eso incumple el contrato —la terna se verifico unica sobre los 5,68 millones
    de eventos— y significa que una medida real se esta perdiendo. Se distinguen
    comparando solo las columnas del dominio, no las de instrumentacion.
    """
    indices_clave = [columnas.index(c) for c in claves]
    indices_dominio = [i for i, c in enumerate(columnas)
                       if c not in claves and c not in COLUMNAS_INSTRUMENTACION]

    por_clave: dict[tuple, object] = {}
    colisiones: list[tuple] = []
    for fila in filas:
        clave = tuple(fila[i] for i in indices_clave)
        previa = por_clave.get(clave)
        if previa is not None:
            medida_previa = tuple(previa[i] for i in indices_dominio)
            medida_actual = tuple(fila[i] for i in indices_dominio)
            if medida_previa != medida_actual:
                colisiones.append((clave, (medida_previa, medida_actual)))
        por_clave[clave] = fila
    return list(por_clave.values()), colisiones


def make_upsert_writer(props: dict, table: str, conflict_cols: list[str],
                       reintentos: int = 5, espera_reintento: float = 3.0):
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
        # Se recoge SIN deduplicar y se agrupa aqui, en vez de usar
        # dropDuplicates: cuesta lo mismo —una sola pasada, y el lote son unos
        # cientos de filas— y permite distinguir los dos casos que
        # dropDuplicates confunde en silencio.
        columnas = batch_df.columns
        filas, colisiones = _deduplicar(batch_df.collect(), columnas, conflict_cols)
        if not filas:
            return

        for clave, medidas in colisiones:
            logger.error("[batch %d] COLISION DE CLAVE NATURAL en %s: %s comparte %s con "
                         "medidas distintas %s. Se escribe una y la otra SE PIERDE",
                         batch_id, table, clave, conflict_cols, medidas)

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
        # ingested_at se refresca en el UPDATE, no solo en el INSERT.
        #
        # Sin esto, al reescribir una fila existente se actualizaba
        # sim_publish_ts al instante de publicacion nuevo pero ingested_at
        # conservaba el de la primera escritura, dejando un par incoherente. La
        # latencia calculada como ingested_at - sim_publish_ts salia entonces
        # NEGATIVA, porque la fila parecia haberse escrito antes de publicarse.
        # Ocurre siempre que se reprocesa el log de Kafka, que es la operacion
        # basica de una arquitectura Kappa.
        sql = (
            f"INSERT INTO {table} ({', '.join(columnas)}) VALUES %s "
            f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET "
            + ", ".join(f"{c} = EXCLUDED.{c}" for c in actualizables)
            + ", ingested_at = now()"
        )

        # REINTENTOS ANTE UN FALLO DE LA BASE DE DATOS. Sin esto, una caida de
        # unos segundos mataba la consulta entera: la excepcion de psycopg2 sube
        # por foreachBatch y Structured Streaming la convierte en
        # FOREACH_BATCH_USER_FUNCTION_ERROR, que termina la query. Medido el 19
        # de agosto de 2026 tumbando TimescaleDB 15 s.
        #
        # Solo se reintenta OperationalError, que es "no pude hablar con el
        # servidor". Un error de datos —una violacion de restriccion, un tipo
        # incorrecto— no se reintenta: eso es un fallo del contrato y tiene que
        # salir a la luz, no repetirse cinco veces.
        for intento in range(1, reintentos + 1):
            conn = None
            try:
                conn = psycopg2.connect(
                    host=props["host"], port=props["port"], dbname=props["dbname"],
                    user=props["user"], password=props["password"],
                )
                with conn, conn.cursor() as cur:
                    execute_values(cur, sql, valores, page_size=500)
                logger.info("[batch %d] %d filas -> %s", batch_id, len(filas), table)
                return
            except psycopg2.OperationalError as exc:
                if intento == reintentos:
                    logger.error("[batch %d] %s sigue inaccesible tras %d intentos: %s",
                                 batch_id, table, reintentos, exc)
                    raise
                logger.warning("[batch %d] %s inaccesible (intento %d/%d), reintento en "
                               "%.0f s: %s", batch_id, table, intento, reintentos,
                               espera_reintento, str(exc).strip().splitlines()[0])
                _time.sleep(espera_reintento)
            finally:
                if conn is not None:
                    conn.close()

    return write_batch


def load_reference_tables(spark: SparkSession, dim_path: Path, base_path: Path,
                          props: dict) -> None:
    """Vuelca las dos tablas de referencia en PostgreSQL al arrancar.

    Es lo que hace autosuficiente a Power BI: con la dimension de edificios y la
    linea base por contador puede calcular por si mismo la intensidad
    energetica, las lecturas atipicas y cualquier umbral que el analista quiera
    ajustar, con un join sencillo en lugar de recalcular percentiles sobre todo
    el historico.

    Se cargan aqui, y no con un script aparte, para que el origen sea el mismo
    Parquet que alimenta el broadcast join y no puedan divergir. Son 498 y 652
    filas con UPSERT, asi que reiniciar el job las deja igual.
    """
    import psycopg2
    from psycopg2.extras import execute_values

    def entero(v):
        # year_built y floor_count llegan como float por los nulos del origen.
        return None if v is None else int(v)

    edificios = [(r.building_id, r.site_id, r.primary_use,
                  entero(r.square_feet), entero(r.year_built), entero(r.floor_count))
                 for r in spark.read.parquet(str(dim_path)).collect()]
    baseline = [(r.building_id, r.meter_type, r.baseline_p25, r.baseline_p50,
                 r.baseline_p75, r.baseline_iqr)
                for r in spark.read.parquet(str(base_path)).collect()]

    conn = psycopg2.connect(host=props["host"], port=props["port"], dbname=props["dbname"],
                            user=props["user"], password=props["password"])
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO buildings
                    (building_id, site_id, primary_use, square_feet, year_built, floor_count)
                VALUES %s
                ON CONFLICT (building_id) DO UPDATE SET
                    site_id = EXCLUDED.site_id, primary_use = EXCLUDED.primary_use,
                    square_feet = EXCLUDED.square_feet, year_built = EXCLUDED.year_built,
                    floor_count = EXCLUDED.floor_count
            """, edificios, page_size=500)
            execute_values(cur, """
                INSERT INTO sensor_baseline
                    (building_id, meter_type, baseline_p25, baseline_p50,
                     baseline_p75, baseline_iqr)
                VALUES %s
                ON CONFLICT (building_id, meter_type) DO UPDATE SET
                    baseline_p25 = EXCLUDED.baseline_p25, baseline_p50 = EXCLUDED.baseline_p50,
                    baseline_p75 = EXCLUDED.baseline_p75, baseline_iqr = EXCLUDED.baseline_iqr
            """, baseline, page_size=500)
        logger.info("Referencia cargada en PostgreSQL: %d edificios, %d lineas base",
                    len(edificios), len(baseline))
    finally:
        conn.close()
