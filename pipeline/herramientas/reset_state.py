"""
Deja el pipeline en el estado limpio que exige cualquier medicion de KPI.

Toda cifra publicada en la memoria del trabajo se obtuvo "sobre el stack
completo, estado limpio (topico recreado, tablas vacias, checkpoints borrados)".
Ese estado se conseguia a mano, con una secuencia de ordenes que no estaba
escrita en ninguna parte: bastaba olvidar una para medir sobre datos de la
prueba anterior y no notarlo, porque el sintoma —unas latencias mejores de lo
que tocaba, un throughput que no cuadra— no se distingue de un buen resultado.

Los tres estados que hay que borrar, y por que cada uno:

  * CHECKPOINTS de Spark. Guardan hasta que offset se leyo. Si sobreviven, el
    job reanuda donde lo dejo y no vuelve a procesar los eventos de la prueba.
  * TOPICOS de Kafka. Se recrean en vez de vaciarse porque el log conserva 7
    dias de retencion: un consumidor que empiece por `earliest` leeria tambien
    lo de ayer.
  * TABLAS de ambos sumideros. La escritura es un UPSERT idempotente, asi que
    reprocesar no duplica filas, pero si deja mezcladas dos mediciones en la
    misma tabla y los percentiles salen de la union de ambas.

Uso:
    python reset_state.py                # pide confirmacion
    python reset_state.py --yes          # sin preguntar
    python reset_state.py --yes --all    # trunca tambien las tablas de referencia
"""

import argparse
import logging
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.conexiones import (  # noqa: E402
    POSTGRES,
    TIMESCALE,
    anadir_argumentos_bd,
    anadir_argumentos_kafka,
    props_bd,
)
from common.logging_setup import configurar_logging  # noqa: E402

logger = logging.getLogger("reset_state")

CONTENEDOR_KAFKA = "tfm-kafka"
KAFKA_TOPICS = "/opt/kafka/bin/kafka-topics.sh"
BOOTSTRAP_INTERNO = "kafka:9092"

DIRECTORIO_CHECKPOINTS = Path(__file__).resolve().parents[1] / "spark" / "checkpoints"

# Procesos que deben estar parados: recrear un topico o borrar un checkpoint
# mientras el job corre lo deja leyendo de algo que ya no existe.
PROCESOS_INCOMPATIBLES = ("telemetry_streaming.py", "mqtt_kafka_bridge.py",
                          "mqtt_simulator.py", "load_ladder.py")

TABLAS_MEDICION = {
    TIMESCALE: ["telemetry_metrics", "streaming_progress"],
    POSTGRES: ["telemetry_events"],
}
# Se regeneran solas: el job de Spark las vuelve a cargar desde los Parquet en
# cada arranque, con UPSERT. Truncarlas no cuesta nada pero tampoco hace falta
# para aislar una medicion, asi que queda detras de --all.
TABLAS_REFERENCIA = {POSTGRES: ["buildings", "sensor_baseline"]}


# --------------------------------------------------------------------------
def procesos_en_marcha() -> list[str]:
    """Devuelve los procesos del pipeline que siguen vivos."""
    vivos = []
    for nombre in PROCESOS_INCOMPATIBLES:
        r = subprocess.run(["pgrep", "-f", nombre], capture_output=True, text=True)
        if r.returncode == 0 and r.stdout.strip():
            vivos.append(f"{nombre} (pid {', '.join(r.stdout.split())})")
    return vivos


def kafka_topics(*argumentos: str) -> str:
    """Ejecuta kafka-topics.sh dentro del contenedor del broker.

    Se usa la misma herramienta que crea los topicos en el arranque del stack
    (el servicio `kafka-init` del compose) en lugar de un cliente Python: no
    anade una dependencia mas que mantener al dia con la version del broker, y
    lo que hace es literalmente lo mismo que ya esta documentado en el README.
    """
    r = subprocess.run(
        ["docker", "exec", CONTENEDOR_KAFKA, KAFKA_TOPICS,
         "--bootstrap-server", BOOTSTRAP_INTERNO, *argumentos],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"kafka-topics.sh fallo: {r.stderr.strip() or r.stdout.strip()}")
    return r.stdout


def descripcion_topico(topico: str) -> tuple[int, int] | None:
    """Particiones y factor de replica actuales, o None si el topico no existe.

    Se leen antes de borrar para recrearlo EXACTAMENTE igual. Fijar aqui un 3
    escrito a mano crearia una segunda fuente de verdad frente al compose, y una
    prueba de carga sobre un numero de particiones distinto del habitual mide
    otro sistema.
    """
    salida = kafka_topics("--describe", "--topic", topico)
    for linea in salida.splitlines():
        if "PartitionCount:" in linea:
            partes = linea.replace("\t", " ").split()
            particiones = int(partes[partes.index("PartitionCount:") + 1])
            replicas = int(partes[partes.index("ReplicationFactor:") + 1])
            return particiones, replicas
    return None


def recrear_topico(topico: str, particiones_defecto: int) -> None:
    actual = descripcion_topico(topico)
    if actual:
        particiones, replicas = actual
        logger.info("Borrando %s (%d particiones, replica %d)", topico, particiones, replicas)
        kafka_topics("--delete", "--topic", topico)
    else:
        particiones, replicas = particiones_defecto, 1
        logger.info("El topico %s no existia; se creara con %d particiones",
                    topico, particiones)

    # El borrado es asincrono: crear de inmediato falla con TopicExistsException.
    for _ in range(60):
        if topico not in kafka_topics("--list").split():
            break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"El topico {topico} sigue existiendo 30 s despues de borrarlo")

    kafka_topics("--create", "--topic", topico,
                 "--partitions", str(particiones), "--replication-factor", str(replicas))
    logger.info("Recreado %s con %d particiones", topico, particiones)


def truncar(props: dict, tablas: list[str]) -> None:
    """Vacia las tablas indicadas informando de cuantas filas habia.

    El recuento previo no es adorno: es la unica prueba de que la medicion
    anterior se borro de verdad, y queda en el log junto a la orden que lo hizo.
    """
    import psycopg2

    conn = psycopg2.connect(**props)
    try:
        with conn, conn.cursor() as cur:
            for tabla in tablas:
                cur.execute(f"SELECT count(*) FROM {tabla}")
                antes = cur.fetchone()[0]
                cur.execute(f"TRUNCATE TABLE {tabla}")
                logger.info("  %-20s %s filas borradas", tabla, f"{antes:,}")
    finally:
        conn.close()


def borrar_checkpoints() -> None:
    if not DIRECTORIO_CHECKPOINTS.exists():
        logger.info("No hay checkpoints que borrar en %s", DIRECTORIO_CHECKPOINTS)
        return
    subdirs = sorted(d.name for d in DIRECTORIO_CHECKPOINTS.iterdir() if d.is_dir())
    shutil.rmtree(DIRECTORIO_CHECKPOINTS)
    logger.info("Checkpoints borrados: %s", ", ".join(subdirs) or "(vacio)")


# --------------------------------------------------------------------------
def run(args: argparse.Namespace) -> int:
    vivos = procesos_en_marcha()
    if vivos:
        logger.error("Hay procesos del pipeline en marcha; paralos antes de continuar:")
        for v in vivos:
            logger.error("  - %s", v)
        return 1

    tablas = {cual: list(t) for cual, t in TABLAS_MEDICION.items()}
    if args.all:
        for cual, extra in TABLAS_REFERENCIA.items():
            tablas[cual].extend(extra)

    logger.warning("Se va a BORRAR de forma irreversible:")
    logger.warning("  checkpoints : %s", DIRECTORIO_CHECKPOINTS)
    logger.warning("  topicos     : %s, %s", args.topic, args.dlq_topic)
    for cual, lista in tablas.items():
        logger.warning("  %-12s: %s", cual, ", ".join(lista))

    if not args.yes:
        try:
            if input("Escribe 'si' para continuar: ").strip().lower() != "si":
                logger.info("Cancelado; no se ha tocado nada")
                return 1
        except EOFError:
            logger.error("Sin terminal interactiva: usa --yes si es lo que quieres")
            return 1

    borrar_checkpoints()
    recrear_topico(args.topic, particiones_defecto=3)
    recrear_topico(args.dlq_topic, particiones_defecto=1)
    for cual, lista in tablas.items():
        logger.info("Truncando en %s:", cual)
        truncar(props_bd(args, cual), lista)

    logger.info("--- Estado limpio: el pipeline puede arrancar para una medicion nueva ---")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deja el pipeline en estado limpio (TFM)")
    anadir_argumentos_kafka(p)
    anadir_argumentos_bd(p)
    p.add_argument("--yes", action="store_true", help="No pedir confirmacion")
    p.add_argument("--all", action="store_true",
                   help="Truncar tambien buildings y sensor_baseline. No hace falta para "
                        "aislar una medicion: el job de Spark las recarga al arrancar")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("reset_state")
    try:
        sys.exit(run(parse_args()))
    except (RuntimeError, OSError) as exc:
        logger.error("%s", exc)
        sys.exit(1)
