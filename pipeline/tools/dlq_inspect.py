"""
Muestra en pantalla los eventos que el bridge rechazo a la DLQ (Objetivo 2):
motivo, topico MQTT de origen y payload original integro.

`kpi_report.py` ya cuenta cuantos hay (offsets de `iot.telemetry.dlq`), pero
no ensena su contenido. Este script lee el topico -retencion infinita, ver
`reset_state.py`- desde el principio e imprime cada uno.

Uso:
    python dlq_inspect.py
    python dlq_inspect.py --desde 2026-08-20   # solo lo rechazado desde esa fecha (UTC)
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.connection_args import anadir_argumentos_kafka
from common.logging_setup import configurar_logging

logger = logging.getLogger("dlq_inspect")


def _fecha_utc(epoch_ms: int | None) -> str:
    if not epoch_ms:
        return "(sin bridge_ts)"
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def run(args: argparse.Namespace) -> int:
    # consumer_timeout_ms trata el topico como un fichero de una vez: se
    # detiene solo cuando lleva 5 s sin recibir nada nuevo, en vez de
    # bloquearse esperando mensajes que no van a llegar.
    try:
        consumer = KafkaConsumer(
            args.dlq_topic,
            bootstrap_servers=args.bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
        )
    except KafkaError as exc:
        raise RuntimeError(f"No se pudo conectar a Kafka ({args.bootstrap_servers}): {exc}")

    total = 0
    for msg in consumer:
        try:
            registro = json.loads(msg.value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning("[%s] registro de la DLQ ilegible: %s", msg.offset, exc)
            continue

        # bridge_ts es cuando el bridge proceso el rechazo, no cuando se
        # publico el evento original (que puede venir del historico de 2016).
        bridge_ts = registro.get("bridge_ts")
        if args.desde_ms is not None and (bridge_ts is None or bridge_ts < args.desde_ms):
            continue

        total += 1
        logger.info("[%s] %s | topico=%s | motivo=%s", msg.offset,
                    _fecha_utc(registro.get("bridge_ts")),
                    registro.get("topico_mqtt"), registro.get("error"))
        logger.info("       payload: %s", registro.get("payload_original"))

    consumer.close()
    if total == 0:
        logger.info("La DLQ esta vacia (o el filtro no encontro nada).")
    else:
        logger.info("--- %d eventos rechazados ---", total)
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Muestra los eventos rechazados a la DLQ (TFM)")
    anadir_argumentos_kafka(p)
    p.add_argument("--desde", default=None, metavar="AAAA-MM-DD",
                   help="Solo eventos rechazados en o despues de esta fecha (UTC, "
                        "medianoche). Por defecto, desde el principio")
    args = p.parse_args()
    if args.desde:
        try:
            umbral = datetime.strptime(args.desde, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            p.error(f"--desde debe tener formato AAAA-MM-DD, no {args.desde!r}")
        args.desde_ms = int(umbral.timestamp() * 1000)
    else:
        args.desde_ms = None
    return args


if __name__ == "__main__":
    configurar_logging("dlq_inspect")
    try:
        sys.exit(run(parse_args()))
    except (RuntimeError, OSError) as exc:
        logger.error("%s", exc)
        sys.exit(1)
