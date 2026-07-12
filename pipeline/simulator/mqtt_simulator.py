"""
Simulador de comunicacion IoT - Fase 1 (basico, sincrono)

Publica telemetria de consumo electrico via MQTT, a partir del historico
en Parquet, replicando la topologia de topicos definida para el TFM:

    iot/{company_id}/{site_id}/{machine_id}/telemetry

`sensor_id` no identifica un dispositivo estable (rota entre maquinas en el
dataset original), por lo que la identidad del "sensor simulado" es
`machine_id`, no `sensor_id`. `sensor_id` viaja como campo del payload.

Esta version es intencionalmente simple (un solo hilo, cliente MQTT unico):
valida el flujo extremo a extremo antes de anadir el modo asincrono/carga
(Objetivo 5) y la serializacion Avro via Apicurio (Objetivo 2).

Uso:
    python mqtt_simulator.py --parquet-path ../data/power_measurements_parquet \
        --broker-host localhost --broker-port 1883 --rate 20 --limit 5000
"""

import argparse
import json
import logging
import signal
import time
from pathlib import Path

import pandas as pd
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("mqtt_simulator")

TOPIC_TEMPLATE = "iot/{company_id}/{site_id}/{machine_id}/telemetry"

# Campos que representan timestamps historicos del dataset original.
# NO deben usarse para medir la latencia extremo a extremo del pipeline:
# esa medicion se hace con sim_publish_ts, generado en el instante del publish.
_TIMESTAMP_FIELDS = ("timestamp", "ingest_ts")


def build_topic(row: pd.Series) -> str:
    return TOPIC_TEMPLATE.format(
        company_id=row["company_id"],
        site_id=row["site_id"],
        machine_id=row["machine_id"],
    )


def _to_native(value):
    """Convierte tipos de pandas/numpy a tipos nativos serializables en JSON."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def build_payload(row: pd.Series) -> dict:
    """Construye el payload del evento. Los campos coinciden con las columnas
    del dataset (futuro esquema Avro v1), mas sim_publish_ts."""
    payload = {}
    for key, value in row.items():
        if key in _TIMESTAMP_FIELDS:
            payload[key] = pd.Timestamp(value).isoformat() if pd.notna(value) else None
        else:
            payload[key] = _to_native(value)

    payload["sim_publish_ts"] = int(time.time() * 1000)  # epoch millis
    return payload


def load_dataset(parquet_path: str) -> pd.DataFrame:
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(
            f"No se encontro {path}. Descarga primero el parquet desde "
            "el volumen de Databricks (ver README del simulador)."
        )
    df = pd.read_parquet(path)
    df = df.sort_values("timestamp").reset_index(drop=True)
    logger.info(
        "Dataset cargado: %d filas, %d machine_id unicos",
        len(df), df["machine_id"].nunique(),
    )
    return df


class GracefulShutdown:
    def __init__(self):
        self.stop = False
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def _handler(self, *_args):
        logger.info("Senal de parada recibida, cerrando simulador...")
        self.stop = True


def run(args: argparse.Namespace) -> None:
    df = load_dataset(args.parquet_path)
    if args.limit:
        df = df.head(args.limit)

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="tfm-simulator",
    )
    client.on_connect = lambda c, u, flags, reason_code, props=None: logger.info(
        "Conectado al broker MQTT (reason_code=%s)", reason_code
    )
    client.on_disconnect = lambda c, u, dc, reason_code=None, props=None: logger.warning(
        "Desconectado del broker (reason_code=%s)", reason_code
    )

    logger.info("Conectando a %s:%d ...", args.broker_host, args.broker_port)
    client.connect(args.broker_host, args.broker_port, keepalive=30)
    client.loop_start()

    shutdown = GracefulShutdown()
    interval = 1.0 / args.rate if args.rate > 0 else 0
    published, failed = 0, 0

    try:
        for _, row in df.iterrows():
            if shutdown.stop:
                break

            topic = build_topic(row)
            payload = build_payload(row)

            result = client.publish(topic, json.dumps(payload), qos=args.qos)
            result.wait_for_publish(timeout=5)

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                published += 1
            else:
                failed += 1
                logger.warning("Fallo al publicar en %s (rc=%s)", topic, result.rc)

            if published and published % 500 == 0:
                logger.info("Publicados: %d | Fallidos: %d", published, failed)

            if interval:
                time.sleep(interval)

    finally:
        client.loop_stop()
        client.disconnect()
        total = published + failed
        loss_rate = (failed / total * 100) if total else 0
        logger.info(
            "Fin de la simulacion. Publicados=%d Fallidos=%d Tasa de perdida=%.3f%%",
            published, failed, loss_rate,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulador MQTT de telemetria IoT (TFM)")
    parser.add_argument("--parquet-path", required=True,
                         help="Ruta local al directorio/archivo Parquet con la telemetria")
    parser.add_argument("--broker-host", default="localhost")
    parser.add_argument("--broker-port", type=int, default=1883)
    parser.add_argument("--qos", type=int, default=1, choices=[0, 1, 2],
                         help="Nivel de QoS MQTT (por defecto 1, ver Objetivo 1)")
    parser.add_argument("--rate", type=float, default=20.0,
                         help="Eventos por segundo (0 = sin limite, maxima velocidad)")
    parser.add_argument("--limit", type=int, default=None,
                         help="Numero maximo de filas a publicar (para pruebas rapidas)")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
