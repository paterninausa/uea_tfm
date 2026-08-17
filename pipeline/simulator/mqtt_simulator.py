"""
Simulador de sensores IoT: reproduce telemetria historica sobre MQTT.

Lee el subconjunto de ASHRAE preparado por `pipeline/data/prepare_ashrae.py` y
publica cada lectura como un mensaje MQTT, reproduciendo la topologia de
topicos del trabajo:

    iot/{building_id}/{meter_type}/telemetry

UN SENSOR ES EL PAR (edificio, tipo de contador). Un mismo edificio con
contador de electricidad y de agua fria son dos sensores con series
independientes; el subconjunto en uso tiene 652.

EL TOPICO SOLO IDENTIFICA AL SENSOR. No lleva nivel de emplazamiento: `site_id`
es derivable de `building_id` a traves de la tabla de dimension, igual que lo
eran `event_id` y `sensor_id` antes de eliminarlos. Ponerlo tambien en el
topico creaba una segunda fuente de verdad —topico y dimension podrian
discrepar si un edificio se reasignara— sin que nada lo consumiera: el bridge
se suscribe a `iot/#` y nunca parte el topico. El emplazamiento entra en el
analisis donde importa, en el broadcast join de Spark contra la dimension.

EL PAYLOAD SOLO LLEVA LO QUE EMITIRIA EL CONTADOR: identidad, instante y
medida, mas `sim_publish_ts`.

Uso:
    python mqtt_simulator.py --rate 100 --limit 5000
    python mqtt_simulator.py --max-sensors 100          # carga base, Objetivo 5
    python mqtt_simulator.py --rate 0 --rebase-end now  # demostracion en vivo
"""

import argparse
import json
import logging
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
import pandas as pd
from paho.mqtt.enums import CallbackAPIVersion

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("mqtt_simulator")

TOPIC_TEMPLATE = "iot/{building_id}/{meter_type}/telemetry"

AQUI = Path(__file__).parent


def cargar(telemetry_path: Path) -> pd.DataFrame:
    """Carga la tabla de hechos. No necesita la dimension: el topico se
    construye solo con los campos que emite el propio contador."""
    if not telemetry_path.exists():
        raise FileNotFoundError(
            f"No se encontro {telemetry_path}. Genera los datos primero: "
            "python pipeline/data/prepare_ashrae.py"
        )

    df = pd.read_parquet(telemetry_path)
    logger.info("Cargados %s eventos | %s sensores | %s edificios",
                f"{len(df):,}",
                f"{df.groupby(['building_id', 'meter_type']).ngroups:,}",
                f"{df['building_id'].nunique():,}")
    return df


def filtrar_sensores(df: pd.DataFrame, max_sensores: int | None) -> pd.DataFrame:
    """Se queda con los primeros N sensores en orden determinista.

    El orden fijo importa para el Objetivo 5: hace que la seleccion de 100
    sensores sea un SUBCONJUNTO de la de 250, esta de la de 500, y asi. La
    degradacion de throughput se mide entonces sobre los mismos sensores mas
    otros, y no comparando dos muestras aleatorias distintas, que no serian
    comparables entre si.
    """
    if not max_sensores:
        return df

    sensores = (df[["building_id", "meter_type"]].drop_duplicates()
                .sort_values(["building_id", "meter_type"]).reset_index(drop=True))
    if max_sensores >= len(sensores):
        logger.info("Se pidieron %d sensores y solo hay %d: se usan todos",
                    max_sensores, len(sensores))
        return df

    df = df.merge(sensores.head(max_sensores), on=["building_id", "meter_type"], how="inner")
    logger.info("Filtrado a %d sensores -> %s eventos", max_sensores, f"{len(df):,}")
    return df


def rebasar(df: pd.DataFrame, destino: str) -> pd.DataFrame:
    """Desplaza las marcas de tiempo para que la ULTIMA caiga en `destino`.

    Los datos son de 2016. Sin desplazarlos, un panel de Grafana configurado a
    "ultimas 6 horas" sale vacio y la demostracion en vivo pierde el efecto de
    tiempo real. Se aplica un unico offset constante a todo el conjunto, de modo
    que las distancias relativas entre eventos —y por tanto los ciclos diario y
    estacional— se conservan intactas.

    Se ancla la ULTIMA marca y no la primera para que todo el historico quede en
    el pasado respecto al instante indicado, que es lo que esperan los paneles.
    Anclando la primera, el replay acelerado generaria marcas en el futuro.
    """
    instante = datetime.now(timezone.utc) if destino == "now" else datetime.fromisoformat(destino)
    if instante.tzinfo is None:
        instante = instante.replace(tzinfo=timezone.utc)

    offset = pd.Timestamp(instante).tz_localize(None) - df["timestamp"].max()
    df = df.copy()
    df["timestamp"] = df["timestamp"] + offset
    logger.info("Marcas desplazadas %+.1f dias: el rango pasa a ser %s -> %s",
                offset.total_seconds() / 86400, df["timestamp"].min(), df["timestamp"].max())
    return df


def build_topic(fila) -> str:
    return TOPIC_TEMPLATE.format(building_id=fila.building_id, meter_type=fila.meter_type)


def build_payload(fila) -> dict:
    """Payload con los campos del contrato y nada mas.

    `timestamp` se envia como cadena ISO-8601 y el bridge lo convierte a epoch
    en milisegundos, que es lo que declara el esquema Avro. Se mantiene en ISO
    aqui porque hace legible el trafico al depurar con `mosquitto_sub`.
    """
    return {
        "building_id": int(fila.building_id),
        "meter_type": str(fila.meter_type),
        "timestamp": fila.timestamp.isoformat(),
        "meter_reading": float(fila.meter_reading),
        # Instante real del publish(): origen de tiempo del KPI de latencia del
        # Objetivo 1, y unico campo del payload que no emite el contador.
        "sim_publish_ts": int(time.time() * 1000),
    }


class GracefulShutdown:
    def __init__(self):
        self.stop = False
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def _handler(self, *_args):
        logger.info("Senal de parada recibida, cerrando simulador...")
        self.stop = True


def run(args: argparse.Namespace) -> int:
    df = cargar(args.telemetry)
    df = filtrar_sensores(df, args.max_sensors)
    if args.rebase_end:
        df = rebasar(df, args.rebase_end)

    # Orden cronologico: el watermark de Spark asume que el tiempo de evento
    # avanza, y un flujo desordenado haria que se descartaran lecturas tardias.
    df = df.sort_values("timestamp").reset_index(drop=True)
    if args.limit:
        df = df.head(args.limit)

    client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=args.client_id)
    client.on_connect = lambda c, u, f, rc, p=None: logger.info(
        "Conectado al broker MQTT (reason_code=%s)", rc)
    client.on_disconnect = lambda c, u, flags, rc=None, p=None: logger.warning(
        "Desconectado del broker (reason_code=%s)", rc)

    logger.info("Conectando a %s:%d ...", args.broker_host, args.broker_port)
    client.connect(args.broker_host, args.broker_port, keepalive=30)
    client.loop_start()

    shutdown = GracefulShutdown()
    intervalo = 1.0 / args.rate if args.rate > 0 else 0
    publicados = fallidos = 0
    t0 = time.monotonic()

    logger.info("Publicando %s eventos%s", f"{len(df):,}",
                f" a {args.rate:g} ev/s" if args.rate else " sin limite de tasa")
    try:
        for fila in df.itertuples(index=False):
            if shutdown.stop:
                break

            topico = build_topic(fila)
            resultado = client.publish(topico, json.dumps(build_payload(fila)), qos=args.qos)
            # wait_for_publish bloquea hasta que el broker confirma el QoS 1. Es
            # lo que hace fiable el contador de perdidas, y a la vez el techo de
            # throughput del simulador: se midieron ~740 ev/s, muy por encima de
            # los 50 ev/s que exige el Objetivo 3.
            resultado.wait_for_publish(timeout=5)

            if resultado.rc == mqtt.MQTT_ERR_SUCCESS:
                publicados += 1
            else:
                fallidos += 1
                logger.warning("Fallo al publicar en %s (rc=%s)", topico, resultado.rc)

            if publicados and publicados % 5000 == 0:
                logger.info("Publicados: %s | Fallidos: %d | %.1f ev/s", f"{publicados:,}",
                            fallidos, publicados / max(1e-9, time.monotonic() - t0))

            if intervalo:
                time.sleep(intervalo)

    finally:
        client.loop_stop()
        client.disconnect()
        total = publicados + fallidos
        duracion = time.monotonic() - t0
        logger.info("--- Fin de la simulacion ---")
        logger.info("  publicados      : %s", f"{publicados:,}")
        logger.info("  fallidos        : %d", fallidos)
        logger.info("  tasa de perdida : %.4f%%", (fallidos / total * 100) if total else 0.0)
        logger.info("  duracion        : %.1f s", duracion)
        logger.info("  throughput      : %.1f ev/s", publicados / max(1e-9, duracion))

    return 0 if fallidos == 0 else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simulador MQTT de telemetria IoT (TFM)")
    p.add_argument("--telemetry", type=Path, default=AQUI / "../data/ashrae_telemetry.parquet",
                   help="Tabla de hechos generada por prepare_ashrae.py")
    p.add_argument("--broker-host", default="localhost")
    p.add_argument("--broker-port", type=int, default=1883)
    p.add_argument("--client-id", default="tfm-simulator")
    p.add_argument("--qos", type=int, default=1, choices=[0, 1, 2],
                   help="QoS MQTT (1 por defecto, ver Objetivo 1)")
    p.add_argument("--rate", type=float, default=100.0,
                   help="Eventos por segundo (0 = sin limite, para pruebas de carga)")
    p.add_argument("--limit", type=int, default=None,
                   help="Numero maximo de eventos a publicar")
    p.add_argument("--max-sensors", type=int, default=None,
                   help="Publica solo los primeros N sensores, en orden determinista. "
                        "Para la escalera de carga del Objetivo 5: 100, 250, 500, 652")
    p.add_argument("--rebase-end", metavar="ISO|now", default=None,
                   help="Desplaza las marcas de tiempo para que la ultima caiga en este "
                        "instante. Para la demostracion en vivo: los datos son de 2016 y "
                        "los paneles miran a fechas recientes")
    return p.parse_args()


if __name__ == "__main__":
    try:
        raise SystemExit(run(parse_args()))
    except (FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        raise SystemExit(1)
    except ConnectionRefusedError:
        logger.error("No se pudo conectar al broker MQTT. Levanta el stack: "
                     "docker compose -f pipeline/docker-compose.yml up -d")
        raise SystemExit(1)
