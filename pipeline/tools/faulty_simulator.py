"""
Publica una muestra de la tabla de hechos con eventos invalidos
para ejercitar la ruta real de validacion del bridge
(Objetivo 2) y dejar constancia en la DLQ.

Cada evento publicado -real o invalido- lleva EXACTAMENTE los mismos campos
que publica el simulador real (`build_payload()` en `mqtt_simulator.py`):
`building_id`, `meter_type`, `timestamp`, `meter_reading`, `sim_publish_ts`.

Uso:
    python faulty_simulator.py --limite 10000 --fallas 300
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.logging_setup import configurar_logging
from simulator.simulator_helper import RUTA_TELEMETRIA, preparar

logger = logging.getLogger("faulty_simulator")

TOPIC_TEMPLATE = "iot/{building_id}/{meter_type}/telemetry"
RUTA_SALIDA = Path(__file__).resolve().parents[1] / "data" / "faulty_events.json"


# --------------------------------------------------------------------------
# Los seis casos, cada uno corrompe UN campo de un evento real 
# --------------------------------------------------------------------------
def _fallo_meter_reading_inf(base: dict) -> dict:
    return dict(base, meter_reading=float("inf"))


def _fallo_meter_reading_nan(base: dict) -> dict:
    return dict(base, meter_reading=float("nan"))


def _fallo_building_id_ausente(base: dict) -> dict:
    e = dict(base)
    del e["building_id"]
    return e


def _fallo_timestamp_nulo(base: dict) -> dict:
    return dict(base, timestamp=None)


def _fallo_meter_reading_texto(base: dict) -> dict:
    return dict(base, meter_reading="no-numerico")


def _fallo_building_id_numero(base: dict) -> dict:
    return dict(base, building_id=float(base["building_id"]))


# (nombre del motivo, funcion). El nombre solo se usa para el log de esta
# herramienta -nunca viaja en el evento publicado.
FALLOS = [
    ("meter_reading_inf", _fallo_meter_reading_inf),
    ("meter_reading_nan", _fallo_meter_reading_nan),
    ("building_id_ausente", _fallo_building_id_ausente),
    ("timestamp_nulo", _fallo_timestamp_nulo),
    ("meter_reading_texto", _fallo_meter_reading_texto),
    ("building_id_numero", _fallo_building_id_numero),
]


# --------------------------------------------------------------------------
# Construccion del fichero JSON
# --------------------------------------------------------------------------
def _evento_real(fila) -> dict:
    """Mismo contrato que build_payload(), sin sim_publish_ts (se sella al
    publicar) y sin las conversiones str()/float() de por medio -aqui los
    valores YA vienen del tipo correcto porque no se ha tocado nada."""
    return {
        "building_id": str(fila.building_id),
        "meter_type": str(fila.meter_type),
        "timestamp": fila.timestamp.isoformat(),
        "meter_reading": float(fila.meter_reading),
    }


def construir(args: argparse.Namespace) -> list[tuple[dict, str | None]]:
    """Devuelve pares (evento, motivo). `motivo` es None para los eventos
    reales -el marcador vive solo en memoria, en esta tupla, nunca en el
    propio evento que se publica o se guarda."""

    df = preparar(args.telemetry).tail(args.limite)
    filas = list(df.itertuples(index=False))
    if not filas:
        raise RuntimeError("No hay eventos que publicar (revisa --limite y la ruta del Parquet)")

    fallas = args.fallas
    if fallas > len(filas):
        logger.warning("--fallas %d supera --limite %d; se recorta a %d",
                       fallas, len(filas), len(filas))
        fallas = len(filas)

    # Posiciones equiespaciadas sobre los eventos reales YA ordenados por
    # tiempo: con --limite 10000 --fallas 300, una posicion cada 10000/300
    # ~= 33 eventos, no elegidas al azar. El motivo se reparte por turno entre
    # los 6 (round-robin)
    inserciones: dict[int, list] = {}
    if fallas > 0:
        paso = len(filas) / fallas
        for k in range(fallas):
            idx = min(int(k * paso), len(filas) - 1)
            inserciones.setdefault(idx, []).append(FALLOS[k % len(FALLOS)])

    # Cada invalido se inserta JUSTO DESPUES de su vecino real, no se ordena
    # todo junto al final
    pares: list[tuple[dict, str | None]] = []
    n_invalidos = 0
    for i, fila in enumerate(filas):
        base = _evento_real(fila)
        pares.append((base, None))
        for nombre, fallo in inserciones.get(i, ()):
            pares.append((fallo(base), nombre))
            n_invalidos += 1

    logger.info("Construidos %d eventos reales + %d invalidos (~1 cada %d eventos)",
               len(filas), n_invalidos, round(len(filas) / fallas) if fallas else 0)
    return pares


def guardar(pares: list[tuple[dict, str | None]]) -> None:
    """Sobrescribe pipeline/data/faulty_events.json."""
    eventos = [evento for evento, _ in pares]
    RUTA_SALIDA.parent.mkdir(parents=True, exist_ok=True)
    RUTA_SALIDA.write_text(json.dumps(eventos, indent=2))
    logger.info("Guardado %s (%d eventos, %.1f KB)",
               RUTA_SALIDA, len(eventos), RUTA_SALIDA.stat().st_size / 1024)


# --------------------------------------------------------------------------
# Publicacion via MQTT, contra el bridge real
# --------------------------------------------------------------------------
def publicar(pares: list[tuple[dict, str | None]], args: argparse.Namespace) -> None:
    """Reproduce los eventos en orden, con el reloj comprimido por
    --acelerar: el mismo principio que el simulador real (`_programa()`), pero
    en una sola conexion secuencial.
    """
    cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=args.client_id)
    cliente.connect(args.broker_host, args.broker_port)
    cliente.loop_start()

    # El invalido con timestamp=None no tiene marca con la que calcular su
    # instante: se publica sin espera, en cuanto le toca el turno.
    con_marca = [e for e, _ in pares if e["timestamp"] is not None]
    t_sim0 = con_marca[0]["timestamp"] if con_marca else None

    publicados, invalidos_publicados = 0, 0
    inicio = time.monotonic()
    for evento, motivo in pares:
        if evento["timestamp"] is not None and t_sim0 is not None:
            delta = (datetime.fromisoformat(evento["timestamp"])
                    - datetime.fromisoformat(t_sim0)).total_seconds()
            objetivo = delta / args.acelerar
            espera = inicio + objetivo - time.monotonic()
            if espera > 0:
                time.sleep(espera)

        # Copia limpia: exactamente los campos del contrato
        publicable = dict(evento)
        topico = TOPIC_TEMPLATE.format(
            building_id=publicable.get("building_id", "sin-building-id"),
            meter_type=publicable.get("meter_type", "sin-meter-type"))
        publicable["sim_publish_ts"] = int(time.time() * 1000)

        cliente.publish(topico, payload=json.dumps(publicable), qos=args.qos)
        publicados += 1
        if motivo:
            invalidos_publicados += 1
            logger.info("  [invalido %s] %s", motivo, topico)

    time.sleep(2)  # margen para que paho vacie el buffer de salida
    cliente.loop_stop()
    cliente.disconnect()
    logger.info("Publicados %d eventos (%d invalidos) en %.1f s",
               publicados, invalidos_publicados, time.monotonic() - inicio)


# --------------------------------------------------------------------------
def run(args: argparse.Namespace) -> int:
    pares = construir(args)
    guardar(pares)
    publicar(pares, args)
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Publica una muestra real + eventos invalidos, para ejercitar la DLQ (TFM)")
    p.add_argument("--telemetry", type=Path, default=RUTA_TELEMETRIA,
                   help="Tabla de hechos generada por prepare_ashrae.py")
    p.add_argument("--limite", type=int, default=10000,
                   help="Eventos reales a tomar (cola cronologica, terminando en la fecha "
                        "mas reciente del Parquet)")
    p.add_argument("--fallas", type=int, default=6,
                   help="Eventos invalidos a mezclar, repartidos a partes iguales entre los "
                        "6 motivos de rechazo y esparcidos a posiciones equiespaciadas dentro "
                        "de --limite (p.ej. --limite 10000 --fallas 300 da 1 invalido cada 33 "
                        "eventos reales, ~50 de cada motivo)")
    p.add_argument("--acelerar", type=float, default=2000.0,
                   help="Factor de compresion del reloj, igual que en mqtt_simulator.py")
    p.add_argument("--broker-host", default="localhost")
    p.add_argument("--broker-port", type=int, default=1883)
    p.add_argument("--qos", type=int, default=1, choices=[0, 1, 2])
    p.add_argument("--client-id", default="tfm-faulty-sim")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("faulty_simulator")
    try:
        sys.exit(run(parse_args()))
    except (RuntimeError, OSError, FileNotFoundError) as exc:
        logger.error("%s", exc)
        sys.exit(1)
