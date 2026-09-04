"""
Publica una muestra de la tabla de hechos con un puñado de eventos invalidos
mezclados dentro, para ejercitar la ruta real de validacion del bridge
(Objetivo 2) y dejar constancia en la DLQ.

A diferencia de ensuciar `ashrae_telemetry.parquet`, aqui los eventos
invalidos se construyen a mano, sin pasar por `build_payload()` de
`mqtt_simulator.py`: esa funcion castiga con `str()`/`float()` cada campo, asi
que un `building_id` numerico o un `meter_reading` de texto llegarian
neutralizados (convertidos de vuelta a un valor valido) o, en el peor caso,
tumbarian al simulador real con una excepcion que su unico `except
aiomqtt.MqttError` no cubre. Construyendo el diccionario aqui, sin ese casting,
los seis casos llegan intactos al bridge.

Cada vez que se ejecuta: toma la COLA cronologica de `--limite` eventos reales
de `ashrae_telemetry.parquet` -los que terminan en la fecha mas reciente de la
tabla, que con `prepare_ashrae.py --fecha-final` es hoy o cerca-, le mezcla
`--fallas` eventos invalidos (repartidos a partes iguales entre los 6 motivos
de rechazo, cada uno tomado de un evento real con un solo campo corrompido) en
posiciones equiespaciadas dentro del lote -con `--limite 10000 --fallas 300`,
uno cada `10000/300 ~= 33` eventos reales-, sobrescribe
`pipeline/data/faulty_events.json` con el conjunto ordenado por tiempo, y lo
publica via MQTT contra Mosquitto con las marcas de tiempo comprimidas por
`--acelerar` (igual principio que el simulador real: dividir el avance del
reloj de evento por el factor), para que el bridge los reciba, los valide y
desvie los invalidos a la DLQ.

Los eventos invalidos llevan un campo extra `_origen` (con el motivo) que
Avro ignora sin mas -"campos adicionales en el payload: se ignoran sin dejar
constancia" (FAULT_HANDLING.md)- pero que el bridge SI guarda integro en el
`payload_original` de la DLQ, asi que se ve con `dlq_inspect.py` sin tener que
adivinar cual evento corresponde a cual motivo.

Uso:
    python faulty_simulator.py
    python faulty_simulator.py --limite 5000 --acelerar 1000
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import paho.mqtt.client as mqtt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.logging_setup import configurar_logging  # noqa: E402
from simulator.simulator_helper import RUTA_TELEMETRIA, preparar  # noqa: E402

logger = logging.getLogger("faulty_simulator")

TOPIC_TEMPLATE = "iot/{building_id}/{meter_type}/telemetry"
RUTA_SALIDA = Path(__file__).resolve().parents[1] / "data" / "faulty_events.json"


# --------------------------------------------------------------------------
# Los seis casos, cada uno corrompe UN campo de un evento real y por lo demas
# lo deja intacto. Se aplican sobre el dict ya construido (4 campos del
# contrato, sin sim_publish_ts: eso se sella en el momento de publicar, igual
# que hace build_payload() en el simulador real).
# --------------------------------------------------------------------------
def _fallo_meter_reading_inf(base: dict) -> dict:
    e = dict(base, meter_reading=float("inf"))
    e["_origen"] = "meter_reading_inf"
    return e


def _fallo_meter_reading_nan(base: dict) -> dict:
    e = dict(base, meter_reading=float("nan"))
    e["_origen"] = "meter_reading_nan"
    return e


def _fallo_building_id_ausente(base: dict) -> dict:
    e = dict(base)
    del e["building_id"]
    e["_origen"] = "building_id_ausente"
    return e


def _fallo_timestamp_nulo(base: dict) -> dict:
    e = dict(base, timestamp=None)
    e["_origen"] = "timestamp_nulo"
    return e


def _fallo_meter_reading_texto(base: dict) -> dict:
    e = dict(base, meter_reading="no-numerico")
    e["_origen"] = "meter_reading_texto"
    return e


def _fallo_building_id_numero(base: dict) -> dict:
    # base["building_id"] ya es str (p.ej. "156"); float() lo vuelve numero
    # de verdad, no una cadena que parezca numero.
    e = dict(base, building_id=float(base["building_id"]))
    e["_origen"] = "building_id_numero"
    return e


FALLOS = [
    _fallo_meter_reading_inf,
    _fallo_meter_reading_nan,
    _fallo_building_id_ausente,
    _fallo_timestamp_nulo,
    _fallo_meter_reading_texto,
    _fallo_building_id_numero,
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


def construir(args: argparse.Namespace) -> list[dict]:
    # preparar() sin limite/ultimas_semanas: solo ordena por tiempo. La COLA
    # (.tail, no .head) es la que termina en la fecha mas reciente de la
    # tabla -hoy, tras `prepare_ashrae.py --fecha-final`-, al contrario que
    # `--limite` en mqtt_simulator.py, que toma el PREFIJO desde el principio
    # del historico.
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
    # los 6 (round-robin), asi que la cuenta por motivo difiere como mucho en
    # 1 entre si, sin necesidad de que 6 divida exacto a --fallas.
    inserciones: dict[int, list] = {}
    if fallas > 0:
        paso = len(filas) / fallas
        for k in range(fallas):
            idx = min(int(k * paso), len(filas) - 1)
            inserciones.setdefault(idx, []).append(FALLOS[k % len(FALLOS)])

    # Cada invalido se inserta JUSTO DESPUES de su vecino real, no se ordena
    # todo junto al final: un evento con timestamp=None (el motivo
    # timestamp_nulo) no tiene con que compararse en un sort por tiempo, y
    # agruparia los 50 al final en vez de esparcirlos como se pide.
    todos = []
    n_invalidos = 0
    for i, fila in enumerate(filas):
        base = _evento_real(fila)
        todos.append(base)
        for fallo in inserciones.get(i, ()):
            todos.append(fallo(base))
            n_invalidos += 1

    logger.info("Construidos %d eventos reales + %d invalidos (~1 cada %d eventos)",
               len(filas), n_invalidos, round(len(filas) / fallas) if fallas else 0)
    return todos


def guardar(eventos: list[dict]) -> None:
    """Sobrescribe pipeline/data/faulty_events.json sin preguntar: es un
    artefacto de prueba regenerado en cada ejecucion, no la tabla de hechos
    real (esa nunca se toca)."""
    RUTA_SALIDA.parent.mkdir(parents=True, exist_ok=True)
    RUTA_SALIDA.write_text(json.dumps(eventos, indent=2))
    logger.info("Guardado %s (%d eventos, %.1f KB)",
               RUTA_SALIDA, len(eventos), RUTA_SALIDA.stat().st_size / 1024)


# --------------------------------------------------------------------------
# Publicacion via MQTT, contra el bridge real
# --------------------------------------------------------------------------
def publicar(eventos: list[dict], args: argparse.Namespace) -> None:
    """Reproduce los eventos en orden, con el reloj comprimido por
    --acelerar: el mismo principio que el simulador real (`_programa()`), pero
    en una sola conexion secuencial -aqui no hace falta modelar 652 sensores
    concurrentes, solo publicar una muestra con unas pocas anomalias dentro.
    """
    cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=args.client_id)
    cliente.connect(args.broker_host, args.broker_port)
    cliente.loop_start()

    # El unico invalido con timestamp=None (construir() los ordena al final,
    # True > False en la clave de sort) no tiene marca con la que calcular su
    # instante: se publica sin espera, en cuanto le toca el turno.
    con_marca = [e for e in eventos if e["timestamp"] is not None]
    t_sim0 = con_marca[0]["timestamp"] if con_marca else None

    publicados, invalidos_publicados = 0, 0
    inicio = time.monotonic()
    for evento in eventos:
        if evento["timestamp"] is not None and t_sim0 is not None:
            from datetime import datetime
            delta = (datetime.fromisoformat(evento["timestamp"])
                    - datetime.fromisoformat(t_sim0)).total_seconds()
            objetivo = delta / args.acelerar
            espera = inicio + objetivo - time.monotonic()
            if espera > 0:
                time.sleep(espera)

        origen = evento.pop("_origen", None)
        publicable = dict(evento)
        # building_id puede faltar (ese es justo uno de los seis casos); solo
        # se usa para el topico si esta presente, con un valor de repuesto si no.
        topico = TOPIC_TEMPLATE.format(
            building_id=publicable.get("building_id", "sin-building-id"),
            meter_type=publicable.get("meter_type", "sin-meter-type"))
        publicable["sim_publish_ts"] = int(time.time() * 1000)

        cliente.publish(topico, payload=json.dumps(publicable), qos=args.qos)
        publicados += 1
        if origen:
            invalidos_publicados += 1
            logger.info("  [invalido %s] %s", origen, topico)

    time.sleep(2)  # margen para que paho vacie el buffer de salida
    cliente.loop_stop()
    cliente.disconnect()
    logger.info("Publicados %d eventos (%d invalidos) en %.1f s",
               publicados, invalidos_publicados, time.monotonic() - inicio)


# --------------------------------------------------------------------------
def run(args: argparse.Namespace) -> int:
    eventos = construir(args)
    guardar(eventos)
    publicar(eventos, args)
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
