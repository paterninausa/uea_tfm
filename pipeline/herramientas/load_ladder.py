"""
Escalera de carga del Objetivo 5: degradacion al crecer el numero de sensores.

El objetivo pide ">= 500 sensores concurrentes con degradacion de throughput
< 20% respecto a una carga base de 100". Esta herramienta ejecuta esa escalera
invocando al simulador una vez por peldano, con el MISMO `--speedup` y distinto
`--max-sensors`.

POR QUE EL SPEEDUP SE MANTIENE Y LA TASA NO. Con una tasa global fija, 100 y 652
sensores publican los mismos eventos por segundo: se reparte la misma carga entre
mas identidades y no se escala nada —de ahi salia la degradacion del 0,6% que no
significaba gran cosa—. `--speedup` fija la cadencia POR SENSOR, asi que la carga
total crece con el numero de contadores, que es lo que dice el objetivo:

    tasa agregada = n_sensores x speedup / 3600

POR QUE SE MIDE EN EL CONSUMO Y NO EN EL PRODUCTOR. El PUBACK de Mosquitto
significa "aceptado", no "entregado al pipeline": se midio al broker confirmando
a 7.324 ev/s mientras el bridge llevaba consumidos 8.230 de 40.000, con el resto
encolado y la latencia MQTT->Kafka disparada de 2 ms a 683 ms. Un peldano que
solo mire lo que el productor consigue publicar dara siempre una cifra
optimista. Aqui cada peldano se mide con lo que llega al final del recorrido:
mensajes en Kafka, filas en PostgreSQL y duracion de micro-lote de Spark.

SE REPITE EL PRIMER PELDANO AL FINAL, EN CALIENTE, y no es opcional: la primera
version de esta escalera, ejecutada solo en orden creciente, concluyo que el
throughput MEJORA al anadir sensores (1.062 -> 1.332 ev/s). El sesgo era de
calentamiento y solo se ve repitiendo el peldano base cuando el sistema ya lleva
rato en marcha.

Requiere el stack levantado y el bridge y el job de Spark en marcha.

Uso:
    python load_ladder.py --ladder 100,250,500,652 --speedup 2000
"""

import argparse
import json
import logging
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
from common.logging_setup import DIRECTORIO_LOGS, configurar_logging  # noqa: E402
from common.proceso import evento_de_parada  # noqa: E402

logger = logging.getLogger("load_ladder")

RAIZ = Path(__file__).resolve().parents[1]
SIMULADOR = RAIZ / "simulator" / "mqtt_simulator.py"
RESULTADOS = DIRECTORIO_LOGS / "ultima_escalera.json"

CONTENEDOR_KAFKA = "tfm-kafka"
PROCESOS_NECESARIOS = ("mqtt_kafka_bridge.py", "telemetry_streaming.py")


def procesos_ausentes() -> list[str]:
    faltan = []
    for nombre in PROCESOS_NECESARIOS:
        if subprocess.run(["pgrep", "-f", nombre], capture_output=True).returncode != 0:
            faltan.append(nombre)
    return faltan


def mensajes_en_kafka(topico: str) -> int:
    r = subprocess.run(
        ["docker", "exec", CONTENEDOR_KAFKA, "/opt/kafka/bin/kafka-get-offsets.sh",
         "--bootstrap-server", "kafka:9092", "--topic", topico, "--time", "-1"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"kafka-get-offsets.sh fallo: {r.stderr.strip()}")
    return sum(int(l.rsplit(":", 1)[1]) for l in r.stdout.splitlines() if ":" in l)


def contar(props: dict, tabla: str) -> int:
    import psycopg2

    conn = psycopg2.connect(**props)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {tabla}")
            return cur.fetchone()[0]
    finally:
        conn.close()


def micro_lote_p95(props_ts: dict, desde: str) -> float | None:
    """p95 de la duracion de micro-lote registrada durante el peldano."""
    import psycopg2

    conn = psycopg2.connect(**props_ts)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)
                FROM streaming_progress
                WHERE trigger_ts >= %s AND num_input_rows > 0
            """, (desde,))
            return cur.fetchone()[0]
    finally:
        conn.close()


def esperar_drenaje(props_pg: dict, quietud: float, timeout: float, parada) -> int:
    """Espera a que las filas en PostgreSQL dejen de crecer.

    Sin esto, el recuento de un peldano se solaparia con el siguiente: cuando el
    simulador termina, al pipeline todavia le quedan mensajes en vuelo, y
    atribuirlos al peldano equivocado deforma justo la comparacion que se busca.
    """
    limite = time.monotonic() + timeout
    ultimo, estable_desde = -1, None
    while time.monotonic() < limite and not parada.is_set():
        actual = contar(props_pg, "telemetry_events")
        if actual == ultimo:
            if estable_desde is None:
                estable_desde = time.monotonic()
            elif time.monotonic() - estable_desde >= quietud:
                return actual
        else:
            ultimo, estable_desde = actual, None
        time.sleep(2)
    return contar(props_pg, "telemetry_events")


def ejecutar_peldano(args, sensores: int, etiqueta: str, parada) -> dict:
    props_pg = props_bd(args, POSTGRES)
    props_ts = props_bd(args, TIMESCALE)

    kafka_antes = mensajes_en_kafka(args.topic)
    filas_antes = contar(props_pg, "telemetry_events")
    marca = time.strftime("%Y-%m-%d %H:%M:%S")
    tasa_teorica = sensores * args.speedup / 3600.0

    logger.info("[%s] %d sensores | speedup x%s | tasa teorica %.1f ev/s",
                etiqueta, sensores, f"{args.speedup:,.0f}", tasa_teorica)

    t0 = time.monotonic()
    orden = [sys.executable, str(SIMULADOR),
             "--speedup", str(args.speedup),
             "--max-sensors", str(sensores),
             "--limit", str(args.events_per_step),
             "--rebase-end", "now"]
    completado = subprocess.run(orden)
    duracion_productor = time.monotonic() - t0

    filas_despues = esperar_drenaje(props_pg, args.quietud, args.timeout, parada)
    kafka_despues = mensajes_en_kafka(args.topic)
    dlq = mensajes_en_kafka(args.dlq_topic)

    entregados = kafka_despues - kafka_antes
    persistidos = filas_despues - filas_antes
    resultado = {
        "etiqueta": etiqueta,
        "sensores": sensores,
        "speedup": args.speedup,
        "tasa_teorica_ev_s": round(tasa_teorica, 1),
        # El simulador devuelve 1 si no sostuvo su propia agenda. En ese caso el
        # peldano mide la maquina del productor y no el pipeline, y hay que
        # decirlo en vez de publicar la cifra.
        "productor_sostuvo_el_ritmo": completado.returncode == 0,
        "duracion_productor_s": round(duracion_productor, 1),
        "entregados_a_kafka": entregados,
        "persistidos_en_postgresql": persistidos,
        "en_dlq": dlq,
        "throughput_entregado_ev_s": round(entregados / max(1e-9, duracion_productor), 1),
        "micro_lote_p95_ms": micro_lote_p95(props_ts, marca),
    }
    logger.info("[%s] entregados a Kafka %s | persistidos %s | %.1f ev/s | micro-lote p95 %s ms",
                etiqueta, f"{entregados:,}", f"{persistidos:,}",
                resultado["throughput_entregado_ev_s"], resultado["micro_lote_p95_ms"])
    if not resultado["productor_sostuvo_el_ritmo"]:
        logger.warning("[%s] EL SIMULADOR NO SOSTUVO EL RITMO: este peldano no es valido "
                       "como medida del pipeline", etiqueta)
    return resultado


def run(args: argparse.Namespace) -> int:
    parada = evento_de_parada("escalera")

    faltan = procesos_ausentes()
    if faltan:
        logger.error("La escalera mide el recorrido completo; faltan por arrancar:")
        for f in faltan:
            logger.error("  - %s", f)
        return 1

    peldanos = [int(x) for x in args.ladder.split(",")]
    resultados = []
    for n in peldanos:
        if parada.is_set():
            break
        resultados.append(ejecutar_peldano(args, n, f"{n}-sensores", parada))

    if not parada.is_set() and len(peldanos) > 1:
        logger.info("Repitiendo el peldano base en caliente, para descontar el "
                    "sesgo de calentamiento")
        resultados.append(ejecutar_peldano(
            args, peldanos[0], f"{peldanos[0]}-sensores-caliente", parada))

    _resumen(resultados)
    RESULTADOS.write_text(json.dumps(
        {"instante": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
         "speedup": args.speedup,
         "eventos_por_peldano": args.events_per_step,
         "peldanos": resultados}, indent=2))
    logger.info("Resultados en %s (los lee kpi_report.py)", RESULTADOS)
    return 0


def _resumen(resultados: list[dict]) -> None:
    logger.info("--- Escalera de carga (medida en el consumo) ---")
    logger.info("  %-26s %10s %14s %14s %12s", "peldano", "sensores",
                "entregado ev/s", "persistidos", "lote p95")
    for r in resultados:
        logger.info("  %-26s %10s %14s %14s %12s", r["etiqueta"], r["sensores"],
                    f"{r['throughput_entregado_ev_s']:,.1f}",
                    f"{r['persistidos_en_postgresql']:,}", r["micro_lote_p95_ms"])

    calientes = [r for r in resultados if r["etiqueta"].endswith("-caliente")]
    otros = [r for r in resultados if not r["etiqueta"].endswith("-caliente")]
    if calientes and otros:
        base = calientes[-1]["throughput_entregado_ev_s"]
        mayor = max(otros, key=lambda r: r["sensores"])
        if base:
            degradacion = (base - mayor["throughput_entregado_ev_s"]) / base * 100
            logger.info("  Degradacion %d -> %d sensores, ambos en caliente: %.1f%% "
                        "(objetivo < 20%%)", calientes[-1]["sensores"],
                        mayor["sensores"], degradacion)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Escalera de carga del Objetivo 5 (TFM)")
    anadir_argumentos_bd(p)
    anadir_argumentos_kafka(p)
    p.add_argument("--ladder", default="100,250,500,652", metavar="N,N,N",
                   help="Sensores de cada peldano")
    p.add_argument("--speedup", type=float, default=2000.0,
                   help="Aceleracion del reloj, IDENTICA en todos los peldanos: es lo que "
                        "hace que la carga crezca con el numero de sensores")
    p.add_argument("--events-per-step", type=int, default=20000,
                   help="Eventos publicados en cada peldano")
    p.add_argument("--quietud", type=float, default=10.0,
                   help="Segundos sin filas nuevas para dar por drenado un peldano")
    p.add_argument("--timeout", type=float, default=300.0,
                   help="Espera maxima al drenaje de un peldano")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("load_ladder")
    try:
        sys.exit(run(parse_args()))
    except (RuntimeError, OSError) as exc:
        logger.error("%s", exc)
        sys.exit(1)
