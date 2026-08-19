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
from common.connection_args import (  # noqa: E402
    POSTGRES,
    TIMESCALE,
    anadir_argumentos_bd,
    anadir_argumentos_kafka,
    props_bd,
)
from common.logging_setup import DIRECTORIO_LOGS, configurar_logging  # noqa: E402
from common.stop_event import evento_de_parada  # noqa: E402

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


def marca_bd(props: dict):
    """Instante actual SEGUN EL SERVIDOR de base de datos.

    No vale `time.strftime()` del proceso: los contenedores corren en UTC y los
    scripts en la hora local de la maquina, asi que una marca local usada como
    filtro sobre `ingested_at` se interpreta como UTC y cae dos horas en el
    futuro. El sintoma no es un error, sino percentiles a NULL, que es facil
    confundir con "no hubo trafico". Pidiendole la hora al servidor que escribe
    la columna, la comparacion es entre relojes que ya coinciden.
    """
    import psycopg2

    conn = psycopg2.connect(**props)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT now()")
            return cur.fetchone()[0]
    finally:
        conn.close()


def latencia_ingesta(props_pg: dict, desde) -> tuple[float | None, float | None]:
    """Latencia p50/p95 de las filas escritas durante el peldano.

    ES LA SENAL QUE DELATA LA SATURACION ANTES QUE NINGUNA OTRA. El recuento de
    entregados sigue cuadrando mientras el sistema aguanta la cola: lo que se
    hunde primero no es cuanto llega, sino cuanto tarda en llegar. Un peldano que
    entrega el 100% con la latencia disparada ya esta pasado de vueltas.
    """
    import psycopg2

    conn = psycopg2.connect(**props_pg)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - sim_publish_ts))),
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - sim_publish_ts)))
                FROM telemetry_events WHERE ingested_at >= %s
            """, (desde,))
            p50, p95 = cur.fetchone()
            return (float(p50) if p50 is not None else None,
                    float(p95) if p95 is not None else None)
    finally:
        conn.close()


def ritmo_publicacion(props_pg: dict, desde) -> tuple[int, float | None]:
    """Filas del peldano y ritmo REAL al que se publicaron, en ev/s.

    Se calcula con `sim_publish_ts`, la marca que el propio evento lleva desde
    que sale del sensor: el intervalo entre la primera y la ultima publicacion
    del peldano. Dividir por el tiempo de pared del subproceso mide otra cosa,
    porque ese tiempo incluye el arranque del simulador —leer 5,68 millones de
    filas y abrir 652 conexiones, unos 4 s— y en un peldano rapido, que dura 7 s
    de publicacion, ese arranque se come el 36% del resultado. El sesgo es
    ademas creciente segun sube el ritmo, justo donde se busca el codo.
    """
    import psycopg2

    conn = psycopg2.connect(**props_pg)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*),
                       EXTRACT(EPOCH FROM (max(sim_publish_ts) - min(sim_publish_ts)))
                FROM telemetry_events WHERE ingested_at >= %s
            """, (desde,))
            filas, intervalo = cur.fetchone()
            # float() explicito: EXTRACT(EPOCH ...) devuelve NUMERIC, psycopg2 lo
            # convierte en Decimal y json.dumps no sabe serializarlo. El sintoma
            # aparece al final de la ejecucion, con toda la medicion ya hecha y
            # perdida.
            return filas, (filas / float(intervalo) if intervalo else None)
    finally:
        conn.close()


def micro_lote_p95(props_ts: dict, desde) -> float | None:
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
            valor = cur.fetchone()[0]
            return float(valor) if valor is not None else None
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


def ejecutar_peldano(args, sensores: int, speedup: float, etiqueta: str, parada) -> dict:
    props_pg = props_bd(args, POSTGRES)
    props_ts = props_bd(args, TIMESCALE)

    kafka_antes = mensajes_en_kafka(args.topic)
    filas_antes = contar(props_pg, "telemetry_events")
    marca_pg = marca_bd(props_pg)
    marca_ts = marca_bd(props_ts)
    tasa_teorica = sensores * speedup / 3600.0

    logger.info("[%s] %d sensores | speedup x%s | tasa teorica %.1f ev/s",
                etiqueta, sensores, f"{speedup:,.0f}", tasa_teorica)

    t0 = time.monotonic()
    orden = [sys.executable, str(SIMULADOR),
             "--speedup", str(speedup),
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
        "speedup": speedup,
        "tasa_teorica_ev_s": round(tasa_teorica, 1),
        # El codigo de salida del simulador solo dice si hubo ALGUN pico de
        # retraso por encima de --max-lag, y eso marcaba como invalido un
        # peldano que acabo publicando el 99,3% de lo pedido. Se guarda como
        # dato, pero quien decide es la proporcion entre lo publicado y lo
        # pedido, que es lo que de verdad significa "sostener el ritmo".
        "productor_sin_picos_de_retraso": completado.returncode == 0,
        "duracion_productor_s": round(duracion_productor, 1),
        "entregados_a_kafka": entregados,
        "persistidos_en_postgresql": persistidos,
        "en_dlq": dlq,
        "throughput_entregado_ev_s": round(entregados / max(1e-9, duracion_productor), 1),
        "micro_lote_p95_ms": micro_lote_p95(props_ts, marca_ts),
    }
    _filas, ritmo = ritmo_publicacion(props_pg, marca_pg)
    resultado["ritmo_publicado_ev_s"] = round(ritmo, 1) if ritmo else None
    cumplido = (ritmo / tasa_teorica) if (ritmo and tasa_teorica) else 0
    resultado["fraccion_del_ritmo_pedido"] = round(cumplido, 3)
    resultado["productor_sostuvo_el_ritmo"] = cumplido >= 0.95
    lat_p50, lat_p95 = latencia_ingesta(props_pg, marca_pg)
    resultado["latencia_p50_s"] = round(lat_p50, 3) if lat_p50 else None
    resultado["latencia_p95_s"] = round(lat_p95, 3) if lat_p95 else None
    logger.info("[%s] entregados %s | persistidos %s | ritmo real %s ev/s | "
                "latencia p95 %s s | micro-lote p95 %s ms", etiqueta, f"{entregados:,}",
                f"{persistidos:,}", resultado["ritmo_publicado_ev_s"],
                resultado["latencia_p95_s"], resultado["micro_lote_p95_ms"])
    if not resultado["productor_sostuvo_el_ritmo"]:
        logger.warning("[%s] EL SIMULADOR SOLO PUBLICO EL %.0f%% DEL RITMO PEDIDO: este "
                       "peldano mide su techo, no el del pipeline",
                       etiqueta, cumplido * 100)
    return resultado


def run(args: argparse.Namespace) -> int:
    parada = evento_de_parada("escalera")

    faltan = procesos_ausentes()
    if faltan:
        logger.error("La escalera mide el recorrido completo; faltan por arrancar:")
        for f in faltan:
            logger.error("  - %s", f)
        return 1

    # Dos experimentos distintos con la misma mecanica, y conviene no
    # confundirlos:
    #
    #   --ladder    mismo ritmo, MAS SENSORES  -> degradacion del Objetivo 5
    #   --speedups  mismos sensores, MAS RITMO -> punto de saturacion
    #
    # El primero responde a ">= 500 sensores concurrentes con degradacion < 20%".
    # El segundo a "hasta donde aguanta", que es otra pregunta: anadir sensores a
    # ritmo fijo sube la carga solo hasta el numero de contadores que existen;
    # para pasar de ahi hay que acelerar el reloj.
    if args.speedups:
        fijos = args.max_sensors or 652
        peldanos = [(fijos, float(x)) for x in args.speedups.split(",")]
        nombrar = lambda sensores, ritmo: f"x{ritmo:,.0f}"
    else:
        peldanos = [(int(x), args.speedup) for x in args.ladder.split(",")]
        nombrar = lambda sensores, ritmo: f"{sensores}-sensores"

    resultados = []
    for sensores, ritmo in peldanos:
        if parada.is_set():
            break
        resultados.append(ejecutar_peldano(
            args, sensores, ritmo, nombrar(sensores, ritmo), parada))

    if not parada.is_set() and len(peldanos) > 1:
        logger.info("Repitiendo el peldano base en caliente, para descontar el "
                    "sesgo de calentamiento")
        sensores, ritmo = peldanos[0]
        resultados.append(ejecutar_peldano(
            args, sensores, ritmo, nombrar(sensores, ritmo) + "-caliente", parada))

    _resumen(resultados, bool(args.speedups))
    RESULTADOS.write_text(json.dumps(
        {"instante": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
         "modo": "saturacion" if args.speedups else "degradacion",
         "eventos_por_peldano": args.events_per_step,
         "peldanos": resultados}, indent=2))
    logger.info("Resultados en %s (los lee kpi_report.py)", RESULTADOS)
    return 0


def _resumen(resultados: list[dict], modo_saturacion: bool) -> None:
    logger.info("--- Peldanos (medidos en el consumo) ---")
    cab = "  %-18s %8s %9s %10s %11s %12s %11s %9s"
    logger.info(cab, "peldano", "sensores", "speedup", "pedido", "publicado",
                "persistidos", "latencia p95", "lote p95")
    for r in resultados:
        logger.info(cab, r["etiqueta"], r["sensores"], f"x{r['speedup']:,.0f}",
                    f"{r['tasa_teorica_ev_s']:,.0f}", f"{r['ritmo_publicado_ev_s']:,.1f}",
                    f"{r['persistidos_en_postgresql']:,}",
                    r["latencia_p95_s"], r["micro_lote_p95_ms"])

    calientes = [r for r in resultados if r["etiqueta"].endswith("-caliente")]
    otros = [r for r in resultados if not r["etiqueta"].endswith("-caliente")]

    # La degradacion solo significa algo cuando lo que crece son los sensores.
    # En la rampa de saturacion crece el ritmo, asi que comparar el peldano base
    # con el mayor da un numero enorme y sin sentido: es la carga la que ha
    # cambiado, no el rendimiento.
    if calientes and otros and not modo_saturacion:
        base = calientes[-1]["ritmo_publicado_ev_s"]
        mayor = max(otros, key=lambda r: r["sensores"])
        if base:
            degradacion = (base - (mayor["ritmo_publicado_ev_s"] or 0)) / base * 100
            logger.info("  Degradacion %d -> %d sensores, ambos en caliente: %.1f%% "
                        "(objetivo < 20%%)", calientes[-1]["sensores"],
                        mayor["sensores"], degradacion)

    perdidos = [r for r in otros if r["entregados_a_kafka"] != r["persistidos_en_postgresql"]]
    if not perdidos:
        logger.info("  Sin perdida en ningun peldano: entregado y persistido coinciden")

    # El codo se reconoce por lo que deja de cuadrar. Se distingue de quien lo
    # provoca: si el productor no sostiene su agenda, el techo es SUYO y el
    # pipeline ni se ha despeinado.
    for previo, actual in zip(otros, otros[1:]):
        if not actual["productor_sostuvo_el_ritmo"]:
            logger.warning("  TECHO DEL PRODUCTOR en %s: el simulador no sostiene el ritmo "
                           "pedido, asi que este peldano mide el productor y no el pipeline",
                           actual["etiqueta"])
            break
        lat_previa = previo["latencia_p95_s"] or 0
        lat_actual = actual["latencia_p95_s"] or 0
        if lat_previa and lat_actual > lat_previa * 3:
            logger.warning("  CODO DEL PIPELINE en %s: la latencia p95 se multiplica por "
                           "%.1f (%.2f -> %.2f s)", actual["etiqueta"],
                           lat_actual / lat_previa, lat_previa, lat_actual)
            break


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Escalera de carga del Objetivo 5 (TFM)")
    anadir_argumentos_bd(p)
    anadir_argumentos_kafka(p)
    p.add_argument("--ladder", default="100,250,500,652", metavar="N,N,N",
                   help="Sensores de cada peldano, con el ritmo fijo de --speedup. "
                        "Es la escalera de degradacion del Objetivo 5")
    p.add_argument("--speedups", default=None, metavar="X,X,X",
                   help="Ritmos de cada peldano, con los sensores fijos de --max-sensors. "
                        "Es la rampa que busca el punto de saturacion. Excluye a --ladder")
    p.add_argument("--max-sensors", type=int, default=None,
                   help="Sensores a usar en la rampa de --speedups (por defecto, los 652)")
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
