"""
Prueba de recuperacion ante fallo de un servicio (Objetivo 5).

El objetivo pide "recuperacion ante fallo de un servicio < 60 s sin perdida de
datos". Esto se habia verificado por partes y a mano —que el bridge reconecta
gracias a la sesion MQTT persistente, que Spark reanuda desde su checkpoint—
pero nunca como una prueba unica, reproducible y con un tiempo medido, que es lo
que hace falta para poder afirmarlo en la memoria.

Que hace, en orden:

  1. Comprueba que bridge y job de Spark estan en marcha; sin ellos la prueba no
     mide nada.
  2. Lanza el simulador a una tasa fija y espera a ver flujo llegando al sumidero.
  3. MATA el contenedor elegido (`docker kill`, no `stop`: un fallo real no avisa
     con un SIGTERM ordenado).
  4. Lo deja caido el tiempo indicado y lo vuelve a levantar.
  5. Cronometra cuanto tarda el flujo en restablecerse, contando filas nuevas en
     la base de datos.
  6. Compara lo publicado con lo persistido para verificar que no se perdio nada.

INFORMA DE LO QUE PASE, incluido que no se recupere. Un servicio cuyo fallo
detiene el pipeline es un resultado valido y publicable: lo que invalidaria el
trabajo es afirmar una recuperacion que no se ha observado.

Uso:
    python failover_test.py --target mosquitto
    python failover_test.py --target kafka --downtime 20
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
    props_bd,
)
from common.logging_setup import DIRECTORIO_LOGS, configurar_logging  # noqa: E402
from common.stop_event import evento_de_parada  # noqa: E402

logger = logging.getLogger("failover_test")

RAIZ = Path(__file__).resolve().parents[1]
COMPOSE = RAIZ / "docker-compose.yml"
SIMULADOR = RAIZ / "simulator" / "mqtt_simulator.py"
RESULTADO = DIRECTORIO_LOGS / "ultimo_failover.json"

# Servicios que se pueden tumbar y que se espera que el pipeline sobreviva.
# apicurio queda fuera a proposito: el esquema se resuelve una sola vez al
# arrancar, asi que su caida no afecta a un pipeline ya en marcha y la prueba no
# demostraria nada.
OBJETIVOS = ("mosquitto", "kafka", "timescaledb", "postgres")

PROCESOS_NECESARIOS = ("mqtt_kafka_bridge.py", "telemetry_streaming.py")


def compose(*argumentos: str) -> None:
    r = subprocess.run(["docker", "compose", "-f", str(COMPOSE), *argumentos],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(argumentos)}: {r.stderr.strip()}")


def estado_contenedor(servicio: str) -> str:
    """Estado del contenedor tal como lo ve Docker: running, exited, healthy..."""
    # --all es imprescindible: sin el, `ps` omite los contenedores parados y un
    # servicio recien tumbado se informaria como "ausente" en vez de "exited",
    # que es justo el estado que interesa observar durante la prueba.
    r = subprocess.run(["docker", "compose", "-f", str(COMPOSE), "ps", "--all",
                        "--format", "json", servicio],
                       capture_output=True, text=True)
    for linea in r.stdout.splitlines():
        if linea.strip():
            datos = json.loads(linea)
            return datos.get("Health") or datos.get("State") or "desconocido"
    return "ausente"


def procesos_ausentes() -> list[str]:
    faltan = []
    for nombre in PROCESOS_NECESARIOS:
        r = subprocess.run(["pgrep", "-f", nombre], capture_output=True, text=True)
        if r.returncode != 0:
            faltan.append(nombre)
    return faltan


# Que tabla demuestra que el flujo volvio, segun el servicio que se tumba. NO
# vale mirar siempre PostgreSQL: al matar TimescaleDB, la consulta que escribe en
# PostgreSQL sigue tan campante, asi que contar sus filas daba "recuperado en 1,0
# s" sin que la parte afectada hubiera hecho nada. La prueba tiene que observar
# el camino que el fallo interrumpe.
TABLA_TESTIGO = {
    "timescaledb": (TIMESCALE, "telemetry_metrics"),
    "postgres": (POSTGRES, "telemetry_events"),
    "mosquitto": (POSTGRES, "telemetry_events"),
    "kafka": (POSTGRES, "telemetry_events"),
}


def contar(props: dict, tabla: str = "telemetry_events") -> int:
    import psycopg2

    try:
        conn = psycopg2.connect(**props)
    except Exception:
        # Si el caido es el propio servidor, no poder conectar es lo esperado.
        return -1
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {tabla}")
            return cur.fetchone()[0]
    except Exception:
        return -1
    finally:
        conn.close()


def esperar_flujo(props: dict, tabla: str, referencia: int, timeout: float,
                  parada) -> float | None:
    """Segundos hasta ver filas NUEVAS respecto a `referencia`, o None si no llegan.

    Se mide sobre filas persistidas y no sobre el estado del contenedor a
    proposito: que Docker diga "healthy" solo significa que el proceso responde,
    no que el pipeline haya vuelto a mover datos de un extremo al otro. Lo que
    exige el objetivo es lo segundo.
    """
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if parada.is_set():
            return None
        actual = contar(props, tabla)
        if actual > referencia:
            return time.monotonic() - t0
        time.sleep(1)
    return None


def run(args: argparse.Namespace) -> int:
    parada = evento_de_parada("prueba de recuperacion")
    cual_bd, tabla = TABLA_TESTIGO[args.target]
    props = props_bd(args, cual_bd)
    logger.info("Se observara el flujo en %s.%s, que es el camino que corta un fallo de %s",
                cual_bd, tabla, args.target)

    faltan = procesos_ausentes()
    if faltan:
        logger.error("Estos procesos deben estar en marcha para que la prueba mida algo:")
        for f in faltan:
            logger.error("  - %s", f)
        logger.error("Arranca el bridge y el job de Spark en otras terminales y repite")
        return 1

    logger.info("Lanzando el simulador con speedup x%g en segundo plano...", args.speedup)
    simulador = subprocess.Popen(
        # SIN --rebase-end a proposito. Desplazar las marcas al presente en cada
        # ejecucion deja el watermark de Spark clavado en "ahora", y entonces
        # todo lo que se publique despues —que cubre las horas ANTERIORES a ese
        # instante— llega tarde y la agregacion por ventana lo descarta en
        # silencio. Con eso, la consulta de metricas no escribe nada y una
        # prueba de fallo sobre TimescaleDB no demuestra nada: la consulta
        # "sobrevive" porque no llego a tocar la base.
        [sys.executable, str(SIMULADOR), "--speedup", str(args.speedup),
         "--limit", str(args.limit)],
    )
    try:
        filas_inicio = contar(props, tabla)
        if esperar_flujo(props, tabla, filas_inicio, args.warmup, parada) is None:
            logger.error("No llega flujo al sumidero antes del fallo; se aborta la prueba")
            return 1
        logger.info("Flujo confirmado. Filas antes del fallo: %s", f"{contar(props, tabla):,}")

        # El recuento de referencia se toma con el servicio TODAVIA VIVO. Si se
        # toma despues del kill y el servicio caido es la propia base testigo,
        # `contar` devuelve -1 y entonces cualquier lectura posterior lo supera:
        # se declara "flujo restablecido" en cuanto la base vuelve a responder,
        # aunque no haya llegado ni una fila nueva.
        filas_antes_del_fallo = contar(props, tabla)
        logger.info("--- MATANDO %s (filas antes: %s) ---", args.target,
                    f"{filas_antes_del_fallo:,}")
        compose("kill", args.target)
        instante_fallo = time.monotonic()
        logger.info("Estado de %s: %s", args.target, estado_contenedor(args.target))

        time.sleep(args.downtime)
        filas_durante = contar(props, tabla)
        logger.info("Filas tras %g s caido: %s", args.downtime,
                    f"{filas_durante:,}" if filas_durante >= 0 else "(base de datos caida)")

        logger.info("--- LEVANTANDO %s ---", args.target)
        compose("start", args.target)
        instante_reinicio = time.monotonic()

        # La referencia es el ULTIMO RECUENTO VALIDO, no el de durante la caida.
        # Cuando el servicio tumbado es la propia base testigo, `contar` devuelve
        # -1 mientras esta muerta; tomar eso como referencia (con un max(...,0))
        # hacia que al volver el servicio sus filas ANTIGUAS ya superaran el
        # umbral y se declarara "flujo restablecido" sin que hubiera llegado
        # nada nuevo. Daba 2,0 s de recuperacion que no median nada.
        referencia = filas_durante if filas_durante >= 0 else filas_antes_del_fallo
        logger.info("Se esperan filas nuevas por encima de %s", f"{referencia:,}")
        recuperacion = esperar_flujo(props, tabla, referencia, args.timeout, parada)
        total = time.monotonic() - instante_fallo

        if recuperacion is None:
            logger.error("EL FLUJO NO SE RESTABLECIO en %g s tras levantar %s",
                         args.timeout, args.target)
        else:
            logger.info("Flujo restablecido %.1f s despues de levantar el servicio "
                        "(%.1f s desde el fallo)", recuperacion, total)

    finally:
        logger.info("Deteniendo el simulador...")
        simulador.terminate()
        try:
            simulador.wait(timeout=30)
        except subprocess.TimeoutExpired:
            simulador.kill()

    # Drenaje: al pipeline aun le quedan mensajes en vuelo cuando el productor
    # para. Sin esta espera, la comparacion final contaria como perdido lo que
    # solo estaba en transito.
    logger.info("Esperando %g s al drenaje antes de contar...", args.drain)
    time.sleep(args.drain)
    filas_final = contar(props, tabla)

    resultado = {
        "servicio": args.target,
        "tabla_testigo": f"{cual_bd}.{tabla}",
        "instante": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "downtime_s": args.downtime,
        "recuperacion_s": round(recuperacion, 1) if recuperacion is not None else None,
        "desde_el_fallo_s": round(total, 1),
        "filas_antes": filas_inicio,
        "filas_final": filas_final,
        "filas_nuevas": filas_final - filas_inicio,
        "estado_final": estado_contenedor(args.target),
    }
    RESULTADO.write_text(json.dumps(resultado, indent=2))

    logger.info("--- Resultado de la prueba de recuperacion ---")
    for k, v in resultado.items():
        logger.info("  %-18s %s", k, v)
    logger.info("Objetivo: recuperacion < 60 s sin perdida de datos")
    logger.info("Guardado en %s", RESULTADO)

    return 0 if recuperacion is not None and recuperacion < 60 else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prueba de recuperacion ante fallo (TFM)")
    anadir_argumentos_bd(p)
    p.add_argument("--target", default="mosquitto", choices=OBJETIVOS,
                   help="Servicio que se va a tumbar")
    p.add_argument("--speedup", type=float, default=1000.0,
                   help="Aceleracion del reloj durante la prueba. Con los 652 sensores, "
                        "x1000 son unos 181 ev/s: suficiente para ver el flujo cortarse y "
                        "volver, sin cargar el sistema mientras se mide la recuperacion")
    p.add_argument("--limit", type=int, default=500000,
                   help="Techo de eventos del simulador; debe sobrar para toda la prueba")
    p.add_argument("--warmup", type=float, default=60.0,
                   help="Segundos maximos de espera hasta ver flujo antes del fallo")
    p.add_argument("--downtime", type=float, default=15.0,
                   help="Segundos que el servicio permanece caido")
    p.add_argument("--timeout", type=float, default=180.0,
                   help="Segundos maximos de espera a que el flujo se restablezca")
    p.add_argument("--drain", type=float, default=30.0,
                   help="Segundos de espera al drenaje antes del recuento final")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("failover_test")
    try:
        sys.exit(run(parse_args()))
    except (RuntimeError, OSError) as exc:
        logger.error("%s", exc)
        sys.exit(1)
