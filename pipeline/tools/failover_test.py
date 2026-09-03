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
  3. Provoca el fallo del contenedor elegido, de una de dos formas (`--fallo`):
       kill (por defecto): `docker kill` + reinicio manual con `docker compose
         start`. Es el PEOR CASO: `docker kill` no dispara `restart:
         unless-stopped`, asi que alguien tiene que levantar el servicio.
       oom: baja el limite de memoria hasta forzar un OOM-kill del proceso
         principal. Un OOM SI es un fallo genuino, asi que `restart:
         unless-stopped` rearranca el contenedor SOLO, sin intervencion.
  4. (solo kill) Lo deja caido el tiempo indicado y lo vuelve a levantar.
  5. Cronometra cuanto tarda el flujo en restablecerse contando filas nuevas en
     la base de datos: con kill, desde la orden de reinicio (con el arranque del
     contenedor incluido); con oom, desde el propio OOM.
  6. Compara lo publicado en Kafka (offsets del topico raw) con lo persistido en
     telemetry_events para medir la tasa de perdida.

INFORMA DE LO QUE PASE, incluido que no se recupere. Un servicio cuyo fallo
detiene el pipeline es un resultado valido y publicable: lo que invalidaria el
trabajo es afirmar una recuperacion que no se ha observado.

Uso:
    python failover_test.py --target mosquitto
    python failover_test.py --target postgres --fallo oom
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
    TOPIC_RAW,
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
CONTENEDOR_KAFKA = "tfm-kafka"

# Servicios que se pueden tumbar y que se espera que el pipeline sobreviva.
# apicurio y register-schema quedan fuera a proposito: el esquema se resuelve una
# sola vez al arrancar, asi que su caida no afecta a un pipeline ya en marcha.
OBJETIVOS = ("mosquitto", "kafka", "timescaledb", "postgres", "bridge")

# Piezas que deben estar en marcha para que la prueba mida algo. El job de Spark
# corre en el host (pgrep); el bridge es un contenedor de Compose.
PROCESOS_NECESARIOS = ("stream_processing.py",)
CONTENEDORES_NECESARIOS = ("bridge",)


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


def contenedor_id(servicio: str) -> str:
    r = subprocess.run(["docker", "compose", "-f", str(COMPOSE), "ps", "-q", servicio],
                       capture_output=True, text=True)
    cid = r.stdout.strip()
    if not cid:
        raise RuntimeError(f"no se encuentra el contenedor del servicio '{servicio}'")
    return cid


def _update_memoria(cid: str, valor: str, reintentos: int = 50) -> bool:
    """`docker update --memory`, con reintentos: puede rechazar el contenedor
    mientras esta entre intentos de la politica de reinicio."""
    for _ in range(reintentos):
        r = subprocess.run(
            ["docker", "update", "--memory", valor, "--memory-swap", valor, cid],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            return True
        time.sleep(0.3)
    return False


# Por debajo del RSS de todos los servicios salvo Mosquitto (que usa ~3 MiB, por
# debajo del minimo de 6 MB que admite Docker). Fuerza el OOM-kill del proceso 1.
MEM_OOM = "6m"


def forzar_oom(cid: str, timeout: float = 30.0) -> bool:
    """Baja el limite de memoria hasta que el kernel mata el proceso principal.

    A diferencia de `docker kill`, un OOM es un fallo genuino y no una parada
    intencionada: la politica `restart: unless-stopped` SI rearranca el contenedor
    sola, sin `docker compose start`. El llamador debe devolver el limite despues,
    o el rearranque vuelve a caer en OOM.
    """
    _update_memoria(cid, MEM_OOM)
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        r = subprocess.run(["docker", "inspect", "--format", "{{.State.OOMKilled}}", cid],
                           capture_output=True, text=True)
        if r.stdout.strip() == "true":
            return True
        time.sleep(0.3)
    return False


def procesos_ausentes() -> list[str]:
    faltan = []
    for nombre in PROCESOS_NECESARIOS:
        r = subprocess.run(["pgrep", "-f", nombre], capture_output=True, text=True)
        if r.returncode != 0:
            faltan.append(nombre)
    for servicio in CONTENEDORES_NECESARIOS:
        if estado_contenedor(servicio) not in ("healthy", "running"):
            faltan.append(f"contenedor {servicio}")
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
    "bridge": (POSTGRES, "telemetry_events"),
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


def offsets_topico(topico: str) -> int:
    """Suma de los offsets finales por particion: cuantos mensajes lleva el
    topico. Es el mismo metodo que kpi_report.py; el delta entre dos lecturas es
    lo que el bridge publico en Kafka en ese intervalo. Devuelve -1 si no se
    puede leer (Kafka caido)."""
    r = subprocess.run(
        ["docker", "exec", CONTENEDOR_KAFKA, "/opt/kafka/bin/kafka-get-offsets.sh",
         "--bootstrap-server", "kafka:9092", "--topic", topico, "--time", "-1"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        return -1
    return sum(int(l.rsplit(":", 1)[1]) for l in r.stdout.splitlines() if ":" in l)


def esperar_flujo(props: dict, tabla: str, referencia: int, timeout: float,
                  parada, desde: float | None = None) -> float | None:
    """Segundos hasta ver filas NUEVAS respecto a `referencia`, o None si no llegan.

    Se mide sobre filas persistidas y no sobre el estado del contenedor a
    proposito: que Docker diga "healthy" solo significa que el proceso responde,
    no que el pipeline haya vuelto a mover datos de un extremo al otro. Lo que
    exige el objetivo es lo segundo.

    `desde` fija el origen del cronometro; por defecto es "ahora". El llamador lo
    pone ANTES de ordenar el reinicio del contenedor, para que la recuperacion
    medida incluya lo que Docker tarda en arrancarlo.
    """
    t0 = desde if desde is not None else time.monotonic()
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
    # La tasa de perdida se mide siempre sobre el evento individual: filas de
    # telemetry_events (PostgreSQL) frente a lo publicado en el topico de Kafka.
    props_pg = props_bd(args, POSTGRES)
    logger.info("Se observara el flujo en %s.%s, que es el camino que corta un fallo de %s",
                cual_bd, tabla, args.target)

    if args.fallo == "oom" and args.target == "mosquitto":
        logger.error("--fallo oom no sirve para mosquitto: usa ~3 MiB, por debajo del "
                     "minimo de 6 MB que admite `docker update --memory`. Usa --fallo kill.")
        return 1

    faltan = procesos_ausentes()
    if faltan:
        logger.error("Estos procesos deben estar en marcha para que la prueba mida algo:")
        for f in faltan:
            logger.error("  - %s", f)
        logger.error("Arranca el bridge y el job de Spark en otras terminales y repite")
        return 1

    recrear_al_final = False  # lo pone a True el modo oom si tuvo que meter un limite de memoria

    logger.info("Lanzando el simulador con speedup x%g en segundo plano...", args.speedup)
    simulador = subprocess.Popen(
        # Las marcas se publican TAL CUAL vienen del Parquet (ya datadas en el
        # presente por prepare_ashrae.py --fecha-final); no se desplazan en
        # ejecucion. La prueba debe arrancar de estado limpio: el watermark de
        # Spark es monotono, y un checkpoint ya avanzado de una pasada anterior
        # descartaria por tardio lo que aqui se publique, con lo que la consulta
        # de metricas "sobreviviria" sin llegar a tocar la base y la prueba de
        # fallo sobre TimescaleDB no demostraria nada.
        [sys.executable, str(SIMULADOR), "--acelerar", str(args.speedup),
         "--limite", str(args.limit)],
    )
    try:
        filas_inicio = contar(props, tabla)
        if esperar_flujo(props, tabla, filas_inicio, args.warmup, parada) is None:
            logger.error("No llega flujo al sumidero antes del fallo; se aborta la prueba")
            return 1
        logger.info("Flujo confirmado. Filas antes del fallo: %s", f"{contar(props, tabla):,}")

        # Referencias para la tasa de perdida: lo publicado en Kafka y lo
        # persistido en telemetry_events, ANTES del fallo. Al final se toman otra
        # vez y se comparan los deltas.
        publicados_ini = offsets_topico(TOPIC_RAW)
        eventos_pg_ini = contar(props_pg, "telemetry_events")

        # El recuento de referencia se toma con el servicio TODAVIA VIVO. Si se
        # toma despues del kill y el servicio caido es la propia base testigo,
        # `contar` devuelve -1 y entonces cualquier lectura posterior lo supera:
        # se declara "flujo restablecido" en cuanto la base vuelve a responder,
        # aunque no haya llegado ni una fila nueva.
        filas_antes_del_fallo = contar(props, tabla)

        if args.fallo == "kill":
            logger.info("--- MATANDO %s con docker kill (filas antes: %s) ---",
                        args.target, f"{filas_antes_del_fallo:,}")
            compose("kill", args.target)
            instante_fallo = time.monotonic()
            logger.info("Estado de %s: %s", args.target, estado_contenedor(args.target))
            time.sleep(args.downtime)
            filas_durante = contar(props, tabla)
            logger.info("Filas tras %g s caido: %s", args.downtime,
                        f"{filas_durante:,}" if filas_durante >= 0 else "(base de datos caida)")
            logger.info("--- LEVANTANDO %s a mano ---", args.target)
            # El cronometro de recuperacion arranca ANTES de la orden de reinicio:
            # la cifra incluye lo que Docker tarda en volver a poner en pie el
            # contenedor, no solo lo que tarda el flujo en reanudarse despues.
            instante_reinicio = time.monotonic()
            compose("start", args.target)
        else:  # oom
            cid = contenedor_id(args.target)
            logger.info("--- FORZANDO OOM de %s (limite -> %s; filas antes: %s) ---",
                        args.target, MEM_OOM, f"{filas_antes_del_fallo:,}")
            if not forzar_oom(cid):
                logger.error("No se consiguio forzar el OOM de %s en el plazo", args.target)
                return 1
            instante_fallo = time.monotonic()
            filas_durante = contar(props, tabla)
            # No hay `docker compose start`: `restart: unless-stopped` rearranca
            # solo. Se sube el limite a 2g para que el rearranque no vuelva a caer
            # en OOM; al final se recrea el contenedor para dejarlo como lo pide
            # docker-compose.yml (un `docker update` no toca su config).
            recrear_al_final = True
            logger.info("OOM confirmado; subiendo el limite de memoria a 2g para que "
                        "el rearranque automatico prospere")
            instante_reinicio = time.monotonic()
            if not _update_memoria(cid, "2g"):
                logger.error("No se pudo subir el limite de memoria de %s", args.target)
                return 1

        # La referencia es el ULTIMO RECUENTO VALIDO, no el de durante la caida.
        # Cuando el servicio tumbado es la propia base testigo, `contar` devuelve
        # -1 mientras esta muerta; tomar eso como referencia (con un max(...,0))
        # hacia que al volver el servicio sus filas ANTIGUAS ya superaran el
        # umbral y se declarara "flujo restablecido" sin que hubiera llegado
        # nada nuevo. Daba 2,0 s de recuperacion que no median nada.
        referencia = filas_durante if filas_durante >= 0 else filas_antes_del_fallo
        logger.info("Se esperan filas nuevas por encima de %s", f"{referencia:,}")
        recuperacion = esperar_flujo(props, tabla, referencia, args.timeout, parada,
                                     desde=instante_reinicio)
        total = time.monotonic() - instante_fallo

        if recuperacion is None:
            logger.error("EL FLUJO NO SE RESTABLECIO en %g s tras el fallo de %s",
                         args.timeout, args.target)
        elif args.fallo == "kill":
            logger.info("Flujo restablecido %.1f s despues de ordenar el reinicio "
                        "(incluye el arranque del contenedor; %.1f s desde el fallo)",
                        recuperacion, total)
        else:
            logger.info("Flujo restablecido %.1f s despues del OOM, con `restart: "
                        "unless-stopped` rearrancando el contenedor sin intervencion",
                        recuperacion)

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

    # Tasa de perdida: lo que el bridge publico en Kafka durante la prueba frente
    # a lo que se persistio en telemetry_events. Un delta positivo es perdida; un
    # pequeno delta negativo (mas persistido que publicado) es ruido del desfase
    # de ~1 s entre las dos lecturas o duplicados de Kafka que el UPSERT colapso,
    # y se lleva a cero. Una perdida real, de cientos o miles de filas, se ve.
    off_fin = offsets_topico(TOPIC_RAW)
    publicados = off_fin - publicados_ini if off_fin >= 0 and publicados_ini >= 0 else None
    persistidos = contar(props_pg, "telemetry_events") - eventos_pg_ini
    if publicados and publicados > 0:
        perdidos = max(publicados - persistidos, 0)
        tasa_perdida_pct = round(perdidos / publicados * 100, 4)
    else:
        perdidos, tasa_perdida_pct = None, None

    resultado = {
        "servicio": args.target,
        "modo": args.fallo,
        "tabla_testigo": f"{cual_bd}.{tabla}",
        "instante": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "downtime_s": args.downtime if args.fallo == "kill" else None,
        "recuperacion_s": round(recuperacion, 1) if recuperacion is not None else None,
        "desde_el_fallo_s": round(total, 1),
        "filas_antes": filas_inicio,
        "filas_final": filas_final,
        "filas_nuevas": filas_final - filas_inicio,
        "publicados_kafka": publicados,
        "persistidos_pg": persistidos,
        "eventos_perdidos": perdidos,
        "tasa_perdida_pct": tasa_perdida_pct,
        "estado_final": estado_contenedor(args.target),
    }
    RESULTADO.write_text(json.dumps(resultado, indent=2))

    logger.info("--- Resultado de la prueba de recuperacion ---")
    for k, v in resultado.items():
        logger.info("  %-18s %s", k, v)
    if perdidos:
        logger.warning("PERDIDA DETECTADA: %d de %d eventos publicados no llegaron a "
                       "telemetry_events (%.4f%%)", perdidos, publicados, tasa_perdida_pct)
    logger.info("Objetivo: recuperacion < 60 s sin perdida de datos")
    logger.info("Guardado en %s", RESULTADO)

    # El recrear va AL FINAL, despues de medir: `--force-recreate` reinicia el
    # contenedor otra vez, y si el servicio caido es Kafka, leer sus offsets
    # mientras rehace la recuperacion de segmentos da una cifra sin sentido.
    if recrear_al_final:
        logger.info("Recreando %s para quitar el limite de memoria del test...",
                    args.target)
        subprocess.run(["docker", "compose", "-f", str(COMPOSE), "up", "-d",
                        "--force-recreate", "--no-deps", args.target], check=False)

    return 0 if recuperacion is not None and recuperacion < 60 else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prueba de recuperacion ante fallo (TFM)")
    anadir_argumentos_bd(p)
    p.add_argument("--target", default="mosquitto", choices=OBJETIVOS,
                   help="Servicio que se va a tumbar")
    p.add_argument("--fallo", default="kill", choices=("kill", "oom"),
                   help="kill: docker kill + reinicio manual (peor caso, el que va a la "
                        "memoria). oom: fuerza un OOM-kill, que `restart: unless-stopped` "
                        "rearranca solo (demuestra la auto-recuperacion). oom no sirve para "
                        "mosquitto (usa ~3 MiB, por debajo del minimo de 6 MB de docker)")
    p.add_argument("--acelerar", dest="speedup", metavar="FACTOR", type=float, default=1000.0,
                   help="Aceleracion del reloj durante la prueba. Con los 652 sensores, "
                        "x1000 son unos 181 ev/s: suficiente para ver el flujo cortarse y "
                        "volver, sin cargar el sistema mientras se mide la recuperacion")
    p.add_argument("--limite", dest="limit", metavar="N", type=int, default=500000,
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
