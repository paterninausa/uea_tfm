"""
Prueba del envenenamiento del watermark por un evento con fecha futura.

QUE DEMUESTRA. Que UN SOLO evento con marca de tiempo en el futuro detiene la
agregacion por ventana de TODOS los sensores, no solo la del que lo emitio, y
que lo hace sin producir un error en ninguna parte.

El motivo esta en como funciona Spark Structured Streaming: el watermark es
"a single global watermark for stateful operations" —uno solo para toda la
consulta, no por clave ni por particion— y vale (mayor timestamp visto en
cualquier evento) menos el retraso configurado. Un evento del ano 2036 lo deja
ahi, y a partir de ese instante los eventos legitimos de 2016 llegan por debajo
del watermark y Spark los descarta como TARDIOS, en silencio.

El sintoma es una AUSENCIA, y por eso cuesta tanto verlo en produccion:

  - telemetry_metrics (TimescaleDB, con watermark) deja de crecer -> Grafana se
    congela.
  - telemetry_events (PostgreSQL, sin watermark) sigue creciendo con normalidad.

Medio pipeline parado, medio funcionando, cero errores en los logs.

POR QUE SE PUBLICA DIRECTAMENTE A KAFKA. El bridge ya rechaza estos eventos a la
DLQ, asi que pasar por el no reproduciria nada. Lo que se reproduce aqui es el
caso que la guarda del bridge NO cubre: un evento que YA ESTA en el log de Kafka
—porque entro antes de existir la guarda y sigue dentro de los 7 dias de
retencion, porque alguien publico al topico sin pasar por el bridge, o porque se
reprocesa el log desde el principio—. Es el mismo patron del building_id
huerfano, que se quedaba en Kafka envenenando cualquier arranque posterior.

COMO SE EJECUTAN LAS DOS PASADAS. El filtro de la capa 2 vive en
`aggregate_metrics` y se gobierna con `--margen-futuro` del job de Spark. Un
valor muy alto equivale a desactivarlo:

  # 1. Reproducir el fallo: el job tolera cualquier futuro, luego no filtra nada
  python pipeline/spark/stream_processing.py --trigger "1 second" \
      --margen-futuro 999999999
  python pipeline/tools/watermark_poison_test.py

  # 2. Comprobar la proteccion: el job con su margen por defecto (300 s)
  python pipeline/spark/stream_processing.py --trigger "1 second"
  python pipeline/tools/watermark_poison_test.py

Entre una pasada y otra hay que dejar el estado limpio, porque el watermark
envenenado sobrevive en el checkpoint de la consulta:

  python pipeline/tools/reset_state.py --yes

INFORMA DE LO QUE OBSERVE, no de lo que deberia pasar. Si la agregacion sigue
escribiendo despues de inyectar el evento, lo dice; si se detiene, lo dice. El
script no sabe con que margen corre el job y no intenta adivinarlo: mide el
comportamiento y deja el veredicto a la vista.

POR QUE SE MIDE EN DOS TRAMOS. El envenenamiento NO empieza por un silencio,
sino por una RAFAGA. Al saltar el watermark al ano 2036, todas las ventanas
abiertas quedan de golpe por debajo de el y Spark las cierra y las emite todas
a la vez; solo despues llega el silencio. Mirando las escrituras segundo a
segundo con el filtro desactivado: 46 filas, luego 117 de golpe dos segundos
despues de la inyeccion, y ninguna en los 44 siguientes mientras el simulador
publicaba a 357 ev/s.

Un medidor de filas acumulado entre el principio y el final ve ese pico como
crecimiento y concluye "sigue funcionando", que es justo lo contrario de lo que
pasa. Por eso se mide un tramo de ASENTAMIENTO —que absorbe la rafaga de cierre
forzado— y despues un tramo de REGIMEN, y el veredicto sale del segundo. Es la
misma leccion que el resto del proyecto: un fallo silencioso es una AUSENCIA, y
para verla hay que mirar donde deberia haber actividad, no sumar totales.
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from io import BytesIO
from pathlib import Path

from fastavro import parse_schema, schemaless_writer
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.apicurio import (  # noqa: E402
    encode_header,
    latest_schema,
    schema_registry_client,
)
from common.connection_args import (  # noqa: E402
    POSTGRES,
    TIMESCALE,
    anadir_argumentos_bd,
    anadir_argumentos_kafka,
    props_bd,
)
from common.logging_setup import DIRECTORIO_LOGS, configurar_logging  # noqa: E402
from common.stop_event import evento_de_parada  # noqa: E402

logger = logging.getLogger("watermark_poison_test")

RAIZ = Path(__file__).resolve().parents[1]
SIMULADOR = RAIZ / "simulator" / "mqtt_simulator.py"
RESULTADO = DIRECTORIO_LOGS / "ultimo_envenenamiento.json"

PROCESOS_NECESARIOS = ("mqtt_kafka_bridge.py", "stream_processing.py")

SEGUNDOS_POR_ANO = 365 * 24 * 3600


def procesos_ausentes() -> list[str]:
    faltan = []
    for nombre in PROCESOS_NECESARIOS:
        r = subprocess.run(["pgrep", "-f", nombre], capture_output=True, text=True)
        if r.returncode != 0:
            faltan.append(nombre)
    return faltan


def contar(props: dict, tabla: str) -> int:
    import psycopg2

    try:
        conn = psycopg2.connect(**props)
    except Exception:
        return -1
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {tabla}")
            return cur.fetchone()[0]
    except Exception:
        return -1
    finally:
        conn.close()


def un_building_de_la_dimension(props: dict) -> str | None:
    """Un building_id que SI este en la tabla de referencia.

    Con uno inventado, el evento se apartaria antes de llegar al watermark por
    el filtro de `site_id IS NULL`, y la prueba mediria el mecanismo equivocado:
    saldria "no hubo envenenamiento" porque el evento nunca llego a contar.
    """
    import psycopg2

    try:
        conn = psycopg2.connect(**props)
    except Exception:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT building_id FROM buildings LIMIT 1")
            fila = cur.fetchone()
            return fila[0] if fila else None
    except Exception:
        return None
    finally:
        conn.close()


def esperar(segundos: float, parada) -> bool:
    """Duerme a trozos para poder atender una senal de parada. False si hay que salir."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < segundos:
        if parada.is_set():
            return False
        time.sleep(1)
    return True


def esperar_flujo(props: dict, tabla: str, referencia: int, timeout: float,
                  parada) -> float | None:
    """Segundos hasta ver filas NUEVAS respecto a `referencia`, o None."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if parada.is_set():
            return None
        if contar(props, tabla) > referencia:
            return time.monotonic() - t0
        time.sleep(1)
    return None


def inyectar_evento_futuro(args: argparse.Namespace, building_id: str) -> dict:
    """Publica en Kafka UN evento valido cuya unica anomalia es la fecha.

    Se serializa igual que lo haria el bridge —cabecera con el globalId mas
    payload Avro schemaless contra el esquema del registro— porque el objetivo
    es un evento INDISTINGUIBLE de uno legitimo para el consumidor. Pasa la
    validacion de esquema sin objecion: Avro comprueba la forma, no el
    significado.
    """
    sr_client = schema_registry_client(args.registry_url)
    schema_id, schema_str = latest_schema(sr_client)
    esquema = parse_schema(json.loads(schema_str))

    ahora_ms = int(time.time() * 1000)
    futuro_ms = ahora_ms + int(args.anos_futuro * SEGUNDOS_POR_ANO * 1000)

    evento = {
        "building_id": building_id,
        "meter_type": args.meter_type,
        "timestamp": futuro_ms,
        "meter_reading": 100.0,
        "sim_publish_ts": ahora_ms,
    }

    buf = BytesIO()
    buf.write(encode_header(schema_id))
    schemaless_writer(buf, esquema, evento)

    productor = KafkaProducer(bootstrap_servers=args.bootstrap_servers)
    clave = f"{evento['building_id']}:{evento['meter_type']}"
    try:
        productor.send(args.topic, key=clave.encode("utf-8"), value=buf.getvalue()).get(timeout=30)
        productor.flush()
    finally:
        productor.close()

    from datetime import datetime, timezone
    fecha = datetime.fromtimestamp(futuro_ms / 1000, timezone.utc).isoformat()
    logger.info("Evento inyectado en %s | clave=%s | timestamp=%s",
                args.topic, clave, fecha)
    return {**evento, "timestamp_iso": fecha}


def run(args: argparse.Namespace) -> int:
    parada = evento_de_parada("prueba de envenenamiento del watermark")

    props_metrics = props_bd(args, TIMESCALE)
    props_events = props_bd(args, POSTGRES)

    faltan = procesos_ausentes()
    if faltan:
        logger.error("Estos procesos deben estar en marcha para que la prueba mida algo:")
        for f in faltan:
            logger.error("  - %s", f)
        logger.error("Arranca el bridge y el job de Spark en otras terminales y repite")
        return 1

    building_id = un_building_de_la_dimension(props_events)
    if building_id is None:
        logger.error("No hay filas en `buildings`: el job de Spark carga esa tabla al "
                     "arrancar. Arrancalo y espera a `Consultas en marcha`")
        return 1
    logger.info("Se envenenara con el edificio %s, que SI esta en la dimension", building_id)

    logger.info("Lanzando el simulador con speedup x%g en segundo plano...", args.speedup)
    # Las marcas se publican tal cual vienen del Parquet (ya datadas en el
    # presente por prepare_ashrae.py --fecha-final). Como en failover_test.py, la
    # prueba parte de estado limpio: un watermark ya avanzado en el checkpoint
    # dejaria la agregacion sin escribir y no podria distinguirse "envenenado" de
    # "nunca escribio".
    simulador = subprocess.Popen(
        [sys.executable, str(SIMULADOR), "--acelerar", str(args.speedup),
         "--limite", str(args.limit)],
    )

    try:
        # 1. LINEA BASE: las DOS rutas tienen que estar escribiendo antes de
        #    inyectar nada. Sin esto, una agregacion que nunca arranco pareceria
        #    envenenada.
        base_metrics = contar(props_metrics, "telemetry_metrics")
        base_events = contar(props_events, "telemetry_events")

        if esperar_flujo(props_events, "telemetry_events", base_events,
                         args.warmup, parada) is None:
            logger.error("No llega flujo a telemetry_events; se aborta la prueba")
            return 1
        if esperar_flujo(props_metrics, "telemetry_metrics", base_metrics,
                         args.warmup, parada) is None:
            logger.error("La agregacion no esta escribiendo en telemetry_metrics ANTES de "
                         "inyectar nada. La prueba no podria distinguir el envenenamiento "
                         "de una agregacion que nunca arranco. Causas habituales: el "
                         "checkpoint conserva un watermark ya envenenado de una pasada "
                         "anterior (reset_state.py --yes)")
            return 1

        antes_metrics = contar(props_metrics, "telemetry_metrics")
        antes_events = contar(props_events, "telemetry_events")
        logger.info("Flujo confirmado en las dos rutas | metrics=%s events=%s",
                    f"{antes_metrics:,}", f"{antes_events:,}")

        # 2. EL EVENTO ENVENENADOR
        logger.info("--- INYECTANDO UN evento con fecha %g anos en el futuro ---",
                    args.anos_futuro)
        evento = inyectar_evento_futuro(args, building_id)

        # 3. ASENTAMIENTO: absorbe la rafaga de cierre forzado. Al saltar el
        #    watermark, todas las ventanas abiertas quedan por debajo y Spark
        #    las emite de golpe. Contar filas a traves de ese pico haria pasar
        #    por "sigue funcionando" lo que en realidad es el ultimo suspiro.
        logger.info("Asentamiento de %g s (absorbe la rafaga de cierre forzado)...",
                    args.asentamiento)
        if not esperar(args.asentamiento, parada):
            return 1

        pico_metrics = contar(props_metrics, "telemetry_metrics")
        pico_events = contar(props_events, "telemetry_events")

        # 4. REGIMEN: aqui es donde se decide. El simulador sigue publicando
        #    trafico legitimo; lo que se mide es si ese trafico se agrega.
        logger.info("Observando el REGIMEN durante %g s con el simulador publicando...",
                    args.observacion)
        if not esperar(args.observacion, parada):
            return 1

        despues_metrics = contar(props_metrics, "telemetry_metrics")
        despues_events = contar(props_events, "telemetry_events")

        rafaga_metrics = pico_metrics - antes_metrics
        delta_metrics = despues_metrics - pico_metrics
        delta_events = despues_events - pico_events

        logger.info("--- RESULTADO ---")
        # Sin envenenamiento este tramo es actividad normal; con el, es la rafaga
        # de cierre forzado. El nombre se mantiene neutro y lo interpreta el
        # veredicto, que es quien sabe si hubo regimen despues.
        logger.info("Tramo de asentamiento (%g s tras inyectar): %+d filas en metrics",
                    args.asentamiento, rafaga_metrics)
        logger.info("EN REGIMEN, tras %g s:", args.observacion)
        logger.info("  telemetry_metrics (CON watermark): %s -> %s  (%+d filas)",
                    f"{pico_metrics:,}", f"{despues_metrics:,}", delta_metrics)
        logger.info("  telemetry_events  (SIN watermark): %s -> %s  (%+d filas)",
                    f"{pico_events:,}", f"{despues_events:,}", delta_events)

        if delta_events <= 0:
            veredicto = "INVALIDA"
            logger.error("PRUEBA INVALIDA: tampoco entraron eventos. El simulador dejo de "
                         "publicar o el pipeline esta parado; no se mide nada del watermark")
        elif delta_metrics == 0:
            veredicto = "ENVENENADO"
            logger.warning("WATERMARK ENVENENADO. Un unico evento detuvo la agregacion de "
                           "TODOS los sensores: cerro de golpe %d ventanas y despues no "
                           "escribio ni una fila mas, mientras la ruta de eventos seguia "
                           "recibiendo %d. Ningun error en ningun log.",
                           rafaga_metrics, delta_events)
        else:
            veredicto = "PROTEGIDO"
            logger.info("PROTEGIDO. La agregacion siguio escribiendo %d filas pese al "
                        "evento futuro: el filtro previo al withWatermark lo aparto "
                        "antes de que pudiera mover el watermark.", delta_metrics)

        RESULTADO.parent.mkdir(parents=True, exist_ok=True)
        RESULTADO.write_text(json.dumps({
            "veredicto": veredicto,
            "evento_inyectado": evento,
            "asentamiento_s": args.asentamiento,
            "observacion_s": args.observacion,
            "filas_en_asentamiento": rafaga_metrics,
            "telemetry_metrics": {"antes_de_inyectar": antes_metrics,
                                  "tras_asentamiento": pico_metrics,
                                  "final": despues_metrics,
                                  "delta_en_regimen": delta_metrics},
            "telemetry_events": {"antes_de_inyectar": antes_events,
                                 "tras_asentamiento": pico_events,
                                 "final": despues_events,
                                 "delta_en_regimen": delta_events},
        }, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Resultado en %s", RESULTADO)

        return 0 if veredicto != "INVALIDA" else 1

    finally:
        if simulador.poll() is None:
            simulador.terminate()
            try:
                simulador.wait(timeout=15)
            except subprocess.TimeoutExpired:
                simulador.kill()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--anos-futuro", type=float, default=10.0,
                   help="Cuanto adelanta la marca del evento envenenador")
    p.add_argument("--meter-type", default="electricity",
                   help="Debe ser un simbolo valido del enum del esquema")
    p.add_argument("--acelerar", dest="speedup", metavar="FACTOR", type=float, default=2000.0,
                   help="Factor de aceleracion POR SENSOR del simulador")
    p.add_argument("--limite", dest="limit", metavar="N", type=int, default=200000,
                   help="Tope de eventos del simulador: solo tiene que durar mas que la prueba")
    p.add_argument("--warmup", type=float, default=180.0,
                   help="Espera maxima a ver flujo en cada ruta antes de inyectar")
    p.add_argument("--asentamiento", type=float, default=15.0,
                   help="Segundos que se dejan pasar tras inyectar para absorber la rafaga "
                        "de cierre forzado de ventanas antes de empezar a medir")
    p.add_argument("--observacion", type=float, default=45.0,
                   help="Segundos de observacion EN REGIMEN, ya asentada la rafaga")
    p.add_argument("--registry-url", default="http://localhost:8080")
    anadir_argumentos_kafka(p)
    anadir_argumentos_bd(p)
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("watermark_poison_test")
    sys.exit(run(parse_args()))
