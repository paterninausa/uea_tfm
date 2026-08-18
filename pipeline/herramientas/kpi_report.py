"""
Cuadro de KPIs del trabajo, medido sobre el estado actual del sistema.

Un solo comando que interroga a las cuatro fuentes donde el pipeline deja
constancia de lo que hizo, y emite la tabla en Markdown lista para pegar en la
memoria. Sustituye a la coleccion de consultas sueltas con las que se obtuvieron
las primeras cifras: aquello no era repetible —cada medicion se escribia a
mano— y por tanto tampoco comprobable por un tercero, que es lo que se le exige
a un resultado publicado.

De donde sale cada KPI:

  Objetivo 1  latencia de ingesta y perdida  PostgreSQL + offsets de Kafka
  Objetivo 2  validacion de esquema          Apicurio + topico DLQ
  Objetivo 3  latencia de micro-lote         TimescaleDB.streaming_progress
  Objetivo 4  refresco de los dashboards     API de consultas de Grafana
  Objetivo 5  escalabilidad                  resultado de load_ladder.py

NO MIDE lo que no haya ocurrido: informa de lo que encuentra en el estado
actual. Para que las cifras se correspondan con una prueba concreta hay que
partir de `reset_state.py`, generar la carga y ejecutarlo despues.

Uso:
    python kpi_report.py
    python kpi_report.py --run-id 20260818T143000Z   # otra ejecucion del job
    python kpi_report.py --sin-grafana               # omite el Objetivo 4
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.conexiones import (  # noqa: E402
    POSTGRES,
    TIMESCALE,
    anadir_argumentos_bd,
    anadir_argumentos_kafka,
    props_bd,
)
from common.logging_setup import DIRECTORIO_LOGS, configurar_logging  # noqa: E402
from common.schema_registry import (  # noqa: E402
    DEFAULT_ARTIFACT,
    DEFAULT_GROUP,
    DEFAULT_REGISTRY_URL,
    ApicurioClient,
)

logger = logging.getLogger("kpi_report")

INFORME = DIRECTORIO_LOGS / "informe_kpi.md"
RESULTADOS_ESCALERA = DIRECTORIO_LOGS / "ultima_escalera.json"
DIRECTORIO_DASHBOARDS = Path(__file__).resolve().parents[1] / "docker/grafana/dashboards"

CONTENEDOR_KAFKA = "tfm-kafka"


def consultar(props: dict, sql: str) -> list[tuple]:
    import psycopg2

    conn = psycopg2.connect(**props)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Objetivo 1: ingesta
# --------------------------------------------------------------------------
def kpi_ingesta(props_pg: dict, props_ts: dict) -> dict:
    """Latencia extremo a extremo a grano de evento, y del agregado.

    Se informan las dos por separado porque miden cosas distintas y el objetivo
    escrito las confunde. La de evento (publicacion -> fila en PostgreSQL) mide
    el pipeline. La del agregado (-> fila en TimescaleDB) incluye la espera a que
    cierre la ventana horaria, que depende de cada cuanto mide el contador y no
    de la velocidad del sistema.
    """
    (filas, con_marca, p50, p95, maximo, negativas), = consultar(props_pg, """
        SELECT count(*),
               count(*) FILTER (WHERE sim_publish_ts IS NOT NULL),
               percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - sim_publish_ts))),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - sim_publish_ts))),
               max(EXTRACT(EPOCH FROM (ingested_at - sim_publish_ts))),
               count(*) FILTER (WHERE ingested_at < sim_publish_ts)
        FROM telemetry_events
    """)

    (agregados, ap50, ap95), = consultar(props_ts, """
        SELECT count(*),
               percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - max_sim_publish_ts))),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - max_sim_publish_ts)))
        FROM telemetry_metrics WHERE max_sim_publish_ts IS NOT NULL
    """)

    return {"filas": filas, "con_marca": con_marca, "p50": p50, "p95": p95,
            "max": maximo, "negativas": negativas,
            "agregados": agregados, "agregado_p50": ap50, "agregado_p95": ap95}


def offsets_kafka(topico: str) -> int:
    """Mensajes acumulados en un topico: fin menos principio, por particion.

    Es el recuento exacto de lo que el bridge publico, y no depende de que
    ningun consumidor siga vivo. Se usa la herramienta del propio broker por la
    misma razon que en reset_state.py: es la que ya documenta el README.
    """
    r = subprocess.run(
        ["docker", "exec", CONTENEDOR_KAFKA, "/opt/kafka/bin/kafka-get-offsets.sh",
         "--bootstrap-server", "kafka:9092", "--topic", topico, "--time", "-1"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"kafka-get-offsets.sh fallo: {r.stderr.strip()}")
    finales = sum(int(l.rsplit(":", 1)[1]) for l in r.stdout.splitlines() if ":" in l)

    r = subprocess.run(
        ["docker", "exec", CONTENEDOR_KAFKA, "/opt/kafka/bin/kafka-get-offsets.sh",
         "--bootstrap-server", "kafka:9092", "--topic", topico, "--time", "-2"],
        capture_output=True, text=True,
    )
    iniciales = sum(int(l.rsplit(":", 1)[1]) for l in r.stdout.splitlines() if ":" in l)
    return finales - iniciales


# --------------------------------------------------------------------------
# Objetivo 2: gobernanza de esquema
# --------------------------------------------------------------------------
def kpi_esquema(args) -> dict:
    registro = ApicurioClient(args.registry_url)
    global_id, esquema = registro.latest(args.group, args.artifact)

    validos = offsets_kafka(args.topic)
    invalidos = offsets_kafka(args.dlq_topic)
    total = validos + invalidos
    return {
        "global_id": global_id,
        "nombre": f"{esquema.get('namespace', '')}.{esquema.get('name', '')}",
        "campos": len(esquema.get("fields", [])),
        "validos": validos,
        "dlq": invalidos,
        "pct_validados": (validos / total * 100) if total else None,
    }


# --------------------------------------------------------------------------
# Objetivo 3: procesamiento
# --------------------------------------------------------------------------
def kpi_procesamiento(props_ts: dict, run_id: str | None) -> tuple[str | None, list[tuple]]:
    """Duracion de micro-lote por consulta, sobre una unica ejecucion del job.

    Se filtra por run_id porque mezclar ejecuciones falsea los percentiles: los
    primeros lotes de un arranque en frio son sistematicamente mas lentos, y
    juntar dos arranques duplica ese sesgo sin que se note en el resultado.
    """
    if run_id is None:
        filas = consultar(props_ts, "SELECT max(run_id) FROM streaming_progress")
        run_id = filas[0][0] if filas else None
    if run_id is None:
        return None, []

    return run_id, consultar(props_ts, f"""
        SELECT query_name,
               count(*),
               percentile_cont(0.50) WITHIN GROUP (ORDER BY duration_ms),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms),
               max(duration_ms),
               sum(num_input_rows),
               avg(processed_rows_per_second) FILTER (WHERE num_input_rows > 0)
        FROM streaming_progress
        WHERE run_id = '{run_id}'
        GROUP BY query_name ORDER BY query_name
    """)


# --------------------------------------------------------------------------
# Objetivo 4: visualizacion
# --------------------------------------------------------------------------
def kpi_grafana(url: str, usuario: str, clave: str, rango: str) -> list[dict]:
    """Cronometra cada consulta de panel A TRAVES DE GRAFANA, no contra la base.

    Ejecutar el `rawSql` directamente en PostgreSQL obligaria a reimplementar la
    sustitucion de las macros de Grafana ($__timeFilter, $__timeGroupAlias), que
    no son SQL valido, y ademas mediria otra cosa: el objetivo habla del refresco
    del dashboard, que incluye el trayecto por el servidor de Grafana. Usando su
    API de consultas, la interpolacion la hace quien la definio.
    """
    medidas = []
    for fichero in sorted(DIRECTORIO_DASHBOARDS.glob("*.json")):
        panel_json = json.loads(fichero.read_text())
        for panel in panel_json.get("panels", []):
            objetivos = [t for t in panel.get("targets", []) if t.get("rawSql")]
            if not objetivos:
                continue

            cuerpo = {
                "from": rango, "to": "now",
                "queries": [{
                    "refId": t.get("refId", chr(65 + i)),
                    "datasource": {"type": "postgres", "uid": "timescaledb"},
                    "rawSql": t["rawSql"],
                    "format": t.get("format", "table"),
                    "rawQuery": True,
                    "intervalMs": 60000,
                    "maxDataPoints": 1000,
                } for i, t in enumerate(objetivos)],
            }
            t0 = time.monotonic()
            resp = requests.post(f"{url}/api/ds/query", json=cuerpo,
                                 auth=(usuario, clave), timeout=30)
            ms = (time.monotonic() - t0) * 1000
            medidas.append({
                "dashboard": panel_json.get("title", fichero.stem),
                "panel": panel.get("title", "(sin titulo)"),
                "ms": round(ms, 1),
                "ok": resp.status_code == 200,
            })
            if resp.status_code != 200:
                logger.warning("Grafana respondio %d al panel '%s': %s",
                               resp.status_code, panel.get("title"), resp.text[:200])
    return medidas


# --------------------------------------------------------------------------
# Informe
# --------------------------------------------------------------------------
def _fmt(valor, decimales=3, sufijo=""):
    return "n/d" if valor is None else f"{valor:,.{decimales}f}{sufijo}"


def construir_informe(ingesta, esquema, run_id, procesamiento, grafana, carga) -> str:
    L = ["# Cuadro de KPIs", "",
         f"Generado el {time.strftime('%Y-%m-%d %H:%M:%S')} sobre el estado actual del sistema.",
         ""]

    L += ["## Objetivo 1 — Ingesta", "",
          "| Metrica | Resultado | Objetivo |", "|---|---|---|",
          f"| Eventos persistidos en PostgreSQL | {ingesta['filas']:,} | — |",
          f"| Latencia de ingesta p50 | {_fmt(ingesta['p50'])} s | — |",
          f"| **Latencia de ingesta p95** | **{_fmt(ingesta['p95'])} s** | < 2 s |",
          f"| Latencia de ingesta maxima | {_fmt(ingesta['max'])} s | — |",
          f"| Agregados en TimescaleDB | {ingesta['agregados']:,} | — |",
          f"| Disponibilidad del agregado p50 / p95 | {_fmt(ingesta['agregado_p50'])} / "
          f"{_fmt(ingesta['agregado_p95'])} s | acotada por la cadencia del sensor |", ""]
    if ingesta["negativas"]:
        L += [f"> **Aviso**: {ingesta['negativas']:,} filas tienen `ingested_at` anterior a "
              "`sim_publish_ts`, es decir latencia negativa. Es el sintoma de que el UPSERT no "
              "refresca `ingested_at` al reescribir una fila; las latencias de arriba estan "
              "contaminadas.", ""]

    if esquema:
        L += ["## Objetivo 2 — Gobernanza de esquema", "",
              "| Metrica | Resultado | Objetivo |", "|---|---|---|",
              f"| Esquema vigente | `{esquema['nombre']}` globalId={esquema['global_id']}, "
              f"{esquema['campos']} campos | — |",
              f"| Eventos validados (topico raw) | {esquema['validos']:,} | — |",
              f"| Eventos rechazados (DLQ) | {esquema['dlq']:,} | — |",
              f"| **Validados sobre el total** | **{_fmt(esquema['pct_validados'], 4)}%** | 100% |", ""]

    L += ["## Objetivo 3 — Procesamiento", ""]
    if procesamiento:
        L += [f"Ejecucion del job `run_id={run_id}`.", "",
              "| Consulta | Lotes | Duracion p50 | Duracion p95 | Maxima | Filas | Ritmo medio |",
              "|---|---|---|---|---|---|---|"]
        for nombre, lotes, p50, p95, maximo, filas, ritmo in procesamiento:
            L.append(f"| `{nombre}` | {lotes:,} | {_fmt(p50, 0, ' ms')} | **{_fmt(p95, 0, ' ms')}** | "
                     f"{_fmt(maximo, 0, ' ms')} | {filas or 0:,} | {_fmt(ritmo, 1, ' ev/s')} |")
        L += ["", "Objetivo: duracion de micro-lote < 3.000 ms y throughput sostenido >= 50 ev/s.", ""]
    else:
        L += ["Sin datos en `streaming_progress`: el job no ha corrido con "
              "`--progress-interval` activo.", ""]

    if grafana:
        L += ["## Objetivo 4 — Visualizacion", "",
              "| Dashboard | Panel | Tiempo |", "|---|---|---|"]
        for m in grafana:
            marca = "" if m["ok"] else " ⚠ error"
            L.append(f"| {m['dashboard']} | {m['panel']} | {m['ms']:,.1f} ms{marca} |")
        peor = max(grafana, key=lambda m: m["ms"])
        L += ["", f"Panel mas lento: **{peor['ms']:,.1f} ms** (objetivo < 5.000 ms).", ""]

    if carga:
        L += ["## Objetivo 5 — Escalabilidad", "",
              f"Escalera del {carga['instante']} con speedup x{carga['speedup']:,.0f} "
              f"identico en todos los peldanos, de modo que la carga crece con el numero "
              f"de sensores. **Medido en el consumo**, no en el productor.", "",
              "| Peldano | Sensores | Tasa teorica | Entregado a Kafka | Persistido | Lote p95 |",
              "|---|---|---|---|---|---|"]
        for r in carga["peldanos"]:
            aviso = "" if r["productor_sostuvo_el_ritmo"] else " ⚠ productor saturado"
            L.append(f"| {r['etiqueta']}{aviso} | {r['sensores']} | "
                     f"{r['tasa_teorica_ev_s']:,.1f} ev/s | "
                     f"**{r['throughput_entregado_ev_s']:,.1f} ev/s** | "
                     f"{r['persistidos_en_postgresql']:,} | "
                     f"{_fmt(r['micro_lote_p95_ms'], 0, ' ms')} |")
        L.append("")

    return "\n".join(L)


def run(args: argparse.Namespace) -> int:
    props_pg = props_bd(args, POSTGRES)
    props_ts = props_bd(args, TIMESCALE)

    logger.info("Objetivo 1: consultando latencias de ingesta...")
    ingesta = kpi_ingesta(props_pg, props_ts)

    esquema = None
    try:
        logger.info("Objetivo 2: consultando el registro de esquemas y los topicos...")
        esquema = kpi_esquema(args)
    except Exception as exc:
        logger.warning("No se pudo medir la gobernanza de esquema: %s", exc)

    logger.info("Objetivo 3: consultando el progreso de micro-lote...")
    run_id, procesamiento = kpi_procesamiento(props_ts, args.run_id)

    grafana = None
    if not args.sin_grafana:
        try:
            logger.info("Objetivo 4: cronometrando los paneles a traves de Grafana...")
            grafana = kpi_grafana(args.grafana_url, args.grafana_user,
                                  args.grafana_password, args.rango)
        except requests.RequestException as exc:
            logger.warning("No se pudo consultar Grafana: %s", exc)

    carga = None
    if RESULTADOS_ESCALERA.exists():
        carga = json.loads(RESULTADOS_ESCALERA.read_text())
        logger.info("Objetivo 5: leyendo %s", RESULTADOS_ESCALERA)
    else:
        logger.info("Objetivo 5: sin resultados de escalera (ejecuta load_ladder.py)")

    informe = construir_informe(ingesta, esquema, run_id, procesamiento, grafana, carga)
    INFORME.write_text(informe)
    print(informe)
    logger.info("Informe escrito en %s", INFORME)
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cuadro de KPIs del pipeline (TFM)")
    anadir_argumentos_bd(p)
    anadir_argumentos_kafka(p)
    p.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    p.add_argument("--group", default=DEFAULT_GROUP)
    p.add_argument("--artifact", default=DEFAULT_ARTIFACT)
    p.add_argument("--run-id", default=None,
                   help="Ejecucion del job a analizar (por defecto, la ultima)")
    p.add_argument("--grafana-url", default="http://localhost:3000")
    p.add_argument("--grafana-user", default="admin")
    p.add_argument("--grafana-password", default="admin")
    p.add_argument("--rango", default="now-24h",
                   help="Ventana temporal con la que se consultan los paneles")
    p.add_argument("--sin-grafana", action="store_true",
                   help="Omite el Objetivo 4 (util si Grafana no esta levantado)")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("kpi_report")
    try:
        sys.exit(run(parse_args()))
    except Exception as exc:
        logger.error("%s", exc)
        sys.exit(1)
