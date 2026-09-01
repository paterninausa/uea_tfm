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
  Objetivo 5  recuperacion ante fallo        resultado de failover_test.py

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
from common.apicurio import (  # noqa: E402
    DEFAULT_REGISTRY_URL,
    DEFAULT_SUBJECT,
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

logger = logging.getLogger("kpi_report")

INFORME = DIRECTORIO_LOGS / "informe_kpi.md"
RESULTADO_FAILOVER = DIRECTORIO_LOGS / "ultimo_failover.json"
DIRECTORIO_DASHBOARDS = Path(__file__).resolve().parents[1] / "docker/grafana/dashboards"

CONTENEDOR_KAFKA = "tfm-kafka"
CONTENEDOR_MOSQUITTO = "tfm-mosquitto"


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
def kpi_ingesta(props_pg: dict) -> dict:
    """Latencia extremo a extremo desde la publicacion hasta la persistencia final.

    Se mide sobre el evento individual (publicacion -> fila persistida), que es la
    magnitud que fija el objetivo. `negativas` cuenta los eventos con marca de
    persistencia anterior a la de publicacion: si aparece alguno, la medicion no
    es fiable.
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

    return {"filas": filas, "con_marca": con_marca, "p50": p50, "p95": p95,
            "max": maximo, "negativas": negativas}


def eventos_sin_dimension(props_pg: dict) -> list[tuple]:
    """Edificios presentes en los eventos pero ausentes de la tabla de dimension.

    Estos eventos entran al pipeline sin problema —cumplen el esquema Avro, asi
    que el bridge no los rechaza— y se persisten en telemetry_events, pero NO
    pueden agregarse: sin site_id ni primary_use no hay por donde agruparlos, y
    el job los aparta antes de la ventana.

    Antes de apartarlos, uno solo bastaba para inutilizar la ruta operativa: la
    escritura fallaba con NotNullViolation, el supervisor relanzaba la consulta y
    volvia a fallar con el mismo lote, y el evento se quedaba en Kafka
    envenenando cualquier arranque posterior.

    Que aparezca algo aqui significa que la dimension esta incompleta o que
    alguien esta publicando edificios que no existen.
    """
    return consultar(props_pg, """
        SELECT e.building_id, count(*) AS eventos
        FROM telemetry_events e
        LEFT JOIN buildings b ON b.building_id = e.building_id
        WHERE b.building_id IS NULL
        GROUP BY e.building_id ORDER BY 2 DESC LIMIT 10
    """)


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


def descartes_mosquitto(timeout: int = 12) -> int | None:
    """Mensajes que el broker ACEPTO y nunca entrego, segun su propio medidor.

    ES LA UNICA PERDIDA QUE NO DEJA RASTRO EN NINGUN OTRO SITIO. Medido el 19 de
    agosto de 2026 empujando por encima del punto de saturacion: el productor
    registro 40.000 publicados y 0 fallidos —recibio su PUBACK de cada uno— pero
    al bridge solo llegaron 38.221. Mosquitto habia descartado 1.779 al llenarse
    su cola de salida (`max_queued_messages`, 10.000 en este stack).

    Nadie da error en ese escenario: el productor cree que entrego, el bridge no
    ve ningun hueco, la DLQ esta vacia y los logs del broker callan. Un 4,4% de
    perdida invisible con todos los indicadores del pipeline en verde. Este
    medidor es lo que la convierte en una medida.

    Mosquitto publica las estadisticas $SYS cada 10 s por defecto, de ahi la
    espera.
    """
    r = subprocess.run(
        ["docker", "exec", CONTENEDOR_MOSQUITTO, "mosquitto_sub", "-h", "localhost",
         "-t", "$SYS/broker/publish/messages/dropped", "-C", "1", "-W", str(timeout)],
        capture_output=True, text=True,
    )
    salida = r.stdout.strip()
    return int(salida) if salida.isdigit() else None


# --------------------------------------------------------------------------
# Objetivo 2: gobernanza de esquema
# --------------------------------------------------------------------------
def kpi_esquema(args) -> dict:
    sr_client = schema_registry_client(args.registry_url)
    schema_id, schema_str = latest_schema(sr_client, args.subject)
    esquema = json.loads(schema_str)

    validos = offsets_kafka(args.topic)
    invalidos = offsets_kafka(args.dlq_topic)
    total = validos + invalidos
    return {
        "schema_id": schema_id,
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
DATASOURCE_POR_DEFECTO = {"type": "grafana-postgresql-datasource", "uid": "timescaledb"}


def _datasource_del_panel(target: dict, panel: dict) -> dict:
    """Fuente de datos que declara cada panel, respetando la que trae.

    Un panel puede fijar su fuente a nivel de consulta o de panel. Los paneles de
    latencia de ingesta leen los eventos individuales (sumidero analitico) y el
    resto lee las metricas agregadas (sumidero operacional); imponer una sola
    fuente mandaria las consultas de latencia contra la base equivocada, donde la
    tabla de eventos no existe.
    """
    ds = target.get("datasource") or panel.get("datasource") or DATASOURCE_POR_DEFECTO
    return ds if isinstance(ds, dict) and ds.get("uid") else DATASOURCE_POR_DEFECTO


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
                    "datasource": _datasource_del_panel(t, panel),
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
def _num(valor, decimales=3):
    """Numero en convencion espanola: miles con punto y decimales con coma."""
    if valor is None:
        return "n/d"
    s = f"{valor:,.{decimales}f}"
    return s.replace(",", "\x00").replace(".", ",").replace("\x00", ".")


def _fmt(valor, decimales=3, sufijo=""):
    return "n/d" if valor is None else f"{_num(valor, decimales)}{sufijo}"


# Cada consulta de streaming se nombra por el sumidero que alimenta, no por su
# identificador interno: es asi como se describe en la memoria.
FLUJOS = {
    "eventos-postgresql": "Eventos individuales (sumidero analitico)",
    "metricas-timescaledb": "Metricas agregadas (sumidero operacional)",
}


def construir_informe(ingesta, esquema, run_id, procesamiento, grafana, failover) -> str:
    L = ["# Cuadro de indicadores de rendimiento", "",
         f"Generado el {time.strftime('%Y-%m-%d %H:%M:%S')} sobre el estado del sistema.",
         ""]

    # -- Objetivo 1 --------------------------------------------------------
    L += ["## Objetivo 1: Garantizar la ingesta fiable de telemetria en tiempo real", "",
          "| Indicador | Resultado | Objetivo |", "|---|---|---|",
          f"| Eventos persistidos | {_fmt(ingesta['filas'], 0)} | — |",
          f"| Mensajes publicados en el flujo | {_fmt(ingesta.get('publicados'), 0)} | — |",
          f"| Latencia de ingesta (mediana) | {_fmt(ingesta['p50'])} s | — |",
          f"| **Latencia de ingesta (percentil 95)** | **{_fmt(ingesta['p95'])} s** | < 2 s |",
          f"| Latencia de ingesta (maxima) | {_fmt(ingesta['max'])} s | — |",
          f"| **Tasa de perdida** | **{_fmt(ingesta.get('perdida_pct'), 4, ' %')}** | < 0,1 % |",
          ""]

    if ingesta.get("descartados"):
        L += [f"| Mensajes aceptados por el broker y no entregados | "
              f"{_fmt(ingesta['descartados'], 0)} | 0 |", "",
              "> El broker los confirmo al simulador pero no llego a entregarlos por tener la "
              "cola de salida llena. Es una perdida adicional a la de la tabla, anterior a "
              "Kafka, y no consta en ningun otro indicador.", ""]

    if ingesta.get("persistidos_de_mas"):
        L += ["> Hay mas eventos persistidos que publicados en el flujo: la medicion se ha "
              "tomado sobre un estado que no estaba limpio. Conviene repetirla partiendo de un "
              "estado limpio.", ""]

    if ingesta.get("huerfanos"):
        total = sum(n for _, n in ingesta["huerfanos"])
        L += [f"| Eventos sin edificio de referencia | {_fmt(total, 0)} | 0 |", "",
              "> Se persisten, pero quedan fuera de las metricas agregadas: su edificio no "
              "consta en la tabla de referencia.", ""]

    if ingesta["negativas"]:
        L += [f"> Aviso: {_fmt(ingesta['negativas'], 0)} eventos presentan una latencia "
              "negativa. La medicion no es fiable y debe repetirse.", ""]

    # -- Objetivo 2 ------------------------------------------------------
    if esquema:
        L += ["## Objetivo 2: Garantizar la gobernanza del esquema de datos", "",
              "| Indicador | Resultado | Objetivo |", "|---|---|---|",
              f"| Esquema registrado | `{esquema['nombre']}` | — |",
              f"| Eventos validados contra el esquema | {_fmt(esquema['pct_validados'], 4, ' %')} | 100 % |",
              f"| Eventos con error de validacion | {_fmt(esquema['dlq'], 0)} | 0 |", ""]

    # -- Objetivo 3 ----------------------------------------------------
    L += ["## Objetivo 3: Procesar y enriquecer los datos en streaming con baja latencia", ""]
    if procesamiento:
        L += ["| Flujo de procesamiento | Micro-lotes | Duracion (mediana) | "
              "Duracion (percentil 95) | Duracion (maxima) | Objetivo |",
              "|---|---|---|---|---|---|"]
        for nombre, lotes, p50, p95, maximo, _filas, _ritmo in procesamiento:
            L.append(f"| {FLUJOS.get(nombre, nombre)} | {_fmt(lotes, 0)} | {_fmt(p50, 0, ' ms')} | "
                     f"**{_fmt(p95, 0, ' ms')}** | {_fmt(maximo, 0, ' ms')} | < 3 s |")
        L += [""]
    else:
        L += ["No hay datos de micro-lote registrados para esta ejecucion.", ""]

    # -- Objetivo 4 --------------------------------------------------
    if grafana:
        L += ["## Objetivo 4: Ofrecer visualizacion diferenciada operacional y analitica", "",
              "| Dashboard | Paneles | Refresco mas lento | Objetivo |",
              "|---|---|---|---|"]
        por_dashboard: dict[str, list[dict]] = {}
        for m in grafana:
            por_dashboard.setdefault(m["dashboard"], []).append(m)
        for titulo, paneles in por_dashboard.items():
            peor = max(paneles, key=lambda m: m["ms"])
            fallo = " (con errores)" if any(not m["ok"] for m in paneles) else ""
            L.append(f"| {titulo} | {len(paneles)} | {_num(peor['ms'], 0)} ms{fallo} | < 5 s |")
        L += [""]
        if any(not m["ok"] for m in grafana):
            L += ["> Aviso: algun panel devolvio error. Revisar el registro de actividad.", ""]

    # -- Objetivo 5 ------------------------------------------------
    if failover:
        ok = failover["recuperacion_s"] is not None and failover["recuperacion_s"] < 60
        marca = "✓" if ok else "✗"
        L += ["## Objetivo 5: Validar la resiliencia del sistema", "",
              f"Prueba realizada el {failover['instante']} sobre el servicio "
              f"`{failover['servicio']}`.", "",
              "| Indicador | Resultado | Objetivo |", "|---|---|---|",
              f"| Servicio interrumpido | `{failover['servicio']}` | — |",
              f"| Duracion de la interrupcion | {failover['downtime_s']:g} s | — |",
              f"| **Tiempo de recuperacion** | **{_fmt(failover['recuperacion_s'], 1, ' s')}** {marca} | < 60 s |",
              f"| Eventos persistidos tras la recuperacion | {_fmt(failover['filas_nuevas'], 0)} | > 0 |",
              ""]

    return "\n".join(L)


def run(args: argparse.Namespace) -> int:
    props_pg = props_bd(args, POSTGRES)
    props_ts = props_bd(args, TIMESCALE)

    logger.info("Objetivo 1: consultando latencias de ingesta...")
    ingesta = kpi_ingesta(props_pg)

    logger.info("Objetivo 1: comparando lo publicado en el flujo con lo persistido...")
    try:
        publicados = offsets_kafka(args.topic)
    except RuntimeError as exc:
        logger.warning("No se pudo contar lo publicado en Kafka: %s", exc)
        publicados = None
    ingesta["publicados"] = publicados
    if publicados:
        perdidos = publicados - ingesta["filas"]
        ingesta["perdida_pct"] = max(perdidos, 0) / publicados * 100
        ingesta["persistidos_de_mas"] = perdidos < 0
    else:
        ingesta["perdida_pct"] = None
        ingesta["persistidos_de_mas"] = False

    logger.info("Objetivo 1: leyendo el medidor de descartes del broker...")
    ingesta["descartados"] = descartes_mosquitto()
    ingesta["huerfanos"] = eventos_sin_dimension(props_pg)

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

    failover = None
    if RESULTADO_FAILOVER.exists():
        failover = json.loads(RESULTADO_FAILOVER.read_text())
        logger.info("Objetivo 5: leyendo %s", RESULTADO_FAILOVER)
    else:
        logger.info("Objetivo 5: sin resultados de recuperacion ante fallo disponibles")

    informe = construir_informe(ingesta, esquema, run_id, procesamiento, grafana, failover)
    INFORME.write_text(informe)
    print(informe)
    logger.info("Informe escrito en %s", INFORME)
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cuadro de KPIs del pipeline (TFM)")
    anadir_argumentos_bd(p)
    anadir_argumentos_kafka(p)
    p.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    p.add_argument("--subject", default=DEFAULT_SUBJECT,
                   help="Subject del esquema en el registro (convencion {topic}-value)")
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
