"""
Vigilancia del job: progreso de micro-lote y supervision de las consultas.

Dos cosas que el job hace sobre SI MISMO, no sobre los datos: registrar cuanto
tarda cada micro-lote —la fuente del KPI del Objetivo 3— y comprobar que sus
consultas siguen vivas, relanzando la que caiga.

Separado de `telemetry_streaming.py` por la misma razon que `escritura.py`: alli
esta el procesamiento, aqui la instrumentacion y la resiliencia. Son cosas que se
leen, se prueban y se cambian por separado.
"""

import logging
from datetime import datetime
from pathlib import Path
import time as _time

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Progreso de micro-lote: KPI 3
# --------------------------------------------------------------------------
DDL_PROGRESO = (Path(__file__).resolve().parents[1]
                / "docker/timescaledb/init/02_streaming_progress.sql")


def asegurar_tabla_progreso(props: dict) -> None:
    """Aplica el DDL de `streaming_progress` si aun no existe.

    Los scripts de `docker/timescaledb/init/` solo se ejecutan cuando se CREA el
    volumen. Sin esto, anadir la tabla obligaria a destruir un volumen con datos
    para que apareciera. Se ejecuta el mismo fichero .sql que usa el contenedor,
    en vez de repetir aqui el CREATE TABLE, para que no existan dos definiciones
    del esquema que puedan divergir.
    """
    import psycopg2

    # `with psycopg2.connect(...)` hace commit al salir pero NO cierra la
    # conexion: hay que cerrarla a mano o se acumulan sockets abiertos.
    conn = psycopg2.connect(**props)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL_PROGRESO.read_text())
    finally:
        conn.close()


def _a_timestamp(valor) -> datetime | None:
    """Convierte a datetime las marcas ISO que Spark publica como cadena."""
    if not valor:
        return None
    try:
        return datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
    except ValueError:
        return None


class RegistroProgreso:
    """Vuelca a TimescaleDB el progreso de cada micro-lote.

    Se lee `recentProgress` y no `lastProgress`, que es lo que hacia la version
    anterior: con un trigger de 1 s y un sondeo de 10 s, `lastProgress` deja
    fuera nueve de cada diez lotes. Eso vale para observar por encima, pero no
    para un KPI —la mediana y el p95 de una muestra con huecos no son la
    mediana y el p95 de los lotes—. `recentProgress` conserva los ultimos 100
    informes, asi que sondeando mas a menudo que cada 100 lotes no se pierde
    ninguno.

    Los ya escritos se llevan en un conjunto en memoria porque esos 100
    informes se solapan entre sondeos consecutivos.
    """

    def __init__(self, consultas, props: dict, run_id: str):
        self.consultas = list(consultas)
        self.props = props
        self.run_id = run_id
        self.vistos: set[tuple[str, int]] = set()

    def seguir(self, consultas) -> None:
        """Actualiza la lista de consultas vigiladas.

        Hace falta porque el supervisor relanza las que caen, y el objeto
        StreamingQuery del relanzamiento es OTRO: sin esto, el registro seguiria
        sondeando una consulta muerta y el KPI de micro-lote se quedaria
        congelado justo despues de una recuperacion.
        """
        self.consultas = list(consultas)

    def volcar(self) -> int:
        import psycopg2
        from psycopg2.extras import execute_values

        filas = []
        for q in self.consultas:
            for p in q.recentProgress:
                clave = (q.name, p.get("batchId"))
                if clave in self.vistos:
                    continue
                self.vistos.add(clave)

                d = p.get("durationMs", {}) or {}
                et = p.get("eventTime", {}) or {}
                filas.append((
                    _a_timestamp(p.get("timestamp")), self.run_id, q.name, p.get("batchId"),
                    p.get("numInputRows"),
                    p.get("inputRowsPerSecond"), p.get("processedRowsPerSecond"),
                    d.get("triggerExecution"), d.get("addBatch"), d.get("queryPlanning"),
                    (p.get("sink") or {}).get("numOutputRows"),
                    _a_timestamp(et.get("max")), _a_timestamp(et.get("watermark")),
                ))

        if not filas:
            return 0

        # Un fallo escribiendo telemetria NO puede tumbar el pipeline: se
        # registra y se sigue. Es la excepcion a "fallar ruidosamente", y lo es
        # porque aqui no hay dato del dominio en juego; perder unas metricas de
        # progreso es recuperable repitiendo la medicion, parar la ingesta no.
        # Se abre y se cierra una conexion por volcado en vez de mantener una
        # viva: este hilo sigue corriendo mientras se prueba la recuperacion
        # ante fallo del Objetivo 5, que consiste precisamente en tumbar la base
        # de datos, y una conexion guardada quedaria inservible tras el reinicio.
        conn = None
        try:
            conn = psycopg2.connect(**self.props)
            with conn, conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO streaming_progress (
                        trigger_ts, run_id, query_name, batch_id,
                        num_input_rows, input_rows_per_second, processed_rows_per_second,
                        duration_ms, add_batch_ms, query_planning_ms, sink_num_output_rows,
                        event_time_max, watermark
                    ) VALUES %s
                    ON CONFLICT (trigger_ts, run_id, query_name, batch_id) DO NOTHING
                """, filas, page_size=500)
        except Exception as exc:
            logger.warning("No se pudo registrar el progreso de %d lotes: %s", len(filas), exc)
            # Se reintentaran en el volcado siguiente: al no haberse escrito, se
            # sacan del conjunto de vistos.
            for fila in filas:
                self.vistos.discard((fila[2], fila[3]))
            return 0
        finally:
            if conn is not None:
                conn.close()
        return len(filas)

    def arrancar(self, intervalo: float) -> None:
        import threading

        def bucle():
            while True:
                _time.sleep(intervalo)
                escritos = self.volcar()
                if escritos:
                    logger.info("Progreso registrado: %d lotes nuevos (run_id=%s)",
                                escritos, self.run_id)

        threading.Thread(target=bucle, daemon=True).start()


def supervisar(arrancadores: dict, intervalo: float, max_reinicios: int,
               registro=None) -> int:
    """Vigila todas las consultas y relanza la que se caiga, sin tocar las demas.

    Sustituye a `for q in consultas: q.awaitTermination()`, que esperaba
    SECUENCIALMENTE y por tanto solo vigilaba la primera. Las consecuencias se
    midieron el 19 de agosto de 2026 tumbando cada base de datos por separado:

    - Al caer TimescaleDB moria `metricas-timescaledb`, que era la primera de la
      lista, y su excepcion terminaba el proceso ENTERO, arrastrando a la
      consulta de PostgreSQL que no dependia de ella.
    - Al caer PostgreSQL moria `eventos-postgresql`, que era la segunda, y como
      nadie la esperaba el proceso seguia vivo escribiendo metricas con toda
      normalidad. Media arquitectura parada y ni un aviso: la tabla de eventos
      llevaba veinte minutos congelada mientras el job aparentaba salud.

    Con esto, cada consulta se relanza por su cuenta desde su propio checkpoint
    —que es lo que hace que reanudar no pierda ni duplique nada— y la caida de
    un sumidero no toca al otro. El limite de reinicios existe para que un
    servicio que no vuelve no deje al job dando vueltas para siempre: agotado,
    se detiene todo y se dice por que.
    """
    consultas = {nombre: arrancar() for nombre, arrancar in arrancadores.items()}
    reinicios = {nombre: 0 for nombre in arrancadores}
    if registro:
        registro.seguir(consultas.values())
    logger.info("Consultas en marcha: %s", list(consultas))

    while consultas:
        _time.sleep(intervalo)
        for nombre in list(consultas):
            consulta = consultas[nombre]
            if consulta.isActive:
                continue

            motivo = consulta.exception()
            logger.error("LA CONSULTA %s SE HA DETENIDO: %s", nombre,
                         str(motivo).strip().splitlines()[0] if motivo else "sin excepcion")

            if reinicios[nombre] >= max_reinicios:
                logger.error("Agotados los %d reinicios de %s; se abandona esa consulta",
                             max_reinicios, nombre)
                del consultas[nombre]
                continue

            reinicios[nombre] += 1
            logger.warning("Relanzando %s desde su checkpoint (reinicio %d de %d)...",
                           nombre, reinicios[nombre], max_reinicios)
            try:
                consultas[nombre] = arrancadores[nombre]()
                if registro:
                    registro.seguir(consultas.values())
            except Exception as exc:
                logger.error("No se pudo relanzar %s: %s", nombre, exc)
                del consultas[nombre]

    logger.error("No queda ninguna consulta activa; el job termina")
    return 1
