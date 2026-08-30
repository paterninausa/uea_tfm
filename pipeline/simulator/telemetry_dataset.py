"""
Interpretacion del dataset historico de ASHRAE.

Aqui vive todo lo que significa "convertir el Parquet de ASHRAE en trafico de
sensores": que subconjunto se reproduce y con que marcas de tiempo lo hace.

AQUI NO SE SABE QUE EXISTE MQTT. Este modulo decide QUE filas se reproducen y
con que marcas de tiempo; el topico, el payload y la conexion son cosa del
productor, en `mqtt_simulator.py`. La frontera se nota en que
aqui no hay una sola linea de protocolo, y alli no hay ninguna decision sobre los
datos.

Vive en un fichero aparte del simulador porque el orden en que se aplican el
filtrado, el recorte y el rebase temporal es una decision con consecuencias
medidas, y merece un sitio donde este documentada y no mezclada con el codigo de
publicacion. Estuvo en `common/replay.py` mientras habia dos productores; al
quedar uno solo, se movio aqui.

UN SENSOR ES EL PAR (edificio, tipo de medidor). Un mismo edificio con medidor
de electricidad y de agua fria son dos sensores con series independientes; el
subconjunto en uso tiene 652.
"""

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

RUTA_TELEMETRIA = Path(__file__).resolve().parents[1] / "data" / "ashrae_telemetry.parquet"


# --------------------------------------------------------------------------
# Seleccion del subconjunto a publicar
# --------------------------------------------------------------------------
def cargar(telemetry_path: Path) -> pd.DataFrame:
    """Carga la tabla de hechos.

    No necesita la dimension de edificios: todo lo que identifica a un medidor
    —`building_id` y `meter_type`— viaja ya en la propia lectura. El resto de
    atributos los incorpora Spark mas adelante con un broadcast join.
    """
    if not telemetry_path.exists():
        raise FileNotFoundError(
            f"No se encontro {telemetry_path}. Genera los datos primero: "
            "python pipeline/data/prepare_ashrae.py"
        )

    df = pd.read_parquet(telemetry_path)
    logger.info("Cargados %s eventos | %s sensores | %s edificios",
                f"{len(df):,}",
                f"{df.groupby(['building_id', 'meter_type']).ngroups:,}",
                f"{df['building_id'].nunique():,}")
    return df


def filtrar_sensores(df: pd.DataFrame, max_sensores: int | None) -> pd.DataFrame:
    """Se queda con los primeros N sensores en orden determinista.

    El orden fijo importa para el Objetivo 5: hace que la seleccion de 100
    sensores sea un SUBCONJUNTO de la de 250, esta de la de 500, y asi. La
    degradacion de throughput se mide entonces sobre los mismos sensores mas
    otros, y no comparando dos muestras aleatorias distintas, que no serian
    comparables entre si.
    """
    if not max_sensores:
        return df

    sensores = (df[["building_id", "meter_type"]].drop_duplicates()
                .sort_values(["building_id", "meter_type"]).reset_index(drop=True))
    if max_sensores >= len(sensores):
        logger.info("Se pidieron %d sensores y solo hay %d: se usan todos",
                    max_sensores, len(sensores))
        return df

    df = df.merge(sensores.head(max_sensores), on=["building_id", "meter_type"], how="inner")
    logger.info("Filtrado a %d sensores -> %s eventos", max_sensores, f"{len(df):,}")
    return df


def filtrar_ultimas_semanas(df: pd.DataFrame, semanas: int) -> pd.DataFrame:
    """Se queda con las ULTIMAS `semanas` de datos, medidas desde la marca mas
    reciente del conjunto.

    Es la seleccion de ventana de la demo. Con el Parquet ya reubicado por
    `prepare_ashrae.py --fecha-final`, la ultima marca cae en fecha reciente, asi
    que la cola de N semanas termina en el presente y los paneles de Grafana
    configurados a "ultimas N semanas" se llenan de izquierda a derecha segun
    avanza el replay. Sustituye al viejo `--limite` (que tomaba el PREFIJO —las
    primeras N semanas de 2016— y dependia de `--traer-a now` para reanclarlo).
    """
    corte = df["timestamp"].max() - pd.Timedelta(weeks=semanas)
    recorte = df[df["timestamp"] >= corte].reset_index(drop=True)
    logger.info("Ultimas %d semanas: %s -> %s (%s eventos de %s)", semanas,
                recorte["timestamp"].min(), recorte["timestamp"].max(),
                f"{len(recorte):,}", f"{len(df):,}")
    return recorte


def rebasar(df: pd.DataFrame, destino: str) -> pd.DataFrame:
    """Desplaza las marcas de tiempo para que la ULTIMA caiga en `destino`.

    HERRAMIENTA DE MEDICION, NO DEL CAMINO DE DATOS. La reubicacion al presente
    del Parquet que ven Grafana y Power BI la hace ahora `prepare_ashrae.py
    --fecha-final`, de una vez y fijada en disco. Aqui se conserva por un motivo
    distinto: `load_ladder.py` lo usa para que cada peldano reciba marcas nuevas
    —y por tanto claves naturales nuevas— de forma que el recuento de filas
    persistidas por peldano y la deteccion de drenaje por `count(*)` funcionen.
    Sin ese reanclaje, un peldano que republica los mismos eventos (el base
    repetido en caliente, o el solape entre 100/250/500 sensores) haria UPSERT
    idempotente, `count(*)` no creceria y la herramienta reportaria una perdida
    falsa. La demo NO debe usarlo: para acotar su ventana esta `--ultimas-semanas`.

    Se aplica un unico offset constante a todo el conjunto, de modo que las
    distancias relativas entre eventos —y por tanto los ciclos diario y
    estacional— se conservan intactas.

    Se ancla la ULTIMA marca y no la primera para que todo el historico quede en
    el pasado respecto al instante indicado. Anclando la primera, el replay
    acelerado generaria marcas en el futuro, y un evento con fecha futura envenena
    el watermark de Spark: adelanta el reloj de evento y hace que los eventos
    ordenados posteriores se descarten en silencio por tardios.
    """
    instante = datetime.now(timezone.utc) if destino == "now" else datetime.fromisoformat(destino)
    if instante.tzinfo is None:
        instante = instante.replace(tzinfo=timezone.utc)

    offset = pd.Timestamp(instante).tz_localize(None) - df["timestamp"].max()
    df = df.copy()
    df["timestamp"] = df["timestamp"] + offset
    logger.info("Marcas desplazadas %+.1f dias: el rango pasa a ser %s -> %s",
                offset.total_seconds() / 86400, df["timestamp"].min(), df["timestamp"].max())
    return df


def preparar(telemetry_path: Path | None = None, max_sensores: int | None = None,
             limite: int | None = None, ultimas_semanas: int | None = None,
             rebase_end: str | None = None, df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Aplica los pasos EN EL ORDEN QUE IMPORTA y devuelve lo publicable.

    El orden no es arbitrario y por eso esta encapsulado aqui en vez de repetido
    en cada productor:

    1. Filtrar sensores antes que nada, para que el recorte posterior caiga sobre
       el subconjunto de sensores pedido y no sobre el dataset entero.
    2. Ordenar cronologicamente: el watermark de Spark asume que el tiempo de
       evento avanza, y un flujo desordenado haria que se descartaran lecturas
       como tardias.
    3. Recortar la ventana DESPUES de ordenar. Dos selecciones, para dos usos
       distintos y NO combinables:
         - `ultimas_semanas`: la COLA de N semanas -> la demo, cuya ventana debe
           terminar en el presente (el Parquet ya viene reubicado a fecha reciente).
         - `limite`: el PREFIJO de N eventos -> la escalera de carga, que solo
           necesita publicar un numero fijo por peldano.
    4. Rebasar al final, sobre lo que realmente se va a publicar. SOLO lo usa la
       escalera de carga (ver `rebasar`); la demo no lo toca.

    Se admite un DataFrame ya cargado (`df`) para la escalera de carga: son
    5,68 millones de filas y releer el Parquet en cada peldano anadiria a la
    medicion un tiempo de arranque que no tiene nada que ver con el pipeline.
    """
    if df is None:
        df = cargar(telemetry_path)
    df = filtrar_sensores(df, max_sensores)
    df = df.sort_values("timestamp").reset_index(drop=True)
    if ultimas_semanas:
        df = filtrar_ultimas_semanas(df, ultimas_semanas)
    if limite:
        df = df.head(limite)
    if rebase_end:
        df = rebasar(df, rebase_end)
    return df


def anadir_argumentos_dataset(p: argparse.ArgumentParser) -> None:
    """Opciones de seleccion del subconjunto, identicas en ambos productores."""
    p.add_argument("--telemetry", type=Path, default=RUTA_TELEMETRIA,
                   help="Tabla de hechos generada por prepare_ashrae.py")
    p.add_argument("--limite", dest="limit", metavar="N", type=int, default=None,
                   help="Numero maximo de eventos a publicar (PREFIJO temporal). Para la "
                        "escalera de carga: un numero fijo de eventos por peldano")
    p.add_argument("--ultimas-semanas", dest="ultimas_semanas", metavar="N", type=int, default=None,
                   help="Publica solo la COLA de las ultimas N semanas del historico. Para la "
                        "demo en vivo: con el Parquet ya reubicado por prepare_ashrae.py "
                        "--fecha-final, la ventana termina en el presente")
    p.add_argument("--max-sensors", type=int, default=None,
                   help="Publica solo los primeros N sensores, en orden determinista. "
                        "Para la escalera de carga del Objetivo 5: 100, 250, 500, 652")
    p.add_argument("--traer-a", dest="rebase_end", metavar="ISO|now", default=None,
                   help="HERRAMIENTA DE MEDICION (load_ladder), no para la demo. Desplaza las "
                        "marcas para que la ultima caiga en este instante, dando a cada peldano "
                        "claves naturales nuevas. La reubicacion del camino de datos la hace "
                        "prepare_ashrae.py --fecha-final")
