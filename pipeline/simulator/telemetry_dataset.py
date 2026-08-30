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
ordenado cronologico y el recorte de ventana es una decision con consecuencias
medidas, y merece un sitio donde este documentada y no mezclada con el codigo de
publicacion. Estuvo en `common/replay.py` mientras habia dos productores; al
quedar uno solo, se movio aqui.

Las marcas de tiempo NO se tocan aqui: vienen ya datadas en el presente desde
`prepare_ashrae.py --fecha-final`. No hay ningun desplazamiento en tiempo de
ejecucion.

UN SENSOR ES EL PAR (edificio, tipo de medidor). Un mismo edificio con medidor
de electricidad y de agua fria son dos sensores con series independientes; el
subconjunto en uso tiene 652.
"""

import argparse
import logging
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


def filtrar_ultimas_semanas(df: pd.DataFrame, semanas: int) -> pd.DataFrame:
    """Se queda con las ULTIMAS `semanas` de datos, medidas desde la marca mas
    reciente del conjunto.

    Es la seleccion de ventana de la demo. Con el Parquet ya reubicado por
    `prepare_ashrae.py --fecha-final`, la ultima marca cae en fecha reciente, asi
    que la cola de N semanas termina en el presente y los paneles de Grafana
    configurados a "ultimas N semanas" se llenan de izquierda a derecha segun
    avanza el replay. Es la alternativa a `--limite`, que toma el PREFIJO (el
    principio del historico) en vez de la cola.
    """
    corte = df["timestamp"].max() - pd.Timedelta(weeks=semanas)
    recorte = df[df["timestamp"] >= corte].reset_index(drop=True)
    logger.info("Ultimas %d semanas: %s -> %s (%s eventos de %s)", semanas,
                recorte["timestamp"].min(), recorte["timestamp"].max(),
                f"{len(recorte):,}", f"{len(df):,}")
    return recorte


def preparar(telemetry_path: Path | None = None,
             limite: int | None = None, ultimas_semanas: int | None = None,
             df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Aplica los pasos EN EL ORDEN QUE IMPORTA y devuelve lo publicable.

    El orden no es arbitrario y por eso esta encapsulado aqui en vez de repetido
    en cada productor:

    1. Ordenar cronologicamente: el watermark de Spark asume que el tiempo de
       evento avanza, y un flujo desordenado haria que se descartaran lecturas
       como tardias.
    2. Recortar la ventana DESPUES de ordenar. Dos selecciones, para dos usos
       distintos y NO combinables:
         - `ultimas_semanas`: la COLA de N semanas -> la demo, cuya ventana debe
           terminar en el presente (el Parquet ya viene reubicado a fecha reciente).
         - `limite`: el PREFIJO de N eventos.

    Las marcas de tiempo se publican TAL CUAL vienen del Parquet: la reubicacion
    al presente la hace `prepare_ashrae.py --fecha-final`, de una vez y fijada en
    disco. No hay desplazamiento en tiempo de ejecucion.

    Se admite un DataFrame ya cargado (`df`) para evitar releer 5,68 millones de
    filas del Parquet en invocaciones repetidas.
    """
    if df is None:
        df = cargar(telemetry_path)
    df = df.sort_values("timestamp").reset_index(drop=True)
    if ultimas_semanas:
        df = filtrar_ultimas_semanas(df, ultimas_semanas)
    if limite:
        df = df.head(limite)
    return df


def anadir_argumentos_dataset(p: argparse.ArgumentParser) -> None:
    """Opciones de seleccion del subconjunto, identicas en ambos productores."""
    p.add_argument("--telemetry", type=Path, default=RUTA_TELEMETRIA,
                   help="Tabla de hechos generada por prepare_ashrae.py")
    p.add_argument("--limite", dest="limit", metavar="N", type=int, default=None,
                   help="Numero maximo de eventos a publicar (PREFIJO temporal)")
    p.add_argument("--ultimas-semanas", dest="ultimas_semanas", metavar="N", type=int, default=None,
                   help="Publica solo la COLA de las ultimas N semanas del historico. Para la "
                        "demo en vivo: con el Parquet ya reubicado por prepare_ashrae.py "
                        "--fecha-final, la ventana termina en el presente")
