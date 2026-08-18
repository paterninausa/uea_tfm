"""
Reproduccion del dataset historico como mensajes MQTT.

Aqui vive todo lo que significa "convertir el Parquet de ASHRAE en trafico de
sensores": que subconjunto se publica, con que marcas de tiempo, en que topico y
con que payload.

Esta separado de `pipeline/simulator/mqtt_simulator.py` porque son dos cosas
distintas: este modulo INTERPRETA el dataset y el simulador lo EMITE por MQTT. La
frontera se nota en que aqui no hay ni una linea de protocolo, y alli no hay
ninguna decision sobre los datos.

Hubo un segundo productor —un generador de carga asincrono— que tambien lo usaba.
Se retiro en agosto de 2026 al asumir el simulador su funcion, pero la separacion
se mantiene: el orden en que se aplican el filtrado, el recorte y el rebase
temporal es una decision con consecuencias medidas, y merece un sitio propio
donde este documentada y no mezclada con el codigo de publicacion.

UN SENSOR ES EL PAR (edificio, tipo de contador). Un mismo edificio con contador
de electricidad y de agua fria son dos sensores con series independientes; el
subconjunto en uso tiene 652.

EL TOPICO SOLO IDENTIFICA AL SENSOR. No lleva nivel de emplazamiento: `site_id`
es derivable de `building_id` a traves de la tabla de dimension. Ponerlo tambien
en el topico creaba una segunda fuente de verdad —topico y dimension podrian
discrepar si un edificio se reasignara— sin que nada lo consumiera: el bridge se
suscribe a `iot/#` y nunca parte el topico. El emplazamiento entra en el analisis
donde importa, en el broadcast join de Spark contra la dimension.

EL PAYLOAD SOLO LLEVA LO QUE EMITIRIA EL CONTADOR: identidad, instante y medida,
mas `sim_publish_ts`.
"""

import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

TOPIC_TEMPLATE = "iot/{building_id}/{meter_type}/telemetry"

RUTA_TELEMETRIA = Path(__file__).resolve().parents[1] / "data" / "ashrae_telemetry.parquet"


# --------------------------------------------------------------------------
# Seleccion del subconjunto a publicar
# --------------------------------------------------------------------------
def cargar(telemetry_path: Path) -> pd.DataFrame:
    """Carga la tabla de hechos. No necesita la dimension: el topico se
    construye solo con los campos que emite el propio contador."""
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


def rebasar(df: pd.DataFrame, destino: str) -> pd.DataFrame:
    """Desplaza las marcas de tiempo para que la ULTIMA caiga en `destino`.

    Los datos son de 2016. Sin desplazarlos, un panel de Grafana configurado a
    "ultimas 6 horas" sale vacio y la demostracion en vivo pierde el efecto de
    tiempo real. Se aplica un unico offset constante a todo el conjunto, de modo
    que las distancias relativas entre eventos —y por tanto los ciclos diario y
    estacional— se conservan intactas.

    Se ancla la ULTIMA marca y no la primera para que todo el historico quede en
    el pasado respecto al instante indicado, que es lo que esperan los paneles.
    Anclando la primera, el replay acelerado generaria marcas en el futuro, y un
    evento con fecha futura envenena el watermark de Spark: adelanta el reloj de
    evento y hace que los eventos ordenados posteriores se descarten en silencio
    por tardios.
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
             limite: int | None = None, rebase_end: str | None = None,
             df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Aplica los cuatro pasos EN EL ORDEN QUE IMPORTA y devuelve lo publicable.

    El orden no es arbitrario y por eso esta encapsulado aqui en vez de repetido
    en cada productor:

    1. Filtrar sensores antes que nada, para que el recorte por `limite` caiga
       sobre el subconjunto de sensores pedido y no sobre el dataset entero.
    2. Ordenar cronologicamente: el watermark de Spark asume que el tiempo de
       evento avanza, y un flujo desordenado haria que se descartaran lecturas
       como tardias.
    3. Recortar a `limite` DESPUES de ordenar, para publicar el prefijo temporal
       y no una muestra arbitraria.
    4. Rebasar al final, sobre lo que realmente se va a publicar. Al reves, el
       offset se calculaba sobre el dataset completo y un prefijo corto acababa
       cayendo casi un ano atras, con lo que los paneles de "ultimas 24 horas"
       salian vacios igualmente.

    Se admite un DataFrame ya cargado (`df`) para la escalera de carga: son
    5,68 millones de filas y releer el Parquet en cada peldano anadiria a la
    medicion un tiempo de arranque que no tiene nada que ver con el pipeline.
    """
    if df is None:
        df = cargar(telemetry_path)
    df = filtrar_sensores(df, max_sensores)
    df = df.sort_values("timestamp").reset_index(drop=True)
    if limite:
        df = df.head(limite)
    if rebase_end:
        df = rebasar(df, rebase_end)
    return df


# --------------------------------------------------------------------------
# Mensaje MQTT
# --------------------------------------------------------------------------
def build_topic(fila) -> str:
    return TOPIC_TEMPLATE.format(building_id=fila.building_id, meter_type=fila.meter_type)


def build_payload(fila) -> dict:
    """Payload con los campos del contrato y nada mas.

    `timestamp` se envia como cadena ISO-8601 y el bridge lo convierte a epoch
    en milisegundos, que es lo que declara el esquema Avro. Se mantiene en ISO
    aqui porque hace legible el trafico al depurar con `mosquitto_sub`.

    `sim_publish_ts` se sella en el momento de construir el mensaje, no al cargar
    el dataset: es el origen de tiempo del KPI de latencia del Objetivo 1 y el
    unico campo del payload que no emite el contador.
    """
    return {
        "building_id": int(fila.building_id),
        "meter_type": str(fila.meter_type),
        "timestamp": fila.timestamp.isoformat(),
        "meter_reading": float(fila.meter_reading),
        "sim_publish_ts": int(time.time() * 1000),
    }


# --------------------------------------------------------------------------
# Argumentos comunes a los dos productores
# --------------------------------------------------------------------------
def anadir_argumentos_dataset(p: argparse.ArgumentParser) -> None:
    """Opciones de seleccion del subconjunto, identicas en ambos productores."""
    p.add_argument("--telemetry", type=Path, default=RUTA_TELEMETRIA,
                   help="Tabla de hechos generada por prepare_ashrae.py")
    p.add_argument("--limit", type=int, default=None,
                   help="Numero maximo de eventos a publicar")
    p.add_argument("--max-sensors", type=int, default=None,
                   help="Publica solo los primeros N sensores, en orden determinista. "
                        "Para la escalera de carga del Objetivo 5: 100, 250, 500, 652")
    p.add_argument("--rebase-end", metavar="ISO|now", default=None,
                   help="Desplaza las marcas de tiempo para que la ultima caiga en este "
                        "instante. Para la demostracion en vivo: los datos son de 2016 y "
                        "los paneles miran a fechas recientes")


def anadir_argumentos_mqtt(p: argparse.ArgumentParser, client_id: str) -> None:
    """Conexion al broker, identica en ambos productores."""
    p.add_argument("--broker-host", default="localhost")
    p.add_argument("--broker-port", type=int, default=1883)
    p.add_argument("--client-id", default=client_id)
    p.add_argument("--qos", type=int, default=1, choices=[0, 1, 2],
                   help="QoS MQTT (1 por defecto, ver Objetivo 1)")
