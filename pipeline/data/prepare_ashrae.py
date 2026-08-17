"""
Preparacion del dataset ASHRAE GEPIII para el pipeline del TFM.

Une las lecturas de contador con los metadatos de edificio, se queda con el
subconjunto de emplazamientos elegido y produce un unico Parquet plano, que es
lo que reproduce el simulador MQTT.

Es un paso de UN SOLO USO, independiente del pipeline en ejecucion. Funciona
con los ficheros originales de Kaggle (CSV) o con los mismos datos exportados a
Parquet; el formato se detecta por la extension.

    train + building_metadata  ->  ashrae_telemetry.parquet

SUBCONJUNTO POR DEFECTO: emplazamientos 2, 3 y 5.

Se eligieron midiendo el dataset completo, no por conveniencia:

  - Cubren 652 sensores (pares edificio-contador), holgadamente por encima de
    los 500 concurrentes que exige el Objetivo 5, y permiten muestrear la carga
    base de 100 para medir la degradacion de throughput.
  - Reducen las combinaciones de agregacion de 193 a 46, y el volumen de 17,7 M
    a 5,7 M de eventos: unos 128 minutos de reproduccion frente a 6,6 horas.
  - El emplazamiento 3 aporta volumen con la mejor calidad del dataset (274
    sensores, 0,1% de lecturas a cero); el 2 aporta la variedad de contadores y
    el ciclo estacional de refrigeracion (el consumo de agua fria pasa de 152 en
    enero a 528 en agosto); el 5 tiene completitud del 100%.

El emplazamiento 14 queda excluido por construccion al no estar en la lista: se
comprobo por correlacion cruzada de su perfil diario que sus marcas de tiempo
van 5 horas por delante del resto (pico a las 18h frente a las 13-14h, con
correlacion de forma 0,99). Los demas emplazamientos estan en hora local y son
mutuamente comparables sin conversion.

Uso:
    python prepare_ashrae.py \
        --train ./raw/train.parquet \
        --metadata ./raw/building_metadata.parquet \
        --output ./ashrae_telemetry.parquet
"""

import argparse
from pathlib import Path

import pandas as pd

# Codigos de contador segun la documentacion de la competicion.
METER_TYPES = {0: "electricity", 1: "chilledwater", 2: "steam", 3: "hotwater"}

# Emplazamientos del subconjunto. Ver el razonamiento en el docstring.
DEFAULT_SITES = (2, 3, 5)


def read_any(path: Path) -> pd.DataFrame:
    """Lee CSV o Parquet segun la extension del fichero."""
    if not path.exists():
        raise FileNotFoundError(f"No se encontro {path}")
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_parquet(path)


def make_event_id(df: pd.DataFrame) -> pd.Series:
    """Identificador de evento DETERMINISTA y legible: B{edificio}-M{contador}-{epoch}.

    ASHRAE no trae identificador de evento y el pipeline lo necesita como clave
    primaria en PostgreSQL. Se deriva de (edificio, contador, instante), que es
    unico en el dataset porque cada contador tiene como mucho una lectura por
    hora.

    Que sea derivado y no un UUID aleatorio es deliberado: con un UUID,
    reprocesar el log de Kafka —la operacion basica de una arquitectura Kappa—
    insertaria filas duplicadas, porque cada reproceso inventaria
    identificadores nuevos. Siendo derivado, el mismo evento produce siempre la
    misma clave y el UPSERT lo absorbe.

    Se prefiere la forma legible a un hash: un identificador que aparece en el
    log de la DLQ dice directamente de que sensor e instante procede, sin tener
    que cruzarlo con nada. Ocupa lo mismo que un hash truncado.
    """
    epoch = df["timestamp"].astype("int64") // 10**9  # nanosegundos -> segundos
    return (
        "B" + df["building_id"].astype(str)
        + "-M" + df["meter"].astype(str)
        + "-" + epoch.astype(str)
    )


def prepare(train_path: Path, meta_path: Path, output_path: Path, sites: tuple) -> None:
    print(f"Leyendo metadatos de edificio: {meta_path}")
    meta = read_any(meta_path)
    meta_sub = meta[meta["site_id"].isin(sites)]
    print(f"  {len(meta)} edificios en total -> {len(meta_sub)} en los emplazamientos {list(sites)}")

    print(f"Leyendo lecturas de contador: {train_path}")
    train = read_any(train_path)
    print(f"  {len(train):,} lecturas en total")

    # El join hace tambien de filtro: solo sobreviven las lecturas de edificios
    # de los emplazamientos elegidos.
    df = train.merge(meta_sub, on="building_id", how="inner")
    print(f"  {len(df):,} lecturas tras filtrar por emplazamiento")

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["meter_type"] = df["meter"].map(METER_TYPES)
    if df["meter_type"].isna().any():
        codigos = sorted(df.loc[df["meter_type"].isna(), "meter"].unique())
        raise ValueError(f"Codigos de contador desconocidos en los datos: {codigos}")

    # Identidad del sensor simulado: el par edificio-contador. Un mismo edificio
    # con contador de electricidad y de agua fria son dos sensores distintos,
    # con series independientes.
    df["sensor_id"] = "B" + df["building_id"].astype(str) + "-M" + df["meter"].astype(str)
    df["event_id"] = make_event_id(df)

    columnas = [
        "event_id", "timestamp",
        "site_id", "building_id", "sensor_id",
        "meter", "meter_type", "meter_reading",
        "primary_use", "square_feet", "year_built", "floor_count",
    ]
    df = df[columnas]

    # Orden cronologico: el simulador reproduce el historico en secuencia, y el
    # watermark de Spark asume que el tiempo de evento avanza. Un fichero
    # desordenado haria que se descartaran lecturas como tardias.
    df = df.sort_values("timestamp").reset_index(drop=True)

    resumen(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    tam = output_path.stat().st_size / 1_048_576
    print(f"\nEscrito {output_path} ({tam:.1f} MB)")


def resumen(df: pd.DataFrame) -> None:
    print("\n--- Resumen del subconjunto ---")
    print(f"  eventos                : {len(df):,}")
    print(f"  sensores (edificio-contador): {df['sensor_id'].nunique():,}")
    print(f"  edificios              : {df['building_id'].nunique():,}")
    print(f"  emplazamientos         : {sorted(int(s) for s in df['site_id'].unique())}")
    print(f"  tipos de contador      : {sorted(df['meter_type'].unique())}")
    print(f"  usos de edificio       : {df['primary_use'].nunique()}")
    combos = df.groupby(["site_id", "primary_use", "meter_type"]).ngroups
    print(f"  combinaciones de agregacion (site x uso x contador): {combos}")
    print(f"  rango temporal         : {df['timestamp'].min()} -> {df['timestamp'].max()}")
    print(f"  event_id unicos        : {df['event_id'].is_unique}")

    print("\n  nulos por columna (los que existen):")
    nulos = df.isna().sum()
    for col, n in nulos[nulos > 0].items():
        print(f"    {col:<16} {n:>8,}  ({100*n/len(df):.1f}%)")
    if (nulos == 0).all():
        print("    ninguna")

    ceros = (df["meter_reading"] == 0).sum()
    print(f"\n  lecturas a cero        : {ceros:,} ({100*ceros/len(df):.1f}%)")
    print("  (no se eliminan: son parte del dato real y sirven de material para"
          " el informe de deteccion de anomalias)")

    print("\n  eventos por tipo de contador:")
    for tipo, n in df["meter_type"].value_counts().items():
        print(f"    {tipo:<14} {n:>10,}")


def parse_args() -> argparse.Namespace:
    aqui = Path(__file__).parent
    p = argparse.ArgumentParser(description="Prepara el subconjunto ASHRAE para el pipeline (TFM)")
    p.add_argument("--train", type=Path, default=aqui / "raw/train.parquet",
                   help="Lecturas de contador (train.csv de Kaggle o su equivalente en Parquet)")
    p.add_argument("--metadata", type=Path, default=aqui / "raw/building_metadata.parquet",
                   help="Metadatos de edificio (building_metadata.csv o Parquet)")
    p.add_argument("--output", type=Path, default=aqui / "ashrae_telemetry.parquet")
    p.add_argument("--sites", type=int, nargs="+", default=list(DEFAULT_SITES),
                   help="Emplazamientos a incluir (por defecto 2 3 5)")
    return p.parse_args()


if __name__ == "__main__":
    a = parse_args()
    prepare(a.train, a.metadata, a.output, tuple(a.sites))
