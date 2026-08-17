"""
Preparacion del dataset ASHRAE GEPIII para el pipeline del TFM.

Une las lecturas de contador con los metadatos de edificio, se queda con el
subconjunto de emplazamientos elegido y produce las tres tablas planas que
alimentan el pipeline.

Es un paso de UN SOLO USO, independiente del pipeline en ejecucion. Funciona
con los ficheros originales de Kaggle (CSV) o con los mismos datos exportados a
Parquet; el formato se detecta por la extension.

    train + building_metadata  ->  ashrae_telemetry.parquet
                                   ashrae_buildings.parquet
                                   ashrae_sensor_baseline.parquet

SUBCONJUNTO ELEGIDO: emplazamientos 2, 3 y 5.

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

Produce TRES ficheros, con nombres fijos, en este mismo directorio:

  - `ashrae_telemetry.parquet`       la tabla de hechos, lo que emite el contador
  - `ashrae_buildings.parquet`       la dimension con los atributos del edificio
  - `ashrae_sensor_baseline.parquet` los cuartiles del historico de cada contador,
                                     que usa Spark para detectar picos atipicos y
                                     Power BI para ajustar el umbral

Los nombres NO son configurables, y es deliberado: el simulador y el job de
Spark ya los tienen cableados como valores por defecto, asi que un nombre
distinto no lo seguiria nadie. Ademas, al no derivarse unas rutas de otras, dos
salidas no pueden colisionar y sobra la comprobacion que antes hacia falta.

Uso:
    python prepare_ashrae.py
    python prepare_ashrae.py --train ./raw/train.csv --metadata ./raw/building_metadata.csv
    python prepare_ashrae.py --sites 2 3 5
"""

import argparse
from pathlib import Path

import pandas as pd

# Codigos de contador segun la documentacion de la competicion.
METER_TYPES = {0: "electricity", 1: "chilledwater", 2: "steam", 3: "hotwater"}

# Emplazamientos del subconjunto. Ver el razonamiento en el docstring.
DEFAULT_SITES = (2, 3, 5)

# Nombres de salida, fijos: los consumidores del pipeline los tienen cableados.
AQUI = Path(__file__).parent
SALIDA_TELEMETRIA = AQUI / "ashrae_telemetry.parquet"
SALIDA_EDIFICIOS = AQUI / "ashrae_buildings.parquet"
SALIDA_LINEA_BASE = AQUI / "ashrae_sensor_baseline.parquet"


def read_any(path: Path) -> pd.DataFrame:
    """Lee CSV o Parquet segun la extension del fichero."""
    if not path.exists():
        raise FileNotFoundError(f"No se encontro {path}")
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_parquet(path)


def prepare(train_path: Path, meta_path: Path, sites: tuple) -> None:
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

    # meter_type es una DECODIFICACION del codigo de contador, no un campo
    # derivado: la correspondencia 0->electricity ... 3->hotwater viene de la
    # documentacion de la competicion y es biyectiva, asi que no anade ni pierde
    # informacion. Se prefiere al codigo crudo porque permite declararlo como
    # enum en Avro, que se serializa exactamente igual (un indice varint) pero
    # deja el dominio explicito en el contrato.
    df["meter_type"] = df["meter"].map(METER_TYPES)
    if df["meter_type"].isna().any():
        codigos = sorted(df.loc[df["meter_type"].isna(), "meter"].unique())
        raise ValueError(f"Codigos de contador desconocidos en los datos: {codigos}")

    # ---- TABLA DE HECHOS: solo lo que emitiria el contador -------------------
    # Sin event_id, sin sensor_id y sin caracteristicas del edificio. Un contador
    # real envia quien es y cuanto midio; cualquier otra cosa la
    # anadiriamos nosotros y dejaria de reproducir su comportamiento.
    telemetria = df[["building_id", "meter_type", "timestamp", "meter_reading"]]
    # Orden cronologico: el simulador reproduce el historico en secuencia y el
    # watermark de Spark asume que el tiempo de evento avanza. Un fichero
    # desordenado haria que se descartaran lecturas como tardias.
    telemetria = telemetria.sort_values("timestamp").reset_index(drop=True)

    # ---- TABLA DE DIMENSION: caracteristicas estaticas del edificio ----------
    # No viajan en cada evento: son atributos del edificio, no medidas del
    # sensor. Spark las incorpora con un broadcast join y Power BI las consume
    # como dimension de un esquema en estrella.
    dimension = (
        meta_sub[["building_id", "site_id", "primary_use",
                  "square_feet", "year_built", "floor_count"]]
        .sort_values("building_id").reset_index(drop=True)
    )

    # ---- LINEA BASE POR SENSOR: referencia para la deteccion de picos --------
    # Mediana y cuartiles del historico de cada contador. El job de Spark la
    # incorpora por broadcast join y marca como atipica toda lectura que supere
    # p75 + 5*IQR de su PROPIO sensor. Un umbral global no serviria: el consumo
    # va de 801 a 850.354 pies cuadrados de edificio y de un medio a otro cambia
    # la unidad, asi que cada contador solo es comparable consigo mismo.
    #
    # Que la referencia se calcule sobre el mismo historico que luego se
    # reproduce es lo habitual en cualquier linea base: describe el
    # comportamiento normal observado. En produccion se recalcularia
    # periodicamente con los datos acumulados.
    g = telemetria.groupby(["building_id", "meter_type"], observed=True)["meter_reading"]
    linea_base = g.agg(
        baseline_p25=lambda s: s.quantile(0.25),
        baseline_p50="median",
        baseline_p75=lambda s: s.quantile(0.75),
    ).reset_index()
    linea_base["baseline_iqr"] = linea_base.baseline_p75 - linea_base.baseline_p25

    resumen(telemetria, dimension, linea_base)

    print()
    for datos, ruta in ((telemetria, SALIDA_TELEMETRIA),
                        (dimension, SALIDA_EDIFICIOS),
                        (linea_base, SALIDA_LINEA_BASE)):
        ruta.parent.mkdir(parents=True, exist_ok=True)
        datos.to_parquet(ruta, index=False)
        print(f"Escrito {ruta} ({ruta.stat().st_size / 1024:.0f} KB)")


def resumen(tel: pd.DataFrame, dim: pd.DataFrame, base: pd.DataFrame) -> None:
    n = len(tel)
    print("\n--- Tabla de hechos (lo que emite el sensor) ---")
    print(f"  columnas               : {list(tel.columns)}")
    print(f"  eventos                : {n:,}")
    print(f"  sensores (edificio + contador): {tel.groupby(['building_id','meter_type'], observed=True).ngroups}")
    print(f"  rango temporal         : {tel['timestamp'].min()} -> {tel['timestamp'].max()}")
    print(f"  nulos                  : {int(tel.isna().sum().sum())}")

    # La clave natural se COMPRUEBA, no se asume: es lo que sostiene la
    # idempotencia del pipeline. Si dejara de ser unica, el UPSERT de
    # PostgreSQL perderia eventos en silencio.
    clave = ["building_id", "meter_type", "timestamp"]
    grupos = tel.groupby(clave, observed=True).ngroups
    print(f"\n  clave natural {tuple(clave)}:")
    print(f"    grupos = {grupos:,} sobre {n:,} filas -> {'UNICA' if grupos == n else 'COLISIONA'}")
    # (building_id, timestamp) no basta: un edificio puede tener varios
    # contadores midiendo a la misma hora.
    sin_contador = tel.groupby(["building_id", "timestamp"], observed=True).ngroups
    print(f"    sin meter_type seria {sin_contador:,} grupos -> perderia {n - sin_contador:,} eventos")

    ceros = (tel["meter_reading"] == 0).sum()
    print(f"\n  lecturas a cero        : {ceros:,} ({100*ceros/n:.1f}%)")
    print("  (no se eliminan: son dato real y material para el informe de anomalias)")

    print("\n  eventos por tipo de contador:")
    for tipo, c in tel["meter_type"].value_counts().items():
        print(f"    {tipo:<14} {c:>10,}")

    print("\n--- Tabla de dimension (atributos del edificio) ---")
    print(f"  columnas               : {list(dim.columns)}")
    print(f"  edificios              : {len(dim):,}")
    print(f"  emplazamientos         : {sorted(int(s) for s in dim['site_id'].unique())}")
    print(f"  usos de edificio       : {dim['primary_use'].nunique()}")
    print("  nulos por columna:")
    nulos = dim.isna().sum()
    for col, c in nulos[nulos > 0].items():
        print(f"    {col:<16} {c:>5,} de {len(dim)}  ({100*c/len(dim):.1f}%)")

    print("\n--- Linea base por sensor (referencia de deteccion de picos) ---")
    print(f"  sensores               : {len(base):,}")
    print(f"  columnas               : {list(base.columns)}")
    sin_dispersion = (base.baseline_iqr == 0).sum()
    print(f"  sensores con IQR = 0   : {sin_dispersion} (quedan exentos de la regla de picos)")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prepara el subconjunto ASHRAE para el pipeline (TFM)")
    p.add_argument("--train", type=Path, default=AQUI / "raw/train.parquet",
                   help="Lecturas de contador (train.csv de Kaggle o su equivalente en Parquet)")
    p.add_argument("--metadata", type=Path, default=AQUI / "raw/building_metadata.parquet",
                   help="Metadatos de edificio (building_metadata.csv o Parquet)")
    p.add_argument("--sites", type=int, nargs="+", default=list(DEFAULT_SITES),
                   help="Emplazamientos a incluir (por defecto 2 3 5)")
    return p.parse_args()


if __name__ == "__main__":
    a = parse_args()
    prepare(a.train, a.metadata, tuple(a.sites))
