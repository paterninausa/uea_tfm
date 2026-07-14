"""
Conversion del dataset original de Kaggle (Power Telemetry) a Parquet.

Dataset: https://www.kaggle.com/datasets/khalilaraoui/power-telemetry
Fichero: Power_measurements.xlsx

Este script es un paso de preparacion de datos de UN SOLO USO: no forma
parte del pipeline en ejecucion (simulador -> NanoMQ -> Kafka -> PyFlink).
Solo convierte el .xlsx original a .parquet para que el simulador
(pipeline/simulator/mqtt_simulator.py) lo lea eficientemente.

Uso:
    python convert_to_parquet.py \
        --input ./raw/Power_measurements.xlsx \
        --output ./power_measurements.parquet
"""

import argparse
from pathlib import Path

import pandas as pd


def convert(input_path: Path, output_path: Path) -> None:
    if not input_path.exists():
        raise FileNotFoundError(
            f"No se encontro {input_path}. Descarga primero el dataset "
            "(ver README.md de este directorio)."
        )

    print(f"Leyendo {input_path} ...")
    df = pd.read_excel(input_path, engine="openpyxl")
    print(f"Filas: {len(df)}, columnas: {len(df.columns)}")
    print(f"Columnas: {list(df.columns)}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    size_mb = output_path.stat().st_size / 1_048_576
    print(f"Escrito {output_path} ({size_mb:.1f} MB)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convierte Power_measurements.xlsx (Kaggle) a Parquet"
    )
    parser.add_argument("--input", required=True, type=Path,
                         help="Ruta al Power_measurements.xlsx descargado de Kaggle")
    parser.add_argument("--output", required=True, type=Path,
                         help="Ruta de salida para el Parquet")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    convert(args.input, args.output)
