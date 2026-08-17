"""
Este script es un paso de preparacion de datos, solo convierte el .xlsx 
a .parquet para que el simulador (pipeline/simulator/mqtt_simulator.py) 
lo lea eficientemente.

Uso:
    python convert_to_parquet.py \
        --input ./raw/<name>.xlsx \
        --output ./<name>.parquet
"""

import argparse
from pathlib import Path

import pandas as pd


def convert(input_path: Path, output_path: Path) -> None:
    if not input_path.exists():
        raise FileNotFoundError(
            f"No se encontro {input_path}. Descarga primero el dataset"
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
        description="Convierte .xlsx a Parquet"
    )
    parser.add_argument("--input", required=True, type=Path,
                         help="Ruta del archivo .xlsx")
    parser.add_argument("--output", required=True, type=Path,
                         help="Ruta de salida para el Parquet")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    convert(args.input, args.output)
