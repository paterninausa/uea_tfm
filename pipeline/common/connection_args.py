"""
Argumentos de conexion a las bases de datos y a Kafka, en un solo sitio.

Los comparten CINCO piezas: el job de Spark y las cuatro de `tools/`. Sin esto,
cada uno declararia sus propios `--timescale-host`, `--postgres-port`,
`--postgres-password`... y bastaria con que uno quedara desactualizado para que
una medicion fuera contra una base de datos distinta de la que escribe el
pipeline, que es el tipo de error que no da ningun sintoma: simplemente sale
vacio.

Los valores por defecto vienen de pipeline/.env (via python-dotenv), el mismo
archivo que lee Docker Compose para las variables ${...} de docker-compose.yml
-un solo sitio donde cambiar una contraseña, no dos que puedan desincronizarse.
Si no existe .env, se cae a los valores de pipeline/.env.example. Son
credenciales de desarrollo local, no secretos: el stack no se expone fuera de
la maquina.

Las contraseñas de las dos bases son variables SEPARADAS a proposito: hasta el
29-08-2026 compartian un unico --db-password, y eso hacia que cambiar la
credencial de una base (p.ej. Postgres, para Power BI) rompiera en silencio la
conexion de las herramientas a la otra (TimescaleDB, para Grafana) si no se
recordaba pasar el valor explicito en cada invocacion.
"""

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

# Ruta fija, no relativa al directorio de trabajo: los scripts se invocan
# desde sitios distintos (raiz del repo, pipeline/, etc.), y __file__ es lo
# unico que no depende de eso. connection_args.py vive en pipeline/common/,
# asi que .env esta un nivel por encima.
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Las dos mitades del doble sumidero, por su papel y no por su tecnologia:
# TimescaleDB sirve el consumo operacional y PostgreSQL el analitico.
TIMESCALE = "timescale"
POSTGRES = "postgres"


def anadir_argumentos_bd(p: argparse.ArgumentParser) -> None:
    p.add_argument("--timescale-host", default="localhost")
    p.add_argument("--timescale-port", type=int, default=5432)
    p.add_argument("--timescale-db", default="tfm_metrics")
    p.add_argument("--timescale-password",
                    default=os.environ.get("TIMESCALE_PASSWORD", "tfm_dev_password"))
    p.add_argument("--postgres-host", default="localhost")
    p.add_argument("--postgres-port", type=int, default=5433)
    p.add_argument("--postgres-db", default="tfm_analytics")
    p.add_argument("--postgres-password",
                    default=os.environ.get("POSTGRES_PASSWORD", "tfm_dev_password"))
    p.add_argument("--db-user", default="tfm")


def props_bd(args: argparse.Namespace, cual: str) -> dict:
    """Devuelve los parametros con las claves que espera `psycopg2.connect`."""
    if cual == TIMESCALE:
        host, port, db = args.timescale_host, args.timescale_port, args.timescale_db
        password = args.timescale_password
    elif cual == POSTGRES:
        host, port, db = args.postgres_host, args.postgres_port, args.postgres_db
        password = args.postgres_password
    else:
        raise ValueError(f"Base de datos desconocida: {cual!r}")
    return {"host": host, "port": port, "dbname": db,
            "user": args.db_user, "password": password}


def anadir_argumentos_kafka(p: argparse.ArgumentParser) -> None:
    p.add_argument("--bootstrap-servers", default="localhost:29092",
                   help="Listener EXTERNO de Kafka: el interno (kafka:9092) solo "
                        "resuelve desde dentro de la red de contenedores")
    p.add_argument("--topic", default="iot.telemetry.raw")
    p.add_argument("--dlq-topic", default="iot.telemetry.dlq")
