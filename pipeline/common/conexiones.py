"""
Parametros de conexion a las bases de datos y a Kafka, en un solo sitio.

Los comparten el job de Spark y las cuatro herramientas de `herramientas/`. Sin
esto, cada uno declararia sus propios `--timescale-host`, `--postgres-port`,
`--db-password`... y bastaria con que uno quedara desactualizado para que una
medicion fuera contra una base de datos distinta de la que escribe el pipeline,
que es el tipo de error que no da ningun sintoma: simplemente sale vacio.

Los valores por defecto son los del `docker-compose.yml` del proyecto. Son
credenciales de desarrollo local, no secretos: el stack no se expone fuera de la
maquina.
"""

import argparse

# Las dos mitades del doble sumidero, por su papel y no por su tecnologia:
# TimescaleDB sirve el consumo operacional y PostgreSQL el analitico.
TIMESCALE = "timescale"
POSTGRES = "postgres"


def anadir_argumentos_bd(p: argparse.ArgumentParser) -> None:
    p.add_argument("--timescale-host", default="localhost")
    p.add_argument("--timescale-port", type=int, default=5432)
    p.add_argument("--timescale-db", default="tfm_metrics")
    p.add_argument("--postgres-host", default="localhost")
    p.add_argument("--postgres-port", type=int, default=5433)
    p.add_argument("--postgres-db", default="tfm_analytics")
    p.add_argument("--db-user", default="tfm")
    p.add_argument("--db-password", default="tfm_dev_password")


def props_bd(args: argparse.Namespace, cual: str) -> dict:
    """Devuelve los parametros con las claves que espera `psycopg2.connect`."""
    if cual == TIMESCALE:
        host, port, db = args.timescale_host, args.timescale_port, args.timescale_db
    elif cual == POSTGRES:
        host, port, db = args.postgres_host, args.postgres_port, args.postgres_db
    else:
        raise ValueError(f"Base de datos desconocida: {cual!r}")
    return {"host": host, "port": port, "dbname": db,
            "user": args.db_user, "password": args.db_password}


def anadir_argumentos_kafka(p: argparse.ArgumentParser) -> None:
    p.add_argument("--bootstrap-servers", default="localhost:29092",
                   help="Listener EXTERNO de Kafka: el interno (kafka:9092) solo "
                        "resuelve desde dentro de la red de contenedores")
    p.add_argument("--topic", default="iot.telemetry.raw")
    p.add_argument("--dlq-topic", default="iot.telemetry.dlq")
