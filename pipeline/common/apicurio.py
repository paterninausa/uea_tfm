"""
Resolucion del esquema contra Apicurio (API compatible con Confluent) y formato
de cable de los eventos.

El productor serializa con el `AvroSerializer` de confluent-kafka, que escribe el
formato de cable de facto del ecosistema Kafka:

    [ 1 byte  ] byte magico 0x00
    [ 4 bytes ] id del esquema en el registro (big-endian)
    [ resto   ] payload Avro binario schemaless

Este modulo comparte con el consumidor (Spark) y con las herramientas de medida
la resolucion del esquema y el tamano de esa cabecera, para que ninguna pieza
pueda divergir de las demas.

El id que viaja en el cable es el que asigna la API ccompat del registro —el
protocolo de Confluent, subjects en un namespace plano—, no el globalId de la
API nativa de Apicurio: son espacios de identificadores distintos. La unica
registracion gobernada es la de ccompat (ver `pipeline/schemas/register_schema.py`).

NO CONFUNDIR CON `pipeline/schemas/register_schema.py`. Aquel es el script que
REGISTRA el contrato y sus reglas, y se ejecuta a mano cuando el esquema cambia.
Este es la biblioteca que RESUELVE el esquema para leer/escribir, y la importan
el bridge, el job de Spark y las herramientas de medida en cada arranque.
"""

import struct

from confluent_kafka.schema_registry import SchemaRegistryClient

from common.connection_args import TOPIC_RAW

DEFAULT_REGISTRY_URL = "http://localhost:8080"

# Subject del proyecto. El sufijo "-value" sigue la convencion TopicNameStrategy
# de Confluent (el esquema describe el VALOR de los mensajes del topico), asi que
# se DERIVA del nombre del topico en lugar de repetir el literal: si cambia
# KAFKA_TOPIC_RAW en .env, el subject lo sigue.
DEFAULT_SUBJECT = f"{TOPIC_RAW}-value"

# Formato de cable de Confluent: byte magico 0x00 + id de esquema de 4 bytes
# big-endian, delante del payload Avro schemaless.
MAGIC_BYTE = 0x00
HEADER_FORMAT = ">BI"  # 1 byte magico + 4 bytes de id
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 5


class SchemaRegistryError(RuntimeError):
    """No se pudo resolver un esquema contra el registro."""


def ccompat_url(base_url: str = DEFAULT_REGISTRY_URL) -> str:
    """Endpoint compatible con Confluent del registro."""
    return f"{base_url.rstrip('/')}/apis/ccompat/v7"


def schema_registry_client(base_url: str = DEFAULT_REGISTRY_URL) -> SchemaRegistryClient:
    """Cliente del registro que habla el protocolo de Confluent."""
    return SchemaRegistryClient({"url": ccompat_url(base_url)})


def latest_schema(client: SchemaRegistryClient,
                  subject: str = DEFAULT_SUBJECT) -> tuple[int, str]:
    """Devuelve (schema_id, schema_str) de la ultima version del subject.

    Lo usa el PRODUCTOR (bridge), que serializa siempre con la version vigente,
    y las herramientas que inyectan un evento suelto. El CONSUMIDOR usa
    `all_schemas`: necesita reconocer cualquier version que pueda haber en el
    topico, no solo la ultima.

    Se resuelve UNA vez al arrancar: adoptar una version nueva se hace
    reiniciando el servicio, no en caliente.
    """
    try:
        rs = client.get_latest_version(subject)
    except Exception as exc:
        raise SchemaRegistryError(
            f"No se pudo resolver el subject '{subject}' en el registro. "
            "Registralo antes: python pipeline/schemas/register_schema.py"
        ) from exc
    return rs.schema_id, rs.schema.schema_str


def all_schemas(client: SchemaRegistryClient,
                subject: str = DEFAULT_SUBJECT) -> dict[int, str]:
    """Devuelve {schema_id: schema_str} de TODAS las versiones registradas del subject.

    El consumidor (Spark) decodifica cada mensaje con el esquema con el que se
    escribio: la cabecera de cable lleva el schema_id, y con este mapa el job
    elige el `from_avro` correcto por fila. Asi varias versiones del contrato
    pueden convivir en el topico y el productor y el consumidor se despliegan
    sin coordinar una ventana comun.

    Se resuelve UNA vez al arrancar. Una version registrada DESPUES no se
    reconoce hasta reiniciar el proceso; el orden de despliegue —consumidor
    antes que productor— garantiza que nunca haya en Kafka bytes de una version
    que el job todavia no conozca (el productor tambien resuelve su esquema al
    arrancar, con `latest_schema`).
    """
    try:
        versiones = client.get_versions(subject)
    except Exception as exc:
        raise SchemaRegistryError(
            f"No se pudieron listar las versiones del subject '{subject}' en el registro. "
            "Registralo antes: python pipeline/schemas/register_schema.py"
        ) from exc
    if not versiones:
        raise SchemaRegistryError(
            f"El subject '{subject}' no tiene ninguna version registrada. "
            "Registralo antes: python pipeline/schemas/register_schema.py")
    esquemas: dict[int, str] = {}
    for v in versiones:
        rv = client.get_version(subject, v)
        esquemas[rv.schema_id] = rv.schema.schema_str
    return esquemas


def encode_header(schema_id: int) -> bytes:
    """Construye la cabecera de 5 bytes del formato de Confluent.

    El productor normal no la usa —la genera el `AvroSerializer`—; la usan las
    herramientas que inyectan directamente en Kafka saltandose el bridge, como
    la prueba de envenenamiento del watermark.
    """
    return struct.pack(HEADER_FORMAT, MAGIC_BYTE, schema_id)
