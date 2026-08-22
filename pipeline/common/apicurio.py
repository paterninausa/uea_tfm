"""
Cliente del registro de esquemas Apicurio y formato de cable de los eventos.

Dos cosas que van juntas porque una depende de la otra: resolver el esquema Avro
vigente contra Apicurio, y la cabecera de 4 bytes —el `globalId` en big-endian—
que precede al payload Avro schemaless. Sin esa cabecera, el consumidor tendria
que asumir con que version se escribio cada mensaje; con ella, cada evento
declara la suya.

NO SE INCLUYE EL BYTE MAGICO del formato de Confluent. Ese byte identifica la
convencion de transporte y permitiria distinguir versiones del propio formato de
cabecera, pero en este sistema no discrimina nada: en el topico existe un unico
formato, lo escribe un unico productor y el consumidor lo asume sin comprobarlo.
Se omite en consecuencia con el criterio del proyecto de no mantener elementos
que no cumplen funcion. La contrapartida es que la cabecera deja de coincidir
con la del ecosistema Kafka, de modo que un consumidor generico que espere el
byte magico leeria el mensaje desalineado.

NO CONFUNDIR CON `pipeline/schemas/register_schema.py`. Aquel es el script que
REGISTRA el contrato y sus reglas, y se ejecuta a mano cuando el esquema cambia.
Este es la biblioteca que LEE el registro, y la importan el bridge, el job de
Spark y el informe de KPIs en cada arranque. Se llamaba `schema_registry.py`, que
era practicamente el mismo nombre al reves.
"""

import json
import logging
import struct
from typing import Any

import requests

logger = logging.getLogger(__name__)

HEADER_FORMAT = ">I"  # 4 bytes: globalId big-endian
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 4

DEFAULT_REGISTRY_URL = "http://localhost:8080"
DEFAULT_GROUP = "iot"
DEFAULT_ARTIFACT = "iot.telemetry.raw-value"


class SchemaRegistryError(RuntimeError):
    """No se pudo resolver un esquema contra el registro."""


def encode_header(global_id: int) -> bytes:
    """Construye la cabecera de 4 bytes que precede al payload Avro."""
    return struct.pack(HEADER_FORMAT, global_id)



class ApicurioClient:
    """Acceso de solo lectura a los esquemas del registro, con cache.

    Los esquemas se cachean en memoria por globalId porque son inmutables: en
    Apicurio, una version registrada nunca cambia de contenido; publicar un
    esquema nuevo crea un globalId nuevo. Por eso la cache no necesita
    invalidacion y el consumidor puede resolver el esquema de cada mensaje sin
    una llamada HTTP por evento.
    """

    def __init__(self, base_url: str = DEFAULT_REGISTRY_URL, timeout: int = 15):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._cache: dict[int, dict[str, Any]] = {}

    def _api(self, *parts: str) -> str:
        return "/".join([self.base_url, "apis/registry/v3", *parts])

    def check(self) -> None:
        """Comprueba que el registro responde. Falla con una orden accionable."""
        try:
            resp = requests.get(self._api("system/info"), timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise SchemaRegistryError(
                f"No se pudo contactar con el registro en {self.base_url}. "
                "Levanta el stack: docker compose -f pipeline/docker-compose.yml up -d"
            ) from exc
        info = resp.json()
        logger.info("Registro de esquemas: %s %s", info.get("name"), info.get("version"))

    def latest(self, group: str = DEFAULT_GROUP,
               artifact: str = DEFAULT_ARTIFACT) -> tuple[int, dict[str, Any]]:
        """Devuelve (global_id, esquema) de la ultima version del artefacto.

        Lo usa el PRODUCTOR al arrancar, para saber con que esquema debe
        serializar. Se resuelve una sola vez: si se registrara una version
        nueva, el bridge la tomaria al reiniciarse, no en caliente. Es
        deliberado — cambiar de esquema a mitad de un flujo sin control
        explicito es justo lo que la gobernanza pretende evitar.
        """
        url = self._api("groups", group, "artifacts", artifact, "versions", "branch=latest")
        try:
            resp = requests.get(url, timeout=self.timeout)
        except requests.RequestException as exc:
            raise SchemaRegistryError(f"Error de red al resolver {group}/{artifact}: {exc}") from exc

        if resp.status_code == 404:
            raise SchemaRegistryError(
                f"El artefacto {group}/{artifact} no esta registrado. "
                "Registralo antes: python pipeline/schemas/register_schema.py"
            )
        if resp.status_code != 200:
            raise SchemaRegistryError(f"Respuesta inesperada del registro: {resp.status_code} {resp.text}")

        global_id = resp.json()["globalId"]
        schema = self.by_global_id(global_id)
        logger.info("Esquema resuelto: %s/%s version %s (globalId=%d)",
                    group, artifact, resp.json()["version"], global_id)
        return global_id, schema

    def by_global_id(self, global_id: int) -> dict[str, Any]:
        """Devuelve el esquema correspondiente a un globalId, con cache.

        Lo usa el CONSUMIDOR: cada mensaje lleva en su cabecera el globalId con
        el que fue escrito, y esto es lo que lo traduce al esquema real.
        """
        if global_id in self._cache:
            return self._cache[global_id]

        try:
            resp = requests.get(self._api("ids", "globalIds", str(global_id)), timeout=self.timeout)
        except requests.RequestException as exc:
            raise SchemaRegistryError(f"Error de red al resolver globalId={global_id}: {exc}") from exc

        if resp.status_code == 404:
            raise SchemaRegistryError(f"No existe ningun esquema con globalId={global_id}")
        if resp.status_code != 200:
            raise SchemaRegistryError(f"Respuesta inesperada del registro: {resp.status_code} {resp.text}")

        schema = json.loads(resp.text)
        self._cache[global_id] = schema
        logger.debug("Esquema globalId=%d cacheado", global_id)
        return schema
