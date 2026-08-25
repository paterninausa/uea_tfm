"""
Registro del contrato de datos en Apicurio via la API compatible con Confluent
(ccompat) (Objetivo 2).

Publica un esquema `.avsc` como un *subject* del registro y aplica sobre el la
regla de compatibilidad. NO forma parte del pipeline en ejecucion: se ejecuta a
mano cuando cambia el contrato. El bridge y el job de Spark solo LEEN del
registro.

Se usa la API ccompat —y no la nativa v3 de Apicurio— porque el productor
serializa con el `AvroSerializer` de confluent-kafka, que resuelve el esquema
por el protocolo de Confluent: subjects en un namespace plano, sin el concepto
de "grupo" propio de Apicurio. Es el estandar de facto del ecosistema Kafka.
Registrar por aqui hace que exista UN solo esquema —el que el productor
referencia y el que viaja identificado en cada mensaje—, en lugar de la
registracion nativa (grupo iot) que el AvroSerializer no ve y que duplicaria el
esquema sin gobernar.

Es idempotente: si el `.avsc` coincide con lo registrado, el registro devuelve
el id de la version existente y no crea un duplicado, de modo que ejecutarlo
sirve tambien de comprobacion.

Uso:
    python register_schema.py
    python register_schema.py --schema telemetry_event_v2.avsc
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.logging_setup import configurar_logging  # noqa: E402
from fastavro import parse_schema

logger = logging.getLogger("register_schema")

# API compatible con Confluent. El AvroSerializer de confluent-kafka habla este
# protocolo, no la API nativa v3 de Apicurio.
CCOMPAT_URL = "http://localhost:8080/apis/ccompat/v7"

# El subject sigue la convencion TopicNameStrategy de Confluent: {topic}-value,
# porque el esquema describe el VALOR de los mensajes del topico
# iot.telemetry.raw. En el protocolo de Confluent no hay grupos: los subjects
# viven en un namespace plano.
SUBJECT = "iot.telemetry.raw-value"

# FULL_TRANSITIVE: toda version nueva debe ser compatible hacia atras Y hacia
# adelante, y no solo con la version anterior sino con todas las anteriores. Es
# la regla que sostiene el Objetivo 2; con BACKWARD a secas, un consumidor
# antiguo podria romperse ante un evento nuevo.
COMPATIBILITY = "FULL_TRANSITIVE"

# NO hay equivalente a la regla VALIDITY de la API nativa en el protocolo de
# Confluent: el registro valida la sintaxis del esquema de forma implicita al
# registrarlo, y ademas `load_and_validate` ya lo comprueba en local con
# fastavro antes de enviar nada.

TIMEOUT = 15


class RegistryError(RuntimeError):
    """Error devuelto por la API del registro, o incumplimiento de una regla."""


def _ccompat(*parts: str) -> str:
    return "/".join([CCOMPAT_URL, *parts])


def _raise_if_rule_violation(resp) -> None:
    """Traduce una violacion de regla a un error legible.

    Se comprobo contra Apicurio 3.3.1 que la API ccompat devuelve una
    incompatibilidad como HTTP 409 con el detalle en el campo `message`
    (RuleViolationException). No usa la estructura name/causes de la API nativa.
    """
    if resp.status_code not in (409, 422):
        return
    try:
        detalle = resp.json()
    except ValueError:
        return
    mensaje = detalle.get("message", "")
    if "RuleViolation" not in mensaje and "ncompatible" not in mensaje:
        return

    logger.error("El registro RECHAZO el esquema: viola la regla de compatibilidad activa.")
    logger.error("  %s", mensaje)
    raise RegistryError("esquema rechazado por las reglas del registro (ver detalle arriba)")


def check_registry() -> None:
    """Comprueba que el registro responde, con un mensaje accionable si no."""
    try:
        resp = requests.get(_ccompat("subjects"), timeout=TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RegistryError(
            f"No se pudo contactar con el registro ccompat en {CCOMPAT_URL}. "
            "Levanta el stack: docker compose -f pipeline/docker-compose.yml up -d"
        ) from exc
    logger.info("Registro ccompat accesible (%d subjects)", len(resp.json()))


def load_and_validate(schema_path: Path) -> str:
    """Lee el .avsc y lo valida con fastavro. Devuelve el contenido como texto.

    Validar en local antes de enviar nada evita dejar el registro a medias por
    un fichero mal formado.
    """
    if not schema_path.exists():
        raise FileNotFoundError(f"No se encontro el esquema {schema_path}")

    raw = schema_path.read_text(encoding="utf-8")
    try:
        schema = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RegistryError(f"{schema_path} no es JSON valido: {exc}") from exc

    try:
        parse_schema(schema)
    except Exception as exc:
        raise RegistryError(
            f"{schema_path} no es un esquema Avro valido: {type(exc).__name__}: {exc}"
        ) from exc

    nombre = f"{schema.get('namespace', '')}.{schema.get('name', '?')}".strip(".")
    logger.info("Esquema valido: %s (%d campos)", nombre, len(schema.get("fields", [])))
    return raw


def subject_versions() -> list:
    """Devuelve la lista de numeros de version del subject, o [] si no existe."""
    resp = requests.get(_ccompat("subjects", SUBJECT, "versions"), timeout=TIMEOUT)
    if resp.status_code == 404:
        return []
    if resp.status_code != 200:
        raise RegistryError(f"Respuesta inesperada al consultar el subject: {resp.status_code}")
    return resp.json()


def _collect_enums(nodo, acc: dict | None = None) -> dict:
    """Recorre el esquema y devuelve {nombre_enum: [simbolos]}, incluidos los anidados."""
    acc = {} if acc is None else acc
    if isinstance(nodo, dict):
        if nodo.get("type") == "enum" and "name" in nodo:
            acc[nodo["name"]] = list(nodo.get("symbols", []))
        for v in nodo.values():
            _collect_enums(v, acc)
    elif isinstance(nodo, list):
        for v in nodo:
            _collect_enums(v, acc)
    return acc


def check_enum_order(contenido_nuevo: str) -> None:
    """Impide reordenar o insertar simbolos de enum respecto a la version registrada.

    Esta comprobacion NO la hace Apicurio. Se verifico que su regla
    COMPATIBILITY, incluso en FULL_TRANSITIVE, acepta tanto reordenar los
    simbolos de un enum como insertar uno en medio: desde la especificacion de
    Avro son cambios compatibles, porque un lector que use el esquema del
    ESCRITOR resuelve los simbolos por nombre.

    Este pipeline no hace eso: el job de Spark deserializa todo el flujo con un
    unico esquema, y Avro codifica un enum como el INDICE del simbolo.
    Comprobado: un evento escrito con `symbols: [electricity, chilledwater, ...]`
    y leido con el array reordenado devuelve 'chilledwater' donde ponia
    'electricity', sin excepcion y con el resto de campos intactos.

    Ademas, tras un reordenamiento los mensajes anteriores del topico quedan
    permanentemente ilegibles con el esquema vigente, lo que destruye la
    reproducibilidad del log en que se apoya la arquitectura Kappa.

    Por eso solo se admite ANADIR simbolos al final, que conserva el indice de
    todos los existentes.
    """
    if not subject_versions():
        return

    resp = requests.get(_ccompat("subjects", SUBJECT, "versions", "latest"), timeout=TIMEOUT)
    if resp.status_code != 200:
        logger.warning("No se pudo leer la version registrada para comparar enums (HTTP %s)",
                       resp.status_code)
        return

    # ccompat devuelve el esquema como una cadena JSON en el campo `schema`.
    registrado = resp.json().get("schema")
    viejos = _collect_enums(json.loads(registrado))
    nuevos = _collect_enums(json.loads(contenido_nuevo))

    for nombre, simbolos_viejos in viejos.items():
        simbolos_nuevos = nuevos.get(nombre)
        if simbolos_nuevos is None:
            continue
        prefijo = simbolos_nuevos[: len(simbolos_viejos)]
        if prefijo != simbolos_viejos:
            logger.error("El enum '%s' altera el orden de los simbolos ya registrados.", nombre)
            logger.error("  registrado: %s", simbolos_viejos)
            logger.error("  propuesto : %s", simbolos_nuevos)
            for i, (a, b) in enumerate(zip(simbolos_viejos, prefijo)):
                if a != b:
                    logger.error("  el indice %d pasa de '%s' a '%s': todo evento ya escrito "
                                 "con '%s' se leeria como '%s'", i, a, b, a, b)
            raise RegistryError(
                f"Cambio de indice en el enum '{nombre}'. Solo se permite anadir simbolos al "
                "final; reordenar o insertar corrompe en silencio los datos ya escritos."
            )


def set_compatibility(nivel: str) -> None:
    """Fija la regla de compatibilidad del subject (PUT /config/{subject})."""
    resp = requests.put(_ccompat("config", SUBJECT),
                        json={"compatibility": nivel}, timeout=TIMEOUT)
    if resp.status_code != 200:
        raise RegistryError(
            f"No se pudo configurar la compatibilidad: {resp.status_code} {resp.text}")
    logger.info("Regla COMPATIBILITY = %s", nivel)


def register(contenido: str) -> int:
    """Registra el esquema bajo el subject y devuelve su id.

    POST /subjects/{s}/versions es idempotente: si el esquema ya esta registrado
    bajo el subject, devuelve el id existente en lugar de crear un duplicado.
    """
    resp = requests.post(_ccompat("subjects", SUBJECT, "versions"),
                         json={"schemaType": "AVRO", "schema": contenido}, timeout=TIMEOUT)
    _raise_if_rule_violation(resp)
    if resp.status_code != 200:
        raise RegistryError(f"No se pudo registrar el esquema: {resp.status_code} {resp.text}")
    return resp.json()["id"]


def run(args: argparse.Namespace) -> int:
    check_registry()
    contenido = load_and_validate(Path(args.schema))
    check_enum_order(contenido)

    versiones_antes = subject_versions()
    existia = bool(versiones_antes)

    if existia:
        # Con el subject ya creado, la regla se fija ANTES de registrar para que
        # gobierne el contenido nuevo.
        set_compatibility(COMPATIBILITY)

    schema_id = register(contenido)
    versiones_despues = subject_versions()

    if not existia:
        # En el primer registro no hay con que comparar, asi que la regla se fija
        # despues de crear el subject sin perder ninguna comprobacion.
        set_compatibility(COMPATIBILITY)
        logger.info("Subject creado: %s (schema id=%s)", SUBJECT, schema_id)
    elif len(versiones_despues) > len(versiones_antes):
        logger.info("Version nueva registrada: %s (schema id=%s)",
                    versiones_despues[-1], schema_id)
    else:
        logger.info("Sin cambios: el contenido ya estaba registrado (schema id=%s).", schema_id)

    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Registra el contrato de datos en Apicurio (TFM)")
    p.add_argument("--schema", default=str(Path(__file__).parent / "telemetry_event_v1.avsc"),
                   help="Ruta al fichero .avsc a registrar")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("register_schema")
    try:
        sys.exit(run(parse_args()))
    except (RegistryError, FileNotFoundError) as exc:
        logger.error("%s", exc)
        sys.exit(1)
