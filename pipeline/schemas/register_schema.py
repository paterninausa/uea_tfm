"""
Registro del contrato de datos en Apicurio Schema Registry (Objetivo 2).

Publica un esquema `.avsc` como artefacto del registro y aplica sobre el las
reglas de gobernanza. NO forma parte del pipeline en ejecucion: se ejecuta a
mano cuando cambia el contrato. El bridge y el job de Spark solo LEEN del
registro, a traves de `pipeline/common/schema_registry.py`.

Es idempotente, de modo que ejecutarlo sirve tambien de comprobacion: si el
`.avsc` coincide con lo registrado, lo dice y no toca nada.

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

REGISTRY_URL = "http://localhost:8080"

# Un unico artefacto en todo el proyecto. El sufijo "-value" sigue la convencion
# TopicNameStrategy: el esquema describe el VALOR de los mensajes del topico
# iot.telemetry.raw.
GROUP = "iot"
ARTIFACT = "iot.telemetry.raw-value"

# FULL_TRANSITIVE: toda version nueva debe ser compatible hacia atras Y hacia
# adelante, y no solo con la version anterior sino con todas las anteriores. Es
# la regla que sostiene el Objetivo 2; con BACKWARD a secas, un consumidor
# antiguo podria romperse ante un evento nuevo.
COMPATIBILITY = "FULL_TRANSITIVE"

# VALIDITY=FULL hace que el registro rechace contenido que no sea un esquema
# Avro valido, en lugar de almacenarlo y fallar mas tarde en los consumidores.
VALIDITY = "FULL"

TIMEOUT = 15


class RegistryError(RuntimeError):
    """Error devuelto por la API del registro, o incumplimiento de una regla."""


def _api(*parts: str) -> str:
    return "/".join([REGISTRY_URL, "apis/registry/v3", *parts])


def _artifact_url() -> str:
    return _api("groups", GROUP, "artifacts", ARTIFACT)


def _raise_if_rule_violation(resp) -> None:
    """Traduce una violacion de regla a un error legible.

    Se comprobo contra Apicurio 3.3.1 que una incompatibilidad se devuelve como
    HTTP 400 con name=RuleViolationException, no como 409. Por eso la deteccion
    se hace por el campo `name` del cuerpo y no por el codigo de estado.
    """
    if resp.status_code not in (400, 409):
        return
    try:
        detalle = resp.json()
    except ValueError:
        return
    if detalle.get("name") != "RuleViolationException":
        return

    logger.error("El registro RECHAZO el esquema: viola una regla de gobernanza activa.")
    for causa in detalle.get("causes", []):
        contexto = causa.get("context", "")
        logger.error("  - %s%s", causa.get("description"), f"  (en {contexto})" if contexto else "")
    raise RegistryError("esquema rechazado por las reglas del registro (ver causas arriba)")


def check_registry() -> None:
    """Comprueba que el registro responde, con un mensaje accionable si no."""
    try:
        resp = requests.get(_api("system/info"), timeout=TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RegistryError(
            f"No se pudo contactar con el registro en {REGISTRY_URL}. "
            "Levanta el stack: docker compose -f pipeline/docker-compose.yml up -d"
        ) from exc
    info = resp.json()
    logger.info("Registro accesible: %s %s", info.get("name"), info.get("version"))


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


def artifact_exists() -> bool:
    resp = requests.get(_artifact_url(), timeout=TIMEOUT)
    if resp.status_code in (200, 404):
        return resp.status_code == 200
    raise RegistryError(f"Respuesta inesperada al consultar el artefacto: {resp.status_code}")


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
    if not artifact_exists():
        return

    resp = requests.get(_artifact_url() + "/versions/branch=latest/content", timeout=TIMEOUT)
    if resp.status_code != 200:
        logger.warning("No se pudo leer la version registrada para comparar enums (HTTP %s)",
                       resp.status_code)
        return

    viejos = _collect_enums(json.loads(resp.text))
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


def set_rule(rule_type: str, config: str) -> None:
    """Configura una regla sobre el artefacto (la crea o actualiza)."""
    rules_url = _artifact_url() + "/rules"
    resp = requests.post(rules_url, json={"ruleType": rule_type, "config": config}, timeout=TIMEOUT)
    if resp.status_code == 409:
        resp = requests.put(f"{rules_url}/{rule_type}",
                            json={"ruleType": rule_type, "config": config}, timeout=TIMEOUT)
    if resp.status_code not in (200, 204):
        raise RegistryError(f"No se pudo configurar la regla {rule_type}: {resp.status_code}")
    logger.info("Regla %s = %s", rule_type, config)


def upsert(contenido: str) -> tuple[str, bool]:
    """Registra el contenido y devuelve (version, es_nueva).

    Se usa el endpoint de creacion de artefacto con
    ifExists=FIND_OR_CREATE_VERSION, y no el de anadir version, porque es el
    unico con semantica idempotente: si el contenido coincide con una version
    ya registrada, devuelve ESA version en lugar de crear un duplicado. El
    endpoint /versions crea una version nueva en cada llamada aunque el
    contenido sea identico.
    """
    versiones_antes = set()
    if artifact_exists():
        resp = requests.get(_artifact_url() + "/versions", timeout=TIMEOUT)
        resp.raise_for_status()
        versiones_antes = {v["version"] for v in resp.json().get("versions", [])}

    body = {
        "artifactId": ARTIFACT,
        "artifactType": "AVRO",
        "firstVersion": {"content": {"content": contenido, "contentType": "application/json"}},
    }
    resp = requests.post(_api("groups", GROUP, "artifacts"),
                         params={"ifExists": "FIND_OR_CREATE_VERSION"},
                         json=body, timeout=TIMEOUT)
    _raise_if_rule_violation(resp)
    if resp.status_code not in (200, 201):
        raise RegistryError(f"No se pudo registrar el artefacto: {resp.status_code} {resp.text}")

    version = resp.json()["version"]["version"]
    return version, version not in versiones_antes


def run(args: argparse.Namespace) -> int:
    check_registry()
    contenido = load_and_validate(Path(args.schema))
    check_enum_order(contenido)

    existia = artifact_exists()
    if existia:
        # Con el artefacto ya creado, las reglas se aplican ANTES de registrar
        # para que gobiernen el contenido nuevo.
        set_rule("VALIDITY", VALIDITY)
        set_rule("COMPATIBILITY", COMPATIBILITY)

    version, es_nueva = upsert(contenido)

    if not existia:
        # Las reglas no pueden asociarse a un artefacto que aun no existe, asi
        # que en el primer registro se crea antes y se configuran despues. La
        # primera version no tiene con que compararse, de modo que no se pierde
        # ninguna comprobacion por este orden.
        set_rule("VALIDITY", VALIDITY)
        set_rule("COMPATIBILITY", COMPATIBILITY)
        logger.info("Artefacto creado: %s/%s version %s", GROUP, ARTIFACT, version)
    elif es_nueva:
        logger.info("Version nueva registrada: %s", version)
    else:
        logger.info("Sin cambios: el contenido ya estaba registrado como version %s.", version)

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
