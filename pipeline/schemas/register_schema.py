"""
Registro de esquemas Avro en Apicurio Schema Registry (Objetivo 2).

Registra un esquema `.avsc` como artefacto del registro y configura sobre el
las reglas de gobernanza. Es idempotente: ejecutarlo dos veces con el mismo
contenido no crea una version nueva.

El esquema se valida localmente con fastavro ANTES de enviarlo al registro, de
modo que un `.avsc` mal formado falla aqui y no deja el registro en un estado
a medias.

Uso tipico (registrar el esquema v1 de telemetria):

    python register_schema.py --schema telemetry_event_v1.avsc

Comprobar si una evolucion seria aceptada, sin registrarla (util para validar
un esquema candidato contra las reglas de compatibilidad):

    python register_schema.py --schema telemetry_event_v2.avsc --dry-run

Consultar lo que hay registrado ahora mismo:

    python register_schema.py --show
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import requests
from fastavro import parse_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("register_schema")

# Grupo y artefacto por defecto. El artefacto sigue la convencion
# "<topico>-value": el esquema describe el VALOR de los mensajes del topico
# iot.telemetry.raw (la clave se serializa aparte, como string). Mantener esta
# convencion permite que el bridge y el job de Spark resuelvan el esquema por
# el nombre del topico, sin configuracion adicional.
DEFAULT_REGISTRY_URL = "http://localhost:8080"
DEFAULT_GROUP = "iot"
DEFAULT_ARTIFACT = "iot.telemetry.raw-value"

# FULL_TRANSITIVE: toda version nueva debe ser compatible hacia atras Y hacia
# adelante, y no solo con la version inmediatamente anterior sino con todas las
# anteriores. Es la regla que sostiene literalmente el Objetivo 2 ("evolucion
# compatible hacia adelante/atras"): con BACKWARD a secas, un consumidor
# antiguo podria romperse ante un evento nuevo.
DEFAULT_COMPATIBILITY = "FULL_TRANSITIVE"

# VALIDITY=FULL hace que el registro rechace contenido que no sea un esquema
# Avro valido, en lugar de almacenarlo y fallar mas tarde en los consumidores.
DEFAULT_VALIDITY = "FULL"

TIMEOUT = 15


class RegistryError(RuntimeError):
    """Error devuelto por la API del registro."""


def _raise_if_rule_violation(resp) -> None:
    """Traduce una violacion de regla a un error legible.

    Se comprobo contra Apicurio 3.3.1 que una incompatibilidad se devuelve como
    HTTP 400 con name=RuleViolationException, no como 409. Por eso la deteccion
    se hace por el campo `name` del cuerpo y no por el codigo de estado: es lo
    unico estable entre los distintos endpoints.
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


def load_and_validate(schema_path: Path) -> str:
    """Lee el .avsc y lo valida con fastavro. Devuelve el contenido como texto."""
    if not schema_path.exists():
        raise FileNotFoundError(f"No se encontro el esquema {schema_path}")

    raw = schema_path.read_text(encoding="utf-8")
    try:
        schema = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RegistryError(f"{schema_path} no es JSON valido: {exc}") from exc

    # parse_schema lanza si el esquema no es Avro valido (tipos desconocidos,
    # enums sin simbolos, defaults incoherentes con el tipo...). Se envuelve en
    # RegistryError para que el fallo salga como un mensaje legible y no como
    # un traceback de fastavro.
    try:
        parse_schema(schema)
    except Exception as exc:
        raise RegistryError(
            f"{schema_path} no es un esquema Avro valido: {type(exc).__name__}: {exc}"
        ) from exc

    nombre = f"{schema.get('namespace', '')}.{schema.get('name', '?')}".strip(".")
    logger.info("Esquema valido: %s (%d campos)", nombre, len(schema.get("fields", [])))
    return raw


def _url(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), "apis/registry/v3", *parts])


def artifact_url(base: str, group: str, artifact: str) -> str:
    return _url(base, "groups", group, "artifacts", artifact)


def check_registry(base: str) -> None:
    """Falla pronto y con un mensaje util si el registro no esta accesible."""
    try:
        resp = requests.get(_url(base, "system/info"), timeout=TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RegistryError(
            f"No se pudo contactar con el registro en {base}. "
            "Levanta el stack: docker compose -f pipeline/docker-compose.yml up -d"
        ) from exc
    info = resp.json()
    logger.info("Registro accesible: %s %s", info.get("name"), info.get("version"))


def artifact_exists(base: str, group: str, artifact: str) -> bool:
    resp = requests.get(artifact_url(base, group, artifact), timeout=TIMEOUT)
    if resp.status_code == 200:
        return True
    if resp.status_code == 404:
        return False
    raise RegistryError(f"Respuesta inesperada al consultar el artefacto: {resp.status_code} {resp.text}")


def upsert_artifact(base: str, group: str, artifact: str, content: str) -> dict:
    """Registra el contenido, exista o no el artefacto.

    Se usa siempre el endpoint de creacion de artefacto con
    ifExists=FIND_OR_CREATE_VERSION, y no el de anadir version, porque es el
    unico que da semantica idempotente: si el artefacto ya existe y el
    contenido coincide con una version registrada, devuelve ESA version en
    lugar de crear un duplicado. El endpoint /versions, en cambio, crea una
    version nueva cada vez aunque el contenido sea identico.

    Si el contenido es nuevo, se crea una version y las reglas de
    compatibilidad ya configuradas sobre el artefacto se aplican igualmente.
    """
    body = {
        "artifactId": artifact,
        "artifactType": "AVRO",
        "firstVersion": {"content": {"content": content, "contentType": "application/json"}},
    }
    resp = requests.post(
        _url(base, "groups", group, "artifacts"),
        params={"ifExists": "FIND_OR_CREATE_VERSION"},
        json=body,
        timeout=TIMEOUT,
    )
    _raise_if_rule_violation(resp)
    if resp.status_code not in (200, 201):
        raise RegistryError(f"No se pudo registrar el artefacto: {resp.status_code} {resp.text}")
    return resp.json()


def add_version(base: str, group: str, artifact: str, content: str, dry_run: bool) -> dict:
    """Anade una version nueva. Con dry_run, el registro comprueba las reglas
    de compatibilidad pero NO persiste nada."""
    body = {"content": {"content": content, "contentType": "application/json"}}
    resp = requests.post(
        artifact_url(base, group, artifact) + "/versions",
        params={"dryRun": "true"} if dry_run else None,
        json=body,
        timeout=TIMEOUT,
    )
    _raise_if_rule_violation(resp)
    if resp.status_code not in (200, 201):
        raise RegistryError(f"No se pudo anadir la version: {resp.status_code} {resp.text}")
    return resp.json()


def set_rule(base: str, group: str, artifact: str, rule_type: str, config: str) -> None:
    """Configura una regla sobre el artefacto (crea o actualiza)."""
    rules_url = artifact_url(base, group, artifact) + "/rules"
    resp = requests.post(rules_url, json={"ruleType": rule_type, "config": config}, timeout=TIMEOUT)
    if resp.status_code == 409:
        # La regla ya existe: se actualiza su configuracion.
        resp = requests.put(f"{rules_url}/{rule_type}", json={"ruleType": rule_type, "config": config}, timeout=TIMEOUT)
    if resp.status_code not in (200, 204):
        raise RegistryError(f"No se pudo configurar la regla {rule_type}: {resp.status_code} {resp.text}")
    logger.info("Regla %s = %s", rule_type, config)


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


def check_enum_order(base: str, group: str, artifact: str, contenido_nuevo: str) -> None:
    """Impide reordenar o insertar simbolos de enum respecto a la version registrada.

    Esta comprobacion NO la hace Apicurio. Se verifico que su regla
    COMPATIBILITY, incluso en FULL_TRANSITIVE, acepta tanto reordenar los
    simbolos de un enum como insertar uno en medio: desde el punto de vista de
    la resolucion de esquemas de Avro son cambios compatibles, porque un lector
    que use el esquema del ESCRITOR resuelve los simbolos por nombre.

    El problema es que este pipeline no hace eso: el job de Spark deserializa
    todo el flujo con un unico esquema (`from_avro` recibe uno solo). Y Avro
    codifica un enum como el INDICE del simbolo, no como su nombre. Comprobado:
    un evento escrito con `symbols: [electricity, chilledwater, ...]` y leido
    con el array reordenado alfabeticamente devuelve 'chilledwater' donde ponia
    'electricity', sin lanzar ninguna excepcion y con el resto de campos
    intactos.

    Por eso solo se admite ANADIR simbolos al final: eso conserva el indice de
    todos los existentes. Cualquier otra modificacion del array se rechaza aqui.
    """
    if not artifact_exists(base, group, artifact):
        return

    resp = requests.get(
        artifact_url(base, group, artifact) + "/versions/branch=latest/content",
        timeout=TIMEOUT,
    )
    if resp.status_code != 200:
        logger.warning("No se pudo leer la version registrada para comparar enums (HTTP %s)",
                       resp.status_code)
        return

    viejos = _collect_enums(json.loads(resp.text))
    nuevos = _collect_enums(json.loads(contenido_nuevo))

    for nombre, simbolos_viejos in viejos.items():
        simbolos_nuevos = nuevos.get(nombre)
        if simbolos_nuevos is None:
            continue  # el enum ya no existe: lo juzgan las reglas del registro
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


def _version_numbers(base: str, group: str, artifact: str) -> set:
    """Numeros de version registrados ahora mismo, para distinguir despues si
    una ejecucion ha creado version nueva o ha reutilizado una existente."""
    resp = requests.get(artifact_url(base, group, artifact) + "/versions", timeout=TIMEOUT)
    resp.raise_for_status()
    return {v["version"] for v in resp.json().get("versions", [])}


def show_state(base: str, group: str, artifact: str) -> int:
    """Imprime el estado actual del artefacto en el registro."""
    if not artifact_exists(base, group, artifact):
        logger.warning("El artefacto %s/%s no esta registrado todavia.", group, artifact)
        return 1

    versions = requests.get(artifact_url(base, group, artifact) + "/versions", timeout=TIMEOUT).json()
    logger.info("Artefacto %s/%s — %d version(es)", group, artifact, versions.get("count", 0))
    for v in versions.get("versions", []):
        logger.info("  version %-4s globalId=%-4s creada=%s", v.get("version"), v.get("globalId"), v.get("createdOn"))

    rules = requests.get(artifact_url(base, group, artifact) + "/rules", timeout=TIMEOUT).json()
    for rule in rules:
        cfg = requests.get(artifact_url(base, group, artifact) + f"/rules/{rule}", timeout=TIMEOUT).json()
        logger.info("  regla %s = %s", rule, cfg.get("config"))
    return 0


def run(args: argparse.Namespace) -> int:
    check_registry(args.registry_url)

    if args.show:
        return show_state(args.registry_url, args.group, args.artifact)

    content = load_and_validate(Path(args.schema))
    existe = artifact_exists(args.registry_url, args.group, args.artifact)

    # Comprobacion propia, previa a la del registro: Apicurio no detecta los
    # cambios de indice en los enums. Se aplica tambien en dry-run, para que
    # sirva de verificacion antes de proponer una evolucion.
    check_enum_order(args.registry_url, args.group, args.artifact, content)

    if args.dry_run:
        if not existe:
            logger.info("[dry-run] El artefacto no existe: se crearia como version 1 (no hay versiones previas contra las que comprobar compatibilidad).")
            return 0
        resultado = add_version(args.registry_url, args.group, args.artifact, content, dry_run=True)
        logger.info("[dry-run] Esquema COMPATIBLE con la regla %s; se registraria como version %s. "
                    "No se ha persistido nada.", args.compatibility, resultado["version"])
        return 0

    if not existe:
        # Las reglas no se pueden asociar a un artefacto que todavia no existe,
        # asi que en el primer registro se crea antes y se configuran despues.
        # La primera version no tiene con que compararse, de modo que no se
        # pierde ninguna comprobacion por este orden.
        resultado = upsert_artifact(args.registry_url, args.group, args.artifact, content)
        logger.info("Artefacto creado: %s/%s version %s",
                    args.group, args.artifact, resultado["version"]["version"])
        set_rule(args.registry_url, args.group, args.artifact, "VALIDITY", args.validity)
        set_rule(args.registry_url, args.group, args.artifact, "COMPATIBILITY", args.compatibility)
    else:
        set_rule(args.registry_url, args.group, args.artifact, "VALIDITY", args.validity)
        set_rule(args.registry_url, args.group, args.artifact, "COMPATIBILITY", args.compatibility)
        antes = _version_numbers(args.registry_url, args.group, args.artifact)
        resultado = upsert_artifact(args.registry_url, args.group, args.artifact, content)
        version = resultado["version"]["version"]
        if version in antes:
            logger.info("Sin cambios: el contenido ya estaba registrado como version %s.", version)
        else:
            logger.info("Version nueva registrada: %s", version)

    return show_state(args.registry_url, args.group, args.artifact)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Registra esquemas Avro en Apicurio (TFM)")
    p.add_argument("--schema", default=str(Path(__file__).parent / "telemetry_event_v1.avsc"),
                   help="Ruta al fichero .avsc a registrar")
    p.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    p.add_argument("--group", default=DEFAULT_GROUP)
    p.add_argument("--artifact", default=DEFAULT_ARTIFACT)
    p.add_argument("--compatibility", default=DEFAULT_COMPATIBILITY,
                   choices=["NONE", "BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD",
                            "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE"])
    p.add_argument("--validity", default=DEFAULT_VALIDITY, choices=["NONE", "SYNTAX_ONLY", "FULL"])
    p.add_argument("--dry-run", action="store_true",
                   help="Comprueba la compatibilidad contra las reglas sin registrar nada")
    p.add_argument("--show", action="store_true",
                   help="Solo muestra el estado actual del artefacto en el registro")
    return p.parse_args()


if __name__ == "__main__":
    try:
        sys.exit(run(parse_args()))
    except (RegistryError, FileNotFoundError) as exc:
        logger.error("%s", exc)
        sys.exit(1)
