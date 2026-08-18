"""
Configuracion unica del registro de actividad de todos los procesos del pipeline.

Cada proceso escribe a la vez por consola —para verlo mientras corre— y a un
fichero propio bajo `pipeline/logs/`, de modo que despues de una prueba quede el
rastro de lo que hizo cada pieza. Los ficheros de un mismo proceso se van
acumulando por rotacion en lugar de sobrescribirse: una medicion suele constar
de varios arranques (parar el simulador, reiniciar el job, repetir un peldano de
carga) y la evidencia interesante casi nunca esta en el ultimo de ellos.

CADA ARRANQUE ESCRIBE LA ORDEN COMPLETA como primera linea. Es lo que convierte
el log en evidencia utilizable en la memoria del trabajo: sin los argumentos, una
cifra de throughput no se puede atribuir a una configuracion concreta ni
reproducir. Con ellos, el fichero dice literalmente como se genero.

Uso:
    from common.logging_setup import configurar_logging
    logger = configurar_logging("simulator")
"""

import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

# pipeline/logs/, hermano de common/. Se resuelve desde este fichero y no desde
# el directorio de trabajo: los scripts se lanzan indistintamente desde la raiz
# del repo o desde su propia carpeta, y el destino del log no debe depender de
# eso.
DIRECTORIO_LOGS = Path(__file__).resolve().parents[1] / "logs"

FORMATO = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

# 3 MB por fichero y 3 de respaldo: 12 MB por proceso en el peor caso. Suficiente
# para cubrir varios arranques completos del sistema sin vigilar el disco, que es
# el criterio con el que se eligio.
TAMANO_MAX_BYTES = 3 * 1024 * 1024
COPIAS_RESPALDO = 3


def configurar_logging(nombre: str, nivel: int = logging.INFO,
                       directorio: Path | None = None) -> logging.Logger:
    """Deja el logging del proceso listo y devuelve su logger.

    Se configura el logger RAIZ, no solo el del proceso: asi los mensajes de las
    librerias (paho, kafka-python, py4j) caen en el mismo fichero. Cuando un
    productor de Kafka reintenta o un cliente MQTT se reconecta, ese aviso viene
    de la libreria, y es justo el que hace falta para explicar despues un hueco
    en las metricas.

    Es idempotente: si se llama dos veces en el mismo proceso no duplica
    handlers, que produciria cada linea repetida.
    """
    destino = directorio or DIRECTORIO_LOGS
    destino.mkdir(parents=True, exist_ok=True)
    fichero = destino / f"{nombre}.log"

    raiz = logging.getLogger()
    raiz.setLevel(nivel)

    ya_configurado = any(getattr(h, "_tfm_handler", False) for h in raiz.handlers)
    if not ya_configurado:
        formateador = logging.Formatter(FORMATO)

        consola = logging.StreamHandler(sys.stderr)
        consola.setFormatter(formateador)
        consola._tfm_handler = True
        raiz.addHandler(consola)

        rotatorio = RotatingFileHandler(
            fichero, maxBytes=TAMANO_MAX_BYTES, backupCount=COPIAS_RESPALDO,
            encoding="utf-8",
        )
        rotatorio.setFormatter(formateador)
        rotatorio._tfm_handler = True
        raiz.addHandler(rotatorio)

    logger = logging.getLogger(nombre)
    _cabecera_de_arranque(logger, fichero)
    return logger


def _cabecera_de_arranque(logger: logging.Logger, fichero: Path) -> None:
    """Marca el inicio de una ejecucion con su orden completa y su PID.

    El separador visible importa mas de lo que parece: con append, distinguir
    donde termina una ejecucion y empieza la siguiente a base de leer marcas de
    tiempo es incomodo, y en una sesion de medicion se hace decenas de veces.
    """
    logger.info("=" * 78)
    logger.info("ARRANQUE %s | pid=%d | log=%s",
                time.strftime("%Y-%m-%d %H:%M:%S"), os.getpid(), fichero)
    logger.info("ORDEN: %s", " ".join(sys.argv))
    logger.info("=" * 78)
