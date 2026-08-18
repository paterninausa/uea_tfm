"""
Parada ordenada de los procesos de larga duracion del pipeline.

Simulador, generador de carga y prueba de recuperacion se interrumpen con Ctrl-C
a mitad de una medicion mas veces de las que se completan, y los tres necesitan
lo mismo: enterarse de la senal, terminar el mensaje que tenian entre manos y
cerrar sus conexiones publicando el resumen. Matar el proceso en seco pierde el
resumen, que suele ser el unico sitio donde quedaba la cifra que se estaba
midiendo.
"""

import logging
import signal
import threading

logger = logging.getLogger(__name__)


def evento_de_parada(descripcion: str = "proceso") -> threading.Event:
    """Registra los manejadores de SIGINT/SIGTERM y devuelve el Event que activan.

    Se usa un Event y no una bandera booleana porque los hilos que esperan
    —la ventana de mensajes en vuelo del generador, por ejemplo— pueden
    despertarse con `wait(timeout)` en lugar de sondear.
    """
    parada = threading.Event()

    def manejador(*_args):
        logger.info("Senal de parada recibida, cerrando %s...", descripcion)
        parada.set()

    signal.signal(signal.SIGINT, manejador)
    signal.signal(signal.SIGTERM, manejador)
    return parada


async def evento_de_parada_async(descripcion: str = "proceso") -> "asyncio.Event":
    """Version para procesos asyncio; hay que llamarla con el bucle ya en marcha.

    No vale reutilizar el Event de threading de arriba: una corrutina dormida en
    `asyncio.sleep` no se entera de que alguien lo activo hasta que despierta
    sola, y el simulador duerme entre publicacion y publicacion. Con un Event de
    asyncio la espera se puede abandonar en cuanto llega la senal, que es lo que
    hace que Ctrl-C cierre las conexiones en vez de dejarlas colgando.
    """
    import asyncio

    parada = asyncio.Event()
    bucle = asyncio.get_running_loop()

    def manejador():
        logger.info("Senal de parada recibida, cerrando %s...", descripcion)
        parada.set()

    for senal in (signal.SIGINT, signal.SIGTERM):
        bucle.add_signal_handler(senal, manejador)
    return parada
