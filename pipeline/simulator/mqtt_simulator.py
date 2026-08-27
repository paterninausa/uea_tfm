"""
Simulador del parque de medidores: reproduce el historico de ASHRAE sobre MQTT.

Ocupa el lugar de los 652 sensores reales. Publica lo que emitirian ellos, en sus
mismos topicos y con el mismo QoS, y por defecto **abre una conexion MQTT por
sensor**, que es como se veria el parque desde el broker.

EL PARQUE REAL GENERA 0,1797 EV/S. Verificado sobre el Parquet: cada medidor
mide una vez por hora —mediana y p95 del intervalo son exactamente 3600 s, sin
dispersion— y hay 652 medidores con datos en el 99,2% de las horas de 2016. Ese
es el caso de uso: **menos de una quinta parte de un evento por segundo**, sobre
un historico de 8.784 horas.

Por eso el simulador acelera el reloj. `--acelerar` es el factor:

    tasa agregada = n_sensores x acelerar / 3600

    acelerar    cadencia por sensor    con 652 sensores    ano completo en
        1              1 h                 0,18 ev/s          8.784 h
    2.000              1,8 s                 362 ev/s            4,4 h
    7.145              0,5 s               1.294 ev/s             74 min

Es un factor POR SENSOR, no una tasa global, y esa diferencia importa para el
Objetivo 5: con una tasa global fija, pasar de 100 a 652 sensores reparte los
mismos eventos entre mas identidades y no escala nada. Con `--acelerar`, cada
medidor mantiene su cadencia y la carga crece con el numero de medidores, que
es lo que significa "sensores concurrentes".

POR QUE NO SE REPRODUCEN LAS RAFAGAS. Los medidores reales miden en la hora en
punto, asi que un replay literal publicaria los 652 de golpe y luego callaria. No
se hace, y no por comodidad: con un factor alto esa rafaga desborda el sistema.
Con un drenaje del bridge de unos 4.000 ev/s, el replay fiel se sostiene hasta
~x22.000 (652 / (3600/acelerar) <= 4.000); por encima, la cola de Mosquitto
—10.000 mensajes segun `mosquitto.conf`— se llena en menos de tres segundos y el
broker empieza a descartar EN SILENCIO. En su lugar, los sensores se escalonan de
forma determinista dentro de cada intervalo, lo que equivale a suponer que sus
relojes no estan sincronizados al milisegundo: mas realista que la rafaga
perfecta, y sin perdida artificial.

NO HAY OPCIONES DE BANCO DE PRUEBAS AQUI. Existio un `load_generator.py` aparte
que alcanzaba tasas altas con una VENTANA DE MENSAJES EN VUELO: publicaba N
mensajes sin esperar confirmacion desde un solo cliente. Se retiro en agosto de
2026 y no debe volver. El motivo: esa ventana era un artificio para que UN
cliente hiciera el trabajo de 652, y `--clients` consigue lo mismo sin inventar
nada, porque 652 conexiones con un mensaje en vuelo cada una dan 652 mensajes en
vuelo por la via realista. Todo lo que mide y orquesta vive en `tools/`.

Uso:
    python mqtt_simulator.py --acelerar 2000
    python mqtt_simulator.py --acelerar 2000 --max-sensors 100   # peldano del Objetivo 5
    python mqtt_simulator.py --acelerar 500 --traer-a now        # demostracion en vivo
    python mqtt_simulator.py --acelerar 2000 --clients 1         # una sola conexion
"""

import argparse
import asyncio
import heapq
import json
import logging
import sys
import time
from contextlib import suppress
from pathlib import Path

import aiomqtt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.logging_setup import configurar_logging  # noqa: E402
from common.stop_event import evento_de_parada_async  # noqa: E402
from telemetry_dataset import anadir_argumentos_dataset, preparar  # noqa: E402

logger = logging.getLogger("mqtt_simulator")

TOPIC_TEMPLATE = "iot/{building_id}/{meter_type}/telemetry"

# Cadencia del medidor real, en segundos. Es la constante de la que sale todo:
# el factor de aceleracion, la tasa agregada y el escalonado entre sensores.
PERIODO_SENSOR_S = 3600.0


def build_topic(fila) -> str:
    """Topico del sensor que emite esta lectura.

    EL TOPICO SOLO IDENTIFICA AL SENSOR. No lleva nivel de emplazamiento:
    `site_id` es derivable de `building_id` a traves de la tabla de dimension.
    Ponerlo tambien aqui creaba una segunda fuente de verdad —topico y dimension
    podrian discrepar si un edificio se reasignara— sin que nada lo consumiera:
    el bridge se suscribe a `iot/#` y nunca parte el topico. El emplazamiento
    entra en el analisis donde importa, en el broadcast join de Spark.
    """
    return TOPIC_TEMPLATE.format(building_id=fila.building_id, meter_type=fila.meter_type)


def build_payload(fila) -> dict:
    """Mensaje con los campos del contrato y nada mas.

    `timestamp` se envia como cadena ISO-8601 y el bridge lo convierte a epoch en
    milisegundos, que es lo que declara el esquema Avro. Se mantiene en ISO aqui
    porque hace legible el trafico al depurar con `mosquitto_sub`.

    `sim_publish_ts` se sella AL CONSTRUIR EL MENSAJE, no al leer el dataset: es
    el instante real de emision y el origen de tiempo del KPI de latencia del
    Objetivo 1. Por eso esta funcion vive en el productor y no en el modulo que
    interpreta los datos: lo que hace no es leer una fila, es emitirla.
    """
    return {
        "building_id": str(fila.building_id),
        "meter_type": str(fila.meter_type),
        "timestamp": fila.timestamp.isoformat(),
        "meter_reading": float(fila.meter_reading),
        "sim_publish_ts": int(time.time() * 1000),
    }


def anadir_argumentos_mqtt(p: argparse.ArgumentParser, client_id: str) -> None:
    """Conexion al broker."""
    p.add_argument("--broker-host", default="localhost")
    p.add_argument("--broker-port", type=int, default=1883)
    p.add_argument("--client-id", default=client_id)
    p.add_argument("--qos", type=int, default=1, choices=[0, 1, 2],
                   help="QoS MQTT (1 por defecto, ver Objetivo 1)")


class Contadores:
    """Estado agregado de la simulacion. Un solo hilo: no hacen falta locks."""

    def __init__(self):
        self.publicados = 0
        self.fallidos = 0
        self.reconexiones = 0
        self.retraso_max_s = 0.0
        self.t0 = time.monotonic()

    def retraso(self, segundos: float) -> None:
        self.retraso_max_s = max(self.retraso_max_s, segundos)

    def reconexion(self) -> None:
        self.reconexiones += 1

    def resumen(self) -> dict:
        duracion = time.monotonic() - self.t0
        total = self.publicados + self.fallidos
        return {
            "publicados": self.publicados,
            "fallidos": self.fallidos,
            "reconexiones": self.reconexiones,
            "tasa_perdida_pct": (self.fallidos / total * 100) if total else 0.0,
            "duracion_s": duracion,
            "throughput_ev_s": self.publicados / max(1e-9, duracion),
            "retraso_max_s": self.retraso_max_s,
        }


def repartir(df, n_clientes: int) -> list[list]:
    """Agrupa los eventos por conexion, respetando la identidad del sensor.

    UN SENSOR NUNCA SE PARTE ENTRE DOS CONEXIONES: sus lecturas tienen que salir
    en orden, y dos clientes publicando el mismo topico en paralelo no lo
    garantizan.

    Con `--clients` igual al numero de sensores, cada medidor tiene su propia
    conexion, que es el parque real. Con un valor menor, cada conexion agrupa
    varios medidores, y eso tambien modela algo real: en un edificio los
    medidores hablan BACnet o Modbus con una pasarela, y es la pasarela la que
    publica por todos. El numero de conexiones MQTT de un despliegue de verdad
    no lo fija el numero de sensores, sino el de pasarelas.
    """
    sensores = list(df.groupby(["building_id", "meter_type"], sort=True))
    grupos: list[list] = [[] for _ in range(min(n_clientes, len(sensores)))]

    for idx, (_clave, filas) in enumerate(sensores):
        # Desfase del sensor dentro de su intervalo, como fraccion de 0 a 1: es
        # lo que evita que los 652 publiquen a la vez. Determinista y no
        # aleatorio, para que dos ejecuciones con los mismos argumentos sean
        # comparables entre si.
        grupos[idx % len(grupos)].append((filas, idx / len(sensores)))
    return grupos


def _programa(filas, fraccion: float, t_sim0, speedup: float):
    """Instantes de publicacion de UN sensor, en segundos desde el arranque.

    El instante sale del tiempo de evento comprimido por `speedup`, mas el
    desfase propio del sensor dentro de su intervalo. Ese desfase es lo que
    impide que los 652 medidores publiquen a la vez: equivale a suponer que sus
    relojes no estan sincronizados al milisegundo, que es lo que ocurre en un
    parque real y ademas evita la rafaga que desbordaria al bridge.
    """
    desfase = fraccion * PERIODO_SENSOR_S / speedup
    for fila in filas.itertuples(index=False):
        yield (fila.timestamp - t_sim0).total_seconds() / speedup + desfase, fila


async def cliente_mqtt(indice: int, trabajo: list, args, contadores: Contadores,
                       parada, t0: float, t_sim0) -> None:
    """Una conexion MQTT publicando las lecturas de los sensores que le tocaron.

    RECONECTA Y REINTIENTA LO NO CONFIRMADO, que es lo que hace un medidor real:
    los medidores inteligentes guardan el perfil de carga y lo vuelcan cuando
    recuperan el enlace. Sin esto, el comportamiento medido al tumbar el broker
    era el peor posible: `aiomqtt` no reconecta por su cuenta, asi que cada
    `publish` lanzaba MqttError, se contaba como fallo y se pasaba al evento
    siguiente. El simulador quemaba su agenda entera a velocidad de CPU —12.814
    publicados frente a 35.597 fallidos en 40 s— sin volver a publicar nada y sin
    recuperarse aunque el broker volviera.

    El evento que no llega a confirmarse se guarda en `pendiente` y es el primero
    que sale tras reconectar. Su `sim_publish_ts` se sella entonces, no antes:
    la marca dice cuando se EMITIO de verdad, que es lo que mide el KPI.
    """
    identificador = f"{args.client_id}-{indice}"
    # La agenda se construye UNA vez y se conserva entre reconexiones: es un
    # iterador perezoso, asi que reanudar es seguir consumiendolo, no repetirlo.
    agenda = heapq.merge(
        *(_programa(filas, fraccion, t_sim0, args.speedup)
          for filas, fraccion in trabajo),
        key=lambda x: x[0],
    )
    pendiente = None
    intentos = 0

    while not parada.is_set():
        try:
            async with aiomqtt.Client(args.broker_host, args.broker_port,
                                      identifier=identificador) as cliente:
                intentos = 0
                while not parada.is_set():
                    if pendiente is None:
                        pendiente = next(agenda, None)
                        if pendiente is None:
                            return
                    instante, fila = pendiente

                    espera = (t0 + instante) - time.monotonic()
                    if espera > 0:
                        # Se espera SOBRE la senal de parada en vez de dormir a
                        # secas: asi una interrupcion se atiende al instante y el
                        # `async with` cierra la conexion como es debido.
                        try:
                            await asyncio.wait_for(parada.wait(), timeout=espera)
                            return
                        except asyncio.TimeoutError:
                            pass
                    else:
                        # El simulador va por detras de su agenda porque el
                        # speedup pedido supera lo que esta maquina sostiene, o
                        # porque acaba de reconectar y arrastra lo acumulado.
                        contadores.retraso(-espera)

                    await cliente.publish(build_topic(fila),
                                          payload=json.dumps(build_payload(fila)),
                                          qos=args.qos)
                    contadores.publicados += 1
                    pendiente = None

        except aiomqtt.MqttError as exc:
            intentos += 1
            contadores.reconexion()
            if intentos > args.max_reconexiones:
                logger.error("[%s] sin conexion tras %d intentos, se abandona este sensor: %s",
                             identificador, intentos, exc)
                return
            logger.warning("[%s] conexion perdida (intento %d de %d), reintento en %.0f s: %s",
                           identificador, intentos, args.max_reconexiones,
                           args.espera_reconexion, exc)
            try:
                await asyncio.wait_for(parada.wait(), timeout=args.espera_reconexion)
                return
            except asyncio.TimeoutError:
                pass


async def informar(contadores: Contadores, intervalo: float, parada) -> None:
    while not parada.is_set():
        await asyncio.sleep(intervalo)
        r = contadores.resumen()
        logger.info("publicados=%s fallidos=%d | %.1f ev/s efectivos",
                    f"{r['publicados']:,}", r["fallidos"], r["throughput_ev_s"])


async def simular(args, df) -> Contadores:
    parada = await evento_de_parada_async("simulador")
    n_sensores = df.groupby(["building_id", "meter_type"]).ngroups
    n_clientes = n_sensores if args.clients <= 0 else min(args.clients, n_sensores)

    tasa_teorica = n_sensores * args.speedup / PERIODO_SENSOR_S
    logger.info("Parque simulado: %d sensores | %d conexiones MQTT | speedup x%s",
                n_sensores, n_clientes, f"{args.speedup:,.0f}")
    logger.info("Cadencia por sensor: %.3f s | tasa agregada teorica: %.1f ev/s",
                PERIODO_SENSOR_S / args.speedup, tasa_teorica)

    grupos = repartir(df, n_clientes)
    contadores = Contadores()
    t_sim0 = df["timestamp"].min()
    t0 = time.monotonic()

    tareas = [asyncio.create_task(cliente_mqtt(i, g, args, contadores, parada, t0, t_sim0))
              for i, g in enumerate(grupos) if g]
    vigilante = asyncio.create_task(informar(contadores, args.report_interval, parada))

    await asyncio.gather(*tareas)
    vigilante.cancel()
    with suppress(asyncio.CancelledError):
        await vigilante
    return contadores


def run(args: argparse.Namespace) -> int:
    df = preparar(args.telemetry, args.max_sensors, args.limit, args.rebase_end)
    contadores = asyncio.run(simular(args, df))

    r = contadores.resumen()
    logger.info("--- Fin de la simulacion ---")
    logger.info("  publicados      : %s", f"{r['publicados']:,}")
    logger.info("  fallidos        : %d", r["fallidos"])
    logger.info("  conexiones caidas: %d", r["reconexiones"])
    logger.info("  tasa de perdida : %.4f%%", r["tasa_perdida_pct"])
    logger.info("  duracion        : %.1f s", r["duracion_s"])
    logger.info("  throughput      : %.1f ev/s", r["throughput_ev_s"])
    logger.info("  retraso maximo  : %.2f s", r["retraso_max_s"])
    if r["retraso_max_s"] > args.max_lag:
        logger.warning(
            "EL SIMULADOR NO SOSTUVO EL RITMO PEDIDO: llego a ir %.1f s por detras de su "
            "agenda. El throughput de arriba mide esta maquina, no el pipeline; repite con "
            "un --speedup menor o reparte la carga en mas procesos.", r["retraso_max_s"])
        return 1
    return 0 if r["fallidos"] == 0 else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Simulador del parque de medidores IoT (TFM)")
    anadir_argumentos_dataset(p)
    anadir_argumentos_mqtt(p, client_id="tfm-sim")
    p.add_argument("--acelerar", dest="speedup", metavar="FACTOR", type=float, default=2000.0,
                   help="Cuantas veces mas rapido avanza el reloj simulado. Es un factor "
                        "POR SENSOR: la tasa agregada es n_sensores x acelerar / 3600")
    p.add_argument("--clients", type=int, default=0,
                   help="Conexiones MQTT simultaneas. 0 (por defecto) abre una por sensor, "
                        "que es el parque real; un valor menor agrupa varios sensores por "
                        "conexion, como haria una pasarela de edificio")
    p.add_argument("--report-interval", type=float, default=10.0,
                   help="Segundos entre informes de progreso")
    p.add_argument("--max-reconexiones", type=int, default=20,
                   help="Intentos de reconexion por sensor antes de abandonarlo. Un medidor "
                        "real reintenta y vuelca lo acumulado cuando recupera el enlace")
    p.add_argument("--espera-reconexion", type=float, default=3.0,
                   help="Segundos entre intentos de reconexion")
    p.add_argument("--max-lag", type=float, default=1.0,
                   help="Retraso maximo tolerado respecto a la agenda, en segundos. Por "
                        "encima, la ejecucion se marca como no valida: el simulador no "
                        "sostuvo el ritmo y sus cifras no miden el pipeline")
    return p.parse_args()


if __name__ == "__main__":
    configurar_logging("mqtt_simulator")
    try:
        raise SystemExit(run(parse_args()))
    except (FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        raise SystemExit(1)
    except aiomqtt.MqttError as exc:
        logger.error("No se pudo hablar con el broker MQTT: %s. Levanta el stack: "
                     "docker compose -f pipeline/docker-compose.yml up -d", exc)
        raise SystemExit(1)
