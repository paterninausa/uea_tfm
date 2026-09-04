"""
Microservicio puente MQTT -> Kafka con validacion Avro (Objetivos 1 y 2).

Se suscribe a la telemetria que publican los sensores simulados en Mosquitto,
valida cada evento contra el esquema Avro registrado en Apicurio, lo serializa
y lo publica en Kafka. Lo que no valida no se descarta: se desvia al topico de
mensajes muertos (DLQ) junto con el motivo del rechazo.

Existe porque Mosquitto no tiene puente nativo a Kafka. Se evaluo NanoMQ por
tenerlo, pero se verifico que esa funcion es exclusiva de EMQX Enterprise (de
pago), de modo que el puente es codigo propio.

    iot/{building_id}/{meter_type}/telemetry          iot.telemetry.raw
        (JSON, QoS 1)  --> [ bridge ] --> Avro + cabecera de esquema
                                    \\
                                     --> iot.telemetry.dlq (JSON + motivo)

Uso:
    python mqtt_kafka_bridge.py
    python mqtt_kafka_bridge.py --report-interval 5 --verbose
"""

import argparse
import json
import logging
import math
import re
import signal
import socket
import sys
import threading
import time
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.apicurio import (  # noqa: E402
    DEFAULT_REGISTRY_URL,
    DEFAULT_SUBJECT,
    SchemaRegistryError,
    latest_schema,
    schema_registry_client,
)
from common.connection_args import TOPIC_DLQ, TOPIC_RAW  # noqa: E402
from common.logging_setup import configurar_logging  # noqa: E402

logger = logging.getLogger("bridge")

# Campo que el simulador publica como cadena ISO-8601 y que el esquema Avro
# declara como long/timestamp-millis. La conversion es responsabilidad del
# bridge: el simulador emite el formato del dataset original y el contrato Avro
# exige epoch en milisegundos.
ISO_TIMESTAMP_FIELDS = ("timestamp",)

MQTT_TOPIC_FILTER = "iot/#"


def iso_to_epoch_millis(value: str) -> int:
    """Convierte una marca de tiempo ISO-8601 a epoch en milisegundos.

    Las marcas del dataset vienen sin zona horaria ("2025-01-01T00:00:00"). Se
    interpretan como UTC de forma explicita: dejarlas a merced de la zona local
    del contenedor haria que el mismo evento cayera en una ventana temporal
    distinta segun la maquina donde corriera el bridge, lo que arruinaria la
    reproducibilidad de las agregaciones de Spark.
    """
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _clase_de_motivo(motivo: str) -> str:
    """Quita los valores concretos para que el desglose agrupe de verdad.

    Sin esto, "timestamp en el futuro (2099-01-01...)" y "timestamp en el futuro
    (2027-01-01...)" cuentan como dos motivos distintos, y con trafico real el
    resumen se convertiria en cientos de lineas de "1 x ...". El valor exacto no
    se pierde: va integro al registro de la DLQ y al aviso individual de cada
    rechazo.
    """
    return re.sub(r"\([^)]*\)|'[^']*'|\"[^\"]*\"", "...", motivo)


def validar_dominio(evento: dict, margen_futuro_s: float) -> None:
    """Comprueba lo que el esquema Avro NO puede comprobar.

    Avro valida la FORMA —que los campos esten y sean del tipo declarado— pero
    no el significado. Se comprobo que pasan sin objecion un `meter_reading` de
    1e308, uno negativo y una marca de tiempo del ano 2099.

    Las dos guardas de aqui cubren los dos casos que hacen dano de verdad:

    FECHA FUTURA. Es el peor de todos y no da ningun error: Spark adelanta su
    watermark a ese instante y **descarta como tardio todo el trafico legitimo
    que llegue despues**. El pipeline sigue vivo, los medidores del bridge
    dicen que todo va bien, y los agregados simplemente dejan de escribirse.
    Ningun medidor puede medir manana; se admite un margen pequeno por
    desajuste de relojes.

    No se pone limite por abajo a proposito: el simulador reproduce el historico
    de 2016 y sus marcas son legitimamente antiguas.

    LECTURA IMPOSIBLE. Un valor negativo no existe en un medidor, y un infinito
    o un NaN arruinan la suma y la media de toda la ventana en la que caigan: un
    solo evento basta para dejar sin sentido un agregado horario completo.
    """
    marca = evento.get("timestamp")
    if isinstance(marca, int):
        limite = (time.time() + margen_futuro_s) * 1000
        if marca > limite:
            raise ValueError(
                f"timestamp en el futuro ({datetime.fromtimestamp(marca / 1000, timezone.utc)}), "
                f"lo que envenenaria el watermark de Spark")

    lectura = evento.get("meter_reading")
    if isinstance(lectura, (int, float)):
        if not math.isfinite(lectura):
            raise ValueError(f"meter_reading no finito ({lectura})")
        if lectura < 0:
            raise ValueError(f"meter_reading negativo ({lectura}), imposible en un medidor")


class Stats:
    """Medidores del bridge, base de la evidencia de los KPIs.

    Las actualizaciones van bajo lock porque los callbacks de entrega de Kafka
    se ejecutan en el hilo de red del productor, no en el que recibe de MQTT.
    """

    def __init__(self, latency_window: int = 10000):
        self._lock = threading.Lock()
        self.recibidos = 0
        self.publicados = 0
        self.dlq = 0
        self.fallos_kafka = 0
        # Ventana deslizante de latencias: acotada para que un proceso de larga
        # duracion no crezca en memoria sin limite.
        self.latencias_ms = deque(maxlen=latency_window)
        self.motivos: Counter[str] = Counter()
        self._t0 = time.monotonic()

    def recibido(self):
        with self._lock:
            self.recibidos += 1

    def publicado(self, latencia_ms: float | None):
        with self._lock:
            self.publicados += 1
            if latencia_ms is not None:
                self.latencias_ms.append(latencia_ms)

    def a_dlq(self, motivo: str = ""):
        with self._lock:
            self.dlq += 1
            # Se agrupa aqui, en el momento del rechazo, en vez de consumir
            # despues el topico: el bridge ya sabe por que rechazo cada evento y
            # asi el desglose sale en el log del proceso que lo produjo, sin
            # depender de que alguien vaya a mirar la DLQ.
            self.motivos[_clase_de_motivo(motivo)] += 1

    def fallo_kafka(self):
        with self._lock:
            self.fallos_kafka += 1

    def snapshot(self) -> dict:
        with self._lock:
            lat = sorted(self.latencias_ms)
            total = self.recibidos
            # Perdida = lo recibido de MQTT que no llego a ningun topico de
            # Kafka. Lo desviado a la DLQ NO cuenta como perdida: esta
            # persistido y es recuperable; cuenta como evento invalido.
            perdidos = max(0, total - self.publicados - self.dlq)
            return {
                "recibidos": total,
                "publicados": self.publicados,
                "dlq": self.dlq,
                "fallos_kafka": self.fallos_kafka,
                "perdidos": perdidos,
                "tasa_perdida_pct": (perdidos / total * 100) if total else 0.0,
                "eventos_por_segundo": total / max(1e-9, time.monotonic() - self._t0),
                "latencia_p50_ms": lat[len(lat) // 2] if lat else None,
                "latencia_p95_ms": lat[int(len(lat) * 0.95)] if lat else None,
                "latencia_max_ms": lat[-1] if lat else None,
                "motivos_dlq": self.motivos.most_common(),
            }


class Bridge:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.stats = Stats()
        self._parada = threading.Event()

        logger.info("Instancia del bridge: client_id=%s, grupo compartido=%s",
                    args.client_id, args.shared_group or "(ninguno)")

        # El esquema se resuelve UNA vez al arrancar: el bridge produce siempre
        # con la version vigente en ese momento. Si se registra una version
        # nueva, se adopta reiniciando el servicio, no en caliente.
        sr_client = schema_registry_client(args.registry_url)
        self.schema_id, schema_str = latest_schema(sr_client, args.subject)
        logger.info("Esquema resuelto: subject %s (schema id=%d)", args.subject, self.schema_id)

        # AvroSerializer de confluent-kafka: serializa el evento contra el
        # esquema y le antepone la cabecera de cable de Confluent (byte magico +
        # id). auto.register.schemas=False -> el productor NUNCA crea esquemas;
        # la gobernanza vive en register_schema.py. Resuelve el id una sola vez
        # (lo cachea), asi que serializa en proceso sin red por evento.
        # Si el evento no encaja en el esquema (campo ausente, tipo incorrecto,
        # simbolo de enum fuera del dominio), LANZA: esa excepcion es el
        # mecanismo de validacion que alimenta la DLQ.
        self.serializer = AvroSerializer(
            sr_client, schema_str, conf={"auto.register.schemas": False})
        self.ser_ctx = SerializationContext(args.topic, MessageField.VALUE)

        self.producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            # acks="all": el lider espera confirmacion de todas las replicas en
            # sincronia antes de dar por escrito el mensaje. Es la unica
            # configuracion coherente con el objetivo de perdida < 0.1%.
            acks="all",
            retries=5,
            linger_ms=args.linger_ms,
            max_in_flight_requests_per_connection=5,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

        # Sin --shared-group, suscripcion normal: cada instancia recibe una
        # copia de cada mensaje (el comportamiento de siempre, una sola
        # instancia). Con --shared-group, el prefijo $share/<grupo>/ le pide
        # al broker que REPARTA los mensajes entre las instancias del mismo
        # grupo en vez de duplicarlos — es lo que hace falta para escalar el
        # bridge horizontalmente. Mosquitto lo soporta desde la 1.6, sin
        # cambiar de broker ni de version de protocolo MQTT.
        self._topic_filter = (
            f"$share/{args.shared_group}/{MQTT_TOPIC_FILTER}"
            if args.shared_group else MQTT_TOPIC_FILTER
        )

        self.mqtt_client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=args.client_id,
            # Sesion persistente: si el bridge cae, el broker retiene los
            # mensajes QoS 1 de esta suscripcion y los entrega al reconectar.
            # Es parte de la recuperacion sin perdida de datos del Objetivo 5.
            clean_session=False,
        )
        # Backoff de reconexion acotado. Por defecto paho crece exponencialmente
        # hasta 120 s, y eso se midio caro: tras una caida de 15 s el broker
        # volvia pero el bridge tardaba 17 s mas en resuscribirse. En esa ventana
        # los 652 productores ya publicaban con normalidad y Mosquitto encolaba
        # los mensajes para la sesion persistente del bridge hasta llenar
        # `max_queued_messages` (10.000) y descartar 6.595 en silencio. El
        # consumidor tardon es lo que provoca la perdida, no el productor.
        self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=5)

        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect
        self.mqtt_client.on_message = self._on_message

    # ---------------------------------------------------------------- MQTT --
    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code != 0:
            logger.error("Fallo al conectar con el broker MQTT (reason_code=%s)", reason_code)
            return
        logger.info("Conectado a MQTT; suscribiendo a %s (QoS %d)", self._topic_filter, self.args.qos)
        client.subscribe(self._topic_filter, qos=self.args.qos)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code=None, properties=None):
        if not self._parada.is_set():
            logger.warning("Desconectado de MQTT (reason_code=%s); paho reintentara", reason_code)

    def _on_message(self, client, userdata, msg):
        """Procesa el evento en el hilo de red de paho.

        El trabajo por mensaje es corto (decodificar JSON, convertir dos
        campos, serializar Avro) y `producer.send` es asincrono: encola en el
        buffer del productor y vuelve. Por eso no se introduce una cola de
        trabajo intermedia, que anadiria un punto donde perder mensajes ante
        una caida. Si las pruebas de carga del Objetivo 5 muestran que este
        hilo se satura, el siguiente paso seria un pool de trabajadores.
        """
        self.stats.recibido()
        try:
            evento = self._a_registro_avro(msg.payload)
            validar_dominio(evento, self.args.margen_futuro)
            self._publicar(evento, msg.topic)
        except Exception as exc:
            self._a_dlq(msg, exc)

    # ----------------------------------------------------------- Transform --
    def _a_registro_avro(self, payload: bytes) -> dict:
        """JSON crudo de MQTT -> registro conforme al esquema Avro.

        No se rellenan campos ausentes ni se corrigen valores fuera de dominio
        a proposito: si el evento no cumple el contrato, debe fallar y acabar
        en la DLQ. Ahi esta la evidencia del "100% de eventos validados contra
        el esquema" del Objetivo 2; un bridge indulgente la haria imposible.
        """
        evento = json.loads(payload.decode("utf-8"))
        for campo in ISO_TIMESTAMP_FIELDS:
            if campo in evento and isinstance(evento[campo], str):
                evento[campo] = iso_to_epoch_millis(evento[campo])
        return evento

    def _serializar(self, evento: dict) -> bytes:
        """Cabecera de cable de Confluent (byte magico + id) + payload Avro.

        El AvroSerializer lanza si el evento no encaja en el esquema (campo
        ausente, tipo incorrecto, simbolo de enum fuera del dominio): esa
        excepcion es el mecanismo de validacion que desvia a la DLQ.
        """
        return self.serializer(evento, self.ser_ctx)

    # ------------------------------------------------------------- Salidas --
    def _publicar(self, evento: dict, topico_mqtt: str) -> None:
        datos = self._serializar(evento)

        # LA CLAVE DE KAFKA IDENTIFICA AL SENSOR, es decir el par (edificio,
        # tipo de medidor): garantiza que todas las lecturas de un mismo
        # medidor caen en la misma particion y por tanto se procesan en orden,
        # que es lo que necesitan las agregaciones por ventana de Spark.
        #
        # No confundirla con la clave natural del evento, que ademas incluye
        # timestamp: esa identifica una lectura concreta y usarla aqui
        # repartiria al azar las lecturas de un mismo medidor, una por
        # particion, perdiendo el orden.
        #
        # Un valor None NO es inocuo: el productor reparte esos mensajes en
        # round-robin y el orden por sensor se pierde en silencio, sin ningun
        # error. Por eso se construye con acceso directo y no con .get(): si el
        # evento no trae estos campos, no cumple el contrato y debe acabar en
        # la DLQ, no publicarse sin clave.
        clave = f"{evento['building_id']}:{evento['meter_type']}"

        latencia = None
        sim_ts = evento.get("sim_publish_ts")
        if isinstance(sim_ts, int):
            latencia = time.time() * 1000 - sim_ts

        futuro = self.producer.send(self.args.topic, key=clave, value=datos)
        futuro.add_callback(lambda _meta, lat=latencia: self.stats.publicado(lat))
        futuro.add_errback(lambda exc, t=topico_mqtt: self._error_kafka(exc, t))

    def _error_kafka(self, exc: KafkaError, topico_mqtt: str) -> None:
        """Un fallo de entrega tras agotar los reintentos es perdida real."""
        self.stats.fallo_kafka()
        logger.error("Kafka no acepto un evento de %s: %s", topico_mqtt, exc)

    def _a_dlq(self, msg, exc: Exception) -> None:
        """Desvia a la DLQ el evento que no supero la validacion.

        Se envia como JSON, no como Avro: por definicion no cumple el esquema,
        asi que no puede serializarse con el. Se conserva el payload original
        intacto para poder reprocesarlo una vez corregida la causa.
        """
        motivo = f"{type(exc).__name__}: {exc}"
        self.stats.a_dlq(motivo)
        logger.warning("Evento rechazado de %s -> DLQ (%s)", msg.topic, motivo)

        registro = {
            "error": motivo,
            "topico_mqtt": msg.topic,
            "bridge_ts": int(time.time() * 1000),
            "payload_original": msg.payload.decode("utf-8", errors="replace"),
        }
        try:
            self.producer.send(self.args.dlq_topic, value=json.dumps(registro).encode("utf-8"))
        except Exception as dlq_exc:
            # Si tampoco se puede escribir en la DLQ, el evento se pierde: hay
            # que dejar constancia explicita en el log.
            logger.error("FALLO AL ESCRIBIR EN LA DLQ, evento perdido: %s", dlq_exc)

    # -------------------------------------------------------------- Ciclo ---
    def _informar(self) -> None:
        s = self.stats.snapshot()
        p95 = f"{s['latencia_p95_ms']:.0f}ms" if s["latencia_p95_ms"] is not None else "n/d"
        p50 = f"{s['latencia_p50_ms']:.0f}ms" if s["latencia_p50_ms"] is not None else "n/d"
        logger.info(
            "recibidos=%d publicados=%d dlq=%d perdidos=%d (%.4f%%) | %.1f ev/s | latencia MQTT->Kafka p50=%s p95=%s",
            s["recibidos"], s["publicados"], s["dlq"], s["perdidos"],
            s["tasa_perdida_pct"], s["eventos_por_segundo"], p50, p95,
        )
        self._desglosar_dlq(s)

    def _desglosar_dlq(self, s: dict) -> None:
        """Por que se rechazaron los eventos, agrupado por motivo.

        Un medidor a secas —"dlq=37"— obliga a ir a leer el topico para saber
        si son 37 sintomas del mismo problema o 37 problemas distintos. Con el
        desglose, el log del bridge responde solo: "37, todos porque meter_type
        trae un simbolo fuera del enum".
        """
        if not s["motivos_dlq"]:
            return
        logger.warning("  desglose de los %d eventos rechazados:", s["dlq"])
        for motivo, veces in s["motivos_dlq"][:5]:
            logger.warning("    %6d x %s", veces, motivo[:120])
        restantes = len(s["motivos_dlq"]) - 5
        if restantes > 0:
            logger.warning("    y %d motivos distintos mas", restantes)

    def run(self) -> int:
        signal.signal(signal.SIGINT, self._senal_parada)
        signal.signal(signal.SIGTERM, self._senal_parada)

        logger.info("Conectando a MQTT %s:%d ...", self.args.broker_host, self.args.broker_port)
        self.mqtt_client.connect(self.args.broker_host, self.args.broker_port, keepalive=30)
        self.mqtt_client.loop_start()
        logger.info("Bridge en marcha. Kafka=%s topico=%s dlq=%s",
                    self.args.bootstrap_servers, self.args.topic, self.args.dlq_topic)

        try:
            while not self._parada.wait(self.args.report_interval):
                self._informar()
        finally:
            self._cerrar()

        s = self.stats.snapshot()
        return 0 if s["perdidos"] == 0 else 1

    def _senal_parada(self, *_args):
        logger.info("Senal de parada recibida, cerrando...")
        self._parada.set()

    def _cerrar(self) -> None:
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        # flush() bloquea hasta que se entrega lo que quede en el buffer del
        # productor: sin esto, cerrar el proceso perderia los ultimos eventos.
        logger.info("Vaciando el buffer del productor de Kafka...")
        self.producer.flush(timeout=30)
        self.producer.close(timeout=10)

        s = self.stats.snapshot()
        logger.info("--- Resumen final ---")
        for k, v in s.items():
            if k == "motivos_dlq":
                continue
            logger.info("  %-22s %s", k, f"{v:.4f}" if isinstance(v, float) else v)
        self._desglosar_dlq(s)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bridge MQTT -> Kafka con validacion Avro (TFM)")
    p.add_argument("--broker-host", default="localhost")
    p.add_argument("--broker-port", type=int, default=1883)
    p.add_argument("--qos", type=int, default=1, choices=[0, 1, 2],
                   help="QoS de la suscripcion MQTT (1 = al menos una vez, por defecto)")
    p.add_argument("--client-id", default=None,
                   help="client_id MQTT. Debe ser estable Y UNICO por instancia: la sesion "
                        "persistente se asocia a el, y dos instancias con el mismo client_id "
                        "hacen que el broker desconecte la mas antigua al conectar la nueva. "
                        "Por defecto se deriva del hostname (tfm-bridge-<hostname>): unico por "
                        "replica al escalar con `docker compose up -d --scale bridge=N`, y "
                        "estable mientras esa replica no se recree")
    p.add_argument("--shared-group", default=None,
                   help="Nombre de grupo para suscripcion compartida ($share/<grupo>/...). "
                        "Sin esto, cada instancia recibe una copia de cada mensaje (correcto "
                        "con una sola instancia, duplica con varias). Con esto, el broker "
                        "reparte los mensajes entre las instancias del grupo: es lo que "
                        "permite escalar el bridge horizontalmente")
    p.add_argument("--bootstrap-servers", default="localhost:29092")
    p.add_argument("--topic", default=TOPIC_RAW)
    p.add_argument("--dlq-topic", default=TOPIC_DLQ)
    p.add_argument("--linger-ms", type=int, default=5,
                   help="Espera del productor para agrupar mensajes. Sube el throughput a "
                        "costa de latencia; 5ms es despreciable frente al KPI de 2s")
    p.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    p.add_argument("--subject", default=DEFAULT_SUBJECT,
                   help="Subject del esquema en el registro (convencion {topic}-value)")
    p.add_argument("--margen-futuro", type=float, default=300.0,
                   help="Segundos de adelanto tolerados en el timestamp de un evento. Por "
                        "encima se rechaza a la DLQ: una fecha futura envenena el watermark "
                        "de Spark y hace que se descarte todo el trafico posterior")
    p.add_argument("--report-interval", type=float, default=10.0,
                   help="Segundos entre informes de metricas")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()
    if not args.client_id:
        # Estable (el hostname del contenedor no cambia entre reinicios de esa
        # replica) y unico (cada replica de `--scale bridge=N` tiene el suyo).
        args.client_id = f"tfm-bridge-{socket.gethostname()}"
    return args


if __name__ == "__main__":
    configurar_logging("bridge")
    args = parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    try:
        sys.exit(Bridge(args).run())
    except SchemaRegistryError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    except ConnectionRefusedError:
        logger.error("No se pudo conectar al broker MQTT en %s:%d. "
                     "Levanta el stack: docker compose -f pipeline/docker-compose.yml up -d",
                     args.broker_host, args.broker_port)
        sys.exit(1)
