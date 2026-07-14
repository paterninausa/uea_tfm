# Mosquitto + Kafka (fase 1 del stack de ingesta)

Primer paso del Docker Compose completo: levanta **solo** el broker MQTT
(Mosquitto) y Kafka (KRaft, un unico nodo), sin el microservicio de bridge
todavia -- se valida cada pieza por separado antes de conectarlas.

## Por que Mosquitto y no NanoMQ

Se evaluo NanoMQ inicialmente por un supuesto "bridge nativo a Kafka", pero
se verifico contra la configuracion real de NanoMQ (`nanomq/nanomq`, HOCON
v0.23.1) que esa capacidad no existe en la version open-source -- solo en
EMQX Enterprise (de pago). Mosquitto es la implementacion de referencia de
MQTT (Eclipse Foundation) y no tiene menos prestaciones reales que NanoMQ
para lo que este TFM ejercita (TCP plano, sin QUIC). El puente MQTT->Kafka
es, en ambos casos, un microservicio propio -- ver `../bridge/` (siguiente
fase).

## Uso

    cd pipeline/broker
    docker compose up -d
    docker compose logs -f kafka-init   # confirma que el topic se creo

## Verificar Mosquitto

    mosquitto_sub -h localhost -t 'iot/#' -v

(en otra terminal, el simulador de `../simulator/` sigue funcionando igual
que en el smoke test, solo que ahora contra el Mosquitto del compose en vez
del contenedor suelto de prueba)

## Verificar Kafka

Listar topics:

    docker exec tfm-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:19092 --list

Consumir mensajes del topic (una vez que el bridge este publicando):

    docker exec -it tfm-kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:19092 \
        --topic iot.telemetry --from-beginning

## Puertos y listeners

| Puerto | Uso |
|---|---|
| `1883` (host) | MQTT, para el simulador y `mosquitto_sub`/`mosquitto_pub` |
| `9092` (host) | Kafka, listener `PLAINTEXT_HOST`, para depurar desde WSL/Ubuntu con herramientas del host |
| `19092` (solo red interna del compose) | Kafka, listener `PLAINTEXT`, el que deben usar el bridge y PyFlink cuando se anadan como servicios del mismo compose |

## Topic de Kafka

`iot.telemetry`, 12 particiones, replication-factor 1 (unico broker). El
particionado por `machine_id` (hash) se hace en el productor (el
microservicio de bridge), no aqui -- Kafka reparte por el key que reciba.

Creacion automatica de topics deshabilitada a proposito
(`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`): el topic se crea explicitamente
via el servicio `kafka-init`, que corre una vez y termina.

## Pendiente (siguiente fase)

- Microservicio bridge MQTT->Kafka (`pipeline/bridge/`), que se anadira a
  este mismo `docker-compose.yml` como servicio adicional
- Una vez el bridge este funcionando, el simulador deja de usarse contra
  Mosquitto "suelto" y pasa a apuntar siempre a este compose
