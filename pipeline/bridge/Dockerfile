# Imagen del microservicio bridge (MQTT -> Kafka, Objetivos 1 y 2).
#
# Solo instala lo que mqtt_kafka_bridge.py importa de verdad: paho-mqtt,
# confluent-kafka[avro] y kafka-python. NO usa el requirements.txt completo
# del proyecto -ese fichero tambien trae pyspark, pandas, aiomqtt, etc. para
# el resto del pipeline y la preparacion del dataset, que este servicio no
# necesita y solo inflarian la imagen.
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    paho-mqtt==2.1.0 \
    confluent-kafka[avro]==2.15.0 \
    kafka-python==2.0.2

# common/ y bridge/ son hermanos en pipeline/: mqtt_kafka_bridge.py hace
# sys.path.insert(0, parents[1]) para importar common.*, así que la imagen
# reproduce la misma disposición relativa.
COPY common/ ./common/
COPY bridge/mqtt_kafka_bridge.py ./bridge/mqtt_kafka_bridge.py

WORKDIR /app/bridge
ENTRYPOINT ["python", "mqtt_kafka_bridge.py"]
