# Pipeline IoT — stack de ingesta (Fase 1)

Capa de ingesta y gobernanza de esquemas del pipeline Kappa del TFM. Las bases
de datos (TimescaleDB / PostgreSQL) y Grafana se anaden en una fase posterior.

```
Simulador (host, venv) --MQTT--> Mosquitto --> bridge --Avro--> Kafka
                                                 |
                                                 +--> Apicurio (esquemas)
```

## Por que Spark no esta containerizado

El job de Spark Structured Streaming se ejecuta desde el `venv` del host contra
el listener externo de Kafka (`localhost:29092`), no como servicio del compose.
Motivo: cada cambio de codigo exigiria reconstruir o volver a copiar la imagen,
y la depuracion desde VS Code es directa ejecutandolo en local. La
containerizacion del job se puede anadir al final para la demostracion.

Esto es lo que obliga a Kafka a publicar **dos listeners**:

| Listener   | Direccion         | Lo usa                                  |
|------------|-------------------|-----------------------------------------|
| `INTERNAL` | `kafka:9092`      | Contenedores de la red `tfm-net`        |
| `EXTERNAL` | `localhost:29092` | Simulador, Spark y utilidades del host  |

Un unico listener no funciona: el nombre `kafka` no resuelve desde el host, y
`localhost` no resuelve al broker desde otro contenedor.

## Servicios y puertos

| Servicio       | Imagen                                | Puerto (host) | Para que |
|----------------|---------------------------------------|---------------|----------|
| `mosquitto`    | `eclipse-mosquitto:2.0.22`            | 1883, 9001    | Broker MQTT |
| `kafka`        | `apache/kafka:4.3.1`                  | 29092         | Log Kappa (modo KRaft, sin Zookeeper) |
| `kafka-init`   | `apache/kafka:4.3.1`                  | —             | Crea los topicos y termina |
| `apicurio`     | `apicurio/apicurio-registry:3.3.1`    | 8080          | API REST del registro de esquemas |
| `apicurio-ui`  | `apicurio/apicurio-registry-ui:3.3.1` | 8888          | UI del registro (contenedor aparte en 3.x) |

Topicos creados por `kafka-init`:

- `iot.telemetry.raw` — eventos Avro publicados por el bridge (3 particiones)
- `iot.telemetry.dlq` — eventos que fallan la validacion de esquema (1 particion)

La auto-creacion de topicos esta **deshabilitada** a proposito: con ella
activada, una errata en un nombre de topico crearia un topico nuevo en silencio
y el sintoma aparecería mucho despues como "no llegan datos".

## Almacenamiento de Apicurio

Se usa `kafkasql` (el registro persiste su estado en topicos de Kafka) en lugar
de `mem`. Dos razones: los esquemas sobreviven a un reinicio del contenedor
—requisito para poder demostrar la evolucion de esquema del Objetivo 2— y no
anade ninguna dependencia nueva, porque Kafka ya esta en el stack.

## Uso

Levantar el stack:

```bash
docker compose -f pipeline/docker-compose.yml up -d
```

Ver el estado (los cuatro servicios de larga duracion deben quedar `healthy`;
`tfm-kafka-init` debe quedar `Exited (0)`, es de un solo uso):

```bash
docker compose -f pipeline/docker-compose.yml ps -a
```

Parar conservando los datos, o borrando tambien los volumenes:

```bash
docker compose -f pipeline/docker-compose.yml down
```

```bash
docker compose -f pipeline/docker-compose.yml down -v
```

## Comprobaciones rapidas

Registro de esquemas operativo:

```bash
curl -s http://localhost:8080/apis/registry/v3/system/info
```

Nota: esta imagen de Apicurio **no** expone los endpoints de health de Quarkus
(`/q/health/ready` y `/health/ready` devuelven 404). Por eso el `healthcheck`
del compose usa `/apis/registry/v3/groups`, que ademas atraviesa la capa de
almacenamiento y confirma que el registro esta realmente operativo.

Topicos existentes:

```bash
docker exec tfm-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Ver la telemetria llegando al broker MQTT (en una terminal aparte, mientras
corre el simulador):

```bash
docker exec tfm-mosquitto mosquitto_sub -h localhost -t 'iot/#' -v
```

UI del registro de esquemas: <http://localhost:8888>

## Estado verificado

Comprobado sobre este stack (Docker 29.7.2, Compose v5.4.0, Java 21 Temurin,
Python 3.11 + PySpark 4.2.0):

- Los cuatro servicios de larga duracion levantan y quedan `healthy`.
- `kafka-init` crea `iot.telemetry.raw` y `iot.telemetry.dlq` y sale con codigo 0.
- Produccion y consumo en Kafka desde el host via `localhost:29092`.
- Apicurio responde en `/apis/registry/v3/system/info` con almacenamiento
  `KafkaSQL`.
- El simulador publica en Mosquitto con la topologia de topicos
  `iot/{company_id}/{site_id}/{machine_id}/telemetry` y 0% de perdida en la
  prueba corta.

- Esquema Avro v1 registrado en Apicurio como `iot/iot.telemetry.raw-value`,
  con las reglas `VALIDITY=FULL` y `COMPATIBILITY=FULL_TRANSITIVE` activas y
  comprobadas contra evoluciones compatibles e incompatibles (ver
  [schemas/README.md](schemas/README.md)).

Pendiente: bridge MQTT→Kafka, job de Spark, sinks (TimescaleDB / PostgreSQL),
Grafana.
