#!/usr/bin/env bash
#
# Arranca y para un clúster Spark standalone en la máquina local.
#
# POR QUE EXISTE. En modo `local[*]` no hay executors: el `*` son hilos dentro
# de la JVM del driver, de modo que no hay nada que pueda fallar por separado ni
# nada que repartir. Con un master y varios workers, el mismo job se ejecuta con
# executors en procesos independientes, que es lo que permite (a) demostrar que
# el job no depende del modo de despliegue y (b) medir la recuperación ante la
# caída de un executor, el único componente que las pruebas de fallo sobre
# contenedores no alcanzan.
#
# POR QUE `spark-class` Y NO `start-master.sh`. La distribución de PySpark
# instalada con pip NO incluye los scripts de clúster en `sbin/` —solo trae
# `spark-daemon.sh` y los del history server—, pero sí trae `bin/spark-class`,
# que es lo que esos scripts invocan por debajo. Se lanzan los daemons
# directamente con él.
#
# LOS WORKERS CORREN EN ESTA MISMA MAQUINA. Eso demuestra distribución entre
# PROCESOS y tolerancia al fallo de un executor; no demuestra distribución entre
# nodos, que exigiría varias máquinas. Conviene no confundir ambas cosas al
# describir los resultados.
#
# Con 10 nucleos para el cluster, la carga del sistema llego a 19,76 sobre 12
# nucleos y el simulador se retraso 9,5 s respecto a su agenda, invalidando la
# medicion. La causa no es el modo distribuido en si: es que el generador de
# carga, el bridge, el driver, los executors y siete contenedores comparten una
# sola maquina, de modo que cuanta mas CPU se da al cluster, menos queda para
# producir los eventos que hay que medir. En un despliegue real el productor y
# el cluster estarian en maquinas distintas.
#
# Uso:
#     bash pipeline/tools/cluster.sh start     # master + $WORKERS workers
#     bash pipeline/tools/cluster.sh status
#     bash pipeline/tools/cluster.sh stop
#
# Variables de entorno para ajustar el tamaño:
#     WORKERS=2  WORKER_CORES=2  WORKER_MEMORY=1g

set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV="$RAIZ/.venv"

WORKERS="${WORKERS:-2}"
WORKER_CORES="${WORKER_CORES:-2}"
WORKER_MEMORY="${WORKER_MEMORY:-1g}"

MASTER_HOST="${MASTER_HOST:-127.0.0.1}"
MASTER_PORT="${MASTER_PORT:-7077}"
# La UI del master NO usa el 8080 por defecto de Spark: ahí escucha Apicurio.
MASTER_WEBUI="${MASTER_WEBUI:-8090}"

DIR_TRABAJO="$RAIZ/pipeline/tools/cluster-work"
DIR_LOGS="$RAIZ/pipeline/logs"

MASTER_URL="spark://${MASTER_HOST}:${MASTER_PORT}"
API="http://${MASTER_HOST}:${MASTER_WEBUI}/json/"

spark_home() {
    "$VENV/bin/python" -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))'
}

esperar_master() {
    local intentos=0
    until curl -fsS "$API" >/dev/null 2>&1; do
        intentos=$((intentos + 1))
        if [ "$intentos" -ge 30 ]; then
            echo "El master no respondió en 60 s. Revisa $DIR_LOGS/spark-master.log" >&2
            return 1
        fi
        sleep 2
    done
}

start() {
    if curl -fsS "$API" >/dev/null 2>&1; then
        echo "Ya hay un master escuchando en $MASTER_URL. Usa 'stop' antes de rearrancar."
        return 1
    fi

    local SH
    SH="$(spark_home)"
    export SPARK_HOME="$SH"
    # Sin esto, Spark resuelve el hostname de WSL a una dirección de loopback y
    # avisa de que no puede enlazar; fijarla evita el aviso y hace determinista
    # la dirección con la que se registran los workers.
    export SPARK_LOCAL_IP="$MASTER_HOST"

    mkdir -p "$DIR_TRABAJO" "$DIR_LOGS"

    echo "Arrancando master en $MASTER_URL (UI en http://${MASTER_HOST}:${MASTER_WEBUI})"
    setsid nohup "$SH/bin/spark-class" org.apache.spark.deploy.master.Master \
        --host "$MASTER_HOST" --port "$MASTER_PORT" --webui-port "$MASTER_WEBUI" \
        > "$DIR_LOGS/spark-master.log" 2>&1 < /dev/null &

    esperar_master

    for i in $(seq 1 "$WORKERS"); do
        echo "Arrancando worker $i ($WORKER_CORES cores, $WORKER_MEMORY)"
        setsid nohup "$SH/bin/spark-class" org.apache.spark.deploy.worker.Worker "$MASTER_URL" \
            --cores "$WORKER_CORES" --memory "$WORKER_MEMORY" \
            --webui-port "$((MASTER_WEBUI + i))" --work-dir "$DIR_TRABAJO/worker$i" \
            > "$DIR_LOGS/spark-worker$i.log" 2>&1 < /dev/null &
    done

    # Los workers tardan unos segundos en registrarse; se espera a que estén
    # todos antes de dar el clúster por listo, porque lanzar el job con menos
    # workers de los previstos daría una medición con menos recursos sin avisar.
    local intentos=0
    until [ "$(vivos)" -ge "$WORKERS" ]; do
        intentos=$((intentos + 1))
        if [ "$intentos" -ge 30 ]; then
            echo "Solo se registraron $(vivos) de $WORKERS workers. Revisa $DIR_LOGS/spark-worker*.log" >&2
            return 1
        fi
        sleep 2
    done

    # La orden lleva --master a proposito: sin el, el job arranca en local[*]
    echo "Clúster listo: $(vivos) workers registrados."
    echo
    echo "Lanza el job contra el clúster con:"
    echo "  python pipeline/spark/stream_processing.py --master $MASTER_URL --trigger \"1 second\""
}

vivos() {
    curl -fsS "$API" 2>/dev/null \
        | "$VENV/bin/python" -c "import json,sys; print(sum(1 for w in json.load(sys.stdin)['workers'] if w['state']=='ALIVE'))" \
        2>/dev/null || echo 0
}

status() {
    if ! curl -fsS "$API" >/dev/null 2>&1; then
        echo "No hay master escuchando en $MASTER_URL"
        return 1
    fi
    curl -fsS "$API" | "$VENV/bin/python" -c "
import json, sys
d = json.load(sys.stdin)
vivos = [w for w in d['workers'] if w['state'] == 'ALIVE']
print(f\"master {d['status']} en {d['url']}\")
print(f\"  workers vivos : {len(vivos)} de {len(d['workers'])} registrados\")
print(f\"  cores totales : {sum(w['cores'] for w in vivos)}\")
print(f\"  memoria total : {sum(w['memory'] for w in vivos)} MB\")
for w in d['workers']:
    print(f\"    {w['id'][-12:]}  {w['state']:5}  {w['coresused']}/{w['cores']} cores  {w['memoryused']}/{w['memory']} MB\")
for a in d['activeapps']:
    print(f\"  app {a['name']}: {a['state']}, {a['cores']} cores, {a['memoryperslave']} MB por executor\")
"
}

stop() {
    local encontrados=0
    for patron in "org.apache.spark.deploy.worker.Worker" "org.apache.spark.deploy.master.Master"; do
        for pid in $(pgrep -f "$patron" || true); do
            kill "$pid" 2>/dev/null && encontrados=$((encontrados + 1))
        done
    done
    if [ "$encontrados" -eq 0 ]; then
        echo "No había procesos del clúster en marcha."
    else
        echo "Parados $encontrados procesos del clúster."
    fi
}

case "${1:-}" in
    start)  start ;;
    stop)   stop ;;
    status) status ;;
    *)      echo "Uso: $0 {start|stop|status}" >&2; exit 1 ;;
esac
