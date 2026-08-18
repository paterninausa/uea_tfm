-- Progreso de cada micro-lote de Spark Structured Streaming.
--
-- Es la fuente del KPI 3 del Objetivo 3 ("latencia de micro-lote < 3 s"). Spark
-- expone estas cifras en `query.lastProgress`, pero solo en memoria y solo la
-- del ultimo lote: al terminar el job se pierden. Persistirlas es lo que hace
-- que el KPI se pueda medir despues de la ejecucion, y no solo mirando la
-- consola mientras corre.
--
-- Vive en TimescaleDB y no en PostgreSQL a proposito: es telemetria operativa,
-- del mismo tipo que los agregados que consume Grafana, asi que la duracion de
-- micro-lote se puede dibujar en el dashboard de estado del pipeline junto a la
-- latencia de ingesta. PostgreSQL es el sumidero analitico de negocio y aqui no
-- pinta nada.
--
-- Este fichero se ejecuta al crear el volumen de TimescaleDB, pero tambien lo
-- aplica el propio job al arrancar (`asegurar_tabla_progreso`), para que anadir
-- esta tabla no obligue a destruir un volumen que ya tiene datos. Por eso todo
-- el DDL es idempotente.

CREATE TABLE IF NOT EXISTS streaming_progress (
    -- Instante en que Spark emitio el informe de progreso.
    trigger_ts    TIMESTAMPTZ NOT NULL,

    -- Identifica la EJECUCION del job. Es imprescindible: al dejar el sistema
    -- en estado limpio se borran los checkpoints, y entonces batch_id vuelve a
    -- empezar en 0. Sin run_id, el lote 0 de una medicion pisaria el lote 0 de
    -- la anterior y no se podrian comparar dos ejecuciones entre si, que es
    -- justo lo que exige la escalera de carga del Objetivo 5.
    run_id        TEXT        NOT NULL,

    -- 'metricas-timescaledb' | 'eventos-postgresql'. Son dos consultas de
    -- streaming independientes y cada una tiene su propia duracion de lote.
    query_name    TEXT        NOT NULL,
    batch_id      BIGINT      NOT NULL,

    -- Volumen y ritmo del lote. input_rows_per_second frente a
    -- processed_rows_per_second es lo que dice si el job va retrasado respecto
    -- a lo que entra por Kafka o si sigue el ritmo.
    num_input_rows            BIGINT,
    input_rows_per_second     DOUBLE PRECISION,
    processed_rows_per_second DOUBLE PRECISION,

    -- EL KPI 3: duracion total del disparo. El resto del desglose se guarda
    -- para poder atribuir esa duracion cuando se sale del objetivo.
    duration_ms               BIGINT,
    add_batch_ms              BIGINT,
    query_planning_ms         BIGINT,
    sink_num_output_rows      BIGINT,

    -- Cierre de ventana. Comparar el maximo tiempo de evento con el watermark
    -- es lo que permitio demostrar que la latencia del agregado la fija la
    -- cadencia horaria del contador y no el pipeline: el watermark solo avanza
    -- cuando llega la hora siguiente entera.
    event_time_max            TIMESTAMPTZ,
    watermark                 TIMESTAMPTZ,

    -- trigger_ts va PRIMERO en la clave porque TimescaleDB exige que todo
    -- indice unico de una hypertable incluya su columna de particionado.
    PRIMARY KEY (trigger_ts, run_id, query_name, batch_id)
);

SELECT create_hypertable(
    'streaming_progress',
    'trigger_ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- Patron de consulta del informe de KPIs: "todos los lotes de la ultima
-- ejecucion, por consulta".
CREATE INDEX IF NOT EXISTS idx_progress_run
    ON streaming_progress (run_id, query_name, trigger_ts DESC);

COMMENT ON TABLE streaming_progress IS
    'Progreso por micro-lote de Spark. Fuente del KPI de latencia de lote del Objetivo 3.';
