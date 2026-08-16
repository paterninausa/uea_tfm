-- Esquema de metricas agregadas por ventana temporal (sumidero operacional).
--
-- Lo alimenta el job de Spark Structured Streaming y lo consume Grafana. Es la
-- mitad "tiempo casi real" del doble sumidero: responde a "que esta pasando
-- ahora en la planta", no a preguntas analiticas de negocio (eso vive en
-- PostgreSQL, ver ../../postgres/init/).

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry_metrics (
    -- Ventana de agregacion (tumbling de 1 hora sobre event time)
    window_start        TIMESTAMPTZ      NOT NULL,
    window_end          TIMESTAMPTZ      NOT NULL,

    -- Claves de agrupacion
    company_id          TEXT             NOT NULL,
    site_id             TEXT             NOT NULL,
    department          TEXT             NOT NULL,

    -- Metricas de consumo
    event_count         BIGINT           NOT NULL,
    distinct_machines   BIGINT           NOT NULL,
    avg_power_watts     DOUBLE PRECISION,
    min_power_watts     DOUBLE PRECISION,
    max_power_watts     DOUBLE PRECISION,
    sum_power_watts     DOUBLE PRECISION,
    avg_power_factor    DOUBLE PRECISION,
    avg_current_amp     DOUBLE PRECISION,
    avg_cpu_load        DOUBLE PRECISION,

    -- Indicadores de calidad, base de la deteccion de anomalias del Objetivo 4
    warn_count          BIGINT           NOT NULL,
    estimated_count     BIGINT           NOT NULL,

    -- Instrumentacion del KPI de latencia extremo a extremo (Objetivo 1).
    -- max_sim_publish_ts es el instante de publicacion MQTT del evento mas
    -- reciente de la ventana; ingested_at lo pone la propia base de datos al
    -- escribir la fila. La latencia extremo a extremo es la diferencia entre
    -- ambos, y por eso se persisten los dos y no solo el resultado: permite
    -- recalcular el KPI a posteriori sobre las filas ya escritas.
    max_sim_publish_ts  TIMESTAMPTZ,
    ingested_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),

    -- Una ventana solo puede tener una fila por combinacion de claves. Es lo
    -- que hace que un reproceso desde Kafka (arquitectura Kappa) sea
    -- idempotente en lugar de duplicar metricas.
    PRIMARY KEY (window_start, company_id, site_id, department)
);

-- Hypertable: TimescaleDB particiona la tabla por tiempo de forma transparente.
-- Con chunks de 7 dias, las consultas tipicas de Grafana (ultimas horas o
-- ultimos dias) tocan uno o dos chunks en vez de recorrer toda la tabla.
-- create_hypertable pertenece a la edicion Apache-2 (imagen -oss), a
-- diferencia de las continuous aggregates y la compresion, que son TSL.
SELECT create_hypertable(
    'telemetry_metrics',
    'window_start',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE
);

-- Indice para el patron de consulta dominante de los dashboards: filtrar por
-- planta o departamento dentro de un rango temporal.
CREATE INDEX IF NOT EXISTS idx_metrics_site_time
    ON telemetry_metrics (site_id, department, window_start DESC);

COMMENT ON TABLE telemetry_metrics IS
    'Metricas agregadas por ventana de 1h sobre event time. Escritas por el job de Spark, consumidas por Grafana.';
