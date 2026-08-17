-- Metricas agregadas por ventana temporal (sumidero operacional).
--
-- Las escribe el job de Spark y las consume Grafana. Es la mitad "tiempo casi
-- real" del doble sumidero: responde a "que esta pasando ahora", no a preguntas
-- analiticas de negocio (eso vive en PostgreSQL).

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry_metrics (
    -- Ventana de agregacion (tumbling de 1 hora sobre event time)
    window_start          TIMESTAMPTZ      NOT NULL,
    window_end            TIMESTAMPTZ      NOT NULL,

    -- Claves de agrupacion. meter_type es OBLIGATORIO en la clave: la unidad de
    -- las lecturas depende del medio, de modo que agregar a traves de tipos de
    -- contador produciria una cifra sin significado fisico.
    site_id               INTEGER          NOT NULL,
    primary_use           TEXT             NOT NULL,
    meter_type            TEXT             NOT NULL,

    -- Metricas de consumo. Las unidades son las del meter_type de la fila.
    event_count           BIGINT           NOT NULL,
    distinct_buildings    BIGINT           NOT NULL,
    avg_reading           DOUBLE PRECISION,
    min_reading           DOUBLE PRECISION,
    max_reading           DOUBLE PRECISION,
    sum_reading           DOUBLE PRECISION,

    -- Consumo por pie cuadrado: cociente de sumas, no media de cocientes, para
    -- que los edificios grandes pesen lo que les corresponde. Es la metrica del
    -- informe de eficiencia operativa, comparable solo dentro de un meter_type.
    -- Se guarda tambien el denominador para que un panel que agregue varios
    -- grupos pueda recalcular el cociente en vez de promediar cocientes.
    sum_square_feet       DOUBLE PRECISION,
    avg_energy_intensity  DOUBLE PRECISION,

    -- Indicadores de calidad. zero_count es la senal que revela los contadores
    -- muertos: una lectura a cero aislada puede ser legitima, pero un grupo con
    -- ceros sostenidos hora tras hora no lo es. Se midio que las rachas llegan a
    -- durar 8.051 horas seguidas en el dataset.
    zero_count            BIGINT           NOT NULL,
    anomaly_count         BIGINT           NOT NULL,

    -- Instrumentacion del KPI de latencia extremo a extremo (Objetivo 1).
    -- Se persisten las dos marcas y no solo su diferencia, para poder recalcular
    -- el KPI a posteriori sobre las filas ya escritas.
    max_sim_publish_ts    TIMESTAMPTZ,
    ingested_at           TIMESTAMPTZ      NOT NULL DEFAULT now(),

    -- Una ventana solo puede tener una fila por combinacion de claves. Es lo que
    -- hace idempotente un reproceso desde Kafka en lugar de duplicar metricas.
    PRIMARY KEY (window_start, site_id, primary_use, meter_type)
);

-- Hypertable: TimescaleDB particiona por tiempo de forma transparente. Con
-- chunks de 7 dias, las consultas tipicas de Grafana tocan uno o dos chunks en
-- vez de recorrer la tabla entera. create_hypertable pertenece a la edicion
-- Apache-2 (imagen -oss), a diferencia de las continuous aggregates.
SELECT create_hypertable(
    'telemetry_metrics',
    'window_start',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE
);

-- Patron de consulta dominante de los dashboards: filtrar por emplazamiento,
-- tipo de contador y rango temporal.
CREATE INDEX IF NOT EXISTS idx_metrics_site_meter_time
    ON telemetry_metrics (site_id, meter_type, window_start DESC);

COMMENT ON TABLE telemetry_metrics IS
    'Agregados por ventana de 1h sobre event time. Escribe Spark, consume Grafana.';
