-- Esquema de eventos individuales enriquecidos (sumidero analitico).
--
-- Lo alimenta el job de Spark y lo consume Power BI. Es la mitad "analitica de
-- negocio" del doble sumidero: conserva el grano de evento para poder cruzar,
-- filtrar y agregar a demanda, a diferencia de TimescaleDB, que solo guarda
-- agregados por ventana ya calculados.
--
-- Separar ambos consumos es uno de los aportes diferenciadores del trabajo:
-- un dashboard operativo que refresca cada pocos segundos y un informe
-- analitico que recorre meses de historico tienen patrones de acceso opuestos,
-- y servirlos desde la misma tabla obliga a penalizar a uno de los dos.

CREATE TABLE IF NOT EXISTS telemetry_events (
    -- Identidad
    event_id             TEXT             PRIMARY KEY,
    event_time           TIMESTAMPTZ      NOT NULL,

    -- Jerarquia organizativa (dimensiones de los informes)
    company_id           TEXT             NOT NULL,
    site_id              TEXT             NOT NULL,
    machine_id           TEXT             NOT NULL,
    machine_model        TEXT,
    department           TEXT,
    country_code         TEXT,
    energy_type          TEXT,

    -- Medidas electricas
    power_watts          DOUBLE PRECISION,
    voltage              INTEGER,
    current_amp          DOUBLE PRECISION,
    power_factor         DOUBLE PRECISION,

    -- Contexto del equipo
    sensor_id            TEXT,
    sensor_status        TEXT,
    measurement_quality  TEXT,
    cpu_load             DOUBLE PRECISION,
    device_state         TEXT,
    user_count           INTEGER,

    -- Campos derivados anadidos por el job de Spark. Se calculan una sola vez
    -- en el pipeline y no en cada consulta de Power BI: la logica de negocio
    -- queda en un unico sitio y los informes no pueden divergir entre si.
    apparent_power_va    DOUBLE PRECISION,
    energy_wh_estimated  DOUBLE PRECISION,
    is_anomaly           BOOLEAN,
    anomaly_reason       TEXT,

    -- Instrumentacion del KPI de latencia (Objetivo 1)
    sim_publish_ts       TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ      NOT NULL DEFAULT now()
);

-- event_id como clave primaria hace idempotente el reproceso desde Kafka: se
-- verifico que es 100% unico en el dataset. Un reintento tras un fallo vuelve
-- a insertar las mismas filas en lugar de duplicarlas.

CREATE INDEX IF NOT EXISTS idx_events_time        ON telemetry_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_machine     ON telemetry_events (machine_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_site_dept   ON telemetry_events (site_id, department);
-- Indice parcial: los informes de anomalias filtran siempre por is_anomaly,
-- que es verdadero en una fraccion pequena de las filas.
CREATE INDEX IF NOT EXISTS idx_events_anomaly     ON telemetry_events (event_time DESC) WHERE is_anomaly;

COMMENT ON TABLE telemetry_events IS
    'Eventos individuales enriquecidos. Escritos por el job de Spark, consumidos por Power BI.';
