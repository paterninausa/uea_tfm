-- Eventos individuales enriquecidos (sumidero analitico).
--
-- Los escribe el job de Spark y los consume Power BI. Es la mitad "analitica de
-- negocio" del doble sumidero: conserva el grano de evento para poder cruzar,
-- filtrar y agregar a demanda, a diferencia de TimescaleDB, que solo guarda
-- agregados ya calculados.
--
-- Separar ambos consumos es uno de los aportes diferenciadores del trabajo: un
-- dashboard operativo que refresca cada pocos segundos y un informe analitico
-- que recorre meses de historico tienen patrones de acceso opuestos, y
-- servirlos desde la misma tabla obliga a penalizar a uno de los dos.

-- ---------------------------------------------------------------------------
-- Dimension de edificios
-- ---------------------------------------------------------------------------
-- Los atributos estaticos no viajan en cada evento: viven aqui. Power BI espera
-- exactamente este modelo en estrella; desnormalizarlos en cada una de los 5,68
-- millones de filas es el antipatron. Ademas, corregir el ano de construccion de
-- un edificio es actualizar una fila y no reprocesar el historico.
CREATE TABLE IF NOT EXISTS buildings (
    building_id   INTEGER  PRIMARY KEY,
    site_id       INTEGER  NOT NULL,
    primary_use   TEXT     NOT NULL,
    square_feet   INTEGER,
    year_built    INTEGER,   -- ausente en 184 de los 498 edificios
    floor_count   INTEGER    -- ausente en 409 de los 498
);

-- ---------------------------------------------------------------------------
-- Hechos: lecturas enriquecidas
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telemetry_events (
    -- Identidad: la CLAVE NATURAL del evento. No hay id inventado; se verifico
    -- que esta terna es unica sobre las 5.682.185 lecturas, sin un solo
    -- duplicado. Al derivarse solo de datos del sensor, un reproceso del log de
    -- Kafka regenera las mismas claves y el UPSERT las absorbe en lugar de
    -- duplicar filas, que es lo que hace idempotente la arquitectura Kappa.
    --
    -- meter_type es imprescindible en la clave: sin el, (building_id,
    -- event_time) colapsaria 1.345.428 eventos, porque un edificio tiene varios
    -- contadores midiendo a la misma hora.
    building_id          INTEGER          NOT NULL,
    meter_type           TEXT             NOT NULL,
    event_time           TIMESTAMPTZ      NOT NULL,

    -- Medida del contador. LA UNIDAD DEPENDE DE meter_type: electricidad, agua
    -- fria, vapor y agua caliente no son comparables ni sumables entre si.
    meter_reading        DOUBLE PRECISION,

    -- Copia desnormalizada de las dos dimensiones que filtran casi todos los
    -- informes. Se aceptan a proposito para que Power BI no necesite el join en
    -- las consultas mas frecuentes; el resto de atributos se consultan en
    -- `buildings`.
    site_id              INTEGER,
    primary_use          TEXT,
    square_feet          INTEGER,

    -- Campos derivados, calculados una sola vez en el pipeline para que la
    -- logica de negocio viva en un unico sitio y dos informes no puedan acabar
    -- con definiciones distintas de la misma metrica.
    energy_intensity     DOUBLE PRECISION,  -- consumo por pie cuadrado
    is_zero_reading      BOOLEAN,           -- indicador de calidad, no anomalia
    is_anomaly           BOOLEAN,
    anomaly_reason       TEXT,

    -- Instrumentacion del KPI de latencia (Objetivo 1)
    sim_publish_ts       TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),

    PRIMARY KEY (building_id, meter_type, event_time)
);

CREATE INDEX IF NOT EXISTS idx_events_time      ON telemetry_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_site_use  ON telemetry_events (site_id, primary_use);
CREATE INDEX IF NOT EXISTS idx_events_meter     ON telemetry_events (meter_type, event_time DESC);
-- Indices parciales: los informes de anomalias y de calidad filtran siempre por
-- su bandera, que es verdadera en una fraccion pequena de las filas.
CREATE INDEX IF NOT EXISTS idx_events_anomaly   ON telemetry_events (event_time DESC) WHERE is_anomaly;
CREATE INDEX IF NOT EXISTS idx_events_zero      ON telemetry_events (event_time DESC) WHERE is_zero_reading;

COMMENT ON TABLE telemetry_events IS
    'Lecturas enriquecidas a grano de evento. Escribe Spark, consume Power BI.';
COMMENT ON TABLE buildings IS
    'Dimension de edificios. Se carga una vez desde ashrae_buildings.parquet.';
