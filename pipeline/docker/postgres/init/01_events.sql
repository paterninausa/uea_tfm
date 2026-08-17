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
-- Linea base por sensor
-- ---------------------------------------------------------------------------
-- Cuartiles del historico de cada contador, calculados una vez por
-- prepare_ashrae.py. Se cargan aqui para que Power BI pueda marcar por si mismo
-- las lecturas atipicas con un join sencillo, en lugar de recalcular
-- percentiles sobre todo el historico en cada consulta.
--
-- Cada contador solo es comparable consigo mismo: los edificios van de 801 a
-- 850.354 pies cuadrados y cada medio tiene su unidad, asi que no existe un
-- umbral global valido.
CREATE TABLE IF NOT EXISTS sensor_baseline (
    building_id    INTEGER          NOT NULL,
    meter_type     TEXT             NOT NULL,
    baseline_p25   DOUBLE PRECISION,
    baseline_p50   DOUBLE PRECISION,
    baseline_p75   DOUBLE PRECISION,
    baseline_iqr   DOUBLE PRECISION,
    PRIMARY KEY (building_id, meter_type)
);

-- ---------------------------------------------------------------------------
-- Hechos: lecturas de contador
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

    -- NO hay campos derivados ni copias de la dimension. Power BI tiene
    -- `buildings` y `sensor_baseline` cargadas y calcula lo suyo con un join:
    -- intensidad energetica como meter_reading/square_feet, lecturas atipicas
    -- comparando contra baseline_p75 + k*baseline_iqr, lecturas a cero como
    -- meter_reading = 0. Persistir esas columnas seria almacenar 5,68 millones
    -- de valores que el consumidor puede derivar, y ademas fijar en el pipeline
    -- unos umbrales que el analista deberia poder ajustar.
    --
    -- Las mismas cifras SI se agregan en TimescaleDB, porque alli no son
    -- reconstruibles: avg_energy_intensity es un cociente de sumas, y
    -- zero_count y anomaly_count resumen una ventana entera.

    -- Instrumentacion del KPI de latencia (Objetivo 1)
    sim_publish_ts       TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),

    PRIMARY KEY (building_id, meter_type, event_time)
);

CREATE INDEX IF NOT EXISTS idx_events_time   ON telemetry_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_meter  ON telemetry_events (meter_type, event_time DESC);
-- Sin indices sobre banderas derivadas: ya no existen como columnas. Power BI
-- filtra por el join contra la dimension y la linea base.

COMMENT ON TABLE telemetry_events IS
    'Lecturas crudas a grano de evento. Escribe Spark, consume Power BI.';
COMMENT ON TABLE sensor_baseline IS
    'Cuartiles historicos por contador. Se carga desde ashrae_sensor_baseline.parquet.';
COMMENT ON TABLE buildings IS
    'Dimension de edificios. Se carga una vez desde ashrae_buildings.parquet.';
