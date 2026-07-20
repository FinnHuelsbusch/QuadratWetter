CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS stations (
    location_id   UUID             PRIMARY KEY,
    name          TEXT,
    display_name  TEXT,
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    has_temp      BOOL NOT NULL DEFAULT true,
    has_humidity  BOOL NOT NULL DEFAULT true,
    has_wind      BOOL NOT NULL DEFAULT true,
    xlsx_matched  BOOL NOT NULL DEFAULT false,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS measurements (
    time          TIMESTAMPTZ      NOT NULL,
    location_id   UUID             NOT NULL REFERENCES stations(location_id),
    metric        TEXT             NOT NULL,
    value         DOUBLE PRECISION,
    warning       TEXT
);

SELECT create_hypertable('measurements', by_range('time', INTERVAL '1 day'), if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS measurements_time_location_metric_idx
    ON measurements (time, location_id, metric);

CREATE INDEX IF NOT EXISTS measurements_location_time_idx
    ON measurements (location_id, time DESC);

ALTER TABLE measurements SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'location_id,metric',
    timescaledb.compress_orderby   = 'time DESC'
);

SELECT add_compression_policy('measurements', INTERVAL '7 days', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS backfill_log (
    date          DATE        PRIMARY KEY,
    imported_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_inserted INTEGER     NOT NULL
);
