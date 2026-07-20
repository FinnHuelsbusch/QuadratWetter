CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS measurements (
    time            TIMESTAMPTZ      NOT NULL,
    -- NULL for rows imported from historical CSV (no UUID in that dataset)
    location_id     UUID,
    station_name    TEXT             NOT NULL,
    metric          TEXT             NOT NULL,
    value           DOUBLE PRECISION,
    warning         TEXT
);

SELECT create_hypertable('measurements', by_range('time', INTERVAL '1 day'), if_not_exists => TRUE);

-- Unique on (time, station_name, metric) so live and historical data merge cleanly.
-- location_id is excluded from the key because historical rows have NULL there.
CREATE UNIQUE INDEX IF NOT EXISTS measurements_time_station_metric_idx
    ON measurements (time, station_name, metric);

CREATE INDEX IF NOT EXISTS measurements_location_time_idx
    ON measurements (location_id, time DESC)
    WHERE location_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS measurements_station_time_idx
    ON measurements (station_name, time DESC);

ALTER TABLE measurements SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'station_name,metric',
    timescaledb.compress_orderby   = 'time DESC'
);

SELECT add_compression_policy('measurements', INTERVAL '7 days', if_not_exists => TRUE);

-- Tracks which historical zip dates have been successfully imported.
-- The backfill skips any date already present here.
CREATE TABLE IF NOT EXISTS backfill_log (
    date DATE PRIMARY KEY,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_inserted INTEGER NOT NULL
);

-- Station metadata populated from the live API on scraper startup.
CREATE TABLE IF NOT EXISTS stations (
    station_name    TEXT        PRIMARY KEY,
    location_id     UUID,
    display_name    TEXT,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    address         TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
