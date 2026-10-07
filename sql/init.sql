CREATE TABLE IF NOT EXISTS bronze_objects (
    market TEXT NOT NULL,
    window_key TEXT NOT NULL,
    object_key TEXT NOT NULL,
    member_name TEXT NOT NULL,
    member_timestamp TIMESTAMP,
    raw_bytes BYTEA NOT NULL,
    row_count BIGINT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (market, window_key, object_key, member_name)
);

CREATE TABLE IF NOT EXISTS silver_trips (
    market TEXT NOT NULL,
    window_key TEXT NOT NULL,
    trip_key TEXT NOT NULL,
    start_station_id TEXT NOT NULL,
    end_station_id TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL,
    PRIMARY KEY (market, window_key, trip_key)
);

CREATE TABLE IF NOT EXISTS silver_rejects (
    market TEXT NOT NULL,
    window_key TEXT NOT NULL,
    trip_key TEXT NOT NULL,
    reason TEXT NOT NULL,
    raw_row JSONB NOT NULL,
    PRIMARY KEY (market, window_key, trip_key)
);

CREATE TABLE IF NOT EXISTS gold_station_daily (
    market TEXT NOT NULL,
    station_id TEXT NOT NULL,
    day DATE NOT NULL,
    departures BIGINT NOT NULL DEFAULT 0,
    arrivals BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (market, station_id, day)
);

CREATE TABLE IF NOT EXISTS operational_loads (
    job TEXT NOT NULL,
    market TEXT NOT NULL,
    window_key TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job, market, window_key)
);