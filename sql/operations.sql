CREATE TABLE IF NOT EXISTS operational_loads (
    job TEXT NOT NULL,
    market TEXT NOT NULL,
    window_key TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job, market, window_key)
);

CREATE TABLE IF NOT EXISTS operational_schedules (
    market TEXT PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO operational_schedules (market, enabled)
VALUES ('jc', FALSE), ('nyc', FALSE)
ON CONFLICT (market) DO NOTHING;
