-- ClickHouse migration: events_raw table
-- Raw event storage partitioned by month for efficient time-range queries

CREATE TABLE IF NOT EXISTS cohort.events_raw (
    id UUID DEFAULT generateUUIDv4(),
    user_id String,
    event_name LowCardinality(String),
    properties String,
    timestamp DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC') DEFAULT now64(3),
    event_date Date DEFAULT toDate(timestamp)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (user_id, event_name, timestamp)
TTL event_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;
