-- ClickHouse migration: events_raw table
-- Raw event storage partitioned by month for efficient time-range queries

CREATE TABLE IF NOT EXISTS events_raw (
    id UUID DEFAULT generateUUIDv4(),
    user_id String,
    event_name LowCardinality(String),
    properties String, -- JSON string
    timestamp DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC') DEFAULT now64(3),

    -- Partitioning and ordering
    event_date Date DEFAULT toDate(timestamp)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (user_id, event_name, timestamp)
TTL event_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- Index for event name lookups
ALTER TABLE events_raw ADD INDEX idx_event_name event_name TYPE bloom_filter(0.01) GRANULARITY 1;

-- Index for timestamp range queries
ALTER TABLE events_raw ADD INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1;
