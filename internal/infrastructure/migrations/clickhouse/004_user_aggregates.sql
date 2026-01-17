-- ClickHouse migration: user_event_aggregates table
-- Pre-computed aggregates for efficient cohort evaluation

CREATE TABLE IF NOT EXISTS cohort.user_event_aggregates (
    user_id String,
    event_name LowCardinality(String),
    time_bucket DateTime,
    event_count UInt64,
    sum_amount Float64,
    min_amount Float64,
    max_amount Float64,
    distinct_values AggregateFunction(uniq, String)
) ENGINE = SummingMergeTree((event_count, sum_amount))
ORDER BY (user_id, event_name, time_bucket)
TTL time_bucket + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
