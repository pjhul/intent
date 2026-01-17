-- ClickHouse migration: user_event_aggregates table
-- Pre-computed aggregates for efficient cohort evaluation
-- Uses SummingMergeTree for automatic aggregation on merge

CREATE TABLE IF NOT EXISTS user_event_aggregates (
    user_id String,
    event_name LowCardinality(String),
    time_bucket DateTime, -- Rounded to 1-minute buckets

    -- Aggregate metrics
    event_count UInt64,
    sum_amount Float64,
    min_amount Float64,
    max_amount Float64,

    -- For distinct count approximation
    distinct_values AggregateFunction(uniq, String)
) ENGINE = SummingMergeTree((event_count, sum_amount))
ORDER BY (user_id, event_name, time_bucket)
TTL time_bucket + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- View for querying aggregates within time windows
CREATE VIEW IF NOT EXISTS user_event_aggregates_view AS
SELECT
    user_id,
    event_name,
    sum(event_count) as total_count,
    sum(sum_amount) as total_sum,
    min(min_amount) as overall_min,
    max(max_amount) as overall_max,
    uniqMerge(distinct_values) as distinct_count
FROM user_event_aggregates
GROUP BY user_id, event_name;
