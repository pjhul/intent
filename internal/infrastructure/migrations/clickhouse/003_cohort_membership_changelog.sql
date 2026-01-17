-- ClickHouse migration: cohort_membership_changelog table
-- Append-only log of all membership changes for audit and replay

CREATE TABLE IF NOT EXISTS cohort.cohort_membership_changelog (
    id UUID DEFAULT generateUUIDv4(),
    cohort_id UUID,
    user_id String,
    prev_status Int8,
    new_status Int8,
    changed_at DateTime64(3, 'UTC') DEFAULT now64(3),
    trigger_event_id Nullable(UUID),
    change_date Date DEFAULT toDate(changed_at)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(change_date)
ORDER BY (cohort_id, user_id, changed_at)
TTL change_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
