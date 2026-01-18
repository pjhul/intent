-- ClickHouse migration: cohort membership tables
-- Two tables: current state and changelog

-- Current membership state using CollapsingMergeTree
-- sign = 1 for join, sign = -1 for leave (cancels out on merge)
CREATE TABLE IF NOT EXISTS cohort_membership_current (
    cohort_id UUID,
    user_id String,
    sign Int8,  -- 1 = member, -1 = cancel/remove
    joined_at DateTime64(3, 'UTC')
) ENGINE = CollapsingMergeTree(sign)
ORDER BY (cohort_id, user_id)
SETTINGS index_granularity = 8192;

-- Append-only changelog for audit and replay
CREATE TABLE IF NOT EXISTS cohort_membership_changelog (
    id UUID DEFAULT generateUUIDv4(),
    cohort_id UUID,
    user_id String,
    prev_status Int8,  -- -1 = out, 1 = in
    new_status Int8,   -- -1 = out, 1 = in
    changed_at DateTime64(3, 'UTC') DEFAULT now64(3),
    trigger_event_id Nullable(UUID),
    change_date Date DEFAULT toDate(changed_at)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(change_date)
ORDER BY (cohort_id, user_id, changed_at)
TTL change_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Index for user lookups on changelog
ALTER TABLE cohort_membership_changelog ADD INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 1;
