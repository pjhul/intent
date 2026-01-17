-- ClickHouse migration: cohort_membership table
-- Uses CollapsingMergeTree for efficient membership state tracking

CREATE TABLE IF NOT EXISTS cohort_membership (
    cohort_id UUID,
    user_id String,
    sign Int8, -- 1 for member, -1 for removal (CollapsingMergeTree sign)
    joined_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    version UInt64
) ENGINE = CollapsingMergeTree(sign)
ORDER BY (cohort_id, user_id)
SETTINGS index_granularity = 8192;

-- Materialized view for current membership state
CREATE TABLE IF NOT EXISTS cohort_membership_current (
    cohort_id UUID,
    user_id String,
    joined_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (cohort_id, user_id)
SETTINGS index_granularity = 8192;
