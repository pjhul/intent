-- ClickHouse migration: Add project_id to all tables
-- Adding project_id for multi-tenancy support

-- Add project_id to events_raw table
ALTER TABLE cohort.events_raw ADD COLUMN IF NOT EXISTS project_id UUID AFTER id;

-- Add project_id to cohort_membership table
ALTER TABLE cohort.cohort_membership ADD COLUMN IF NOT EXISTS project_id UUID AFTER cohort_id;

-- Add project_id to cohort_membership_current table
ALTER TABLE cohort.cohort_membership_current ADD COLUMN IF NOT EXISTS project_id UUID AFTER cohort_id;

-- Add project_id to cohort_membership_changelog table
ALTER TABLE cohort.cohort_membership_changelog ADD COLUMN IF NOT EXISTS project_id UUID AFTER cohort_id;

-- Add project_id to user_event_aggregates table
ALTER TABLE cohort.user_event_aggregates ADD COLUMN IF NOT EXISTS project_id UUID FIRST;

-- Note: ORDER BY keys cannot be modified on existing tables.
-- For new deployments, consider creating new tables with project_id in ORDER BY:
--
-- events_raw: ORDER BY (project_id, user_id, event_name, timestamp)
-- cohort_membership: ORDER BY (project_id, cohort_id, user_id)
-- cohort_membership_current: ORDER BY (project_id, cohort_id, user_id)
-- cohort_membership_changelog: ORDER BY (project_id, cohort_id, user_id, changed_at)
-- user_event_aggregates: ORDER BY (project_id, user_id, event_name, time_bucket)
