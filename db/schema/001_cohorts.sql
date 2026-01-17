-- Cohort definitions stored in PostgreSQL
CREATE TABLE IF NOT EXISTS cohorts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    rules JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'draft',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for querying by status
CREATE INDEX idx_cohorts_status ON cohorts(status);

-- Index for querying by name
CREATE INDEX idx_cohorts_name ON cohorts(name);

-- Trigger to update updated_at on changes
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_cohorts_updated_at
    BEFORE UPDATE ON cohorts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
