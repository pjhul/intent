package inserter

import (
	"context"
	"time"

	"github.com/pjhul/intent/internal/infrastructure/clickhouse"
)

// MembershipInserter handles batch insertion of membership changes into ClickHouse
type MembershipInserter struct {
	client *clickhouse.Client
}

// NewMembershipInserter creates a new membership inserter
func NewMembershipInserter(client *clickhouse.Client) *MembershipInserter {
	return &MembershipInserter{client: client}
}

// InsertBatch inserts a batch of membership changes into ClickHouse
// It writes to both cohort_membership_current and cohort_membership_changelog
func (i *MembershipInserter) InsertBatch(ctx context.Context, changes []MembershipChange) error {
	if len(changes) == 0 {
		return nil
	}

	// Insert into cohort_membership_current (ReplacingMergeTree will handle deduplication)
	if err := i.insertCurrentBatch(ctx, changes); err != nil {
		return err
	}

	// Insert into cohort_membership_changelog for audit trail
	if err := i.insertChangelogBatch(ctx, changes); err != nil {
		return err
	}

	return nil
}

// insertCurrentBatch inserts membership state into cohort_membership_current
func (i *MembershipInserter) insertCurrentBatch(ctx context.Context, changes []MembershipChange) error {
	batch, err := i.client.PrepareBatch(ctx, `
		INSERT INTO cohort_membership_current (cohort_id, user_id, sign, joined_at)
	`)
	if err != nil {
		return err
	}

	for _, c := range changes {
		// CollapsingMergeTree: sign = 1 for join, -1 for leave
		// NewStatus already has the right values: 1 = in, -1 = out
		if err := batch.Append(c.CohortID, c.UserID, c.NewStatus, c.ChangedAt); err != nil {
			return err
		}
	}

	return batch.Send()
}

// insertChangelogBatch inserts all membership changes into cohort_membership_changelog
func (i *MembershipInserter) insertChangelogBatch(ctx context.Context, changes []MembershipChange) error {
	batch, err := i.client.PrepareBatch(ctx, `
		INSERT INTO cohort_membership_changelog (cohort_id, user_id, prev_status, new_status, changed_at, trigger_event_id)
	`)
	if err != nil {
		return err
	}

	for _, c := range changes {
		changedAt := c.ChangedAt
		if changedAt.IsZero() {
			changedAt = time.Now().UTC()
		}

		if err := batch.Append(c.CohortID, c.UserID, c.PrevStatus, c.NewStatus, changedAt, c.TriggerEvent); err != nil {
			return err
		}
	}

	return batch.Send()
}
