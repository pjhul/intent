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
		INSERT INTO cohort_membership_current (cohort_id, user_id, joined_at, updated_at)
	`)
	if err != nil {
		return err
	}

	for _, c := range changes {
		// Only insert for members (is_member = 1)
		if c.IsMember == 1 {
			joinedAt := c.Timestamp
			if err := batch.Append(c.CohortID, c.UserID, joinedAt, c.Timestamp); err != nil {
				return err
			}
		}
	}

	return batch.Send()
}

// insertChangelogBatch inserts all membership changes into cohort_membership_changelog
func (i *MembershipInserter) insertChangelogBatch(ctx context.Context, changes []MembershipChange) error {
	batch, err := i.client.PrepareBatch(ctx, `
		INSERT INTO cohort_membership_changelog (cohort_id, user_id, prev_status, new_status, changed_at)
	`)
	if err != nil {
		return err
	}

	for _, c := range changes {
		// prev_status is unknown from Kafka message, we use 0 as placeholder
		// new_status: 1 for member, -1 for non-member
		prevStatus := int8(0)
		newStatus := int8(1)
		if c.IsMember == 0 {
			newStatus = -1
		}

		changedAt := c.Timestamp
		if changedAt.IsZero() {
			changedAt = time.Now().UTC()
		}

		if err := batch.Append(c.CohortID, c.UserID, prevStatus, newStatus, changedAt); err != nil {
			return err
		}
	}

	return batch.Send()
}
