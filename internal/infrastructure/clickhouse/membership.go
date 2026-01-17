package clickhouse

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// MembershipStatus represents whether a user is in or out of a cohort
type MembershipStatus int8

const (
	MembershipStatusOut MembershipStatus = -1
	MembershipStatusIn  MembershipStatus = 1
)

// Membership represents a user's membership in a cohort (internal to clickhouse)
type Membership struct {
	CohortID  uuid.UUID        `json:"cohort_id"`
	UserID    string           `json:"user_id"`
	Status    MembershipStatus `json:"status"`
	JoinedAt  time.Time        `json:"joined_at"`
	UpdatedAt time.Time        `json:"updated_at"`
	Version   int64            `json:"version"`
}

// IsMember returns true if the user is currently a member
func (m *Membership) IsMember() bool {
	return m.Status == MembershipStatusIn
}

// Member represents a cohort member
type Member struct {
	UserID   string    `json:"user_id"`
	JoinedAt time.Time `json:"joined_at"`
}

// MembershipChange represents a change in cohort membership
type MembershipChange struct {
	CohortID     uuid.UUID        `json:"cohort_id"`
	CohortName   string           `json:"cohort_name"`
	UserID       string           `json:"user_id"`
	PrevStatus   MembershipStatus `json:"prev_status"`
	NewStatus    MembershipStatus `json:"new_status"`
	ChangedAt    time.Time        `json:"changed_at"`
	TriggerEvent *uuid.UUID       `json:"trigger_event,omitempty"`
}

// MembershipRepository handles membership storage in ClickHouse
type MembershipRepository struct {
	client *Client
}

// NewMembershipRepository creates a new membership repository
func NewMembershipRepository(client *Client) *MembershipRepository {
	return &MembershipRepository{client: client}
}

// Upsert inserts or updates a membership record using CollapsingMergeTree
func (r *MembershipRepository) Upsert(ctx context.Context, m *Membership) error {
	// For CollapsingMergeTree, we need to insert two rows to update:
	// 1. A row with sign=-1 to "cancel" the old row (if exists)
	// 2. A row with sign=1 for the new state

	// First, insert the cancellation row if there's an existing record
	r.client.Exec(ctx, `
		INSERT INTO cohort_membership (cohort_id, user_id, sign, joined_at, updated_at, version)
		SELECT cohort_id, user_id, -1, joined_at, now64(3), version
		FROM cohort_membership
		WHERE cohort_id = ? AND user_id = ? AND sign = 1
		LIMIT 1
	`, m.CohortID, m.UserID)

	// Then insert the new row
	return r.client.Exec(ctx, `
		INSERT INTO cohort_membership (cohort_id, user_id, sign, joined_at, updated_at, version)
		VALUES (?, ?, ?, ?, ?, ?)
	`, m.CohortID, m.UserID, m.Status, m.JoinedAt, m.UpdatedAt, m.Version)
}

// UpsertBatch inserts or updates multiple membership records
func (r *MembershipRepository) UpsertBatch(ctx context.Context, memberships []*Membership) error {
	batch, err := r.client.PrepareBatch(ctx, `
		INSERT INTO cohort_membership (cohort_id, user_id, sign, joined_at, updated_at, version)
	`)
	if err != nil {
		return err
	}

	for _, m := range memberships {
		if err := batch.Append(m.CohortID, m.UserID, m.Status, m.JoinedAt, m.UpdatedAt, m.Version); err != nil {
			return err
		}
	}

	return batch.Send()
}

// GetByCohortAndUser retrieves membership for a specific cohort and user
func (r *MembershipRepository) GetByCohortAndUser(ctx context.Context, cohortID uuid.UUID, userID string) (*Membership, error) {
	var m Membership
	err := r.client.QueryRow(ctx, `
		SELECT cohort_id, user_id, sum(sign) as status, min(joined_at) as joined_at, max(updated_at) as updated_at, max(version) as version
		FROM cohort_membership
		WHERE cohort_id = ? AND user_id = ?
		GROUP BY cohort_id, user_id
		HAVING status > 0
	`, cohortID, userID).Scan(&m.CohortID, &m.UserID, &m.Status, &m.JoinedAt, &m.UpdatedAt, &m.Version)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// IsMember checks if a user is a member of a cohort
func (r *MembershipRepository) IsMember(ctx context.Context, cohortID uuid.UUID, userID string) (bool, error) {
	var status int64
	err := r.client.QueryRow(ctx, `
		SELECT sum(sign)
		FROM cohort_membership
		WHERE cohort_id = ? AND user_id = ?
	`, cohortID, userID).Scan(&status)
	if err != nil {
		return false, nil
	}
	return status > 0, nil
}

// GetCohortMembers retrieves all members of a cohort with pagination
func (r *MembershipRepository) GetCohortMembers(ctx context.Context, cohortID uuid.UUID, limit, offset int) ([]Member, int64, error) {
	// Get total count
	var total int64
	if err := r.client.QueryRow(ctx, `
		SELECT count(DISTINCT user_id)
		FROM cohort_membership
		WHERE cohort_id = ?
		GROUP BY cohort_id
		HAVING sum(sign) > 0
	`, cohortID).Scan(&total); err != nil {
		total = 0
	}

	// Get members
	rows, err := r.client.Query(ctx, `
		SELECT user_id, min(joined_at) as joined_at
		FROM cohort_membership
		WHERE cohort_id = ?
		GROUP BY user_id
		HAVING sum(sign) > 0
		ORDER BY joined_at DESC
		LIMIT ? OFFSET ?
	`, cohortID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var members []Member
	for rows.Next() {
		var m Member
		if err := rows.Scan(&m.UserID, &m.JoinedAt); err != nil {
			return nil, 0, err
		}
		members = append(members, m)
	}

	return members, total, nil
}

// GetUserCohorts retrieves all cohorts a user belongs to
func (r *MembershipRepository) GetUserCohorts(ctx context.Context, userID string) ([]uuid.UUID, error) {
	rows, err := r.client.Query(ctx, `
		SELECT cohort_id
		FROM cohort_membership
		WHERE user_id = ?
		GROUP BY cohort_id
		HAVING sum(sign) > 0
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cohortIDs []uuid.UUID
	for rows.Next() {
		var cohortID uuid.UUID
		if err := rows.Scan(&cohortID); err != nil {
			return nil, err
		}
		cohortIDs = append(cohortIDs, cohortID)
	}

	return cohortIDs, nil
}

// GetCohortMemberCount returns the number of members in a cohort
func (r *MembershipRepository) GetCohortMemberCount(ctx context.Context, cohortID uuid.UUID) (int64, error) {
	var count int64
	err := r.client.QueryRow(ctx, `
		SELECT count(DISTINCT user_id)
		FROM cohort_membership
		WHERE cohort_id = ?
		GROUP BY cohort_id
		HAVING sum(sign) > 0
	`, cohortID).Scan(&count)
	if err != nil {
		return 0, nil // No members
	}
	return count, nil
}

// RecordChange records a membership change in the changelog
func (r *MembershipRepository) RecordChange(ctx context.Context, change *MembershipChange) error {
	return r.client.Exec(ctx, `
		INSERT INTO cohort_membership_changelog (cohort_id, user_id, prev_status, new_status, changed_at, trigger_event_id)
		VALUES (?, ?, ?, ?, ?, ?)
	`, change.CohortID, change.UserID, change.PrevStatus, change.NewStatus, change.ChangedAt, change.TriggerEvent)
}

// GetChangeHistory retrieves membership change history
func (r *MembershipRepository) GetChangeHistory(ctx context.Context, cohortID *uuid.UUID, userID *string, startTime, endTime time.Time, limit int) ([]*MembershipChange, error) {
	query := `
		SELECT cohort_id, user_id, prev_status, new_status, changed_at, trigger_event_id
		FROM cohort_membership_changelog
		WHERE changed_at >= ? AND changed_at <= ?
	`
	args := []any{startTime, endTime}

	if cohortID != nil {
		query += " AND cohort_id = ?"
		args = append(args, *cohortID)
	}
	if userID != nil {
		query += " AND user_id = ?"
		args = append(args, *userID)
	}

	query += " ORDER BY changed_at DESC LIMIT ?"
	args = append(args, limit)

	rows, err := r.client.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var changes []*MembershipChange
	for rows.Next() {
		var c MembershipChange
		if err := rows.Scan(&c.CohortID, &c.UserID, &c.PrevStatus, &c.NewStatus, &c.ChangedAt, &c.TriggerEvent); err != nil {
			return nil, err
		}
		changes = append(changes, &c)
	}

	return changes, nil
}

// DeleteCohortMemberships removes all memberships for a cohort
func (r *MembershipRepository) DeleteCohortMemberships(ctx context.Context, cohortID uuid.UUID) error {
	// Insert cancellation rows for all existing memberships
	return r.client.Exec(ctx, `
		INSERT INTO cohort_membership (cohort_id, user_id, sign, joined_at, updated_at, version)
		SELECT cohort_id, user_id, -1, joined_at, now64(3), version + 1
		FROM cohort_membership
		WHERE cohort_id = ? AND sign = 1
	`, cohortID)
}
