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

// Member represents a cohort member
type Member struct {
	UserID   string    `json:"user_id"`
	JoinedAt time.Time `json:"joined_at"`
}

// Membership represents a user's membership in a cohort
type Membership struct {
	CohortID  uuid.UUID `json:"cohort_id"`
	UserID    string    `json:"user_id"`
	IsMember  bool      `json:"is_member"`
	JoinedAt  time.Time `json:"joined_at"`
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

// GetByCohortAndUser retrieves membership for a specific cohort and user
func (r *MembershipRepository) GetByCohortAndUser(ctx context.Context, cohortID uuid.UUID, userID string) (*Membership, error) {
	var m Membership
	var signSum int64
	err := r.client.QueryRow(ctx, `
		SELECT cohort_id, user_id, sum(sign), min(joined_at)
		FROM cohort_membership_current
		WHERE cohort_id = ? AND user_id = ?
		GROUP BY cohort_id, user_id
		HAVING sum(sign) > 0
	`, cohortID, userID).Scan(&m.CohortID, &m.UserID, &signSum, &m.JoinedAt)
	if err != nil {
		return nil, err
	}
	m.IsMember = signSum > 0
	return &m, nil
}

// IsMember checks if a user is a member of a cohort
func (r *MembershipRepository) IsMember(ctx context.Context, cohortID uuid.UUID, userID string) (bool, error) {
	var signSum int64
	err := r.client.QueryRow(ctx, `
		SELECT sum(sign)
		FROM cohort_membership_current
		WHERE cohort_id = ? AND user_id = ?
	`, cohortID, userID).Scan(&signSum)
	if err != nil {
		return false, nil
	}
	return signSum > 0, nil
}

// GetCohortMembers retrieves all members of a cohort with pagination
func (r *MembershipRepository) GetCohortMembers(ctx context.Context, cohortID uuid.UUID, limit, offset int) ([]Member, int64, error) {
	// Get total count
	var total uint64
	if err := r.client.QueryRow(ctx, `
		SELECT count()
		FROM (
			SELECT user_id
			FROM cohort_membership_current
			WHERE cohort_id = ?
			GROUP BY user_id
			HAVING sum(sign) > 0
		)
	`, cohortID).Scan(&total); err != nil {
		return nil, 0, err
	}

	// Get members
	rows, err := r.client.Query(ctx, `
		SELECT user_id, min(joined_at) as joined_at
		FROM cohort_membership_current
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

	return members, int64(total), nil
}

// GetUserCohorts retrieves all cohorts a user belongs to
func (r *MembershipRepository) GetUserCohorts(ctx context.Context, userID string) ([]uuid.UUID, error) {
	rows, err := r.client.Query(ctx, `
		SELECT cohort_id
		FROM cohort_membership_current
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
	var count uint64
	err := r.client.QueryRow(ctx, `
		SELECT count()
		FROM (
			SELECT user_id
			FROM cohort_membership_current
			WHERE cohort_id = ?
			GROUP BY user_id
			HAVING sum(sign) > 0
		)
	`, cohortID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return int64(count), nil
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

// DeleteCohortMemberships removes all memberships for a cohort by inserting cancellation rows
func (r *MembershipRepository) DeleteCohortMemberships(ctx context.Context, cohortID uuid.UUID) error {
	// Insert sign=-1 rows for all current members to cancel them out
	return r.client.Exec(ctx, `
		INSERT INTO cohort_membership_current (cohort_id, user_id, sign, joined_at)
		SELECT cohort_id, user_id, -1, min(joined_at)
		FROM cohort_membership_current
		WHERE cohort_id = ?
		GROUP BY cohort_id, user_id
		HAVING sum(sign) > 0
	`, cohortID)
}
