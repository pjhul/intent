package membership

import (
	"time"

	"github.com/google/uuid"
)

// MembershipStatus represents whether a user is in or out of a cohort
type MembershipStatus int8

const (
	MembershipStatusOut MembershipStatus = -1
	MembershipStatusIn  MembershipStatus = 1
)

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

// IsEntry returns true if this change represents entering a cohort
func (mc *MembershipChange) IsEntry() bool {
	return mc.PrevStatus == MembershipStatusOut && mc.NewStatus == MembershipStatusIn
}

// IsExit returns true if this change represents exiting a cohort
func (mc *MembershipChange) IsExit() bool {
	return mc.PrevStatus == MembershipStatusIn && mc.NewStatus == MembershipStatusOut
}

// UserCohortsResponse represents the cohorts a user belongs to
type UserCohortsResponse struct {
	UserID  string             `json:"user_id"`
	Cohorts []CohortMembership `json:"cohorts"`
}

// CohortMembership represents a cohort membership entry for a user
type CohortMembership struct {
	CohortID   uuid.UUID `json:"cohort_id"`
	CohortName string    `json:"cohort_name"`
	JoinedAt   time.Time `json:"joined_at,omitempty"`
}

// CohortMembersResponse represents the members of a cohort
type CohortMembersResponse struct {
	CohortID uuid.UUID `json:"cohort_id"`
	Members  []Member  `json:"members"`
	Total    int64     `json:"total"`
	Limit    int       `json:"limit"`
	Offset   int       `json:"offset"`
}

// Member represents a cohort member
type Member struct {
	UserID   string    `json:"user_id"`
	JoinedAt time.Time `json:"joined_at"`
}

// StreamSubscription represents a subscription to cohort change events
type StreamSubscription struct {
	ID        string      `json:"id"`
	CohortIDs []uuid.UUID `json:"cohort_ids,omitempty"`
	UserIDs   []string    `json:"user_ids,omitempty"`
	CreatedAt time.Time   `json:"created_at"`
}

// MatchesChange returns true if the subscription matches the given change
func (s *StreamSubscription) MatchesChange(change *MembershipChange) bool {
	// If no filters, match everything
	if len(s.CohortIDs) == 0 && len(s.UserIDs) == 0 {
		return true
	}

	// Check cohort ID filter
	if len(s.CohortIDs) > 0 {
		matched := false
		for _, id := range s.CohortIDs {
			if id == change.CohortID {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Check user ID filter
	if len(s.UserIDs) > 0 {
		matched := false
		for _, id := range s.UserIDs {
			if id == change.UserID {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	return true
}
