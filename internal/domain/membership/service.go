package membership

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// MembershipRepository interface for membership storage
type MembershipRepository interface {
	GetByCohortAndUser(ctx context.Context, cohortID uuid.UUID, userID string) (*StoredMembership, error)
	GetUserCohorts(ctx context.Context, userID string) ([]uuid.UUID, error)
	GetCohortMembers(ctx context.Context, cohortID uuid.UUID, limit, offset int) ([]StoredMember, int64, error)
	GetCohortMemberCount(ctx context.Context, cohortID uuid.UUID) (int64, error)
}

// StoredMembership represents membership data from storage
type StoredMembership struct {
	CohortID  uuid.UUID
	UserID    string
	Status    int8
	JoinedAt  time.Time
	UpdatedAt time.Time
	Version   int64
}

// IsMember returns true if the user is currently a member
func (m *StoredMembership) IsMember() bool {
	return m.Status > 0
}

// StoredMember represents a member from storage
type StoredMember struct {
	UserID   string
	JoinedAt time.Time
}

// CohortGetter interface for getting cohort details
type CohortGetter interface {
	GetCohortName(ctx context.Context, id uuid.UUID) (string, error)
}

// MembershipCache interface for caching
type MembershipCache interface {
	GetMembership(ctx context.Context, cohortID uuid.UUID, userID string) (*CachedMembership, bool)
	SetMembership(ctx context.Context, cohortID uuid.UUID, userID string, membership *CachedMembership) error
	InvalidateMembership(ctx context.Context, cohortID uuid.UUID, userID string) error
	GetUserCohorts(ctx context.Context, userID string) ([]uuid.UUID, bool)
	SetUserCohorts(ctx context.Context, userID string, cohortIDs []uuid.UUID) error
	InvalidateUserCohorts(ctx context.Context, userID string) error
	GetCohortMemberCount(ctx context.Context, cohortID uuid.UUID) (int64, bool)
	SetCohortMemberCount(ctx context.Context, cohortID uuid.UUID, count int64) error
	InvalidateCohort(ctx context.Context, cohortID uuid.UUID) error
}

// CachedMembership represents cached membership data
type CachedMembership struct {
	IsMember bool
	JoinedAt time.Time
}

// Service handles membership business logic
type Service struct {
	membershipRepo MembershipRepository
	cohortGetter   CohortGetter
	cache          MembershipCache
}

// NewService creates a new membership service
func NewService(membershipRepo MembershipRepository, cohortGetter CohortGetter, cache MembershipCache) *Service {
	return &Service{
		membershipRepo: membershipRepo,
		cohortGetter:   cohortGetter,
		cache:          cache,
	}
}

// CheckMembershipResponse represents the response for membership check
type CheckMembershipResponse struct {
	UserID   string     `json:"user_id"`
	CohortID uuid.UUID  `json:"cohort_id"`
	IsMember bool       `json:"is_member"`
	JoinedAt *time.Time `json:"joined_at,omitempty"`
}

// CheckMembership checks if a user is a member of a cohort
func (s *Service) CheckMembership(ctx context.Context, cohortID uuid.UUID, userID string) (*CheckMembershipResponse, error) {
	// Check cache first
	if s.cache != nil {
		if cached, ok := s.cache.GetMembership(ctx, cohortID, userID); ok {
			var joinedAt *time.Time
			if cached.IsMember {
				joinedAt = &cached.JoinedAt
			}
			return &CheckMembershipResponse{
				UserID:   userID,
				CohortID: cohortID,
				IsMember: cached.IsMember,
				JoinedAt: joinedAt,
			}, nil
		}
	}

	// Query storage
	membership, err := s.membershipRepo.GetByCohortAndUser(ctx, cohortID, userID)
	if err != nil {
		// No membership found
		if s.cache != nil {
			s.cache.SetMembership(ctx, cohortID, userID, &CachedMembership{IsMember: false})
		}
		return &CheckMembershipResponse{
			UserID:   userID,
			CohortID: cohortID,
			IsMember: false,
		}, nil
	}

	isMember := membership.IsMember()

	// Update cache
	if s.cache != nil {
		s.cache.SetMembership(ctx, cohortID, userID, &CachedMembership{
			IsMember: isMember,
			JoinedAt: membership.JoinedAt,
		})
	}

	var joinedAt *time.Time
	if isMember {
		joinedAt = &membership.JoinedAt
	}

	return &CheckMembershipResponse{
		UserID:   userID,
		CohortID: cohortID,
		IsMember: isMember,
		JoinedAt: joinedAt,
	}, nil
}

// GetUserCohorts returns all cohorts a user belongs to
func (s *Service) GetUserCohorts(ctx context.Context, userID string) (*UserCohortsResponse, error) {
	// Check cache
	if s.cache != nil {
		if cohortIDs, ok := s.cache.GetUserCohorts(ctx, userID); ok {
			cohorts := make([]CohortMembership, 0, len(cohortIDs))
			for _, id := range cohortIDs {
				name := ""
				if s.cohortGetter != nil {
					name, _ = s.cohortGetter.GetCohortName(ctx, id)
				}
				cohorts = append(cohorts, CohortMembership{
					CohortID:   id,
					CohortName: name,
				})
			}
			return &UserCohortsResponse{
				UserID:  userID,
				Cohorts: cohorts,
			}, nil
		}
	}

	// Query storage
	cohortIDs, err := s.membershipRepo.GetUserCohorts(ctx, userID)
	if err != nil {
		return nil, err
	}

	cohorts := make([]CohortMembership, 0, len(cohortIDs))
	for _, id := range cohortIDs {
		name := ""
		if s.cohortGetter != nil {
			name, _ = s.cohortGetter.GetCohortName(ctx, id)
		}
		cohorts = append(cohorts, CohortMembership{
			CohortID:   id,
			CohortName: name,
		})
	}

	// Update cache
	if s.cache != nil {
		s.cache.SetUserCohorts(ctx, userID, cohortIDs)
	}

	return &UserCohortsResponse{
		UserID:  userID,
		Cohorts: cohorts,
	}, nil
}

// GetCohortMembers returns members of a cohort with pagination
func (s *Service) GetCohortMembers(ctx context.Context, cohortID uuid.UUID, limit, offset int) (*CohortMembersResponse, error) {
	if limit <= 0 {
		limit = 100
	}

	members, total, err := s.membershipRepo.GetCohortMembers(ctx, cohortID, limit, offset)
	if err != nil {
		return nil, err
	}

	memberList := make([]Member, len(members))
	for i, m := range members {
		memberList[i] = Member{
			UserID:   m.UserID,
			JoinedAt: m.JoinedAt,
		}
	}

	return &CohortMembersResponse{
		CohortID: cohortID,
		Members:  memberList,
		Total:    total,
		Limit:    limit,
		Offset:   offset,
	}, nil
}

// CohortStats represents statistics for a cohort
type CohortStats struct {
	CohortID    uuid.UUID `json:"cohort_id"`
	MemberCount int64     `json:"member_count"`
	LastUpdated time.Time `json:"last_updated"`
}

// GetCohortStats returns statistics for a cohort
func (s *Service) GetCohortStats(ctx context.Context, cohortID uuid.UUID) (*CohortStats, error) {
	// Check cache
	if s.cache != nil {
		if count, ok := s.cache.GetCohortMemberCount(ctx, cohortID); ok {
			return &CohortStats{
				CohortID:    cohortID,
				MemberCount: count,
				LastUpdated: time.Now(),
			}, nil
		}
	}

	count, err := s.membershipRepo.GetCohortMemberCount(ctx, cohortID)
	if err != nil {
		return nil, err
	}

	// Update cache
	if s.cache != nil {
		s.cache.SetCohortMemberCount(ctx, cohortID, count)
	}

	return &CohortStats{
		CohortID:    cohortID,
		MemberCount: count,
		LastUpdated: time.Now(),
	}, nil
}

// InvalidateCacheForUser invalidates cache entries when membership changes
func (s *Service) InvalidateCacheForUser(ctx context.Context, userID string, cohortID uuid.UUID) {
	if s.cache != nil {
		s.cache.InvalidateMembership(ctx, cohortID, userID)
		s.cache.InvalidateUserCohorts(ctx, userID)
	}
}

// InvalidateCacheForCohort invalidates all cache entries for a cohort
func (s *Service) InvalidateCacheForCohort(ctx context.Context, cohortID uuid.UUID) {
	if s.cache != nil {
		s.cache.InvalidateCohort(ctx, cohortID)
	}
}
