package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/pjhul/intent/internal/config"
)

// RedisClient wraps the Redis client
type RedisClient struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisClient creates a new Redis client
func NewRedisClient(cfg config.RedisConfig) *RedisClient {
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
	})

	return &RedisClient{
		client: client,
		ttl:    cfg.CacheTTL,
	}
}

// Ping tests the connection
func (r *RedisClient) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Close closes the connection
func (r *RedisClient) Close() error {
	return r.client.Close()
}

// MembershipCache handles caching of cohort membership
type MembershipCache struct {
	client *RedisClient
}

// NewMembershipCache creates a new membership cache
func NewMembershipCache(client *RedisClient) *MembershipCache {
	return &MembershipCache{client: client}
}

func membershipKey(cohortID uuid.UUID, userID string) string {
	return fmt.Sprintf("membership:%s:%s", cohortID.String(), userID)
}

func userCohortsKey(userID string) string {
	return fmt.Sprintf("user_cohorts:%s", userID)
}

func cohortMembersKey(cohortID uuid.UUID) string {
	return fmt.Sprintf("cohort_members:%s", cohortID.String())
}

// CachedMembership represents cached membership data
type CachedMembership struct {
	IsMember bool      `json:"is_member"`
	JoinedAt time.Time `json:"joined_at,omitempty"`
}

// GetMembership retrieves cached membership status
func (c *MembershipCache) GetMembership(ctx context.Context, cohortID uuid.UUID, userID string) (*CachedMembership, bool) {
	key := membershipKey(cohortID, userID)
	val, err := c.client.client.Get(ctx, key).Result()
	if err != nil {
		return nil, false
	}

	var membership CachedMembership
	if err := json.Unmarshal([]byte(val), &membership); err != nil {
		return nil, false
	}

	return &membership, true
}

// SetMembership caches membership status
func (c *MembershipCache) SetMembership(ctx context.Context, cohortID uuid.UUID, userID string, membership *CachedMembership) error {
	key := membershipKey(cohortID, userID)
	val, err := json.Marshal(membership)
	if err != nil {
		return err
	}

	return c.client.client.Set(ctx, key, val, c.client.ttl).Err()
}

// InvalidateMembership removes cached membership
func (c *MembershipCache) InvalidateMembership(ctx context.Context, cohortID uuid.UUID, userID string) error {
	key := membershipKey(cohortID, userID)
	return c.client.client.Del(ctx, key).Err()
}

// GetUserCohorts retrieves cached cohort IDs for a user
func (c *MembershipCache) GetUserCohorts(ctx context.Context, userID string) ([]uuid.UUID, bool) {
	key := userCohortsKey(userID)
	val, err := c.client.client.Get(ctx, key).Result()
	if err != nil {
		return nil, false
	}

	var cohortIDs []uuid.UUID
	if err := json.Unmarshal([]byte(val), &cohortIDs); err != nil {
		return nil, false
	}

	return cohortIDs, true
}

// SetUserCohorts caches cohort IDs for a user
func (c *MembershipCache) SetUserCohorts(ctx context.Context, userID string, cohortIDs []uuid.UUID) error {
	key := userCohortsKey(userID)
	val, err := json.Marshal(cohortIDs)
	if err != nil {
		return err
	}

	return c.client.client.Set(ctx, key, val, c.client.ttl).Err()
}

// InvalidateUserCohorts removes cached user cohorts
func (c *MembershipCache) InvalidateUserCohorts(ctx context.Context, userID string) error {
	key := userCohortsKey(userID)
	return c.client.client.Del(ctx, key).Err()
}

// GetCohortMemberCount retrieves cached member count
func (c *MembershipCache) GetCohortMemberCount(ctx context.Context, cohortID uuid.UUID) (int64, bool) {
	key := fmt.Sprintf("cohort_count:%s", cohortID.String())
	val, err := c.client.client.Get(ctx, key).Int64()
	if err != nil {
		return 0, false
	}
	return val, true
}

// SetCohortMemberCount caches member count
func (c *MembershipCache) SetCohortMemberCount(ctx context.Context, cohortID uuid.UUID, count int64) error {
	key := fmt.Sprintf("cohort_count:%s", cohortID.String())
	return c.client.client.Set(ctx, key, count, c.client.ttl).Err()
}

// InvalidateCohort invalidates all cache entries for a cohort
func (c *MembershipCache) InvalidateCohort(ctx context.Context, cohortID uuid.UUID) error {
	// Use SCAN to find and delete all related keys
	pattern := fmt.Sprintf("*%s*", cohortID.String())
	iter := c.client.client.Scan(ctx, 0, pattern, 100).Iterator()

	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return err
	}

	if len(keys) > 0 {
		return c.client.client.Del(ctx, keys...).Err()
	}

	return nil
}
