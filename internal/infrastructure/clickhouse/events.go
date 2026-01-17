package clickhouse

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event represents a tracked user event (internal to clickhouse package)
type Event struct {
	ID         uuid.UUID              `json:"id"`
	UserID     string                 `json:"user_id"`
	EventName  string                 `json:"event_name"`
	Properties map[string]any         `json:"properties,omitempty"`
	Timestamp  time.Time              `json:"timestamp"`
	ReceivedAt time.Time              `json:"received_at"`
}

// EventRepository handles event storage in ClickHouse
type EventRepository struct {
	client *Client
}

// NewEventRepository creates a new event repository
func NewEventRepository(client *Client) *EventRepository {
	return &EventRepository{client: client}
}

// Insert inserts a single event
func (r *EventRepository) Insert(ctx context.Context, e *Event) error {
	props, err := json.Marshal(e.Properties)
	if err != nil {
		return err
	}

	return r.client.Exec(ctx, `
		INSERT INTO events_raw (id, user_id, event_name, properties, timestamp, received_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, e.ID, e.UserID, e.EventName, string(props), e.Timestamp, e.ReceivedAt)
}

// InsertBatch inserts multiple events efficiently
func (r *EventRepository) InsertBatch(ctx context.Context, events []*Event) error {
	batch, err := r.client.PrepareBatch(ctx, `
		INSERT INTO events_raw (id, user_id, event_name, properties, timestamp, received_at)
	`)
	if err != nil {
		return err
	}

	for _, e := range events {
		props, err := json.Marshal(e.Properties)
		if err != nil {
			return err
		}
		if err := batch.Append(e.ID, e.UserID, e.EventName, string(props), e.Timestamp, e.ReceivedAt); err != nil {
			return err
		}
	}

	return batch.Send()
}

// GetByUserID retrieves events for a specific user
func (r *EventRepository) GetByUserID(ctx context.Context, userID string, limit, offset int) ([]*Event, error) {
	rows, err := r.client.Query(ctx, `
		SELECT id, user_id, event_name, properties, timestamp, received_at
		FROM events_raw
		WHERE user_id = ?
		ORDER BY timestamp DESC
		LIMIT ? OFFSET ?
	`, userID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanEvents(rows)
}

// GetByUserIDAndEventName retrieves events for a specific user and event name
func (r *EventRepository) GetByUserIDAndEventName(ctx context.Context, userID, eventName string, startTime, endTime *time.Time, limit int) ([]*Event, error) {
	query := `
		SELECT id, user_id, event_name, properties, timestamp, received_at
		FROM events_raw
		WHERE user_id = ? AND event_name = ?
	`
	args := []any{userID, eventName}

	if startTime != nil {
		query += " AND timestamp >= ?"
		args = append(args, *startTime)
	}
	if endTime != nil {
		query += " AND timestamp <= ?"
		args = append(args, *endTime)
	}

	query += " ORDER BY timestamp DESC"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := r.client.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanEvents(rows)
}

// CountByUserIDAndEventName counts events for a user and event name within a time window
func (r *EventRepository) CountByUserIDAndEventName(ctx context.Context, userID, eventName string, startTime, endTime time.Time) (int64, error) {
	var count int64
	err := r.client.QueryRow(ctx, `
		SELECT count()
		FROM events_raw
		WHERE user_id = ? AND event_name = ? AND timestamp >= ? AND timestamp <= ?
	`, userID, eventName, startTime, endTime).Scan(&count)
	return count, err
}

// SumByUserIDAndEventName sums a property for a user and event name within a time window
func (r *EventRepository) SumByUserIDAndEventName(ctx context.Context, userID, eventName, propertyPath string, startTime, endTime time.Time) (float64, error) {
	var sum float64
	err := r.client.QueryRow(ctx, `
		SELECT coalesce(sum(JSONExtractFloat(properties, ?)), 0)
		FROM events_raw
		WHERE user_id = ? AND event_name = ? AND timestamp >= ? AND timestamp <= ?
	`, propertyPath, userID, eventName, startTime, endTime).Scan(&sum)
	return sum, err
}

// HasEventInWindow checks if a user has performed an event within a time window
func (r *EventRepository) HasEventInWindow(ctx context.Context, userID, eventName string, startTime, endTime time.Time) (bool, error) {
	var exists uint8
	err := r.client.QueryRow(ctx, `
		SELECT 1
		FROM events_raw
		WHERE user_id = ? AND event_name = ? AND timestamp >= ? AND timestamp <= ?
		LIMIT 1
	`, userID, eventName, startTime, endTime).Scan(&exists)
	if err != nil {
		return false, nil // No rows means no event
	}
	return exists == 1, nil
}

func scanEvents(rows interface{ Next() bool; Scan(dest ...any) error }) ([]*Event, error) {
	var events []*Event
	for rows.Next() {
		var (
			e        Event
			propsStr string
		)
		if err := rows.Scan(&e.ID, &e.UserID, &e.EventName, &propsStr, &e.Timestamp, &e.ReceivedAt); err != nil {
			return nil, err
		}
		if propsStr != "" {
			if err := json.Unmarshal([]byte(propsStr), &e.Properties); err != nil {
				return nil, err
			}
		}
		events = append(events, &e)
	}
	return events, nil
}

// AggregateResult holds aggregation results
type AggregateResult struct {
	Count         int64
	Sum           float64
	Avg           float64
	Min           float64
	Max           float64
	DistinctCount int64
}

// GetAggregates retrieves aggregate values for a user's events
func (r *EventRepository) GetAggregates(ctx context.Context, userID, eventName, propertyPath string, startTime, endTime time.Time) (*AggregateResult, error) {
	var result AggregateResult
	err := r.client.QueryRow(ctx, `
		SELECT
			count() as cnt,
			coalesce(sum(JSONExtractFloat(properties, ?)), 0) as sm,
			coalesce(avg(JSONExtractFloat(properties, ?)), 0) as av,
			coalesce(min(JSONExtractFloat(properties, ?)), 0) as mn,
			coalesce(max(JSONExtractFloat(properties, ?)), 0) as mx,
			uniqExact(JSONExtractString(properties, ?)) as dc
		FROM events_raw
		WHERE user_id = ? AND event_name = ? AND timestamp >= ? AND timestamp <= ?
	`, propertyPath, propertyPath, propertyPath, propertyPath, propertyPath, userID, eventName, startTime, endTime).Scan(
		&result.Count, &result.Sum, &result.Avg, &result.Min, &result.Max, &result.DistinctCount,
	)
	return &result, err
}

// GetDistinctUserIDs returns distinct user IDs that have performed a specific event
func (r *EventRepository) GetDistinctUserIDs(ctx context.Context, eventName string, startTime, endTime time.Time, limit int) ([]string, error) {
	rows, err := r.client.Query(ctx, `
		SELECT DISTINCT user_id
		FROM events_raw
		WHERE event_name = ? AND timestamp >= ? AND timestamp <= ?
		LIMIT ?
	`, eventName, startTime, endTime, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var userIDs []string
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		userIDs = append(userIDs, userID)
	}
	return userIDs, nil
}
