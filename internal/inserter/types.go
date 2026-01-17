package inserter

import (
	"time"

	"github.com/google/uuid"
)

// RawEvent represents an event from the events.raw Kafka topic
type RawEvent struct {
	ID         uuid.UUID      `json:"id"`
	UserID     string         `json:"user_id"`
	EventName  string         `json:"event_name"`
	Properties map[string]any `json:"properties,omitempty"`
	Timestamp  time.Time      `json:"timestamp"`
	ReceivedAt time.Time      `json:"received_at"`
}

// MembershipChange represents a membership change from the cohort.membership Kafka topic
type MembershipChange struct {
	CohortID  uuid.UUID `json:"cohort_id"`
	UserID    string    `json:"user_id"`
	IsMember  uint8     `json:"is_member"` // 0 or 1
	Timestamp time.Time `json:"timestamp"`
}
