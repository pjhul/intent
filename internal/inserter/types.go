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
// Matches the structure produced by Flink's CohortProcessorJob
type MembershipChange struct {
	CohortID     uuid.UUID  `json:"cohort_id"`
	CohortName   string     `json:"cohort_name"`
	UserID       string     `json:"user_id"`
	PrevStatus   int8       `json:"prev_status"`   // -1 = out, 1 = in
	NewStatus    int8       `json:"new_status"`    // -1 = out, 1 = in
	ChangedAt    time.Time  `json:"changed_at"`
	TriggerEvent *uuid.UUID `json:"trigger_event,omitempty"`
}

// IsMember returns true if the user is now a member (new_status = 1)
func (m MembershipChange) IsMember() bool {
	return m.NewStatus == 1
}
