package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event represents a tracked user event
type Event struct {
	ID         uuid.UUID              `json:"id"`
	UserID     string                 `json:"user_id"`
	EventName  string                 `json:"event_name"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	Timestamp  time.Time              `json:"timestamp"`
	ReceivedAt time.Time              `json:"received_at"`
}

// NewEvent creates a new event with the given parameters
func NewEvent(userID, eventName string, properties map[string]interface{}, timestamp time.Time) *Event {
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	return &Event{
		ID:         uuid.New(),
		UserID:     userID,
		EventName:  eventName,
		Properties: properties,
		Timestamp:  timestamp,
		ReceivedAt: time.Now().UTC(),
	}
}

// ToJSON serializes the event to JSON
func (e *Event) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// EventFromJSON deserializes an event from JSON
func EventFromJSON(data []byte) (*Event, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

// GetProperty retrieves a property value by key, supporting nested keys with dot notation
func (e *Event) GetProperty(key string) (interface{}, bool) {
	if e.Properties == nil {
		return nil, false
	}
	val, ok := e.Properties[key]
	return val, ok
}

// GetPropertyFloat64 retrieves a property value as float64
func (e *Event) GetPropertyFloat64(key string) (float64, bool) {
	val, ok := e.GetProperty(key)
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	default:
		return 0, false
	}
}

// IngestEventRequest represents the request to ingest a single event
type IngestEventRequest struct {
	UserID     string                 `json:"user_id" binding:"required"`
	EventName  string                 `json:"event_name" binding:"required"`
	Properties map[string]interface{} `json:"properties"`
	Timestamp  *time.Time             `json:"timestamp"`
}

// IngestBatchRequest represents the request to ingest multiple events
type IngestBatchRequest struct {
	Events []IngestEventRequest `json:"events" binding:"required,min=1,max=1000"`
}

// IngestEventResponse represents the response after ingesting events
type IngestEventResponse struct {
	EventID   uuid.UUID `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
}

// IngestBatchResponse represents the response after batch ingestion
type IngestBatchResponse struct {
	Ingested int       `json:"ingested"`
	Failed   int       `json:"failed"`
	Errors   []string  `json:"errors,omitempty"`
}

// EventQuery represents parameters for querying events
type EventQuery struct {
	UserID    string     `json:"user_id"`
	EventName string     `json:"event_name"`
	StartTime *time.Time `json:"start_time"`
	EndTime   *time.Time `json:"end_time"`
	Limit     int        `json:"limit"`
	Offset    int        `json:"offset"`
}
