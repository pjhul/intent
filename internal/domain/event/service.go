package event

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// EventRepository interface for event storage
type EventRepository interface {
	Insert(ctx context.Context, e *ClickHouseEvent) error
	InsertBatch(ctx context.Context, events []*ClickHouseEvent) error
	GetByUserID(ctx context.Context, userID string, limit, offset int) ([]*ClickHouseEvent, error)
	GetByUserIDAndEventName(ctx context.Context, userID, eventName string, startTime, endTime *time.Time, limit int) ([]*ClickHouseEvent, error)
	HasEventInWindow(ctx context.Context, userID, eventName string, startTime, endTime time.Time) (bool, error)
	GetAggregates(ctx context.Context, userID, eventName, propertyPath string, startTime, endTime time.Time) (*AggregateResult, error)
}

// ClickHouseEvent represents an event in ClickHouse format
type ClickHouseEvent struct {
	ID         uuid.UUID      `json:"id"`
	UserID     string         `json:"user_id"`
	EventName  string         `json:"event_name"`
	Properties map[string]any `json:"properties,omitempty"`
	Timestamp  time.Time      `json:"timestamp"`
	ReceivedAt time.Time      `json:"received_at"`
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

// EventProducer interface for publishing events
type EventProducer interface {
	ProduceEvent(ctx context.Context, e *Event) error
	ProduceEvents(ctx context.Context, events []*Event) error
}

// Service handles event business logic
type Service struct {
	repo          EventRepository
	kafkaProducer EventProducer
}

// NewService creates a new event service
func NewService(repo EventRepository, producer EventProducer) *Service {
	return &Service{
		repo:          repo,
		kafkaProducer: producer,
	}
}

// Ingest ingests a single event
func (s *Service) Ingest(ctx context.Context, req IngestEventRequest) (*IngestEventResponse, error) {
	timestamp := time.Now().UTC()
	if req.Timestamp != nil {
		timestamp = *req.Timestamp
	}

	evt := NewEvent(req.UserID, req.EventName, req.Properties, timestamp)

	// Publish to Kafka - inserter-service will consume and write to ClickHouse
	if s.kafkaProducer != nil {
		if err := s.kafkaProducer.ProduceEvent(ctx, evt); err != nil {
			return nil, err
		}
	}

	return &IngestEventResponse{
		EventID:   evt.ID,
		Timestamp: evt.Timestamp,
	}, nil
}

// IngestBatch ingests multiple events
func (s *Service) IngestBatch(ctx context.Context, req IngestBatchRequest) (*IngestBatchResponse, error) {
	events := make([]*Event, 0, len(req.Events))

	for _, e := range req.Events {
		timestamp := time.Now().UTC()
		if e.Timestamp != nil {
			timestamp = *e.Timestamp
		}

		evt := NewEvent(e.UserID, e.EventName, e.Properties, timestamp)
		events = append(events, evt)
	}

	// Publish batch to Kafka - inserter-service will consume and write to ClickHouse
	if s.kafkaProducer != nil {
		if err := s.kafkaProducer.ProduceEvents(ctx, events); err != nil {
			return &IngestBatchResponse{
				Ingested: 0,
				Failed:   len(events),
				Errors:   []string{err.Error()},
			}, nil
		}
	}

	return &IngestBatchResponse{
		Ingested: len(events),
		Failed:   0,
	}, nil
}

// GetByUserID retrieves events for a user
func (s *Service) GetByUserID(ctx context.Context, userID string, limit, offset int) ([]*Event, error) {
	if limit <= 0 {
		limit = 100
	}
	chEvents, err := s.repo.GetByUserID(ctx, userID, limit, offset)
	if err != nil {
		return nil, err
	}

	events := make([]*Event, len(chEvents))
	for i, e := range chEvents {
		events[i] = &Event{
			ID:         e.ID,
			UserID:     e.UserID,
			EventName:  e.EventName,
			Properties: e.Properties,
			Timestamp:  e.Timestamp,
			ReceivedAt: e.ReceivedAt,
		}
	}
	return events, nil
}

// HasEventInWindow checks if a user has performed an event in a time window
func (s *Service) HasEventInWindow(ctx context.Context, userID, eventName string, window time.Duration) (bool, error) {
	endTime := time.Now().UTC()
	startTime := endTime.Add(-window)
	return s.repo.HasEventInWindow(ctx, userID, eventName, startTime, endTime)
}

// GetAggregates retrieves aggregates for a user's events
func (s *Service) GetAggregates(ctx context.Context, userID, eventName, propertyPath string, window time.Duration) (*AggregateResult, error) {
	endTime := time.Now().UTC()
	startTime := endTime.Add(-window)
	return s.repo.GetAggregates(ctx, userID, eventName, propertyPath, startTime, endTime)
}
