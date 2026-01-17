package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/pjhul/intent/internal/config"
	"github.com/pjhul/intent/internal/domain/cohort"
	"github.com/pjhul/intent/internal/domain/event"
)

// Producer handles producing messages to Kafka
type Producer struct {
	eventsWriter  *kafka.Writer
	cohortsWriter *kafka.Writer
	cfg           config.KafkaConfig
}

// NewProducer creates a new Kafka producer
func NewProducer(cfg config.KafkaConfig) *Producer {
	eventsWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.EventsTopic,
		Balancer:     &kafka.Hash{}, // Partition by key (user_id)
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	cohortsWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.CohortsTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	return &Producer{
		eventsWriter:  eventsWriter,
		cohortsWriter: cohortsWriter,
		cfg:           cfg,
	}
}

// ProduceEvent publishes an event to Kafka
func (p *Producer) ProduceEvent(ctx context.Context, e *event.Event) error {
	value, err := json.Marshal(e)
	if err != nil {
		return err
	}

	return p.eventsWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.UserID),
		Value: value,
		Time:  time.Now(),
	})
}

// ProduceEvents publishes multiple events to Kafka
func (p *Producer) ProduceEvents(ctx context.Context, events []*event.Event) error {
	messages := make([]kafka.Message, len(events))
	for i, e := range events {
		value, err := json.Marshal(e)
		if err != nil {
			return err
		}
		messages[i] = kafka.Message{
			Key:   []byte(e.UserID),
			Value: value,
			Time:  time.Now(),
		}
	}

	return p.eventsWriter.WriteMessages(ctx, messages...)
}

// ProduceCohortDefinition publishes a cohort definition update to Kafka
func (p *Producer) ProduceCohortDefinition(ctx context.Context, c *cohort.Cohort) error {
	value, err := json.Marshal(c)
	if err != nil {
		return err
	}

	return p.cohortsWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(c.ID.String()),
		Value: value,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "version", Value: []byte(intToBytes(c.Version))},
		},
	})
}

// ProduceCohortDeletion publishes a cohort deletion (tombstone) to Kafka
func (p *Producer) ProduceCohortDeletion(ctx context.Context, cohortID string) error {
	return p.cohortsWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(cohortID),
		Value: nil, // Tombstone
		Time:  time.Now(),
	})
}

// Close closes all writers
func (p *Producer) Close() error {
	if err := p.eventsWriter.Close(); err != nil {
		return err
	}
	return p.cohortsWriter.Close()
}

func intToBytes(i int64) []byte {
	s := ""
	if i == 0 {
		return []byte("0")
	}
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	return []byte(s)
}
