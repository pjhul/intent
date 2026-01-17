package inserter

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

// MessageHandler processes a message and returns an error if processing fails
type MessageHandler[T any] func(ctx context.Context, msg T) error

// Consumer wraps a Kafka consumer with message handling
type Consumer[T any] struct {
	reader  *kafka.Reader
	handler MessageHandler[T]
	name    string
}

// NewConsumer creates a new Kafka consumer
func NewConsumer[T any](brokers []string, topic, groupID, name string, handler MessageHandler[T]) *Consumer[T] {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // Manual commits
	})

	return &Consumer[T]{
		reader:  reader,
		handler: handler,
		name:    name,
	}
}

// Start begins consuming messages. It blocks until context is cancelled.
func (c *Consumer[T]) Start(ctx context.Context) error {
	log.Printf("[%s] starting consumer", c.name)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] consumer stopping: context cancelled", c.name)
			return ctx.Err()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("[%s] error fetching message: %v", c.name, err)
				continue
			}

			var parsed T
			if err := json.Unmarshal(msg.Value, &parsed); err != nil {
				log.Printf("[%s] error unmarshaling message: %v", c.name, err)
				// Commit to skip bad message
				if err := c.reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("[%s] error committing message: %v", c.name, err)
				}
				continue
			}

			if err := c.handler(ctx, parsed); err != nil {
				log.Printf("[%s] error handling message: %v", c.name, err)
				// Don't commit - message will be redelivered
				continue
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("[%s] error committing message: %v", c.name, err)
			}
		}
	}
}

// Close closes the consumer
func (c *Consumer[T]) Close() error {
	log.Printf("[%s] closing consumer", c.name)
	return c.reader.Close()
}
