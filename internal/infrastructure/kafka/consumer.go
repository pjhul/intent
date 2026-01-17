package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/pjhul/intent/internal/config"
	"github.com/pjhul/intent/internal/domain/membership"
)

// MembershipChangeHandler is called when a membership change is received
type MembershipChangeHandler func(ctx context.Context, change *membership.MembershipChange) error

// Consumer handles consuming messages from Kafka
type Consumer struct {
	changesReader *kafka.Reader
	handler       MembershipChangeHandler
	cfg           config.KafkaConfig
}

// NewConsumer creates a new Kafka consumer for membership changes
func NewConsumer(cfg config.KafkaConfig, handler MembershipChangeHandler) *Consumer {
	changesReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.ChangesTopic,
		GroupID:        cfg.ConsumerGroup,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // Manual commits
	})

	return &Consumer{
		changesReader: changesReader,
		handler:       handler,
		cfg:           cfg,
	}
}

// Start begins consuming membership change messages
func (c *Consumer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.changesReader.FetchMessage(ctx)
			if err != nil {
				log.Printf("error fetching message: %v", err)
				continue
			}

			var change membership.MembershipChange
			if err := json.Unmarshal(msg.Value, &change); err != nil {
				log.Printf("error unmarshaling message: %v", err)
				continue
			}

			if err := c.handler(ctx, &change); err != nil {
				log.Printf("error handling message: %v", err)
				continue
			}

			if err := c.changesReader.CommitMessages(ctx, msg); err != nil {
				log.Printf("error committing message: %v", err)
			}
		}
	}
}

// Close closes the consumer
func (c *Consumer) Close() error {
	return c.changesReader.Close()
}

// ChangesBroadcaster broadcasts membership changes to subscribers
type ChangesBroadcaster struct {
	subscribers map[string]chan *membership.MembershipChange
	register    chan *subscriberRequest
	unregister  chan string
	broadcast   chan *membership.MembershipChange
}

type subscriberRequest struct {
	id           string
	subscription *membership.StreamSubscription
	ch           chan *membership.MembershipChange
}

// NewChangesBroadcaster creates a new broadcaster
func NewChangesBroadcaster() *ChangesBroadcaster {
	return &ChangesBroadcaster{
		subscribers: make(map[string]chan *membership.MembershipChange),
		register:    make(chan *subscriberRequest),
		unregister:  make(chan string),
		broadcast:   make(chan *membership.MembershipChange, 100),
	}
}

// Run starts the broadcaster
func (b *ChangesBroadcaster) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-b.register:
			b.subscribers[req.id] = req.ch
		case id := <-b.unregister:
			if ch, ok := b.subscribers[id]; ok {
				close(ch)
				delete(b.subscribers, id)
			}
		case change := <-b.broadcast:
			for _, ch := range b.subscribers {
				select {
				case ch <- change:
				default:
					// Skip slow subscribers
				}
			}
		}
	}
}

// Subscribe registers a new subscriber
func (b *ChangesBroadcaster) Subscribe(id string, sub *membership.StreamSubscription) chan *membership.MembershipChange {
	ch := make(chan *membership.MembershipChange, 100)
	b.register <- &subscriberRequest{id: id, subscription: sub, ch: ch}
	return ch
}

// Unsubscribe removes a subscriber
func (b *ChangesBroadcaster) Unsubscribe(id string) {
	b.unregister <- id
}

// Broadcast sends a change to all subscribers
func (b *ChangesBroadcaster) Broadcast(change *membership.MembershipChange) {
	b.broadcast <- change
}

// HandleChange is used as the consumer handler to broadcast changes
func (b *ChangesBroadcaster) HandleChange(ctx context.Context, change *membership.MembershipChange) error {
	b.Broadcast(change)
	return nil
}
