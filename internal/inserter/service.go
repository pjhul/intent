package inserter

import (
	"context"
	"log"
	"sync"

	"github.com/pjhul/intent/internal/infrastructure/clickhouse"
)

// Service orchestrates the inserter components
type Service struct {
	cfg *Config

	eventsConsumer     *Consumer[RawEvent]
	membershipConsumer *Consumer[MembershipChange]

	eventsBatcher     *Batcher[RawEvent]
	membershipBatcher *Batcher[MembershipChange]

	eventsInserter     *EventsInserter
	membershipInserter *MembershipInserter
}

// NewService creates a new inserter service
func NewService(cfg *Config, chClient *clickhouse.Client) *Service {
	s := &Service{
		cfg:                cfg,
		eventsInserter:     NewEventsInserter(chClient),
		membershipInserter: NewMembershipInserter(chClient),
	}

	// Create batchers with insert functions
	s.eventsBatcher = NewBatcher(
		cfg.BatchSize,
		cfg.FlushInterval,
		s.eventsInserter.InsertBatch,
	)

	s.membershipBatcher = NewBatcher(
		cfg.BatchSize,
		cfg.FlushInterval,
		s.membershipInserter.InsertBatch,
	)

	// Create consumers that feed into batchers
	s.eventsConsumer = NewConsumer(
		cfg.KafkaBrokers,
		cfg.EventsTopic,
		cfg.EventsConsumerGroup,
		"events",
		func(ctx context.Context, event RawEvent) error {
			return s.eventsBatcher.Add(ctx, event)
		},
	)

	s.membershipConsumer = NewConsumer(
		cfg.KafkaBrokers,
		cfg.MembershipTopic,
		cfg.MembershipConsumerGroup,
		"membership",
		func(ctx context.Context, change MembershipChange) error {
			return s.membershipBatcher.Add(ctx, change)
		},
	)

	return s
}

// Start starts the inserter service
func (s *Service) Start(ctx context.Context) error {
	log.Printf("starting inserter service")
	log.Printf("  batch_size: %d", s.cfg.BatchSize)
	log.Printf("  flush_interval: %s", s.cfg.FlushInterval)
	log.Printf("  kafka_brokers: %v", s.cfg.KafkaBrokers)
	log.Printf("  events_topic: %s", s.cfg.EventsTopic)
	log.Printf("  membership_topic: %s", s.cfg.MembershipTopic)

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	// Start events consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.eventsConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			errCh <- err
		}
	}()

	// Start membership consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.membershipConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			errCh <- err
		}
	}()

	// Wait for context cancellation or error
	select {
	case <-ctx.Done():
		log.Printf("context cancelled, shutting down...")
	case err := <-errCh:
		log.Printf("consumer error: %v", err)
		return err
	}

	wg.Wait()
	return nil
}

// Stop gracefully stops the service with final flush
func (s *Service) Stop(ctx context.Context) error {
	log.Printf("stopping inserter service")

	// Stop batchers (performs final flush)
	if err := s.eventsBatcher.Stop(ctx); err != nil {
		log.Printf("error stopping events batcher: %v", err)
	}

	if err := s.membershipBatcher.Stop(ctx); err != nil {
		log.Printf("error stopping membership batcher: %v", err)
	}

	// Close consumers
	if err := s.eventsConsumer.Close(); err != nil {
		log.Printf("error closing events consumer: %v", err)
	}

	if err := s.membershipConsumer.Close(); err != nil {
		log.Printf("error closing membership consumer: %v", err)
	}

	log.Printf("inserter service stopped")
	return nil
}
