package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pjhul/intent/internal/api"
	"github.com/pjhul/intent/internal/api/handlers"
	"github.com/pjhul/intent/internal/config"
	"github.com/pjhul/intent/internal/db"
	"github.com/pjhul/intent/internal/domain/cohort"
	"github.com/pjhul/intent/internal/domain/event"
	"github.com/pjhul/intent/internal/domain/membership"
	"github.com/pjhul/intent/internal/infrastructure/cache"
	"github.com/pjhul/intent/internal/infrastructure/clickhouse"
	"github.com/pjhul/intent/internal/infrastructure/flink"
	"github.com/pjhul/intent/internal/infrastructure/kafka"
	"github.com/pjhul/intent/internal/infrastructure/migrations"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize PostgreSQL connection
	pgPool, err := pgxpool.New(ctx, cfg.PostgreSQL.DSN())
	if err != nil {
		log.Fatalf("failed to connect to PostgreSQL: %v", err)
	}
	defer pgPool.Close()

	// Initialize ClickHouse client for migrations (without database)
	chMigrationClient, err := clickhouse.NewClientForMigrations(cfg.ClickHouse)
	if err != nil {
		log.Fatalf("failed to connect to ClickHouse for migrations: %v", err)
	}

	// Run database migrations
	migrationRunner := migrations.NewMigrationRunner(pgPool, chMigrationClient.Conn())
	if err := migrationRunner.RunAll(ctx); err != nil {
		log.Fatalf("failed to run migrations: %v", err)
	}
	chMigrationClient.Close()

	// Initialize ClickHouse client (with database)
	chClient, err := clickhouse.NewClient(cfg.ClickHouse)
	if err != nil {
		log.Fatalf("failed to connect to ClickHouse: %v", err)
	}
	defer chClient.Close()

	// Initialize Redis client
	redisClient := cache.NewRedisClient(cfg.Redis)
	if err := redisClient.Ping(ctx); err != nil {
		log.Printf("warning: failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	// Initialize Kafka producer
	kafkaProducer := kafka.NewProducer(cfg.Kafka)
	defer kafkaProducer.Close()

	// Initialize Flink job manager
	flinkJobManager := flink.NewJobManager(cfg.Flink)

	// Initialize repositories
	queries := db.New(pgPool)
	eventRepo := clickhouse.NewEventRepository(chClient)
	membershipRepo := clickhouse.NewMembershipRepository(chClient)
	membershipCache := cache.NewMembershipCache(redisClient)

	// Initialize services
	cohortService := cohort.NewService(queries, &kafkaProducerAdapter{kafkaProducer})

	// Initialize recompute worker
	recomputeWorker := cohort.NewRecomputeWorker(
		&clickhouseClientAdapter{chClient},
		cohortService,
	)
	cohortService.SetRecomputeWorker(recomputeWorker)
	recomputeWorker.Start(ctx)

	// Event service no longer writes to ClickHouse directly - inserter-service handles that
	eventService := event.NewService(&eventRepoAdapter{eventRepo}, &eventProducerAdapter{kafkaProducer})
	membershipService := membership.NewService(
		&membershipRepoAdapter{membershipRepo},
		&cohortGetterAdapter{cohortService},
		&membershipCacheAdapter{membershipCache},
	)

	// Initialize change broadcaster
	broadcaster := kafka.NewChangesBroadcaster()
	go broadcaster.Run(ctx)

	// Initialize Kafka consumer for membership changes
	consumer := kafka.NewConsumer(cfg.Kafka, broadcaster.HandleChange)
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("kafka consumer error: %v", err)
		}
	}()
	defer consumer.Close()

	// Initialize handlers
	cohortHandler := handlers.NewCohortHandler(cohortService)
	eventHandler := handlers.NewEventHandler(eventService)
	membershipHandler := handlers.NewMembershipHandler(membershipService)
	wsHandler := handlers.NewWebSocketHandler(&broadcasterAdapter{broadcaster})
	sseHandler := handlers.NewSSEHandler(&broadcasterAdapter{broadcaster})
	flinkHandler := handlers.NewFlinkHandler(flinkJobManager)

	// Setup router
	router := api.NewRouter(
		cohortHandler,
		eventHandler,
		membershipHandler,
		wsHandler,
		sseHandler,
		flinkHandler,
	)

	// Setup Gin engine
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(gin.Logger())

	router.SetupRoutes(engine)

	// Create HTTP server
	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      engine,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// Start server in goroutine
	go func() {
		log.Printf("starting server on %s:%d", cfg.Server.Host, cfg.Server.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down server...")

	// Give outstanding requests 30 seconds to complete
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server forced to shutdown: %v", err)
	}

	log.Println("server stopped")
}

// Adapter types to bridge interfaces

type kafkaProducerAdapter struct {
	producer *kafka.Producer
}

func (a *kafkaProducerAdapter) ProduceCohortDefinition(ctx context.Context, c *cohort.Cohort) error {
	return a.producer.ProduceCohortDefinition(ctx, c)
}

func (a *kafkaProducerAdapter) ProduceCohortDeletion(ctx context.Context, cohortID string) error {
	return a.producer.ProduceCohortDeletion(ctx, cohortID)
}

type eventProducerAdapter struct {
	producer *kafka.Producer
}

func (a *eventProducerAdapter) ProduceEvent(ctx context.Context, e *event.Event) error {
	return a.producer.ProduceEvent(ctx, e)
}

func (a *eventProducerAdapter) ProduceEvents(ctx context.Context, events []*event.Event) error {
	return a.producer.ProduceEvents(ctx, events)
}

type eventRepoAdapter struct {
	repo *clickhouse.EventRepository
}

func (a *eventRepoAdapter) Insert(ctx context.Context, e *event.ClickHouseEvent) error {
	chEvent := &clickhouse.Event{
		ID:         e.ID,
		UserID:     e.UserID,
		EventName:  e.EventName,
		Properties: e.Properties,
		Timestamp:  e.Timestamp,
		ReceivedAt: e.ReceivedAt,
	}
	return a.repo.Insert(ctx, chEvent)
}

func (a *eventRepoAdapter) InsertBatch(ctx context.Context, events []*event.ClickHouseEvent) error {
	chEvents := make([]*clickhouse.Event, len(events))
	for i, e := range events {
		chEvents[i] = &clickhouse.Event{
			ID:         e.ID,
			UserID:     e.UserID,
			EventName:  e.EventName,
			Properties: e.Properties,
			Timestamp:  e.Timestamp,
			ReceivedAt: e.ReceivedAt,
		}
	}
	return a.repo.InsertBatch(ctx, chEvents)
}

func (a *eventRepoAdapter) GetByUserID(ctx context.Context, userID string, limit, offset int) ([]*event.ClickHouseEvent, error) {
	chEvents, err := a.repo.GetByUserID(ctx, userID, limit, offset)
	if err != nil {
		return nil, err
	}
	events := make([]*event.ClickHouseEvent, len(chEvents))
	for i, e := range chEvents {
		events[i] = &event.ClickHouseEvent{
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

func (a *eventRepoAdapter) GetByUserIDAndEventName(ctx context.Context, userID, eventName string, startTime, endTime *time.Time, limit int) ([]*event.ClickHouseEvent, error) {
	chEvents, err := a.repo.GetByUserIDAndEventName(ctx, userID, eventName, startTime, endTime, limit)
	if err != nil {
		return nil, err
	}
	events := make([]*event.ClickHouseEvent, len(chEvents))
	for i, e := range chEvents {
		events[i] = &event.ClickHouseEvent{
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

func (a *eventRepoAdapter) HasEventInWindow(ctx context.Context, userID, eventName string, startTime, endTime time.Time) (bool, error) {
	return a.repo.HasEventInWindow(ctx, userID, eventName, startTime, endTime)
}

func (a *eventRepoAdapter) GetAggregates(ctx context.Context, userID, eventName, propertyPath string, startTime, endTime time.Time) (*event.AggregateResult, error) {
	result, err := a.repo.GetAggregates(ctx, userID, eventName, propertyPath, startTime, endTime)
	if err != nil {
		return nil, err
	}
	return &event.AggregateResult{
		Count:         result.Count,
		Sum:           result.Sum,
		Avg:           result.Avg,
		Min:           result.Min,
		Max:           result.Max,
		DistinctCount: result.DistinctCount,
	}, nil
}

type membershipRepoAdapter struct {
	repo *clickhouse.MembershipRepository
}

func (a *membershipRepoAdapter) GetByCohortAndUser(ctx context.Context, cohortID uuid.UUID, userID string) (*membership.StoredMembership, error) {
	m, err := a.repo.GetByCohortAndUser(ctx, cohortID, userID)
	if err != nil {
		return nil, err
	}
	status := int8(-1)
	if m.IsMember {
		status = 1
	}
	return &membership.StoredMembership{
		CohortID:  m.CohortID,
		UserID:    m.UserID,
		Status:    status,
		JoinedAt:  m.JoinedAt,
		UpdatedAt: m.JoinedAt, // CollapsingMergeTree doesn't track updated_at
		Version:   0,
	}, nil
}

func (a *membershipRepoAdapter) GetUserCohorts(ctx context.Context, userID string) ([]uuid.UUID, error) {
	return a.repo.GetUserCohorts(ctx, userID)
}

func (a *membershipRepoAdapter) GetCohortMembers(ctx context.Context, cohortID uuid.UUID, limit, offset int) ([]membership.StoredMember, int64, error) {
	members, total, err := a.repo.GetCohortMembers(ctx, cohortID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	storedMembers := make([]membership.StoredMember, len(members))
	for i, m := range members {
		storedMembers[i] = membership.StoredMember{
			UserID:   m.UserID,
			JoinedAt: m.JoinedAt,
		}
	}
	return storedMembers, total, nil
}

func (a *membershipRepoAdapter) GetCohortMemberCount(ctx context.Context, cohortID uuid.UUID) (int64, error) {
	return a.repo.GetCohortMemberCount(ctx, cohortID)
}

type cohortGetterAdapter struct {
	service *cohort.Service
}

func (a *cohortGetterAdapter) GetCohortName(ctx context.Context, id uuid.UUID) (string, error) {
	c, err := a.service.GetByID(ctx, id)
	if err != nil {
		return "", err
	}
	return c.Name, nil
}

type membershipCacheAdapter struct {
	cache *cache.MembershipCache
}

func (a *membershipCacheAdapter) GetMembership(ctx context.Context, cohortID uuid.UUID, userID string) (*membership.CachedMembership, bool) {
	cached, ok := a.cache.GetMembership(ctx, cohortID, userID)
	if !ok {
		return nil, false
	}
	return &membership.CachedMembership{
		IsMember: cached.IsMember,
		JoinedAt: cached.JoinedAt,
	}, true
}

func (a *membershipCacheAdapter) SetMembership(ctx context.Context, cohortID uuid.UUID, userID string, m *membership.CachedMembership) error {
	return a.cache.SetMembership(ctx, cohortID, userID, &cache.CachedMembership{
		IsMember: m.IsMember,
		JoinedAt: m.JoinedAt,
	})
}

func (a *membershipCacheAdapter) InvalidateMembership(ctx context.Context, cohortID uuid.UUID, userID string) error {
	return a.cache.InvalidateMembership(ctx, cohortID, userID)
}

func (a *membershipCacheAdapter) GetUserCohorts(ctx context.Context, userID string) ([]uuid.UUID, bool) {
	return a.cache.GetUserCohorts(ctx, userID)
}

func (a *membershipCacheAdapter) SetUserCohorts(ctx context.Context, userID string, cohortIDs []uuid.UUID) error {
	return a.cache.SetUserCohorts(ctx, userID, cohortIDs)
}

func (a *membershipCacheAdapter) InvalidateUserCohorts(ctx context.Context, userID string) error {
	return a.cache.InvalidateUserCohorts(ctx, userID)
}

func (a *membershipCacheAdapter) GetCohortMemberCount(ctx context.Context, cohortID uuid.UUID) (int64, bool) {
	return a.cache.GetCohortMemberCount(ctx, cohortID)
}

func (a *membershipCacheAdapter) SetCohortMemberCount(ctx context.Context, cohortID uuid.UUID, count int64) error {
	return a.cache.SetCohortMemberCount(ctx, cohortID, count)
}

func (a *membershipCacheAdapter) InvalidateCohort(ctx context.Context, cohortID uuid.UUID) error {
	return a.cache.InvalidateCohort(ctx, cohortID)
}

type broadcasterAdapter struct {
	broadcaster *kafka.ChangesBroadcaster
}

func (a *broadcasterAdapter) Subscribe(id string, sub *membership.StreamSubscription) chan *membership.MembershipChange {
	// The kafka broadcaster uses clickhouse.MembershipChange, we need to convert
	// For simplicity, we'll create a channel and forward messages
	ch := make(chan *membership.MembershipChange, 100)

	// Subscribe to internal channel
	internalCh := a.broadcaster.Subscribe(id, sub)

	go func() {
		for change := range internalCh {
			ch <- &membership.MembershipChange{
				CohortID:     change.CohortID,
				CohortName:   change.CohortName,
				UserID:       change.UserID,
				PrevStatus:   membership.MembershipStatus(change.PrevStatus),
				NewStatus:    membership.MembershipStatus(change.NewStatus),
				ChangedAt:    change.ChangedAt,
				TriggerEvent: change.TriggerEvent,
			}
		}
		close(ch)
	}()

	return ch
}

func (a *broadcasterAdapter) Unsubscribe(id string) {
	a.broadcaster.Unsubscribe(id)
}

// clickhouseClientAdapter adapts the clickhouse.Client for the recompute worker
type clickhouseClientAdapter struct {
	client *clickhouse.Client
}

func (a *clickhouseClientAdapter) Query(ctx context.Context, query string, args ...any) (cohort.RowScanner, error) {
	return a.client.Query(ctx, query, args...)
}

func (a *clickhouseClientAdapter) PrepareBatch(ctx context.Context, query string) (cohort.Batch, error) {
	return a.client.PrepareBatch(ctx, query)
}

// Ensure uuid is used
var _ = uuid.New
