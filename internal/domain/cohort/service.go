package cohort

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pjhul/intent/internal/db"
)

var (
	ErrCohortNotFound       = errors.New("cohort not found")
	ErrInvalidRules         = errors.New("invalid cohort rules")
	ErrRecomputeInProgress  = errors.New("recompute already in progress")
	ErrRecomputeJobNotFound = errors.New("recompute job not found")
)

// Service handles cohort business logic
type Service struct {
	queries         *db.Queries
	kafkaProducer   CohortProducer
	recomputeWorker *RecomputeWorker
}

// CohortProducer interface for publishing cohort updates
type CohortProducer interface {
	ProduceCohortDefinition(ctx context.Context, c *Cohort) error
	ProduceCohortDeletion(ctx context.Context, cohortID string) error
}

// NewService creates a new cohort service
func NewService(queries *db.Queries, producer CohortProducer) *Service {
	return &Service{
		queries:       queries,
		kafkaProducer: producer,
	}
}

// SetRecomputeWorker sets the recompute worker for the service
// This is called after service creation to avoid circular dependencies
func (s *Service) SetRecomputeWorker(worker *RecomputeWorker) {
	s.recomputeWorker = worker
}

// Create creates a new cohort
func (s *Service) Create(ctx context.Context, req CreateCohortRequest) (*Cohort, error) {
	rulesJSON, err := json.Marshal(req.Rules)
	if err != nil {
		return nil, ErrInvalidRules
	}

	dbCohort, err := s.queries.CreateCohort(ctx, db.CreateCohortParams{
		Name:        req.Name,
		Description: pgtype.Text{String: req.Description, Valid: req.Description != ""},
		Rules:       rulesJSON,
		Status:      string(CohortStatusDraft),
	})
	if err != nil {
		return nil, err
	}

	cohort := dbCohortToDomain(dbCohort)

	// Publish to Kafka for Flink
	if s.kafkaProducer != nil {
		s.kafkaProducer.ProduceCohortDefinition(ctx, cohort)
	}

	return cohort, nil
}

// GetByID retrieves a cohort by ID
func (s *Service) GetByID(ctx context.Context, id uuid.UUID) (*Cohort, error) {
	pgID := pgtype.UUID{Bytes: id, Valid: true}
	dbCohort, err := s.queries.GetCohort(ctx, pgID)
	if err != nil {
		return nil, ErrCohortNotFound
	}

	return dbCohortToDomain(dbCohort), nil
}

// List retrieves cohorts with pagination
func (s *Service) List(ctx context.Context, limit, offset int) ([]*Cohort, error) {
	dbCohorts, err := s.queries.ListCohorts(ctx, db.ListCohortsParams{
		Limit:  int32(limit),
		Offset: int32(offset),
	})
	if err != nil {
		return nil, err
	}

	cohorts := make([]*Cohort, len(dbCohorts))
	for i, c := range dbCohorts {
		cohorts[i] = dbCohortToDomain(c)
	}

	return cohorts, nil
}

// ListActive retrieves all active cohorts
func (s *Service) ListActive(ctx context.Context) ([]*Cohort, error) {
	dbCohorts, err := s.queries.ListActiveCohorts(ctx)
	if err != nil {
		return nil, err
	}

	cohorts := make([]*Cohort, len(dbCohorts))
	for i, c := range dbCohorts {
		cohorts[i] = dbCohortToDomain(c)
	}

	return cohorts, nil
}

// Update updates a cohort
func (s *Service) Update(ctx context.Context, id uuid.UUID, req UpdateCohortRequest) (*Cohort, error) {
	existing, err := s.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	name := existing.Name
	if req.Name != "" {
		name = req.Name
	}

	description := existing.Description
	if req.Description != "" {
		description = req.Description
	}

	rules := existing.Rules
	if req.Rules != nil {
		rules = *req.Rules
	}

	rulesJSON, err := json.Marshal(rules)
	if err != nil {
		return nil, ErrInvalidRules
	}

	pgID := pgtype.UUID{Bytes: id, Valid: true}
	dbCohort, err := s.queries.UpdateCohort(ctx, db.UpdateCohortParams{
		ID:          pgID,
		Name:        name,
		Description: pgtype.Text{String: description, Valid: description != ""},
		Rules:       rulesJSON,
	})
	if err != nil {
		return nil, err
	}

	cohort := dbCohortToDomain(dbCohort)

	// Update status if provided
	if req.Status != "" && req.Status != cohort.Status {
		dbCohort, err = s.queries.UpdateCohortStatus(ctx, db.UpdateCohortStatusParams{
			ID:     pgID,
			Status: string(req.Status),
		})
		if err != nil {
			return nil, err
		}
		cohort = dbCohortToDomain(dbCohort)
	}

	// Publish update to Kafka
	if s.kafkaProducer != nil {
		s.kafkaProducer.ProduceCohortDefinition(ctx, cohort)
	}

	return cohort, nil
}

// Activate activates a cohort
func (s *Service) Activate(ctx context.Context, id uuid.UUID) (*Cohort, error) {
	// Check if this is first activation (transitioning from draft)
	existing, err := s.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	isFirstActivation := existing.Status == CohortStatusDraft

	pgID := pgtype.UUID{Bytes: id, Valid: true}
	dbCohort, err := s.queries.UpdateCohortStatus(ctx, db.UpdateCohortStatusParams{
		ID:     pgID,
		Status: string(CohortStatusActive),
	})
	if err != nil {
		return nil, ErrCohortNotFound
	}

	cohort := dbCohortToDomain(dbCohort)

	if s.kafkaProducer != nil {
		s.kafkaProducer.ProduceCohortDefinition(ctx, cohort)
	}

	// Trigger recompute on first activation
	if isFirstActivation && s.recomputeWorker != nil {
		go s.TriggerRecompute(context.Background(), id, false)
	}

	return cohort, nil
}

// Deactivate deactivates a cohort
func (s *Service) Deactivate(ctx context.Context, id uuid.UUID) (*Cohort, error) {
	pgID := pgtype.UUID{Bytes: id, Valid: true}
	dbCohort, err := s.queries.UpdateCohortStatus(ctx, db.UpdateCohortStatusParams{
		ID:     pgID,
		Status: string(CohortStatusInactive),
	})
	if err != nil {
		return nil, ErrCohortNotFound
	}

	cohort := dbCohortToDomain(dbCohort)

	if s.kafkaProducer != nil {
		s.kafkaProducer.ProduceCohortDefinition(ctx, cohort)
	}

	return cohort, nil
}

// Delete deletes a cohort
func (s *Service) Delete(ctx context.Context, id uuid.UUID) error {
	pgID := pgtype.UUID{Bytes: id, Valid: true}
	if err := s.queries.DeleteCohort(ctx, pgID); err != nil {
		return ErrCohortNotFound
	}

	if s.kafkaProducer != nil {
		s.kafkaProducer.ProduceCohortDeletion(ctx, id.String())
	}

	return nil
}

func dbCohortToDomain(c db.Cohort) *Cohort {
	var rules Rules
	json.Unmarshal(c.Rules, &rules)

	return &Cohort{
		ID:          uuid.UUID(c.ID.Bytes),
		Name:        c.Name,
		Description: c.Description.String,
		Rules:       rules,
		Status:      CohortStatus(c.Status),
		Version:     c.Version,
		CreatedAt:   c.CreatedAt.Time,
		UpdatedAt:   c.UpdatedAt.Time,
	}
}

// TriggerRecompute triggers a recompute job for a cohort
func (s *Service) TriggerRecompute(ctx context.Context, cohortID uuid.UUID, force bool) (*RecomputeResponse, error) {
	// Verify cohort exists
	cohort, err := s.GetByID(ctx, cohortID)
	if err != nil {
		return nil, err
	}

	// Check if worker is available
	if s.recomputeWorker == nil {
		return nil, errors.New("recompute worker not available")
	}

	// Check if there's already a running job for this cohort (unless force is set)
	if !force && s.recomputeWorker.HasRunningJob(cohortID) {
		return nil, ErrRecomputeInProgress
	}

	// Create and submit the job
	job := NewRecomputeJob(cohortID)
	s.recomputeWorker.SubmitJob(job)

	return &RecomputeResponse{
		JobID:    job.ID,
		CohortID: cohort.ID,
		Status:   job.Status,
		Message:  "Recompute job started",
	}, nil
}

// GetRecomputeJob retrieves the status of a recompute job
func (s *Service) GetRecomputeJob(ctx context.Context, jobID uuid.UUID) (*RecomputeJob, error) {
	if s.recomputeWorker == nil {
		return nil, errors.New("recompute worker not available")
	}

	job, ok := s.recomputeWorker.GetJob(jobID)
	if !ok {
		return nil, ErrRecomputeJobNotFound
	}

	return job, nil
}
