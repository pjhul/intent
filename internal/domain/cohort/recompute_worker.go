package cohort

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ClickHouseClient interface for ClickHouse operations needed by the recompute worker
type ClickHouseClient interface {
	Query(ctx context.Context, query string, args ...any) (RowScanner, error)
	PrepareBatch(ctx context.Context, query string) (Batch, error)
}

// RowScanner interface for scanning query results
type RowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
}

// Batch interface for batch inserts
type Batch interface {
	Append(args ...any) error
	Send() error
}

// RecomputeWorker handles background cohort membership recomputation
type RecomputeWorker struct {
	chClient     ClickHouseClient
	cohortGetter CohortGetter
	jobs         chan *RecomputeJob
	jobStore     map[uuid.UUID]*RecomputeJob
	mu           sync.RWMutex
	batchSize    int
}

// CohortGetter interface for getting cohort definitions
type CohortGetter interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Cohort, error)
}

// NewRecomputeWorker creates a new recompute worker
func NewRecomputeWorker(chClient ClickHouseClient, cohortGetter CohortGetter) *RecomputeWorker {
	return &RecomputeWorker{
		chClient:     chClient,
		cohortGetter: cohortGetter,
		jobs:         make(chan *RecomputeJob, 100),
		jobStore:     make(map[uuid.UUID]*RecomputeJob),
		batchSize:    1000,
	}
}

// Start begins processing recompute jobs
func (w *RecomputeWorker) Start(ctx context.Context) {
	go w.processJobs(ctx)
}

// SubmitJob submits a recompute job for processing
func (w *RecomputeWorker) SubmitJob(job *RecomputeJob) {
	w.mu.Lock()
	w.jobStore[job.ID] = job
	w.mu.Unlock()
	w.jobs <- job
}

// GetJob retrieves the current state of a job
func (w *RecomputeWorker) GetJob(jobID uuid.UUID) (*RecomputeJob, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	job, ok := w.jobStore[jobID]
	return job, ok
}

// HasRunningJob checks if there's a running job for a cohort
func (w *RecomputeWorker) HasRunningJob(cohortID uuid.UUID) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, job := range w.jobStore {
		if job.CohortID == cohortID &&
			(job.Status == RecomputeStatusPending || job.Status == RecomputeStatusRunning) {
			return true
		}
	}
	return false
}

// processJobs continuously processes jobs from the queue
func (w *RecomputeWorker) processJobs(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-w.jobs:
			w.executeJob(ctx, job)
		}
	}
}

// executeJob runs a single recompute job
func (w *RecomputeWorker) executeJob(ctx context.Context, job *RecomputeJob) {
	job.MarkRunning()
	w.updateJob(job)

	log.Printf("starting recompute job %s for cohort %s", job.ID, job.CohortID)

	// Get cohort definition
	cohort, err := w.cohortGetter.GetByID(ctx, job.CohortID)
	if err != nil {
		job.MarkFailed(fmt.Sprintf("failed to get cohort: %v", err))
		w.updateJob(job)
		log.Printf("recompute job %s failed: %v", job.ID, err)
		return
	}

	// Build query from rules
	qb := NewQueryBuilder()
	query, args, err := qb.BuildQuery(cohort.Rules)
	if err != nil {
		job.MarkFailed(fmt.Sprintf("failed to build query: %v", err))
		w.updateJob(job)
		log.Printf("recompute job %s failed: %v", job.ID, err)
		return
	}

	// Get matching users from events
	matchingUsers, err := w.getMatchingUsers(ctx, query, args)
	if err != nil {
		job.MarkFailed(fmt.Sprintf("failed to query matching users: %v", err))
		w.updateJob(job)
		log.Printf("recompute job %s failed: %v", job.ID, err)
		return
	}

	job.Progress.MembersFound = int64(len(matchingUsers))
	w.updateJob(job)

	// Get current members
	currentMembers, err := w.getCurrentMembers(ctx, job.CohortID)
	if err != nil {
		job.MarkFailed(fmt.Sprintf("failed to get current members: %v", err))
		w.updateJob(job)
		log.Printf("recompute job %s failed: %v", job.ID, err)
		return
	}

	// Calculate diff
	toAdd, toRemove := w.CalculateDiff(matchingUsers, currentMembers)
	job.Progress.TotalUsers = int64(len(toAdd) + len(toRemove))
	w.updateJob(job)

	// Apply changes
	now := time.Now().UTC()
	if err := w.applyMembershipChanges(ctx, job, toAdd, toRemove, now); err != nil {
		job.MarkFailed(fmt.Sprintf("failed to apply membership changes: %v", err))
		w.updateJob(job)
		log.Printf("recompute job %s failed: %v", job.ID, err)
		return
	}

	job.Progress.MembersAdded = int64(len(toAdd))
	job.Progress.MembersRemoved = int64(len(toRemove))
	job.Progress.ProcessedUsers = job.Progress.TotalUsers
	job.MarkCompleted()
	w.updateJob(job)

	log.Printf("recompute job %s completed: found=%d, added=%d, removed=%d",
		job.ID, len(matchingUsers), len(toAdd), len(toRemove))
}

// getMatchingUsers executes the query and returns matching user IDs
func (w *RecomputeWorker) getMatchingUsers(ctx context.Context, query string, args []any) (map[string]struct{}, error) {
	rows, err := w.chClient.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make(map[string]struct{})
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		users[userID] = struct{}{}
	}

	return users, nil
}

// getCurrentMembers gets the current members of a cohort from ClickHouse
func (w *RecomputeWorker) getCurrentMembers(ctx context.Context, cohortID uuid.UUID) (map[string]struct{}, error) {
	query := `
		SELECT user_id
		FROM cohort_membership_current
		WHERE cohort_id = ?
		GROUP BY user_id
		HAVING sum(sign) > 0
	`
	rows, err := w.chClient.Query(ctx, query, cohortID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	members := make(map[string]struct{})
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		members[userID] = struct{}{}
	}

	return members, nil
}

// CalculateDiff calculates which users need to be added or removed
func (w *RecomputeWorker) CalculateDiff(matchingUsers, currentMembers map[string]struct{}) (toAdd, toRemove []string) {
	// Users to add: in matchingUsers but not in currentMembers
	for userID := range matchingUsers {
		if _, exists := currentMembers[userID]; !exists {
			toAdd = append(toAdd, userID)
		}
	}

	// Users to remove: in currentMembers but not in matchingUsers
	for userID := range currentMembers {
		if _, exists := matchingUsers[userID]; !exists {
			toRemove = append(toRemove, userID)
		}
	}

	return toAdd, toRemove
}

// applyMembershipChanges inserts membership changes to ClickHouse
func (w *RecomputeWorker) applyMembershipChanges(ctx context.Context, job *RecomputeJob, toAdd, toRemove []string, now time.Time) error {
	// Insert additions
	if len(toAdd) > 0 {
		if err := w.insertMembershipBatch(ctx, job.CohortID, toAdd, 1, now); err != nil {
			return fmt.Errorf("failed to insert additions: %w", err)
		}
		if err := w.insertChangelogBatch(ctx, job.CohortID, toAdd, -1, 1, now); err != nil {
			return fmt.Errorf("failed to insert addition changelog: %w", err)
		}
	}

	// Insert removals
	if len(toRemove) > 0 {
		if err := w.insertMembershipBatch(ctx, job.CohortID, toRemove, -1, now); err != nil {
			return fmt.Errorf("failed to insert removals: %w", err)
		}
		if err := w.insertChangelogBatch(ctx, job.CohortID, toRemove, 1, -1, now); err != nil {
			return fmt.Errorf("failed to insert removal changelog: %w", err)
		}
	}

	return nil
}

// insertMembershipBatch inserts membership records in batches
func (w *RecomputeWorker) insertMembershipBatch(ctx context.Context, cohortID uuid.UUID, userIDs []string, sign int8, now time.Time) error {
	for i := 0; i < len(userIDs); i += w.batchSize {
		end := min(i+w.batchSize, len(userIDs))

		batch, err := w.chClient.PrepareBatch(ctx, `
			INSERT INTO cohort_membership_current (cohort_id, user_id, sign, joined_at)
		`)
		if err != nil {
			return err
		}

		for _, userID := range userIDs[i:end] {
			if err := batch.Append(cohortID, userID, sign, now); err != nil {
				return err
			}
		}

		if err := batch.Send(); err != nil {
			return err
		}
	}

	return nil
}

// insertChangelogBatch inserts changelog records in batches
func (w *RecomputeWorker) insertChangelogBatch(ctx context.Context, cohortID uuid.UUID, userIDs []string, prevStatus, newStatus int8, now time.Time) error {
	for i := 0; i < len(userIDs); i += w.batchSize {
		end := min(i+w.batchSize, len(userIDs))

		batch, err := w.chClient.PrepareBatch(ctx, `
			INSERT INTO cohort_membership_changelog (cohort_id, user_id, prev_status, new_status, changed_at, trigger_event_id)
		`)
		if err != nil {
			return err
		}

		for _, userID := range userIDs[i:end] {
			if err := batch.Append(cohortID, userID, prevStatus, newStatus, now, nil); err != nil {
				return err
			}
		}

		if err := batch.Send(); err != nil {
			return err
		}
	}

	return nil
}

// updateJob updates the job in the store
func (w *RecomputeWorker) updateJob(job *RecomputeJob) {
	w.mu.Lock()
	w.jobStore[job.ID] = job
	w.mu.Unlock()
}
