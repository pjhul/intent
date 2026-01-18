package cohort

import (
	"time"

	"github.com/google/uuid"
)

// RecomputeStatus represents the status of a recompute job
type RecomputeStatus string

const (
	RecomputeStatusPending   RecomputeStatus = "pending"
	RecomputeStatusRunning   RecomputeStatus = "running"
	RecomputeStatusCompleted RecomputeStatus = "completed"
	RecomputeStatusFailed    RecomputeStatus = "failed"
)

// RecomputeProgress tracks the progress of a recompute job
type RecomputeProgress struct {
	TotalUsers     int64 `json:"total_users"`
	ProcessedUsers int64 `json:"processed_users"`
	MembersFound   int64 `json:"members_found"`
	MembersAdded   int64 `json:"members_added"`
	MembersRemoved int64 `json:"members_removed"`
}

// RecomputeJob represents a cohort membership recompute job
type RecomputeJob struct {
	ID          uuid.UUID         `json:"id"`
	CohortID    uuid.UUID         `json:"cohort_id"`
	Status      RecomputeStatus   `json:"status"`
	Progress    RecomputeProgress `json:"progress"`
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	Error       string            `json:"error,omitempty"`
}

// NewRecomputeJob creates a new recompute job for a cohort
func NewRecomputeJob(cohortID uuid.UUID) *RecomputeJob {
	return &RecomputeJob{
		ID:        uuid.New(),
		CohortID:  cohortID,
		Status:    RecomputeStatusPending,
		Progress:  RecomputeProgress{},
		StartedAt: time.Now().UTC(),
	}
}

// MarkRunning sets the job status to running
func (j *RecomputeJob) MarkRunning() {
	j.Status = RecomputeStatusRunning
}

// MarkCompleted sets the job status to completed
func (j *RecomputeJob) MarkCompleted() {
	now := time.Now().UTC()
	j.Status = RecomputeStatusCompleted
	j.CompletedAt = &now
}

// MarkFailed sets the job status to failed with an error message
func (j *RecomputeJob) MarkFailed(err string) {
	now := time.Now().UTC()
	j.Status = RecomputeStatusFailed
	j.CompletedAt = &now
	j.Error = err
}

// UpdateProgress updates the job progress
func (j *RecomputeJob) UpdateProgress(progress RecomputeProgress) {
	j.Progress = progress
}

// RecomputeRequest represents a request to trigger a recompute
type RecomputeRequest struct {
	Force bool `json:"force"`
}

// RecomputeResponse represents the response when triggering a recompute
type RecomputeResponse struct {
	JobID    uuid.UUID       `json:"job_id"`
	CohortID uuid.UUID       `json:"cohort_id"`
	Status   RecomputeStatus `json:"status"`
	Message  string          `json:"message,omitempty"`
}
