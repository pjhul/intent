package cohort

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewRecomputeJob(t *testing.T) {
	cohortID := uuid.New()

	job := NewRecomputeJob(cohortID)

	if job.ID == uuid.Nil {
		t.Error("NewRecomputeJob() should generate a non-nil UUID")
	}
	if job.CohortID != cohortID {
		t.Errorf("CohortID = %v, expected %v", job.CohortID, cohortID)
	}
	if job.Status != RecomputeStatusPending {
		t.Errorf("Status = %q, expected %q", job.Status, RecomputeStatusPending)
	}
	if job.StartedAt.IsZero() {
		t.Error("StartedAt should not be zero")
	}
	if job.CompletedAt != nil {
		t.Error("CompletedAt should be nil for new job")
	}
	if job.Error != "" {
		t.Error("Error should be empty for new job")
	}
	if job.Progress.TotalUsers != 0 {
		t.Errorf("Progress.TotalUsers = %d, expected 0", job.Progress.TotalUsers)
	}
}

func TestRecomputeJob_MarkRunning(t *testing.T) {
	job := NewRecomputeJob(uuid.New())

	job.MarkRunning()

	if job.Status != RecomputeStatusRunning {
		t.Errorf("Status = %q, expected %q", job.Status, RecomputeStatusRunning)
	}
	if job.CompletedAt != nil {
		t.Error("CompletedAt should still be nil after MarkRunning()")
	}
}

func TestRecomputeJob_MarkCompleted(t *testing.T) {
	job := NewRecomputeJob(uuid.New())
	job.MarkRunning()

	beforeComplete := time.Now().UTC()
	time.Sleep(1 * time.Millisecond)
	job.MarkCompleted()
	time.Sleep(1 * time.Millisecond)
	afterComplete := time.Now().UTC()

	if job.Status != RecomputeStatusCompleted {
		t.Errorf("Status = %q, expected %q", job.Status, RecomputeStatusCompleted)
	}
	if job.CompletedAt == nil {
		t.Error("CompletedAt should not be nil after MarkCompleted()")
	} else {
		if job.CompletedAt.Before(beforeComplete) || job.CompletedAt.After(afterComplete) {
			t.Errorf("CompletedAt = %v, expected between %v and %v", job.CompletedAt, beforeComplete, afterComplete)
		}
	}
	if job.Error != "" {
		t.Error("Error should be empty after successful completion")
	}
}

func TestRecomputeJob_MarkFailed(t *testing.T) {
	job := NewRecomputeJob(uuid.New())
	job.MarkRunning()

	errorMsg := "failed to query ClickHouse: connection refused"
	beforeFail := time.Now().UTC()
	time.Sleep(1 * time.Millisecond)
	job.MarkFailed(errorMsg)
	time.Sleep(1 * time.Millisecond)
	afterFail := time.Now().UTC()

	if job.Status != RecomputeStatusFailed {
		t.Errorf("Status = %q, expected %q", job.Status, RecomputeStatusFailed)
	}
	if job.CompletedAt == nil {
		t.Error("CompletedAt should not be nil after MarkFailed()")
	} else {
		if job.CompletedAt.Before(beforeFail) || job.CompletedAt.After(afterFail) {
			t.Errorf("CompletedAt = %v, expected between %v and %v", job.CompletedAt, beforeFail, afterFail)
		}
	}
	if job.Error != errorMsg {
		t.Errorf("Error = %q, expected %q", job.Error, errorMsg)
	}
}

func TestRecomputeJob_UpdateProgress(t *testing.T) {
	job := NewRecomputeJob(uuid.New())

	progress := RecomputeProgress{
		TotalUsers:     1000,
		ProcessedUsers: 500,
		MembersFound:   750,
		MembersAdded:   200,
		MembersRemoved: 50,
	}

	job.UpdateProgress(progress)

	if job.Progress.TotalUsers != 1000 {
		t.Errorf("Progress.TotalUsers = %d, expected 1000", job.Progress.TotalUsers)
	}
	if job.Progress.ProcessedUsers != 500 {
		t.Errorf("Progress.ProcessedUsers = %d, expected 500", job.Progress.ProcessedUsers)
	}
	if job.Progress.MembersFound != 750 {
		t.Errorf("Progress.MembersFound = %d, expected 750", job.Progress.MembersFound)
	}
	if job.Progress.MembersAdded != 200 {
		t.Errorf("Progress.MembersAdded = %d, expected 200", job.Progress.MembersAdded)
	}
	if job.Progress.MembersRemoved != 50 {
		t.Errorf("Progress.MembersRemoved = %d, expected 50", job.Progress.MembersRemoved)
	}
}

func TestRecomputeJob_StateTransitions(t *testing.T) {
	t.Run("pending to running to completed", func(t *testing.T) {
		job := NewRecomputeJob(uuid.New())

		if job.Status != RecomputeStatusPending {
			t.Errorf("initial Status = %q, expected %q", job.Status, RecomputeStatusPending)
		}

		job.MarkRunning()
		if job.Status != RecomputeStatusRunning {
			t.Errorf("after MarkRunning() Status = %q, expected %q", job.Status, RecomputeStatusRunning)
		}

		job.MarkCompleted()
		if job.Status != RecomputeStatusCompleted {
			t.Errorf("after MarkCompleted() Status = %q, expected %q", job.Status, RecomputeStatusCompleted)
		}
	})

	t.Run("pending to running to failed", func(t *testing.T) {
		job := NewRecomputeJob(uuid.New())

		job.MarkRunning()
		if job.Status != RecomputeStatusRunning {
			t.Errorf("after MarkRunning() Status = %q, expected %q", job.Status, RecomputeStatusRunning)
		}

		job.MarkFailed("some error")
		if job.Status != RecomputeStatusFailed {
			t.Errorf("after MarkFailed() Status = %q, expected %q", job.Status, RecomputeStatusFailed)
		}
	})
}

func TestRecomputeStatus_Constants(t *testing.T) {
	if RecomputeStatusPending != "pending" {
		t.Errorf("RecomputeStatusPending = %q, expected pending", RecomputeStatusPending)
	}
	if RecomputeStatusRunning != "running" {
		t.Errorf("RecomputeStatusRunning = %q, expected running", RecomputeStatusRunning)
	}
	if RecomputeStatusCompleted != "completed" {
		t.Errorf("RecomputeStatusCompleted = %q, expected completed", RecomputeStatusCompleted)
	}
	if RecomputeStatusFailed != "failed" {
		t.Errorf("RecomputeStatusFailed = %q, expected failed", RecomputeStatusFailed)
	}
}

func TestRecomputeProgress_ZeroValue(t *testing.T) {
	var progress RecomputeProgress

	if progress.TotalUsers != 0 {
		t.Errorf("TotalUsers = %d, expected 0", progress.TotalUsers)
	}
	if progress.ProcessedUsers != 0 {
		t.Errorf("ProcessedUsers = %d, expected 0", progress.ProcessedUsers)
	}
	if progress.MembersFound != 0 {
		t.Errorf("MembersFound = %d, expected 0", progress.MembersFound)
	}
	if progress.MembersAdded != 0 {
		t.Errorf("MembersAdded = %d, expected 0", progress.MembersAdded)
	}
	if progress.MembersRemoved != 0 {
		t.Errorf("MembersRemoved = %d, expected 0", progress.MembersRemoved)
	}
}

func TestRecomputeJob_ProgressUpdatesAreCumulative(t *testing.T) {
	job := NewRecomputeJob(uuid.New())

	job.UpdateProgress(RecomputeProgress{
		TotalUsers:     100,
		ProcessedUsers: 25,
		MembersFound:   50,
	})

	if job.Progress.ProcessedUsers != 25 {
		t.Errorf("Progress.ProcessedUsers = %d, expected 25", job.Progress.ProcessedUsers)
	}

	job.UpdateProgress(RecomputeProgress{
		TotalUsers:     100,
		ProcessedUsers: 50,
		MembersFound:   50,
		MembersAdded:   10,
	})

	if job.Progress.ProcessedUsers != 50 {
		t.Errorf("Progress.ProcessedUsers = %d, expected 50", job.Progress.ProcessedUsers)
	}
	if job.Progress.MembersAdded != 10 {
		t.Errorf("Progress.MembersAdded = %d, expected 10", job.Progress.MembersAdded)
	}

	job.UpdateProgress(RecomputeProgress{
		TotalUsers:     100,
		ProcessedUsers: 100,
		MembersFound:   50,
		MembersAdded:   20,
		MembersRemoved: 5,
	})

	if job.Progress.ProcessedUsers != 100 {
		t.Errorf("Progress.ProcessedUsers = %d, expected 100", job.Progress.ProcessedUsers)
	}
	if job.Progress.MembersAdded != 20 {
		t.Errorf("Progress.MembersAdded = %d, expected 20", job.Progress.MembersAdded)
	}
	if job.Progress.MembersRemoved != 5 {
		t.Errorf("Progress.MembersRemoved = %d, expected 5", job.Progress.MembersRemoved)
	}
}
