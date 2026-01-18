package cohort_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/pjhul/intent/internal/domain/cohort"
	"github.com/pjhul/intent/internal/mocks"
	"go.uber.org/mock/gomock"
)

func TestRecomputeWorker_CalculateDiff(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCHClient := mocks.NewMockClickHouseClient(ctrl)
	mockQuerier := mocks.NewMockQuerier(ctrl)
	svc := cohort.NewService(mockQuerier, nil)
	worker := cohort.NewRecomputeWorker(mockCHClient, svc)

	t.Run("empty sets", func(t *testing.T) {
		matching := map[string]struct{}{}
		current := map[string]struct{}{}

		toAdd, toRemove := worker.CalculateDiff(matching, current)
		if len(toAdd) != 0 {
			t.Errorf("toAdd length = %d, expected 0", len(toAdd))
		}
		if len(toRemove) != 0 {
			t.Errorf("toRemove length = %d, expected 0", len(toRemove))
		}
	})

	t.Run("all new members", func(t *testing.T) {
		matching := map[string]struct{}{
			"user1": {},
			"user2": {},
			"user3": {},
		}
		current := map[string]struct{}{}

		toAdd, toRemove := worker.CalculateDiff(matching, current)
		if len(toAdd) != 3 {
			t.Errorf("toAdd length = %d, expected 3", len(toAdd))
		}
		if len(toRemove) != 0 {
			t.Errorf("toRemove length = %d, expected 0", len(toRemove))
		}
	})

	t.Run("all removed members", func(t *testing.T) {
		matching := map[string]struct{}{}
		current := map[string]struct{}{
			"user1": {},
			"user2": {},
		}

		toAdd, toRemove := worker.CalculateDiff(matching, current)
		if len(toAdd) != 0 {
			t.Errorf("toAdd length = %d, expected 0", len(toAdd))
		}
		if len(toRemove) != 2 {
			t.Errorf("toRemove length = %d, expected 2", len(toRemove))
		}
	})

	t.Run("no changes", func(t *testing.T) {
		matching := map[string]struct{}{
			"user1": {},
			"user2": {},
		}
		current := map[string]struct{}{
			"user1": {},
			"user2": {},
		}

		toAdd, toRemove := worker.CalculateDiff(matching, current)
		if len(toAdd) != 0 {
			t.Errorf("toAdd length = %d, expected 0", len(toAdd))
		}
		if len(toRemove) != 0 {
			t.Errorf("toRemove length = %d, expected 0", len(toRemove))
		}
	})

	t.Run("mixed adds and removes", func(t *testing.T) {
		matching := map[string]struct{}{
			"user1": {},
			"user3": {},
			"user4": {},
		}
		current := map[string]struct{}{
			"user1": {},
			"user2": {},
		}

		toAdd, toRemove := worker.CalculateDiff(matching, current)
		if len(toAdd) != 2 {
			t.Errorf("toAdd length = %d, expected 2", len(toAdd))
		}
		if len(toRemove) != 1 {
			t.Errorf("toRemove length = %d, expected 1", len(toRemove))
		}

		addMap := make(map[string]bool)
		for _, u := range toAdd {
			addMap[u] = true
		}
		if !addMap["user3"] || !addMap["user4"] {
			t.Error("toAdd should contain user3 and user4")
		}

		if toRemove[0] != "user2" {
			t.Errorf("toRemove[0] = %q, expected user2", toRemove[0])
		}
	})
}

func TestRecomputeWorker_HasRunningJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCHClient := mocks.NewMockClickHouseClient(ctrl)
	mockQuerier := mocks.NewMockQuerier(ctrl)
	svc := cohort.NewService(mockQuerier, nil)
	worker := cohort.NewRecomputeWorker(mockCHClient, svc)

	cohortID := uuid.New()

	t.Run("no jobs", func(t *testing.T) {
		if worker.HasRunningJob(cohortID) {
			t.Error("HasRunningJob() should return false when no jobs exist")
		}
	})

	t.Run("pending job exists", func(t *testing.T) {
		job := cohort.NewRecomputeJob(cohortID)
		worker.SubmitJob(job)

		if !worker.HasRunningJob(cohortID) {
			t.Error("HasRunningJob() should return true when pending job exists")
		}
	})
}

func TestRecomputeWorker_SubmitAndGetJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCHClient := mocks.NewMockClickHouseClient(ctrl)
	mockQuerier := mocks.NewMockQuerier(ctrl)
	svc := cohort.NewService(mockQuerier, nil)
	worker := cohort.NewRecomputeWorker(mockCHClient, svc)

	cohortID := uuid.New()
	job := cohort.NewRecomputeJob(cohortID)

	t.Run("submit job", func(t *testing.T) {
		worker.SubmitJob(job)

		retrievedJob, ok := worker.GetJob(job.ID)
		if !ok {
			t.Error("GetJob() should return true after SubmitJob()")
		}
		if retrievedJob.ID != job.ID {
			t.Errorf("Job ID = %v, expected %v", retrievedJob.ID, job.ID)
		}
		if retrievedJob.CohortID != cohortID {
			t.Errorf("CohortID = %v, expected %v", retrievedJob.CohortID, cohortID)
		}
	})

	t.Run("get non-existent job", func(t *testing.T) {
		nonExistentID := uuid.New()
		_, ok := worker.GetJob(nonExistentID)
		if ok {
			t.Error("GetJob() should return false for non-existent job")
		}
	})
}

func TestNewRecomputeWorker(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCHClient := mocks.NewMockClickHouseClient(ctrl)
	mockQuerier := mocks.NewMockQuerier(ctrl)
	svc := cohort.NewService(mockQuerier, nil)

	worker := cohort.NewRecomputeWorker(mockCHClient, svc)
	if worker == nil {
		t.Error("NewRecomputeWorker() returned nil")
	}
}
