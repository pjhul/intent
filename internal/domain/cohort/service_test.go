package cohort_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pjhul/intent/internal/db"
	"github.com/pjhul/intent/internal/domain/cohort"
	"github.com/pjhul/intent/internal/mocks"
	"go.uber.org/mock/gomock"
)

func TestService_Create(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQuerier := mocks.NewMockQuerier(ctrl)
	mockProducer := mocks.NewMockCohortProducer(ctrl)

	svc := cohort.NewService(mockQuerier, mockProducer)

	t.Run("success", func(t *testing.T) {
		req := cohort.CreateCohortRequest{
			Name:        "Test Cohort",
			Description: "Test Description",
			Rules: cohort.Rules{
				Operator: cohort.OperatorAND,
				Conditions: []cohort.Condition{
					{Type: cohort.ConditionTypeEvent, EventName: "purchase"},
				},
			},
		}

		rulesJSON, _ := json.Marshal(req.Rules)
		cohortID := uuid.New()
		now := time.Now().UTC()

		mockQuerier.EXPECT().
			CreateCohort(gomock.Any(), gomock.Any()).
			Return(db.Cohort{
				ID:          pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:        req.Name,
				Description: pgtype.Text{String: req.Description, Valid: true},
				Rules:       rulesJSON,
				Status:      string(cohort.CohortStatusDraft),
				Version:     1,
				CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockProducer.EXPECT().
			ProduceCohortDefinition(gomock.Any(), gomock.Any()).
			Return(nil)

		c, err := svc.Create(context.Background(), req)
		if err != nil {
			t.Errorf("Create() unexpected error: %v", err)
		}
		if c.Name != req.Name {
			t.Errorf("Name = %q, expected %q", c.Name, req.Name)
		}
		if c.Status != cohort.CohortStatusDraft {
			t.Errorf("Status = %q, expected %q", c.Status, cohort.CohortStatusDraft)
		}
	})

	t.Run("database error", func(t *testing.T) {
		req := cohort.CreateCohortRequest{
			Name: "Test Cohort",
			Rules: cohort.Rules{
				Operator:   cohort.OperatorAND,
				Conditions: []cohort.Condition{{Type: cohort.ConditionTypeEvent, EventName: "signup"}},
			},
		}

		mockQuerier.EXPECT().
			CreateCohort(gomock.Any(), gomock.Any()).
			Return(db.Cohort{}, errors.New("database error"))

		_, err := svc.Create(context.Background(), req)
		if err == nil {
			t.Error("Create() expected error for database failure")
		}
	})
}

func TestService_GetByID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQuerier := mocks.NewMockQuerier(ctrl)
	svc := cohort.NewService(mockQuerier, nil)

	t.Run("found", func(t *testing.T) {
		cohortID := uuid.New()
		now := time.Now().UTC()
		rules := cohort.Rules{Operator: cohort.OperatorAND, Conditions: []cohort.Condition{}}
		rulesJSON, _ := json.Marshal(rules)

		mockQuerier.EXPECT().
			GetCohort(gomock.Any(), pgtype.UUID{Bytes: cohortID, Valid: true}).
			Return(db.Cohort{
				ID:          pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:        "Test Cohort",
				Description: pgtype.Text{String: "Description", Valid: true},
				Rules:       rulesJSON,
				Status:      string(cohort.CohortStatusActive),
				Version:     1,
				CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		c, err := svc.GetByID(context.Background(), cohortID)
		if err != nil {
			t.Errorf("GetByID() unexpected error: %v", err)
		}
		if c.ID != cohortID {
			t.Errorf("ID = %v, expected %v", c.ID, cohortID)
		}
	})

	t.Run("not found", func(t *testing.T) {
		cohortID := uuid.New()

		mockQuerier.EXPECT().
			GetCohort(gomock.Any(), pgtype.UUID{Bytes: cohortID, Valid: true}).
			Return(db.Cohort{}, errors.New("not found"))

		_, err := svc.GetByID(context.Background(), cohortID)
		if !errors.Is(err, cohort.ErrCohortNotFound) {
			t.Errorf("GetByID() error = %v, expected ErrCohortNotFound", err)
		}
	})
}

func TestService_List(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQuerier := mocks.NewMockQuerier(ctrl)
	svc := cohort.NewService(mockQuerier, nil)

	t.Run("pagination", func(t *testing.T) {
		now := time.Now().UTC()
		rules := cohort.Rules{Operator: cohort.OperatorAND, Conditions: []cohort.Condition{}}
		rulesJSON, _ := json.Marshal(rules)

		cohort1ID := uuid.New()
		cohort2ID := uuid.New()

		mockQuerier.EXPECT().
			ListCohorts(gomock.Any(), db.ListCohortsParams{Limit: 10, Offset: 0}).
			Return([]db.Cohort{
				{
					ID:        pgtype.UUID{Bytes: cohort1ID, Valid: true},
					Name:      "Cohort 1",
					Rules:     rulesJSON,
					Status:    string(cohort.CohortStatusActive),
					Version:   1,
					CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
					UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				},
				{
					ID:        pgtype.UUID{Bytes: cohort2ID, Valid: true},
					Name:      "Cohort 2",
					Rules:     rulesJSON,
					Status:    string(cohort.CohortStatusDraft),
					Version:   1,
					CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
					UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				},
			}, nil)

		cohorts, err := svc.List(context.Background(), 10, 0)
		if err != nil {
			t.Errorf("List() unexpected error: %v", err)
		}
		if len(cohorts) != 2 {
			t.Errorf("len(cohorts) = %d, expected 2", len(cohorts))
		}
	})

	t.Run("empty result", func(t *testing.T) {
		mockQuerier.EXPECT().
			ListCohorts(gomock.Any(), db.ListCohortsParams{Limit: 10, Offset: 100}).
			Return([]db.Cohort{}, nil)

		cohorts, err := svc.List(context.Background(), 10, 100)
		if err != nil {
			t.Errorf("List() unexpected error: %v", err)
		}
		if len(cohorts) != 0 {
			t.Errorf("len(cohorts) = %d, expected 0", len(cohorts))
		}
	})
}

func TestService_Update(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQuerier := mocks.NewMockQuerier(ctrl)
	mockProducer := mocks.NewMockCohortProducer(ctrl)
	svc := cohort.NewService(mockQuerier, mockProducer)

	cohortID := uuid.New()
	now := time.Now().UTC()
	rules := cohort.Rules{Operator: cohort.OperatorAND, Conditions: []cohort.Condition{{Type: cohort.ConditionTypeEvent, EventName: "purchase"}}}
	rulesJSON, _ := json.Marshal(rules)

	t.Run("partial update - name only", func(t *testing.T) {
		mockQuerier.EXPECT().
			GetCohort(gomock.Any(), pgtype.UUID{Bytes: cohortID, Valid: true}).
			Return(db.Cohort{
				ID:          pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:        "Original Name",
				Description: pgtype.Text{String: "Original Description", Valid: true},
				Rules:       rulesJSON,
				Status:      string(cohort.CohortStatusDraft),
				Version:     1,
				CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockQuerier.EXPECT().
			UpdateCohort(gomock.Any(), gomock.Any()).
			Return(db.Cohort{
				ID:          pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:        "Updated Name",
				Description: pgtype.Text{String: "Original Description", Valid: true},
				Rules:       rulesJSON,
				Status:      string(cohort.CohortStatusDraft),
				Version:     2,
				CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockProducer.EXPECT().
			ProduceCohortDefinition(gomock.Any(), gomock.Any()).
			Return(nil)

		req := cohort.UpdateCohortRequest{Name: "Updated Name"}
		c, err := svc.Update(context.Background(), cohortID, req)
		if err != nil {
			t.Errorf("Update() unexpected error: %v", err)
		}
		if c.Name != "Updated Name" {
			t.Errorf("Name = %q, expected Updated Name", c.Name)
		}
	})

	t.Run("update with status change", func(t *testing.T) {
		mockQuerier.EXPECT().
			GetCohort(gomock.Any(), pgtype.UUID{Bytes: cohortID, Valid: true}).
			Return(db.Cohort{
				ID:          pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:        "Test Cohort",
				Description: pgtype.Text{String: "Description", Valid: true},
				Rules:       rulesJSON,
				Status:      string(cohort.CohortStatusDraft),
				Version:     1,
				CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockQuerier.EXPECT().
			UpdateCohort(gomock.Any(), gomock.Any()).
			Return(db.Cohort{
				ID:          pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:        "Test Cohort",
				Description: pgtype.Text{String: "Description", Valid: true},
				Rules:       rulesJSON,
				Status:      string(cohort.CohortStatusDraft),
				Version:     2,
				CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockQuerier.EXPECT().
			UpdateCohortStatus(gomock.Any(), db.UpdateCohortStatusParams{
				ID:     pgtype.UUID{Bytes: cohortID, Valid: true},
				Status: string(cohort.CohortStatusActive),
			}).
			Return(db.Cohort{
				ID:          pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:        "Test Cohort",
				Description: pgtype.Text{String: "Description", Valid: true},
				Rules:       rulesJSON,
				Status:      string(cohort.CohortStatusActive),
				Version:     2,
				CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockProducer.EXPECT().
			ProduceCohortDefinition(gomock.Any(), gomock.Any()).
			Return(nil)

		req := cohort.UpdateCohortRequest{Status: cohort.CohortStatusActive}
		c, err := svc.Update(context.Background(), cohortID, req)
		if err != nil {
			t.Errorf("Update() unexpected error: %v", err)
		}
		if c.Status != cohort.CohortStatusActive {
			t.Errorf("Status = %q, expected %q", c.Status, cohort.CohortStatusActive)
		}
	})

	t.Run("update with new rules", func(t *testing.T) {
		newRules := cohort.Rules{
			Operator:   cohort.OperatorOR,
			Conditions: []cohort.Condition{{Type: cohort.ConditionTypeEvent, EventName: "signup"}},
		}
		newRulesJSON, _ := json.Marshal(newRules)

		mockQuerier.EXPECT().
			GetCohort(gomock.Any(), pgtype.UUID{Bytes: cohortID, Valid: true}).
			Return(db.Cohort{
				ID:          pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:        "Test Cohort",
				Description: pgtype.Text{String: "Description", Valid: true},
				Rules:       rulesJSON,
				Status:      string(cohort.CohortStatusDraft),
				Version:     1,
				CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockQuerier.EXPECT().
			UpdateCohort(gomock.Any(), gomock.Any()).
			Return(db.Cohort{
				ID:          pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:        "Test Cohort",
				Description: pgtype.Text{String: "Description", Valid: true},
				Rules:       newRulesJSON,
				Status:      string(cohort.CohortStatusDraft),
				Version:     2,
				CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockProducer.EXPECT().
			ProduceCohortDefinition(gomock.Any(), gomock.Any()).
			Return(nil)

		req := cohort.UpdateCohortRequest{Rules: &newRules}
		c, err := svc.Update(context.Background(), cohortID, req)
		if err != nil {
			t.Errorf("Update() unexpected error: %v", err)
		}
		if c.Rules.Operator != cohort.OperatorOR {
			t.Errorf("Rules.Operator = %q, expected %q", c.Rules.Operator, cohort.OperatorOR)
		}
	})

	t.Run("cohort not found", func(t *testing.T) {
		mockQuerier.EXPECT().
			GetCohort(gomock.Any(), gomock.Any()).
			Return(db.Cohort{}, errors.New("not found"))

		req := cohort.UpdateCohortRequest{Name: "Updated"}
		_, err := svc.Update(context.Background(), cohortID, req)
		if !errors.Is(err, cohort.ErrCohortNotFound) {
			t.Errorf("Update() error = %v, expected ErrCohortNotFound", err)
		}
	})
}

func TestService_Activate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQuerier := mocks.NewMockQuerier(ctrl)
	mockProducer := mocks.NewMockCohortProducer(ctrl)
	svc := cohort.NewService(mockQuerier, mockProducer)

	cohortID := uuid.New()
	now := time.Now().UTC()
	rules := cohort.Rules{Operator: cohort.OperatorAND, Conditions: []cohort.Condition{{Type: cohort.ConditionTypeEvent, EventName: "purchase"}}}
	rulesJSON, _ := json.Marshal(rules)

	t.Run("first activation triggers recompute", func(t *testing.T) {
		mockQuerier.EXPECT().
			GetCohort(gomock.Any(), pgtype.UUID{Bytes: cohortID, Valid: true}).
			Return(db.Cohort{
				ID:        pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:      "Test Cohort",
				Rules:     rulesJSON,
				Status:    string(cohort.CohortStatusDraft),
				Version:   1,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockQuerier.EXPECT().
			UpdateCohortStatus(gomock.Any(), db.UpdateCohortStatusParams{
				ID:     pgtype.UUID{Bytes: cohortID, Valid: true},
				Status: string(cohort.CohortStatusActive),
			}).
			Return(db.Cohort{
				ID:        pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:      "Test Cohort",
				Rules:     rulesJSON,
				Status:    string(cohort.CohortStatusActive),
				Version:   1,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockProducer.EXPECT().
			ProduceCohortDefinition(gomock.Any(), gomock.Any()).
			Return(nil)

		c, err := svc.Activate(context.Background(), cohortID)
		if err != nil {
			t.Errorf("Activate() unexpected error: %v", err)
		}
		if c.Status != cohort.CohortStatusActive {
			t.Errorf("Status = %q, expected %q", c.Status, cohort.CohortStatusActive)
		}
	})

	t.Run("reactivation does not trigger recompute", func(t *testing.T) {
		mockQuerier.EXPECT().
			GetCohort(gomock.Any(), pgtype.UUID{Bytes: cohortID, Valid: true}).
			Return(db.Cohort{
				ID:        pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:      "Test Cohort",
				Rules:     rulesJSON,
				Status:    string(cohort.CohortStatusInactive),
				Version:   2,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockQuerier.EXPECT().
			UpdateCohortStatus(gomock.Any(), db.UpdateCohortStatusParams{
				ID:     pgtype.UUID{Bytes: cohortID, Valid: true},
				Status: string(cohort.CohortStatusActive),
			}).
			Return(db.Cohort{
				ID:        pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:      "Test Cohort",
				Rules:     rulesJSON,
				Status:    string(cohort.CohortStatusActive),
				Version:   2,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockProducer.EXPECT().
			ProduceCohortDefinition(gomock.Any(), gomock.Any()).
			Return(nil)

		c, err := svc.Activate(context.Background(), cohortID)
		if err != nil {
			t.Errorf("Activate() unexpected error: %v", err)
		}
		if c.Status != cohort.CohortStatusActive {
			t.Errorf("Status = %q, expected %q", c.Status, cohort.CohortStatusActive)
		}
	})
}

func TestService_Deactivate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQuerier := mocks.NewMockQuerier(ctrl)
	mockProducer := mocks.NewMockCohortProducer(ctrl)
	svc := cohort.NewService(mockQuerier, mockProducer)

	cohortID := uuid.New()
	now := time.Now().UTC()
	rules := cohort.Rules{Operator: cohort.OperatorAND, Conditions: []cohort.Condition{}}
	rulesJSON, _ := json.Marshal(rules)

	t.Run("success", func(t *testing.T) {
		mockQuerier.EXPECT().
			UpdateCohortStatus(gomock.Any(), db.UpdateCohortStatusParams{
				ID:     pgtype.UUID{Bytes: cohortID, Valid: true},
				Status: string(cohort.CohortStatusInactive),
			}).
			Return(db.Cohort{
				ID:        pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:      "Test Cohort",
				Rules:     rulesJSON,
				Status:    string(cohort.CohortStatusInactive),
				Version:   1,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		mockProducer.EXPECT().
			ProduceCohortDefinition(gomock.Any(), gomock.Any()).
			Return(nil)

		c, err := svc.Deactivate(context.Background(), cohortID)
		if err != nil {
			t.Errorf("Deactivate() unexpected error: %v", err)
		}
		if c.Status != cohort.CohortStatusInactive {
			t.Errorf("Status = %q, expected %q", c.Status, cohort.CohortStatusInactive)
		}
	})

	t.Run("not found", func(t *testing.T) {
		mockQuerier.EXPECT().
			UpdateCohortStatus(gomock.Any(), gomock.Any()).
			Return(db.Cohort{}, errors.New("not found"))

		_, err := svc.Deactivate(context.Background(), cohortID)
		if !errors.Is(err, cohort.ErrCohortNotFound) {
			t.Errorf("Deactivate() error = %v, expected ErrCohortNotFound", err)
		}
	})
}

func TestService_Delete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQuerier := mocks.NewMockQuerier(ctrl)
	mockProducer := mocks.NewMockCohortProducer(ctrl)
	svc := cohort.NewService(mockQuerier, mockProducer)

	t.Run("success", func(t *testing.T) {
		cohortID := uuid.New()

		mockQuerier.EXPECT().
			DeleteCohort(gomock.Any(), pgtype.UUID{Bytes: cohortID, Valid: true}).
			Return(nil)

		mockProducer.EXPECT().
			ProduceCohortDeletion(gomock.Any(), cohortID.String()).
			Return(nil)

		err := svc.Delete(context.Background(), cohortID)
		if err != nil {
			t.Errorf("Delete() unexpected error: %v", err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		cohortID := uuid.New()

		mockQuerier.EXPECT().
			DeleteCohort(gomock.Any(), pgtype.UUID{Bytes: cohortID, Valid: true}).
			Return(errors.New("not found"))

		err := svc.Delete(context.Background(), cohortID)
		if !errors.Is(err, cohort.ErrCohortNotFound) {
			t.Errorf("Delete() error = %v, expected ErrCohortNotFound", err)
		}
	})
}

func TestService_TriggerRecompute(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQuerier := mocks.NewMockQuerier(ctrl)
	mockCHClient := mocks.NewMockClickHouseClient(ctrl)
	svc := cohort.NewService(mockQuerier, nil)

	cohortID := uuid.New()
	now := time.Now().UTC()
	rules := cohort.Rules{Operator: cohort.OperatorAND, Conditions: []cohort.Condition{{Type: cohort.ConditionTypeEvent, EventName: "purchase"}}}
	rulesJSON, _ := json.Marshal(rules)

	worker := cohort.NewRecomputeWorker(mockCHClient, svc)
	svc.SetRecomputeWorker(worker)

	t.Run("success", func(t *testing.T) {
		mockQuerier.EXPECT().
			GetCohort(gomock.Any(), pgtype.UUID{Bytes: cohortID, Valid: true}).
			Return(db.Cohort{
				ID:        pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:      "Test Cohort",
				Rules:     rulesJSON,
				Status:    string(cohort.CohortStatusActive),
				Version:   1,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		resp, err := svc.TriggerRecompute(context.Background(), cohortID, false)
		if err != nil {
			t.Errorf("TriggerRecompute() unexpected error: %v", err)
		}
		if resp.CohortID != cohortID {
			t.Errorf("CohortID = %v, expected %v", resp.CohortID, cohortID)
		}
		if resp.Status != cohort.RecomputeStatusPending {
			t.Errorf("Status = %q, expected %q", resp.Status, cohort.RecomputeStatusPending)
		}
	})

	t.Run("cohort not found", func(t *testing.T) {
		notFoundID := uuid.New()
		mockQuerier.EXPECT().
			GetCohort(gomock.Any(), pgtype.UUID{Bytes: notFoundID, Valid: true}).
			Return(db.Cohort{}, errors.New("not found"))

		_, err := svc.TriggerRecompute(context.Background(), notFoundID, false)
		if !errors.Is(err, cohort.ErrCohortNotFound) {
			t.Errorf("TriggerRecompute() error = %v, expected ErrCohortNotFound", err)
		}
	})

	t.Run("no worker available", func(t *testing.T) {
		svcNoWorker := cohort.NewService(mockQuerier, nil)
		mockQuerier.EXPECT().
			GetCohort(gomock.Any(), pgtype.UUID{Bytes: cohortID, Valid: true}).
			Return(db.Cohort{
				ID:        pgtype.UUID{Bytes: cohortID, Valid: true},
				Name:      "Test Cohort",
				Rules:     rulesJSON,
				Status:    string(cohort.CohortStatusActive),
				Version:   1,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil)

		_, err := svcNoWorker.TriggerRecompute(context.Background(), cohortID, false)
		if err == nil {
			t.Error("TriggerRecompute() expected error when worker not available")
		}
	})
}

func TestNewService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQuerier := mocks.NewMockQuerier(ctrl)
	mockProducer := mocks.NewMockCohortProducer(ctrl)

	svc := cohort.NewService(mockQuerier, mockProducer)
	if svc == nil {
		t.Error("NewService() returned nil")
	}
}

func TestService_ListActive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQuerier := mocks.NewMockQuerier(ctrl)
	svc := cohort.NewService(mockQuerier, nil)

	t.Run("success", func(t *testing.T) {
		now := time.Now().UTC()
		rules := cohort.Rules{Operator: cohort.OperatorAND, Conditions: []cohort.Condition{}}
		rulesJSON, _ := json.Marshal(rules)

		cohortID := uuid.New()

		mockQuerier.EXPECT().
			ListActiveCohorts(gomock.Any()).
			Return([]db.Cohort{
				{
					ID:        pgtype.UUID{Bytes: cohortID, Valid: true},
					Name:      "Active Cohort",
					Rules:     rulesJSON,
					Status:    string(cohort.CohortStatusActive),
					Version:   1,
					CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
					UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				},
			}, nil)

		cohorts, err := svc.ListActive(context.Background())
		if err != nil {
			t.Errorf("ListActive() unexpected error: %v", err)
		}
		if len(cohorts) != 1 {
			t.Errorf("len(cohorts) = %d, expected 1", len(cohorts))
		}
		if cohorts[0].Status != cohort.CohortStatusActive {
			t.Errorf("Status = %q, expected %q", cohorts[0].Status, cohort.CohortStatusActive)
		}
	})

	t.Run("database error", func(t *testing.T) {
		mockQuerier.EXPECT().
			ListActiveCohorts(gomock.Any()).
			Return(nil, errors.New("database error"))

		_, err := svc.ListActive(context.Background())
		if err == nil {
			t.Error("ListActive() expected error for database failure")
		}
	})
}

func TestService_GetRecomputeJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQuerier := mocks.NewMockQuerier(ctrl)
	mockCHClient := mocks.NewMockClickHouseClient(ctrl)
	svc := cohort.NewService(mockQuerier, nil)

	t.Run("no worker available", func(t *testing.T) {
		_, err := svc.GetRecomputeJob(context.Background(), uuid.New())
		if err == nil {
			t.Error("GetRecomputeJob() expected error when worker not available")
		}
	})

	t.Run("job not found", func(t *testing.T) {
		worker := cohort.NewRecomputeWorker(mockCHClient, svc)
		svc.SetRecomputeWorker(worker)

		_, err := svc.GetRecomputeJob(context.Background(), uuid.New())
		if !errors.Is(err, cohort.ErrRecomputeJobNotFound) {
			t.Errorf("GetRecomputeJob() error = %v, expected ErrRecomputeJobNotFound", err)
		}
	})

	t.Run("job found", func(t *testing.T) {
		worker := cohort.NewRecomputeWorker(mockCHClient, svc)
		svc.SetRecomputeWorker(worker)

		cohortID := uuid.New()
		job := cohort.NewRecomputeJob(cohortID)
		worker.SubmitJob(job)

		retrievedJob, err := svc.GetRecomputeJob(context.Background(), job.ID)
		if err != nil {
			t.Errorf("GetRecomputeJob() unexpected error: %v", err)
		}
		if retrievedJob.ID != job.ID {
			t.Errorf("Job ID = %v, expected %v", retrievedJob.ID, job.ID)
		}
	})
}
