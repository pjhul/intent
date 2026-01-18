package inserter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pjhul/intent/internal/inserter"
	"github.com/pjhul/intent/internal/mocks"
	"go.uber.org/mock/gomock"
)

func TestMembershipInserter_InsertBatch_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockCurrentBatch := mocks.NewMockInserterBatch(ctrl)
	mockChangelogBatch := mocks.NewMockInserterBatch(ctrl)

	triggerEvent := uuid.New()
	changes := []inserter.MembershipChange{
		{
			CohortID:     uuid.New(),
			CohortName:   "Test Cohort",
			UserID:       "user1",
			PrevStatus:   -1,
			NewStatus:    1,
			ChangedAt:    time.Now(),
			TriggerEvent: &triggerEvent,
		},
		{
			CohortID:     uuid.New(),
			CohortName:   "Test Cohort 2",
			UserID:       "user2",
			PrevStatus:   1,
			NewStatus:    -1,
			ChangedAt:    time.Now(),
			TriggerEvent: nil,
		},
	}

	// Expect two PrepareBatch calls: one for current, one for changelog
	gomock.InOrder(
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(mockCurrentBatch, nil),
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(mockChangelogBatch, nil),
	)

	// Current batch expectations
	mockCurrentBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		Times(2)
	mockCurrentBatch.EXPECT().
		Send().
		Return(nil)

	// Changelog batch expectations
	mockChangelogBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		Times(2)
	mockChangelogBatch.EXPECT().
		Send().
		Return(nil)

	inserterSvc := inserter.NewMembershipInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), changes)

	if err != nil {
		t.Errorf("InsertBatch returned error: %v", err)
	}
}

func TestMembershipInserter_InsertBatch_EmptyBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)

	// PrepareBatch should NOT be called for empty batch
	mockClient.EXPECT().PrepareBatch(gomock.Any(), gomock.Any()).Times(0)

	inserterSvc := inserter.NewMembershipInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), []inserter.MembershipChange{})

	if err != nil {
		t.Errorf("InsertBatch returned error for empty batch: %v", err)
	}
}

func TestMembershipInserter_InsertBatch_CurrentBatchPrepareFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	expectedErr := errors.New("prepare current batch error")

	changes := []inserter.MembershipChange{
		{
			CohortID:   uuid.New(),
			CohortName: "Test Cohort",
			UserID:     "user1",
			PrevStatus: -1,
			NewStatus:  1,
			ChangedAt:  time.Now(),
		},
	}

	mockClient.EXPECT().
		PrepareBatch(gomock.Any(), gomock.Any()).
		Return(nil, expectedErr)

	inserterSvc := inserter.NewMembershipInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), changes)

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMembershipInserter_InsertBatch_CurrentBatchAppendFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockCurrentBatch := mocks.NewMockInserterBatch(ctrl)
	expectedErr := errors.New("append current batch error")

	changes := []inserter.MembershipChange{
		{
			CohortID:   uuid.New(),
			CohortName: "Test Cohort",
			UserID:     "user1",
			PrevStatus: -1,
			NewStatus:  1,
			ChangedAt:  time.Now(),
		},
	}

	mockClient.EXPECT().
		PrepareBatch(gomock.Any(), gomock.Any()).
		Return(mockCurrentBatch, nil)

	mockCurrentBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(expectedErr)

	inserterSvc := inserter.NewMembershipInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), changes)

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMembershipInserter_InsertBatch_CurrentBatchSendFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockCurrentBatch := mocks.NewMockInserterBatch(ctrl)
	expectedErr := errors.New("send current batch error")

	changes := []inserter.MembershipChange{
		{
			CohortID:   uuid.New(),
			CohortName: "Test Cohort",
			UserID:     "user1",
			PrevStatus: -1,
			NewStatus:  1,
			ChangedAt:  time.Now(),
		},
	}

	mockClient.EXPECT().
		PrepareBatch(gomock.Any(), gomock.Any()).
		Return(mockCurrentBatch, nil)

	mockCurrentBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	mockCurrentBatch.EXPECT().
		Send().
		Return(expectedErr)

	inserterSvc := inserter.NewMembershipInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), changes)

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMembershipInserter_InsertBatch_ChangelogBatchPrepareFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockCurrentBatch := mocks.NewMockInserterBatch(ctrl)
	expectedErr := errors.New("prepare changelog batch error")

	changes := []inserter.MembershipChange{
		{
			CohortID:   uuid.New(),
			CohortName: "Test Cohort",
			UserID:     "user1",
			PrevStatus: -1,
			NewStatus:  1,
			ChangedAt:  time.Now(),
		},
	}

	gomock.InOrder(
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(mockCurrentBatch, nil),
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(nil, expectedErr),
	)

	mockCurrentBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	mockCurrentBatch.EXPECT().
		Send().
		Return(nil)

	inserterSvc := inserter.NewMembershipInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), changes)

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMembershipInserter_InsertBatch_ChangelogBatchAppendFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockCurrentBatch := mocks.NewMockInserterBatch(ctrl)
	mockChangelogBatch := mocks.NewMockInserterBatch(ctrl)
	expectedErr := errors.New("append changelog batch error")

	changes := []inserter.MembershipChange{
		{
			CohortID:   uuid.New(),
			CohortName: "Test Cohort",
			UserID:     "user1",
			PrevStatus: -1,
			NewStatus:  1,
			ChangedAt:  time.Now(),
		},
	}

	gomock.InOrder(
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(mockCurrentBatch, nil),
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(mockChangelogBatch, nil),
	)

	mockCurrentBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	mockCurrentBatch.EXPECT().
		Send().
		Return(nil)

	mockChangelogBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(expectedErr)

	inserterSvc := inserter.NewMembershipInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), changes)

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMembershipInserter_InsertBatch_ChangelogBatchSendFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockCurrentBatch := mocks.NewMockInserterBatch(ctrl)
	mockChangelogBatch := mocks.NewMockInserterBatch(ctrl)
	expectedErr := errors.New("send changelog batch error")

	changes := []inserter.MembershipChange{
		{
			CohortID:   uuid.New(),
			CohortName: "Test Cohort",
			UserID:     "user1",
			PrevStatus: -1,
			NewStatus:  1,
			ChangedAt:  time.Now(),
		},
	}

	gomock.InOrder(
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(mockCurrentBatch, nil),
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(mockChangelogBatch, nil),
	)

	mockCurrentBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	mockCurrentBatch.EXPECT().
		Send().
		Return(nil)

	mockChangelogBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	mockChangelogBatch.EXPECT().
		Send().
		Return(expectedErr)

	inserterSvc := inserter.NewMembershipInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), changes)

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMembershipInserter_InsertBatch_ZeroTimestamp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockCurrentBatch := mocks.NewMockInserterBatch(ctrl)
	mockChangelogBatch := mocks.NewMockInserterBatch(ctrl)

	// Zero timestamp should be replaced with current time
	changes := []inserter.MembershipChange{
		{
			CohortID:   uuid.New(),
			CohortName: "Test Cohort",
			UserID:     "user1",
			PrevStatus: -1,
			NewStatus:  1,
			ChangedAt:  time.Time{}, // Zero value
		},
	}

	gomock.InOrder(
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(mockCurrentBatch, nil),
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(mockChangelogBatch, nil),
	)

	mockCurrentBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	mockCurrentBatch.EXPECT().
		Send().
		Return(nil)

	// The changelog batch should receive a non-zero timestamp
	mockChangelogBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(args ...any) error {
			// changedAt should be the 5th argument (index 4)
			if len(args) >= 5 {
				changedAt, ok := args[4].(time.Time)
				if ok && changedAt.IsZero() {
					t.Error("changelog batch received zero timestamp, expected non-zero")
				}
			}
			return nil
		})

	mockChangelogBatch.EXPECT().
		Send().
		Return(nil)

	inserterSvc := inserter.NewMembershipInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), changes)

	if err != nil {
		t.Errorf("InsertBatch returned error: %v", err)
	}
}

func TestMembershipInserter_InsertBatch_JoinAndLeave(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockCurrentBatch := mocks.NewMockInserterBatch(ctrl)
	mockChangelogBatch := mocks.NewMockInserterBatch(ctrl)

	// Test both join (1) and leave (-1) statuses
	changes := []inserter.MembershipChange{
		{
			CohortID:   uuid.New(),
			CohortName: "Test Cohort",
			UserID:     "user1",
			PrevStatus: -1,
			NewStatus:  1, // Join
			ChangedAt:  time.Now(),
		},
		{
			CohortID:   uuid.New(),
			CohortName: "Test Cohort",
			UserID:     "user2",
			PrevStatus: 1,
			NewStatus:  -1, // Leave
			ChangedAt:  time.Now(),
		},
	}

	gomock.InOrder(
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(mockCurrentBatch, nil),
		mockClient.EXPECT().
			PrepareBatch(gomock.Any(), gomock.Any()).
			Return(mockChangelogBatch, nil),
	)

	mockCurrentBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		Times(2)

	mockCurrentBatch.EXPECT().
		Send().
		Return(nil)

	mockChangelogBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		Times(2)

	mockChangelogBatch.EXPECT().
		Send().
		Return(nil)

	inserterSvc := inserter.NewMembershipInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), changes)

	if err != nil {
		t.Errorf("InsertBatch returned error: %v", err)
	}
}
