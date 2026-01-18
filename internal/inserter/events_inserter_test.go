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

func TestEventsInserter_InsertBatch_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockBatch := mocks.NewMockInserterBatch(ctrl)

	events := []inserter.RawEvent{
		{
			ID:         uuid.New(),
			UserID:     "user1",
			EventName:  "page_view",
			Properties: map[string]any{"page": "/home"},
			Timestamp:  time.Now(),
			ReceivedAt: time.Now(),
		},
		{
			ID:         uuid.New(),
			UserID:     "user2",
			EventName:  "button_click",
			Properties: map[string]any{"button": "submit"},
			Timestamp:  time.Now(),
			ReceivedAt: time.Now(),
		},
	}

	mockClient.EXPECT().
		PrepareBatch(gomock.Any(), gomock.Any()).
		Return(mockBatch, nil)

	mockBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		Times(2)

	mockBatch.EXPECT().
		Send().
		Return(nil)

	inserterSvc := inserter.NewEventsInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), events)

	if err != nil {
		t.Errorf("InsertBatch returned error: %v", err)
	}
}

func TestEventsInserter_InsertBatch_EmptyBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)

	// PrepareBatch should NOT be called for empty batch
	mockClient.EXPECT().PrepareBatch(gomock.Any(), gomock.Any()).Times(0)

	inserterSvc := inserter.NewEventsInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), []inserter.RawEvent{})

	if err != nil {
		t.Errorf("InsertBatch returned error for empty batch: %v", err)
	}
}

func TestEventsInserter_InsertBatch_PrepareBatchError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	expectedErr := errors.New("prepare batch error")

	events := []inserter.RawEvent{
		{
			ID:         uuid.New(),
			UserID:     "user1",
			EventName:  "test_event",
			Timestamp:  time.Now(),
			ReceivedAt: time.Now(),
		},
	}

	mockClient.EXPECT().
		PrepareBatch(gomock.Any(), gomock.Any()).
		Return(nil, expectedErr)

	inserterSvc := inserter.NewEventsInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), events)

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestEventsInserter_InsertBatch_AppendError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockBatch := mocks.NewMockInserterBatch(ctrl)
	expectedErr := errors.New("append error")

	events := []inserter.RawEvent{
		{
			ID:         uuid.New(),
			UserID:     "user1",
			EventName:  "test_event",
			Timestamp:  time.Now(),
			ReceivedAt: time.Now(),
		},
	}

	mockClient.EXPECT().
		PrepareBatch(gomock.Any(), gomock.Any()).
		Return(mockBatch, nil)

	mockBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(expectedErr)

	inserterSvc := inserter.NewEventsInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), events)

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestEventsInserter_InsertBatch_SendError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockBatch := mocks.NewMockInserterBatch(ctrl)
	expectedErr := errors.New("send error")

	events := []inserter.RawEvent{
		{
			ID:         uuid.New(),
			UserID:     "user1",
			EventName:  "test_event",
			Timestamp:  time.Now(),
			ReceivedAt: time.Now(),
		},
	}

	mockClient.EXPECT().
		PrepareBatch(gomock.Any(), gomock.Any()).
		Return(mockBatch, nil)

	mockBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	mockBatch.EXPECT().
		Send().
		Return(expectedErr)

	inserterSvc := inserter.NewEventsInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), events)

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestEventsInserter_InsertBatch_NilProperties(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockBatch := mocks.NewMockInserterBatch(ctrl)

	events := []inserter.RawEvent{
		{
			ID:         uuid.New(),
			UserID:     "user1",
			EventName:  "test_event",
			Properties: nil, // nil properties should marshal to {}
			Timestamp:  time.Now(),
			ReceivedAt: time.Now(),
		},
	}

	mockClient.EXPECT().
		PrepareBatch(gomock.Any(), gomock.Any()).
		Return(mockBatch, nil)

	mockBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	mockBatch.EXPECT().
		Send().
		Return(nil)

	inserterSvc := inserter.NewEventsInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), events)

	if err != nil {
		t.Errorf("InsertBatch returned error: %v", err)
	}
}

func TestEventsInserter_InsertBatch_ComplexProperties(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockBatchPreparer(ctrl)
	mockBatch := mocks.NewMockInserterBatch(ctrl)

	events := []inserter.RawEvent{
		{
			ID:        uuid.New(),
			UserID:    "user1",
			EventName: "complex_event",
			Properties: map[string]any{
				"string":  "value",
				"number":  42,
				"float":   3.14,
				"boolean": true,
				"nested": map[string]any{
					"key": "value",
				},
				"array": []any{1, 2, 3},
			},
			Timestamp:  time.Now(),
			ReceivedAt: time.Now(),
		},
	}

	mockClient.EXPECT().
		PrepareBatch(gomock.Any(), gomock.Any()).
		Return(mockBatch, nil)

	mockBatch.EXPECT().
		Append(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	mockBatch.EXPECT().
		Send().
		Return(nil)

	inserterSvc := inserter.NewEventsInserterWithClient(mockClient)
	err := inserterSvc.InsertBatch(context.Background(), events)

	if err != nil {
		t.Errorf("InsertBatch returned error: %v", err)
	}
}
