package inserter_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pjhul/intent/internal/inserter"
)

func TestMembershipChange_IsMember_True(t *testing.T) {
	change := inserter.MembershipChange{
		CohortID:   uuid.New(),
		CohortName: "Test Cohort",
		UserID:     "user1",
		PrevStatus: -1,
		NewStatus:  1, // Member
		ChangedAt:  time.Now(),
	}

	if !change.IsMember() {
		t.Error("IsMember() should return true when NewStatus is 1")
	}
}

func TestMembershipChange_IsMember_False(t *testing.T) {
	change := inserter.MembershipChange{
		CohortID:   uuid.New(),
		CohortName: "Test Cohort",
		UserID:     "user1",
		PrevStatus: 1,
		NewStatus:  -1, // Not a member
		ChangedAt:  time.Now(),
	}

	if change.IsMember() {
		t.Error("IsMember() should return false when NewStatus is -1")
	}
}

func TestMembershipChange_IsMember_ZeroValue(t *testing.T) {
	change := inserter.MembershipChange{
		CohortID:   uuid.New(),
		CohortName: "Test Cohort",
		UserID:     "user1",
		PrevStatus: 0,
		NewStatus:  0, // Zero value
		ChangedAt:  time.Now(),
	}

	if change.IsMember() {
		t.Error("IsMember() should return false when NewStatus is 0")
	}
}

func TestRawEvent_Fields(t *testing.T) {
	id := uuid.New()
	now := time.Now()
	props := map[string]any{"key": "value"}

	event := inserter.RawEvent{
		ID:         id,
		UserID:     "user123",
		EventName:  "page_view",
		Properties: props,
		Timestamp:  now,
		ReceivedAt: now,
	}

	if event.ID != id {
		t.Errorf("ID = %v, expected %v", event.ID, id)
	}
	if event.UserID != "user123" {
		t.Errorf("UserID = %q, expected %q", event.UserID, "user123")
	}
	if event.EventName != "page_view" {
		t.Errorf("EventName = %q, expected %q", event.EventName, "page_view")
	}
	if event.Properties["key"] != "value" {
		t.Errorf("Properties[key] = %v, expected %q", event.Properties["key"], "value")
	}
	if !event.Timestamp.Equal(now) {
		t.Errorf("Timestamp = %v, expected %v", event.Timestamp, now)
	}
	if !event.ReceivedAt.Equal(now) {
		t.Errorf("ReceivedAt = %v, expected %v", event.ReceivedAt, now)
	}
}

func TestMembershipChange_Fields(t *testing.T) {
	cohortID := uuid.New()
	triggerEvent := uuid.New()
	now := time.Now()

	change := inserter.MembershipChange{
		CohortID:     cohortID,
		CohortName:   "Active Users",
		UserID:       "user456",
		PrevStatus:   -1,
		NewStatus:    1,
		ChangedAt:    now,
		TriggerEvent: &triggerEvent,
	}

	if change.CohortID != cohortID {
		t.Errorf("CohortID = %v, expected %v", change.CohortID, cohortID)
	}
	if change.CohortName != "Active Users" {
		t.Errorf("CohortName = %q, expected %q", change.CohortName, "Active Users")
	}
	if change.UserID != "user456" {
		t.Errorf("UserID = %q, expected %q", change.UserID, "user456")
	}
	if change.PrevStatus != -1 {
		t.Errorf("PrevStatus = %d, expected %d", change.PrevStatus, -1)
	}
	if change.NewStatus != 1 {
		t.Errorf("NewStatus = %d, expected %d", change.NewStatus, 1)
	}
	if !change.ChangedAt.Equal(now) {
		t.Errorf("ChangedAt = %v, expected %v", change.ChangedAt, now)
	}
	if change.TriggerEvent == nil || *change.TriggerEvent != triggerEvent {
		t.Errorf("TriggerEvent = %v, expected %v", change.TriggerEvent, &triggerEvent)
	}
}

func TestMembershipChange_NilTriggerEvent(t *testing.T) {
	change := inserter.MembershipChange{
		CohortID:     uuid.New(),
		CohortName:   "Test Cohort",
		UserID:       "user1",
		PrevStatus:   -1,
		NewStatus:    1,
		ChangedAt:    time.Now(),
		TriggerEvent: nil,
	}

	if change.TriggerEvent != nil {
		t.Error("TriggerEvent should be nil")
	}
}
