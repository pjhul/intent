package cohort

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewCohort(t *testing.T) {
	name := "High Value Customers"
	description := "Customers with purchases over $1000"
	rules := Rules{
		Operator: OperatorAND,
		Conditions: []Condition{
			{
				Type:        ConditionTypeAggregate,
				EventName:   "purchase",
				Aggregation: AggregationSum,
				AggregationField: "amount",
				Operator:    ComparisonGTE,
				Value:       1000,
			},
		},
	}

	cohort := NewCohort(name, description, rules)

	if cohort.ID == uuid.Nil {
		t.Error("NewCohort() should generate a non-nil UUID")
	}
	if cohort.Name != name {
		t.Errorf("Name = %q, expected %q", cohort.Name, name)
	}
	if cohort.Description != description {
		t.Errorf("Description = %q, expected %q", cohort.Description, description)
	}
	if cohort.Status != CohortStatusDraft {
		t.Errorf("Status = %q, expected %q", cohort.Status, CohortStatusDraft)
	}
	if cohort.Version != 1 {
		t.Errorf("Version = %d, expected 1", cohort.Version)
	}
	if cohort.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
	if cohort.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should not be zero")
	}
	if len(cohort.Rules.Conditions) != 1 {
		t.Errorf("Rules.Conditions length = %d, expected 1", len(cohort.Rules.Conditions))
	}
}

func TestCohort_Activate(t *testing.T) {
	cohort := NewCohort("Test", "", Rules{})
	originalUpdatedAt := cohort.UpdatedAt

	time.Sleep(1 * time.Millisecond)
	cohort.Activate()

	if cohort.Status != CohortStatusActive {
		t.Errorf("Status = %q, expected %q", cohort.Status, CohortStatusActive)
	}
	if !cohort.UpdatedAt.After(originalUpdatedAt) {
		t.Error("UpdatedAt should be updated after Activate()")
	}
}

func TestCohort_Deactivate(t *testing.T) {
	cohort := NewCohort("Test", "", Rules{})
	cohort.Activate()
	originalUpdatedAt := cohort.UpdatedAt

	time.Sleep(1 * time.Millisecond)
	cohort.Deactivate()

	if cohort.Status != CohortStatusInactive {
		t.Errorf("Status = %q, expected %q", cohort.Status, CohortStatusInactive)
	}
	if !cohort.UpdatedAt.After(originalUpdatedAt) {
		t.Error("UpdatedAt should be updated after Deactivate()")
	}
}

func TestCohort_Update(t *testing.T) {
	originalRules := Rules{
		Operator: OperatorAND,
		Conditions: []Condition{
			{Type: ConditionTypeEvent, EventName: "signup"},
		},
	}
	cohort := NewCohort("Original Name", "Original Description", originalRules)
	originalVersion := cohort.Version
	originalUpdatedAt := cohort.UpdatedAt

	time.Sleep(1 * time.Millisecond)

	newName := "Updated Name"
	newDescription := "Updated Description"
	newRules := Rules{
		Operator: OperatorOR,
		Conditions: []Condition{
			{Type: ConditionTypeEvent, EventName: "purchase"},
			{Type: ConditionTypeEvent, EventName: "checkout"},
		},
	}

	cohort.Update(newName, newDescription, newRules)

	if cohort.Name != newName {
		t.Errorf("Name = %q, expected %q", cohort.Name, newName)
	}
	if cohort.Description != newDescription {
		t.Errorf("Description = %q, expected %q", cohort.Description, newDescription)
	}
	if cohort.Rules.Operator != OperatorOR {
		t.Errorf("Rules.Operator = %q, expected %q", cohort.Rules.Operator, OperatorOR)
	}
	if len(cohort.Rules.Conditions) != 2 {
		t.Errorf("Rules.Conditions length = %d, expected 2", len(cohort.Rules.Conditions))
	}
	if cohort.Version != originalVersion+1 {
		t.Errorf("Version = %d, expected %d", cohort.Version, originalVersion+1)
	}
	if !cohort.UpdatedAt.After(originalUpdatedAt) {
		t.Error("UpdatedAt should be updated after Update()")
	}
}

func TestCohort_ToJSON(t *testing.T) {
	cohort := NewCohort("Test Cohort", "Test Description", Rules{
		Operator: OperatorAND,
		Conditions: []Condition{
			{Type: ConditionTypeEvent, EventName: "purchase"},
		},
	})

	data, err := cohort.ToJSON()
	if err != nil {
		t.Errorf("ToJSON() unexpected error: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Errorf("failed to unmarshal JSON: %v", err)
	}

	if result["name"] != "Test Cohort" {
		t.Errorf("JSON name = %v, expected Test Cohort", result["name"])
	}
	if result["description"] != "Test Description" {
		t.Errorf("JSON description = %v, expected Test Description", result["description"])
	}
	if result["status"] != string(CohortStatusDraft) {
		t.Errorf("JSON status = %v, expected %s", result["status"], CohortStatusDraft)
	}
}

func TestCohortFromJSON(t *testing.T) {
	t.Run("valid JSON", func(t *testing.T) {
		original := NewCohort("Test Cohort", "Test Description", Rules{
			Operator: OperatorAND,
			Conditions: []Condition{
				{Type: ConditionTypeEvent, EventName: "purchase"},
			},
		})

		data, err := original.ToJSON()
		if err != nil {
			t.Fatalf("ToJSON() unexpected error: %v", err)
		}

		parsed, err := CohortFromJSON(data)
		if err != nil {
			t.Errorf("CohortFromJSON() unexpected error: %v", err)
		}

		if parsed.ID != original.ID {
			t.Errorf("ID = %v, expected %v", parsed.ID, original.ID)
		}
		if parsed.Name != original.Name {
			t.Errorf("Name = %q, expected %q", parsed.Name, original.Name)
		}
		if parsed.Description != original.Description {
			t.Errorf("Description = %q, expected %q", parsed.Description, original.Description)
		}
		if parsed.Status != original.Status {
			t.Errorf("Status = %q, expected %q", parsed.Status, original.Status)
		}
		if parsed.Version != original.Version {
			t.Errorf("Version = %d, expected %d", parsed.Version, original.Version)
		}
		if parsed.Rules.Operator != original.Rules.Operator {
			t.Errorf("Rules.Operator = %q, expected %q", parsed.Rules.Operator, original.Rules.Operator)
		}
		if len(parsed.Rules.Conditions) != len(original.Rules.Conditions) {
			t.Errorf("Rules.Conditions length = %d, expected %d", len(parsed.Rules.Conditions), len(original.Rules.Conditions))
		}
	})

	t.Run("invalid JSON returns error", func(t *testing.T) {
		_, err := CohortFromJSON([]byte("invalid json"))
		if err == nil {
			t.Error("CohortFromJSON() expected error for invalid JSON")
		}
	})

	t.Run("empty JSON returns error", func(t *testing.T) {
		_, err := CohortFromJSON([]byte(""))
		if err == nil {
			t.Error("CohortFromJSON() expected error for empty input")
		}
	})
}

func TestCohort_JSONRoundTrip(t *testing.T) {
	original := NewCohort("Complex Cohort", "With all condition types", Rules{
		Operator: OperatorOR,
		Conditions: []Condition{
			{
				Type:      ConditionTypeEvent,
				EventName: "purchase",
				TimeWindow: &TimeWindow{
					Type:     TimeWindowSliding,
					Duration: "30d",
				},
				PropertyFilters: []PropertyFilter{
					{Key: "amount", Operator: ComparisonGTE, Value: 100.0},
				},
			},
			{
				Type:             ConditionTypeAggregate,
				EventName:        "page_view",
				Aggregation:      AggregationCount,
				Operator:         ComparisonGTE,
				Value:            10,
			},
			{
				Type:         ConditionTypeProperty,
				PropertyName: "subscription_tier",
				Operator:     ComparisonEQ,
				Value:        "premium",
			},
		},
	})

	data, err := original.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() unexpected error: %v", err)
	}

	parsed, err := CohortFromJSON(data)
	if err != nil {
		t.Fatalf("CohortFromJSON() unexpected error: %v", err)
	}

	if len(parsed.Rules.Conditions) != 3 {
		t.Errorf("Rules.Conditions length = %d, expected 3", len(parsed.Rules.Conditions))
	}

	eventCond := parsed.Rules.Conditions[0]
	if eventCond.Type != ConditionTypeEvent {
		t.Errorf("Condition[0].Type = %q, expected %q", eventCond.Type, ConditionTypeEvent)
	}
	if eventCond.TimeWindow == nil {
		t.Error("Condition[0].TimeWindow should not be nil")
	} else if eventCond.TimeWindow.Duration != "30d" {
		t.Errorf("Condition[0].TimeWindow.Duration = %q, expected 30d", eventCond.TimeWindow.Duration)
	}
	if len(eventCond.PropertyFilters) != 1 {
		t.Errorf("Condition[0].PropertyFilters length = %d, expected 1", len(eventCond.PropertyFilters))
	}

	aggCond := parsed.Rules.Conditions[1]
	if aggCond.Type != ConditionTypeAggregate {
		t.Errorf("Condition[1].Type = %q, expected %q", aggCond.Type, ConditionTypeAggregate)
	}
	if aggCond.Aggregation != AggregationCount {
		t.Errorf("Condition[1].Aggregation = %q, expected %q", aggCond.Aggregation, AggregationCount)
	}

	propCond := parsed.Rules.Conditions[2]
	if propCond.Type != ConditionTypeProperty {
		t.Errorf("Condition[2].Type = %q, expected %q", propCond.Type, ConditionTypeProperty)
	}
	if propCond.PropertyName != "subscription_tier" {
		t.Errorf("Condition[2].PropertyName = %q, expected subscription_tier", propCond.PropertyName)
	}
}

func TestTimeWindowType_Constants(t *testing.T) {
	if TimeWindowSliding != "sliding" {
		t.Errorf("TimeWindowSliding = %q, expected sliding", TimeWindowSliding)
	}
	if TimeWindowAbsolute != "absolute" {
		t.Errorf("TimeWindowAbsolute = %q, expected absolute", TimeWindowAbsolute)
	}
}

func TestConditionType_Constants(t *testing.T) {
	if ConditionTypeEvent != "event" {
		t.Errorf("ConditionTypeEvent = %q, expected event", ConditionTypeEvent)
	}
	if ConditionTypeProperty != "property" {
		t.Errorf("ConditionTypeProperty = %q, expected property", ConditionTypeProperty)
	}
	if ConditionTypeAggregate != "aggregate" {
		t.Errorf("ConditionTypeAggregate = %q, expected aggregate", ConditionTypeAggregate)
	}
}

func TestAggregationType_Constants(t *testing.T) {
	if AggregationCount != "count" {
		t.Errorf("AggregationCount = %q, expected count", AggregationCount)
	}
	if AggregationSum != "sum" {
		t.Errorf("AggregationSum = %q, expected sum", AggregationSum)
	}
	if AggregationAvg != "avg" {
		t.Errorf("AggregationAvg = %q, expected avg", AggregationAvg)
	}
	if AggregationMin != "min" {
		t.Errorf("AggregationMin = %q, expected min", AggregationMin)
	}
	if AggregationMax != "max" {
		t.Errorf("AggregationMax = %q, expected max", AggregationMax)
	}
	if AggregationDistinctCount != "distinct_count" {
		t.Errorf("AggregationDistinctCount = %q, expected distinct_count", AggregationDistinctCount)
	}
}

func TestCohortStatus_Constants(t *testing.T) {
	if CohortStatusActive != "active" {
		t.Errorf("CohortStatusActive = %q, expected active", CohortStatusActive)
	}
	if CohortStatusInactive != "inactive" {
		t.Errorf("CohortStatusInactive = %q, expected inactive", CohortStatusInactive)
	}
	if CohortStatusDraft != "draft" {
		t.Errorf("CohortStatusDraft = %q, expected draft", CohortStatusDraft)
	}
}

func TestOperator_Constants(t *testing.T) {
	if OperatorAND != "AND" {
		t.Errorf("OperatorAND = %q, expected AND", OperatorAND)
	}
	if OperatorOR != "OR" {
		t.Errorf("OperatorOR = %q, expected OR", OperatorOR)
	}
}

func TestComparisonOperator_Constants(t *testing.T) {
	if ComparisonEQ != "eq" {
		t.Errorf("ComparisonEQ = %q, expected eq", ComparisonEQ)
	}
	if ComparisonNE != "ne" {
		t.Errorf("ComparisonNE = %q, expected ne", ComparisonNE)
	}
	if ComparisonGT != "gt" {
		t.Errorf("ComparisonGT = %q, expected gt", ComparisonGT)
	}
	if ComparisonGTE != "gte" {
		t.Errorf("ComparisonGTE = %q, expected gte", ComparisonGTE)
	}
	if ComparisonLT != "lt" {
		t.Errorf("ComparisonLT = %q, expected lt", ComparisonLT)
	}
	if ComparisonLTE != "lte" {
		t.Errorf("ComparisonLTE = %q, expected lte", ComparisonLTE)
	}
	if ComparisonIN != "in" {
		t.Errorf("ComparisonIN = %q, expected in", ComparisonIN)
	}
	if ComparisonNIN != "nin" {
		t.Errorf("ComparisonNIN = %q, expected nin", ComparisonNIN)
	}
}
