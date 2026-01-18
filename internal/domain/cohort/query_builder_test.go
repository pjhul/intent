package cohort

import (
	"strings"
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "30 days",
			input:    "30d",
			expected: 30 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "7 days",
			input:    "7d",
			expected: 7 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "24 hours",
			input:    "24h",
			expected: 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "1 week",
			input:    "1w",
			expected: 7 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "2 hours 30 minutes (standard Go duration)",
			input:    "2h30m",
			expected: 2*time.Hour + 30*time.Minute,
			wantErr:  false,
		},
		{
			name:     "1 month",
			input:    "1M",
			expected: 30 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "5 minutes",
			input:    "5m",
			expected: 5 * time.Minute,
			wantErr:  false,
		},
		{
			name:    "invalid string",
			input:   "invalid",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDuration(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseDuration(%q) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("parseDuration(%q) unexpected error: %v", tt.input, err)
				return
			}
			if got != tt.expected {
				t.Errorf("parseDuration(%q) = %v, expected %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestGetComparisonOperator(t *testing.T) {
	qb := NewQueryBuilder()

	tests := []struct {
		name     string
		op       ComparisonOperator
		expected string
		wantErr  bool
	}{
		{name: "eq", op: ComparisonEQ, expected: "=", wantErr: false},
		{name: "ne", op: ComparisonNE, expected: "!=", wantErr: false},
		{name: "gt", op: ComparisonGT, expected: ">", wantErr: false},
		{name: "gte", op: ComparisonGTE, expected: ">=", wantErr: false},
		{name: "lt", op: ComparisonLT, expected: "<", wantErr: false},
		{name: "lte", op: ComparisonLTE, expected: "<=", wantErr: false},
		{name: "in", op: ComparisonIN, expected: "IN", wantErr: false},
		{name: "nin", op: ComparisonNIN, expected: "NOT IN", wantErr: false},
		{name: "invalid", op: ComparisonOperator("invalid"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := qb.getComparisonOperator(tt.op)
			if tt.wantErr {
				if err == nil {
					t.Errorf("getComparisonOperator(%q) expected error, got nil", tt.op)
				}
				return
			}
			if err != nil {
				t.Errorf("getComparisonOperator(%q) unexpected error: %v", tt.op, err)
				return
			}
			if got != tt.expected {
				t.Errorf("getComparisonOperator(%q) = %q, expected %q", tt.op, got, tt.expected)
			}
		})
	}
}

func TestResolveTimeWindow(t *testing.T) {
	fixedTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	qb := NewQueryBuilderWithTime(fixedTime)

	t.Run("nil time window returns nil", func(t *testing.T) {
		start, end, err := qb.resolveTimeWindow(nil)
		if err != nil {
			t.Errorf("resolveTimeWindow(nil) unexpected error: %v", err)
		}
		if start != nil || end != nil {
			t.Error("resolveTimeWindow(nil) expected nil start and end")
		}
	})

	t.Run("sliding window", func(t *testing.T) {
		tw := &TimeWindow{
			Type:     TimeWindowSliding,
			Duration: "7d",
		}
		start, end, err := qb.resolveTimeWindow(tw)
		if err != nil {
			t.Errorf("resolveTimeWindow() unexpected error: %v", err)
		}
		expectedStart := fixedTime.Add(-7 * 24 * time.Hour)
		if start == nil || !start.Equal(expectedStart) {
			t.Errorf("start = %v, expected %v", start, expectedStart)
		}
		if end == nil || !end.Equal(fixedTime) {
			t.Errorf("end = %v, expected %v", end, fixedTime)
		}
	})

	t.Run("sliding window without duration returns error", func(t *testing.T) {
		tw := &TimeWindow{
			Type: TimeWindowSliding,
		}
		_, _, err := qb.resolveTimeWindow(tw)
		if err == nil {
			t.Error("resolveTimeWindow() expected error for sliding window without duration")
		}
	})

	t.Run("absolute window", func(t *testing.T) {
		startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		endTime := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
		tw := &TimeWindow{
			Type:  TimeWindowAbsolute,
			Start: &startTime,
			End:   &endTime,
		}
		start, end, err := qb.resolveTimeWindow(tw)
		if err != nil {
			t.Errorf("resolveTimeWindow() unexpected error: %v", err)
		}
		if start == nil || !start.Equal(startTime) {
			t.Errorf("start = %v, expected %v", start, startTime)
		}
		if end == nil || !end.Equal(endTime) {
			t.Errorf("end = %v, expected %v", end, endTime)
		}
	})

	t.Run("unsupported time window type returns error", func(t *testing.T) {
		tw := &TimeWindow{
			Type: TimeWindowType("unsupported"),
		}
		_, _, err := qb.resolveTimeWindow(tw)
		if err == nil {
			t.Error("resolveTimeWindow() expected error for unsupported type")
		}
	})
}

func TestBuildPropertyFilters(t *testing.T) {
	qb := NewQueryBuilder()

	t.Run("empty filters returns empty string", func(t *testing.T) {
		clause, args := qb.buildPropertyFilters(nil)
		if clause != "" {
			t.Errorf("buildPropertyFilters(nil) clause = %q, expected empty", clause)
		}
		if len(args) != 0 {
			t.Errorf("buildPropertyFilters(nil) args = %v, expected empty", args)
		}
	})

	t.Run("single filter with string value", func(t *testing.T) {
		filters := []PropertyFilter{
			{Key: "country", Operator: ComparisonEQ, Value: "US"},
		}
		clause, args := qb.buildPropertyFilters(filters)
		if !strings.Contains(clause, "JSONExtractString(properties, 'country')") {
			t.Errorf("clause should contain JSONExtractString, got %q", clause)
		}
		if len(args) != 1 || args[0] != "US" {
			t.Errorf("args = %v, expected [US]", args)
		}
	})

	t.Run("single filter with float value", func(t *testing.T) {
		filters := []PropertyFilter{
			{Key: "price", Operator: ComparisonGT, Value: 99.99},
		}
		clause, args := qb.buildPropertyFilters(filters)
		if !strings.Contains(clause, "JSONExtractFloat(properties, 'price')") {
			t.Errorf("clause should contain JSONExtractFloat, got %q", clause)
		}
		if len(args) != 1 || args[0] != 99.99 {
			t.Errorf("args = %v, expected [99.99]", args)
		}
	})

	t.Run("single filter with int value", func(t *testing.T) {
		filters := []PropertyFilter{
			{Key: "quantity", Operator: ComparisonGTE, Value: 5},
		}
		clause, args := qb.buildPropertyFilters(filters)
		if !strings.Contains(clause, "JSONExtractInt(properties, 'quantity')") {
			t.Errorf("clause should contain JSONExtractInt, got %q", clause)
		}
		if len(args) != 1 || args[0] != 5 {
			t.Errorf("args = %v, expected [5]", args)
		}
	})

	t.Run("multiple filters joined with AND", func(t *testing.T) {
		filters := []PropertyFilter{
			{Key: "country", Operator: ComparisonEQ, Value: "US"},
			{Key: "age", Operator: ComparisonGTE, Value: 18},
		}
		clause, args := qb.buildPropertyFilters(filters)
		if !strings.Contains(clause, " AND ") {
			t.Errorf("clause should contain AND, got %q", clause)
		}
		if len(args) != 2 {
			t.Errorf("args length = %d, expected 2", len(args))
		}
	})

	t.Run("filter with invalid operator is skipped", func(t *testing.T) {
		filters := []PropertyFilter{
			{Key: "country", Operator: ComparisonOperator("invalid"), Value: "US"},
		}
		clause, args := qb.buildPropertyFilters(filters)
		if clause != "" {
			t.Errorf("clause = %q, expected empty for invalid operator", clause)
		}
		if len(args) != 0 {
			t.Errorf("args = %v, expected empty", args)
		}
	})
}

func TestBuildQuery(t *testing.T) {
	qb := NewQueryBuilder()

	t.Run("empty conditions returns error", func(t *testing.T) {
		rules := Rules{
			Operator:   OperatorAND,
			Conditions: []Condition{},
		}
		_, _, err := qb.BuildQuery(rules)
		if err == nil {
			t.Error("BuildQuery() expected error for empty conditions")
		}
	})

	t.Run("AND operator uses INTERSECT", func(t *testing.T) {
		rules := Rules{
			Operator: OperatorAND,
			Conditions: []Condition{
				{Type: ConditionTypeEvent, EventName: "purchase"},
				{Type: ConditionTypeEvent, EventName: "signup"},
			},
		}
		query, _, err := qb.BuildQuery(rules)
		if err != nil {
			t.Errorf("BuildQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "INTERSECT") {
			t.Errorf("query should contain INTERSECT for AND, got %q", query)
		}
	})

	t.Run("OR operator uses UNION", func(t *testing.T) {
		rules := Rules{
			Operator: OperatorOR,
			Conditions: []Condition{
				{Type: ConditionTypeEvent, EventName: "purchase"},
				{Type: ConditionTypeEvent, EventName: "signup"},
			},
		}
		query, _, err := qb.BuildQuery(rules)
		if err != nil {
			t.Errorf("BuildQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "UNION") {
			t.Errorf("query should contain UNION for OR, got %q", query)
		}
	})

	t.Run("unsupported condition type returns error", func(t *testing.T) {
		rules := Rules{
			Operator: OperatorAND,
			Conditions: []Condition{
				{Type: ConditionType("unsupported")},
			},
		}
		_, _, err := qb.BuildQuery(rules)
		if err == nil {
			t.Error("BuildQuery() expected error for unsupported condition type")
		}
	})
}

func TestBuildEventConditionQuery(t *testing.T) {
	fixedTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	qb := NewQueryBuilderWithTime(fixedTime)

	t.Run("basic event condition", func(t *testing.T) {
		cond := Condition{
			Type:      ConditionTypeEvent,
			EventName: "purchase",
		}
		query, args, err := qb.buildEventConditionQuery(cond)
		if err != nil {
			t.Errorf("buildEventConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "SELECT DISTINCT user_id FROM events_raw") {
			t.Errorf("query should select distinct user_id from events_raw, got %q", query)
		}
		if !strings.Contains(query, "event_name = ?") {
			t.Errorf("query should filter by event_name, got %q", query)
		}
		if len(args) != 1 || args[0] != "purchase" {
			t.Errorf("args = %v, expected [purchase]", args)
		}
	})

	t.Run("event condition with time window", func(t *testing.T) {
		cond := Condition{
			Type:      ConditionTypeEvent,
			EventName: "purchase",
			TimeWindow: &TimeWindow{
				Type:     TimeWindowSliding,
				Duration: "7d",
			},
		}
		query, args, err := qb.buildEventConditionQuery(cond)
		if err != nil {
			t.Errorf("buildEventConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "timestamp >= ?") {
			t.Errorf("query should have timestamp start filter, got %q", query)
		}
		if !strings.Contains(query, "timestamp <= ?") {
			t.Errorf("query should have timestamp end filter, got %q", query)
		}
		if len(args) != 3 {
			t.Errorf("args length = %d, expected 3", len(args))
		}
	})

	t.Run("event condition with property filters", func(t *testing.T) {
		cond := Condition{
			Type:      ConditionTypeEvent,
			EventName: "purchase",
			PropertyFilters: []PropertyFilter{
				{Key: "amount", Operator: ComparisonGTE, Value: 100.0},
			},
		}
		query, args, err := qb.buildEventConditionQuery(cond)
		if err != nil {
			t.Errorf("buildEventConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "JSONExtractFloat(properties, 'amount')") {
			t.Errorf("query should have property filter, got %q", query)
		}
		if len(args) != 2 {
			t.Errorf("args length = %d, expected 2", len(args))
		}
	})
}

func TestBuildAggregateConditionQuery(t *testing.T) {
	qb := NewQueryBuilder()

	t.Run("count aggregation", func(t *testing.T) {
		cond := Condition{
			Type:        ConditionTypeAggregate,
			EventName:   "purchase",
			Aggregation: AggregationCount,
			Operator:    ComparisonGTE,
			Value:       5,
		}
		query, _, err := qb.buildAggregateConditionQuery(cond)
		if err != nil {
			t.Errorf("buildAggregateConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "count()") {
			t.Errorf("query should contain count(), got %q", query)
		}
		if !strings.Contains(query, "GROUP BY user_id") {
			t.Errorf("query should contain GROUP BY user_id, got %q", query)
		}
		if !strings.Contains(query, "HAVING") {
			t.Errorf("query should contain HAVING, got %q", query)
		}
	})

	t.Run("sum aggregation", func(t *testing.T) {
		cond := Condition{
			Type:             ConditionTypeAggregate,
			EventName:        "purchase",
			Aggregation:      AggregationSum,
			AggregationField: "amount",
			Operator:         ComparisonGTE,
			Value:            1000,
		}
		query, _, err := qb.buildAggregateConditionQuery(cond)
		if err != nil {
			t.Errorf("buildAggregateConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "sum(JSONExtractFloat(properties, 'amount'))") {
			t.Errorf("query should contain sum function, got %q", query)
		}
	})

	t.Run("avg aggregation", func(t *testing.T) {
		cond := Condition{
			Type:             ConditionTypeAggregate,
			EventName:        "purchase",
			Aggregation:      AggregationAvg,
			AggregationField: "amount",
			Operator:         ComparisonGTE,
			Value:            50,
		}
		query, _, err := qb.buildAggregateConditionQuery(cond)
		if err != nil {
			t.Errorf("buildAggregateConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "avg(JSONExtractFloat(properties, 'amount'))") {
			t.Errorf("query should contain avg function, got %q", query)
		}
	})

	t.Run("min aggregation", func(t *testing.T) {
		cond := Condition{
			Type:             ConditionTypeAggregate,
			EventName:        "purchase",
			Aggregation:      AggregationMin,
			AggregationField: "amount",
			Operator:         ComparisonGTE,
			Value:            10,
		}
		query, _, err := qb.buildAggregateConditionQuery(cond)
		if err != nil {
			t.Errorf("buildAggregateConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "min(JSONExtractFloat(properties, 'amount'))") {
			t.Errorf("query should contain min function, got %q", query)
		}
	})

	t.Run("max aggregation", func(t *testing.T) {
		cond := Condition{
			Type:             ConditionTypeAggregate,
			EventName:        "purchase",
			Aggregation:      AggregationMax,
			AggregationField: "amount",
			Operator:         ComparisonLTE,
			Value:            500,
		}
		query, _, err := qb.buildAggregateConditionQuery(cond)
		if err != nil {
			t.Errorf("buildAggregateConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "max(JSONExtractFloat(properties, 'amount'))") {
			t.Errorf("query should contain max function, got %q", query)
		}
	})

	t.Run("distinct_count aggregation", func(t *testing.T) {
		cond := Condition{
			Type:             ConditionTypeAggregate,
			EventName:        "page_view",
			Aggregation:      AggregationDistinctCount,
			AggregationField: "page",
			Operator:         ComparisonGTE,
			Value:            3,
		}
		query, _, err := qb.buildAggregateConditionQuery(cond)
		if err != nil {
			t.Errorf("buildAggregateConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "uniqExact(JSONExtractString(properties, 'page'))") {
			t.Errorf("query should contain uniqExact function, got %q", query)
		}
	})

	t.Run("sum without aggregation_field returns error", func(t *testing.T) {
		cond := Condition{
			Type:        ConditionTypeAggregate,
			EventName:   "purchase",
			Aggregation: AggregationSum,
			Operator:    ComparisonGTE,
			Value:       1000,
		}
		_, _, err := qb.buildAggregateConditionQuery(cond)
		if err == nil {
			t.Error("buildAggregateConditionQuery() expected error for sum without aggregation_field")
		}
	})

	t.Run("unsupported aggregation type returns error", func(t *testing.T) {
		cond := Condition{
			Type:        ConditionTypeAggregate,
			EventName:   "purchase",
			Aggregation: AggregationType("unsupported"),
			Operator:    ComparisonGTE,
			Value:       5,
		}
		_, _, err := qb.buildAggregateConditionQuery(cond)
		if err == nil {
			t.Error("buildAggregateConditionQuery() expected error for unsupported aggregation")
		}
	})
}

func TestBuildConditionQuery(t *testing.T) {
	qb := NewQueryBuilder()

	t.Run("event condition type", func(t *testing.T) {
		cond := Condition{
			Type:      ConditionTypeEvent,
			EventName: "test_event",
		}
		query, args, err := qb.buildConditionQuery(cond)
		if err != nil {
			t.Errorf("buildConditionQuery() unexpected error: %v", err)
		}
		if query == "" || len(args) == 0 {
			t.Error("buildConditionQuery() should return non-empty query and args")
		}
	})

	t.Run("aggregate condition type", func(t *testing.T) {
		cond := Condition{
			Type:        ConditionTypeAggregate,
			EventName:   "test_event",
			Aggregation: AggregationCount,
			Operator:    ComparisonGTE,
			Value:       5,
		}
		query, _, err := qb.buildConditionQuery(cond)
		if err != nil {
			t.Errorf("buildConditionQuery() unexpected error: %v", err)
		}
		if query == "" {
			t.Error("buildConditionQuery() should return non-empty query")
		}
	})

	t.Run("property condition type", func(t *testing.T) {
		cond := Condition{
			Type:         ConditionTypeProperty,
			PropertyName: "test_prop",
			Operator:     ComparisonEQ,
			Value:        "value",
		}
		query, _, err := qb.buildConditionQuery(cond)
		if err != nil {
			t.Errorf("buildConditionQuery() unexpected error: %v", err)
		}
		if query == "" {
			t.Error("buildConditionQuery() should return non-empty query")
		}
	})

	t.Run("unsupported condition type", func(t *testing.T) {
		cond := Condition{
			Type: ConditionType("unsupported"),
		}
		_, _, err := qb.buildConditionQuery(cond)
		if err == nil {
			t.Error("buildConditionQuery() expected error for unsupported type")
		}
	})
}

func TestBuildPropertyConditionQuery(t *testing.T) {
	qb := NewQueryBuilder()

	t.Run("string property value", func(t *testing.T) {
		cond := Condition{
			Type:         ConditionTypeProperty,
			PropertyName: "country",
			Operator:     ComparisonEQ,
			Value:        "US",
		}
		query, args, err := qb.buildPropertyConditionQuery(cond)
		if err != nil {
			t.Errorf("buildPropertyConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "JSONExtractString(properties, 'country')") {
			t.Errorf("query should contain JSONExtractString, got %q", query)
		}
		if len(args) < 1 || args[0] != "US" {
			t.Errorf("first arg should be US, got %v", args)
		}
	})

	t.Run("float property value", func(t *testing.T) {
		cond := Condition{
			Type:         ConditionTypeProperty,
			PropertyName: "score",
			Operator:     ComparisonGTE,
			Value:        75.5,
		}
		query, args, err := qb.buildPropertyConditionQuery(cond)
		if err != nil {
			t.Errorf("buildPropertyConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "JSONExtractFloat(properties, 'score')") {
			t.Errorf("query should contain JSONExtractFloat, got %q", query)
		}
		if len(args) < 1 || args[0] != 75.5 {
			t.Errorf("first arg should be 75.5, got %v", args)
		}
	})

	t.Run("int property value", func(t *testing.T) {
		cond := Condition{
			Type:         ConditionTypeProperty,
			PropertyName: "age",
			Operator:     ComparisonGTE,
			Value:        int64(18),
		}
		query, args, err := qb.buildPropertyConditionQuery(cond)
		if err != nil {
			t.Errorf("buildPropertyConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "JSONExtractInt(properties, 'age')") {
			t.Errorf("query should contain JSONExtractInt, got %q", query)
		}
		if len(args) < 1 {
			t.Errorf("expected at least 1 arg, got %v", args)
		}
	})

	t.Run("property condition with event filter", func(t *testing.T) {
		cond := Condition{
			Type:         ConditionTypeProperty,
			PropertyName: "plan",
			EventName:    "subscription",
			Operator:     ComparisonEQ,
			Value:        "premium",
		}
		query, args, err := qb.buildPropertyConditionQuery(cond)
		if err != nil {
			t.Errorf("buildPropertyConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "event_name = ?") {
			t.Errorf("query should filter by event_name, got %q", query)
		}
		if len(args) != 2 {
			t.Errorf("args length = %d, expected 2", len(args))
		}
	})

	t.Run("property condition with time window", func(t *testing.T) {
		fixedTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
		qb := NewQueryBuilderWithTime(fixedTime)

		cond := Condition{
			Type:         ConditionTypeProperty,
			PropertyName: "tier",
			Operator:     ComparisonEQ,
			Value:        "gold",
			TimeWindow: &TimeWindow{
				Type:     TimeWindowSliding,
				Duration: "30d",
			},
		}
		query, args, err := qb.buildPropertyConditionQuery(cond)
		if err != nil {
			t.Errorf("buildPropertyConditionQuery() unexpected error: %v", err)
		}
		if !strings.Contains(query, "timestamp >= ?") {
			t.Errorf("query should have timestamp start filter, got %q", query)
		}
		if !strings.Contains(query, "timestamp <= ?") {
			t.Errorf("query should have timestamp end filter, got %q", query)
		}
		if len(args) != 3 {
			t.Errorf("args length = %d, expected 3", len(args))
		}
	})
}
