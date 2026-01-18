package cohort

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// QueryBuilder translates cohort rules into ClickHouse SQL queries
type QueryBuilder struct {
	now time.Time
}

// NewQueryBuilder creates a new query builder
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		now: time.Now().UTC(),
	}
}

// NewQueryBuilderWithTime creates a new query builder with a specific reference time
func NewQueryBuilderWithTime(now time.Time) *QueryBuilder {
	return &QueryBuilder{
		now: now,
	}
}

// BuildQuery generates a ClickHouse SQL query that returns user_ids matching the cohort rules
func (qb *QueryBuilder) BuildQuery(rules Rules) (string, []any, error) {
	if len(rules.Conditions) == 0 {
		return "", nil, fmt.Errorf("cohort has no conditions")
	}

	var subqueries []string
	var allArgs []any

	for _, cond := range rules.Conditions {
		subquery, args, err := qb.buildConditionQuery(cond)
		if err != nil {
			return "", nil, fmt.Errorf("failed to build condition query: %w", err)
		}
		subqueries = append(subqueries, subquery)
		allArgs = append(allArgs, args...)
	}

	// Combine subqueries based on operator
	var combiner string
	if rules.Operator == OperatorAND {
		combiner = " INTERSECT "
	} else {
		combiner = " UNION "
	}

	finalQuery := strings.Join(subqueries, combiner)
	return finalQuery, allArgs, nil
}

// buildConditionQuery generates a subquery for a single condition
func (qb *QueryBuilder) buildConditionQuery(cond Condition) (string, []any, error) {
	switch cond.Type {
	case ConditionTypeEvent:
		return qb.buildEventConditionQuery(cond)
	case ConditionTypeAggregate:
		return qb.buildAggregateConditionQuery(cond)
	case ConditionTypeProperty:
		return qb.buildPropertyConditionQuery(cond)
	default:
		return "", nil, fmt.Errorf("unsupported condition type: %s", cond.Type)
	}
}

// buildEventConditionQuery generates a query for event-based conditions
func (qb *QueryBuilder) buildEventConditionQuery(cond Condition) (string, []any, error) {
	startTime, endTime, err := qb.resolveTimeWindow(cond.TimeWindow)
	if err != nil {
		return "", nil, err
	}

	query := `SELECT DISTINCT user_id FROM events_raw WHERE event_name = ?`
	args := []any{cond.EventName}

	if startTime != nil {
		query += ` AND timestamp >= ?`
		args = append(args, *startTime)
	}
	if endTime != nil {
		query += ` AND timestamp <= ?`
		args = append(args, *endTime)
	}

	// Add property filters
	filterClause, filterArgs := qb.buildPropertyFilters(cond.PropertyFilters)
	if filterClause != "" {
		query += " AND " + filterClause
		args = append(args, filterArgs...)
	}

	return query, args, nil
}

// buildAggregateConditionQuery generates a query for aggregate-based conditions
func (qb *QueryBuilder) buildAggregateConditionQuery(cond Condition) (string, []any, error) {
	startTime, endTime, err := qb.resolveTimeWindow(cond.TimeWindow)
	if err != nil {
		return "", nil, err
	}

	// Build the aggregation function
	var aggFunc string
	switch cond.Aggregation {
	case AggregationCount:
		aggFunc = "count()"
	case AggregationSum:
		if cond.AggregationField == "" {
			return "", nil, fmt.Errorf("aggregation_field required for sum")
		}
		aggFunc = fmt.Sprintf("sum(JSONExtractFloat(properties, '%s'))", cond.AggregationField)
	case AggregationAvg:
		if cond.AggregationField == "" {
			return "", nil, fmt.Errorf("aggregation_field required for avg")
		}
		aggFunc = fmt.Sprintf("avg(JSONExtractFloat(properties, '%s'))", cond.AggregationField)
	case AggregationMin:
		if cond.AggregationField == "" {
			return "", nil, fmt.Errorf("aggregation_field required for min")
		}
		aggFunc = fmt.Sprintf("min(JSONExtractFloat(properties, '%s'))", cond.AggregationField)
	case AggregationMax:
		if cond.AggregationField == "" {
			return "", nil, fmt.Errorf("aggregation_field required for max")
		}
		aggFunc = fmt.Sprintf("max(JSONExtractFloat(properties, '%s'))", cond.AggregationField)
	case AggregationDistinctCount:
		if cond.AggregationField == "" {
			return "", nil, fmt.Errorf("aggregation_field required for distinct_count")
		}
		aggFunc = fmt.Sprintf("uniqExact(JSONExtractString(properties, '%s'))", cond.AggregationField)
	default:
		return "", nil, fmt.Errorf("unsupported aggregation type: %s", cond.Aggregation)
	}

	// Build the comparison operator
	compOp, err := qb.getComparisonOperator(cond.Operator)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf(`SELECT user_id FROM events_raw WHERE event_name = ?`)
	args := []any{cond.EventName}

	if startTime != nil {
		query += ` AND timestamp >= ?`
		args = append(args, *startTime)
	}
	if endTime != nil {
		query += ` AND timestamp <= ?`
		args = append(args, *endTime)
	}

	// Add property filters
	filterClause, filterArgs := qb.buildPropertyFilters(cond.PropertyFilters)
	if filterClause != "" {
		query += " AND " + filterClause
		args = append(args, filterArgs...)
	}

	// Add GROUP BY and HAVING
	query += fmt.Sprintf(` GROUP BY user_id HAVING %s %s ?`, aggFunc, compOp)
	args = append(args, cond.Value)

	return query, args, nil
}

// buildPropertyConditionQuery generates a query for property-based conditions
func (qb *QueryBuilder) buildPropertyConditionQuery(cond Condition) (string, []any, error) {
	startTime, endTime, err := qb.resolveTimeWindow(cond.TimeWindow)
	if err != nil {
		return "", nil, err
	}

	compOp, err := qb.getComparisonOperator(cond.Operator)
	if err != nil {
		return "", nil, err
	}

	// For property conditions, we check if the user has any event with the matching property
	var valueExtractor string
	switch v := cond.Value.(type) {
	case float64:
		valueExtractor = fmt.Sprintf("JSONExtractFloat(properties, '%s')", cond.PropertyName)
	case int, int64:
		valueExtractor = fmt.Sprintf("JSONExtractInt(properties, '%s')", cond.PropertyName)
	case string:
		valueExtractor = fmt.Sprintf("JSONExtractString(properties, '%s')", cond.PropertyName)
	default:
		_ = v
		valueExtractor = fmt.Sprintf("JSONExtractString(properties, '%s')", cond.PropertyName)
	}

	query := fmt.Sprintf(`SELECT DISTINCT user_id FROM events_raw WHERE %s %s ?`, valueExtractor, compOp)
	args := []any{cond.Value}

	if cond.EventName != "" {
		query += ` AND event_name = ?`
		args = append(args, cond.EventName)
	}

	if startTime != nil {
		query += ` AND timestamp >= ?`
		args = append(args, *startTime)
	}
	if endTime != nil {
		query += ` AND timestamp <= ?`
		args = append(args, *endTime)
	}

	return query, args, nil
}

// buildPropertyFilters generates WHERE clause conditions for property filters
func (qb *QueryBuilder) buildPropertyFilters(filters []PropertyFilter) (string, []any) {
	if len(filters) == 0 {
		return "", nil
	}

	var clauses []string
	var args []any

	for _, f := range filters {
		compOp, err := qb.getComparisonOperator(f.Operator)
		if err != nil {
			continue
		}

		var valueExtractor string
		switch v := f.Value.(type) {
		case float64:
			valueExtractor = fmt.Sprintf("JSONExtractFloat(properties, '%s')", f.Key)
		case int, int64:
			valueExtractor = fmt.Sprintf("JSONExtractInt(properties, '%s')", f.Key)
		default:
			_ = v
			valueExtractor = fmt.Sprintf("JSONExtractString(properties, '%s')", f.Key)
		}

		clauses = append(clauses, fmt.Sprintf("%s %s ?", valueExtractor, compOp))
		args = append(args, f.Value)
	}

	if len(clauses) == 0 {
		return "", nil
	}

	return strings.Join(clauses, " AND "), args
}

// resolveTimeWindow calculates the actual start and end times from a time window
func (qb *QueryBuilder) resolveTimeWindow(tw *TimeWindow) (*time.Time, *time.Time, error) {
	if tw == nil {
		return nil, nil, nil
	}

	switch tw.Type {
	case TimeWindowSliding:
		if tw.Duration == "" {
			return nil, nil, fmt.Errorf("sliding time window requires duration")
		}
		duration, err := parseDuration(tw.Duration)
		if err != nil {
			return nil, nil, err
		}
		startTime := qb.now.Add(-duration)
		endTime := qb.now
		return &startTime, &endTime, nil

	case TimeWindowAbsolute:
		return tw.Start, tw.End, nil

	default:
		return nil, nil, fmt.Errorf("unsupported time window type: %s", tw.Type)
	}
}

// getComparisonOperator converts our operator type to SQL operator
func (qb *QueryBuilder) getComparisonOperator(op ComparisonOperator) (string, error) {
	switch op {
	case ComparisonEQ:
		return "=", nil
	case ComparisonNE:
		return "!=", nil
	case ComparisonGT:
		return ">", nil
	case ComparisonGTE:
		return ">=", nil
	case ComparisonLT:
		return "<", nil
	case ComparisonLTE:
		return "<=", nil
	case ComparisonIN:
		return "IN", nil
	case ComparisonNIN:
		return "NOT IN", nil
	default:
		return "", fmt.Errorf("unsupported comparison operator: %s", op)
	}
}

// parseDuration parses duration strings like "30d", "7d", "24h", "1w"
func parseDuration(s string) (time.Duration, error) {
	// Regex to match number followed by unit
	re := regexp.MustCompile(`^(\d+)([dhwmM])$`)
	matches := re.FindStringSubmatch(s)
	if matches == nil {
		// Try standard Go duration parsing
		return time.ParseDuration(s)
	}

	value, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, err
	}

	unit := matches[2]
	switch unit {
	case "d":
		return time.Duration(value) * 24 * time.Hour, nil
	case "h":
		return time.Duration(value) * time.Hour, nil
	case "w":
		return time.Duration(value) * 7 * 24 * time.Hour, nil
	case "m":
		return time.Duration(value) * time.Minute, nil
	case "M":
		// Approximate month as 30 days
		return time.Duration(value) * 30 * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported duration unit: %s", unit)
	}
}
