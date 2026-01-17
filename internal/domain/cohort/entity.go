package cohort

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// TimeWindowType defines how time windows are calculated
type TimeWindowType string

const (
	TimeWindowSliding  TimeWindowType = "sliding"
	TimeWindowAbsolute TimeWindowType = "absolute"
)

// ConditionType defines the type of cohort condition
type ConditionType string

const (
	ConditionTypeEvent     ConditionType = "event"
	ConditionTypeProperty  ConditionType = "property"
	ConditionTypeAggregate ConditionType = "aggregate"
)

// AggregationType defines the type of aggregation for aggregate conditions
type AggregationType string

const (
	AggregationCount         AggregationType = "count"
	AggregationSum           AggregationType = "sum"
	AggregationAvg           AggregationType = "avg"
	AggregationMin           AggregationType = "min"
	AggregationMax           AggregationType = "max"
	AggregationDistinctCount AggregationType = "distinct_count"
)

// Operator defines logical operators for combining conditions
type Operator string

const (
	OperatorAND Operator = "AND"
	OperatorOR  Operator = "OR"
)

// ComparisonOperator defines comparison operators for conditions
type ComparisonOperator string

const (
	ComparisonEQ  ComparisonOperator = "eq"
	ComparisonNE  ComparisonOperator = "ne"
	ComparisonGT  ComparisonOperator = "gt"
	ComparisonGTE ComparisonOperator = "gte"
	ComparisonLT  ComparisonOperator = "lt"
	ComparisonLTE ComparisonOperator = "lte"
	ComparisonIN  ComparisonOperator = "in"
	ComparisonNIN ComparisonOperator = "nin"
)

// TimeWindow defines a time-based constraint for conditions
type TimeWindow struct {
	Type     TimeWindowType `json:"type"`
	Duration string         `json:"duration,omitempty"` // e.g., "30d", "7d", "24h"
	Start    *time.Time     `json:"start,omitempty"`
	End      *time.Time     `json:"end,omitempty"`
}

// PropertyFilter allows filtering events by property values
type PropertyFilter struct {
	Key      string             `json:"key"`
	Operator ComparisonOperator `json:"operator"`
	Value    interface{}        `json:"value"`
}

// Condition represents a single cohort membership condition
type Condition struct {
	Type             ConditionType      `json:"type"`
	EventName        string             `json:"event_name,omitempty"`
	PropertyName     string             `json:"property_name,omitempty"`
	Aggregation      AggregationType    `json:"aggregation,omitempty"`
	AggregationField string             `json:"aggregation_field,omitempty"`
	TimeWindow       *TimeWindow        `json:"time_window,omitempty"`
	Operator         ComparisonOperator `json:"operator,omitempty"`
	Value            interface{}        `json:"value,omitempty"`
	PropertyFilters  []PropertyFilter   `json:"property_filters,omitempty"`
}

// Rules defines the cohort membership rules
type Rules struct {
	Operator   Operator    `json:"operator"`
	Conditions []Condition `json:"conditions"`
}

// CohortStatus represents the current status of a cohort
type CohortStatus string

const (
	CohortStatusActive   CohortStatus = "active"
	CohortStatusInactive CohortStatus = "inactive"
	CohortStatusDraft    CohortStatus = "draft"
)

// Cohort represents a cohort definition
type Cohort struct {
	ID          uuid.UUID    `json:"id"`
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Rules       Rules        `json:"rules"`
	Status      CohortStatus `json:"status"`
	Version     int64        `json:"version"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
}

// NewCohort creates a new cohort with the given name and rules
func NewCohort(name, description string, rules Rules) *Cohort {
	now := time.Now().UTC()
	return &Cohort{
		ID:          uuid.New(),
		Name:        name,
		Description: description,
		Rules:       rules,
		Status:      CohortStatusDraft,
		Version:     1,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// Activate sets the cohort status to active
func (c *Cohort) Activate() {
	c.Status = CohortStatusActive
	c.UpdatedAt = time.Now().UTC()
}

// Deactivate sets the cohort status to inactive
func (c *Cohort) Deactivate() {
	c.Status = CohortStatusInactive
	c.UpdatedAt = time.Now().UTC()
}

// Update updates the cohort rules and increments the version
func (c *Cohort) Update(name, description string, rules Rules) {
	c.Name = name
	c.Description = description
	c.Rules = rules
	c.Version++
	c.UpdatedAt = time.Now().UTC()
}

// ToJSON serializes the cohort to JSON
func (c *Cohort) ToJSON() ([]byte, error) {
	return json.Marshal(c)
}

// CohortFromJSON deserializes a cohort from JSON
func CohortFromJSON(data []byte) (*Cohort, error) {
	var cohort Cohort
	if err := json.Unmarshal(data, &cohort); err != nil {
		return nil, err
	}
	return &cohort, nil
}

// CreateCohortRequest represents the request to create a new cohort
type CreateCohortRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	Rules       Rules  `json:"rules" binding:"required"`
}

// UpdateCohortRequest represents the request to update an existing cohort
type UpdateCohortRequest struct {
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Rules       *Rules       `json:"rules"`
	Status      CohortStatus `json:"status"`
}

// CheckMembershipRequest represents the request to check if a user is in a cohort
type CheckMembershipRequest struct {
	UserID string `json:"user_id" binding:"required"`
}

// CheckMembershipResponse represents the response for membership check
type CheckMembershipResponse struct {
	UserID   string    `json:"user_id"`
	CohortID uuid.UUID `json:"cohort_id"`
	IsMember bool      `json:"is_member"`
	JoinedAt *time.Time `json:"joined_at,omitempty"`
}

// CohortStats represents statistics for a cohort
type CohortStats struct {
	CohortID    uuid.UUID `json:"cohort_id"`
	MemberCount int64     `json:"member_count"`
	LastUpdated time.Time `json:"last_updated"`
}
