// TypeScript types matching Go entities

export type CohortStatus = 'active' | 'inactive' | 'draft';

export interface Cohort {
	id: string;
	name: string;
	description?: string;
	rules: Rules;
	status: CohortStatus;
	version: number;
	created_at: string;
	updated_at: string;
}

export interface Rules {
	operator: 'AND' | 'OR';
	conditions: Condition[];
}

export type ConditionType = 'event' | 'property' | 'aggregate';
export type AggregationType = 'count' | 'sum' | 'avg' | 'min' | 'max' | 'distinct_count';
export type ComparisonOperator = 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'nin';

export interface Condition {
	type: ConditionType;
	event_name?: string;
	property_name?: string;
	aggregation?: AggregationType;
	aggregation_field?: string;
	time_window?: TimeWindow;
	operator?: ComparisonOperator;
	value?: unknown;
	property_filters?: PropertyFilter[];
}

export type TimeWindowType = 'sliding' | 'absolute';

export interface TimeWindow {
	type: TimeWindowType;
	duration?: string;
	start?: string;
	end?: string;
}

export interface PropertyFilter {
	property: string;
	operator: ComparisonOperator;
	value: unknown;
}

export type MembershipStatus = -1 | 1;

export interface Member {
	user_id: string;
	joined_at: string;
}

export interface MembershipChange {
	cohort_id: string;
	cohort_name: string;
	user_id: string;
	prev_status: MembershipStatus;
	new_status: MembershipStatus;
	changed_at: string;
}

export interface UserCohort {
	cohort_id: string;
	cohort_name: string;
	joined_at: string;
}

export interface PaginatedResponse<T> {
	data: T[];
	total: number;
	page: number;
	page_size: number;
}

export interface CreateCohortRequest {
	name: string;
	description?: string;
	rules: Rules;
	status?: CohortStatus;
}

export interface UpdateCohortRequest {
	name?: string;
	description?: string;
	rules?: Rules;
	status?: CohortStatus;
}

export interface CohortStats {
	member_count: number;
	last_computed_at?: string;
}
