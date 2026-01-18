import { api } from './client';
import type { Member, UserCohort, PaginatedResponse } from './types';

// API response format from backend
interface MembersApiResponse {
	cohort_id: string;
	members: Member[];
	total: number;
	limit: number;
	offset: number;
}

export async function getCohortMembers(
	cohortId: string,
	page: number = 1,
	pageSize: number = 50
): Promise<PaginatedResponse<Member>> {
	const offset = (page - 1) * pageSize;
	const params = new URLSearchParams({
		limit: pageSize.toString(),
		offset: offset.toString()
	});
	const response = await api<MembersApiResponse>(`/api/v1/cohorts/${cohortId}/members?${params}`);

	// Transform backend response to frontend format
	return {
		data: response.members || [],
		total: response.total,
		page: page,
		page_size: pageSize
	};
}

export async function getUserCohorts(userId: string): Promise<UserCohort[]> {
	return api<UserCohort[]>(`/api/v1/users/${userId}/cohorts`);
}

export async function checkUserMembership(
	cohortId: string,
	userId: string
): Promise<{ is_member: boolean }> {
	return api<{ is_member: boolean }>(`/api/v1/cohorts/${cohortId}/members/${userId}`);
}
