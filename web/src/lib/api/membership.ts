import { api } from './client';
import type { Member, UserCohort, PaginatedResponse } from './types';

function getCohortBasePath(orgSlug: string, projectSlug: string): string {
	return `/api/v1/organizations/${orgSlug}/projects/${projectSlug}/cohorts`;
}

function getUserBasePath(orgSlug: string, projectSlug: string): string {
	return `/api/v1/organizations/${orgSlug}/projects/${projectSlug}/users`;
}

// API response format from backend
interface MembersApiResponse {
	cohort_id: string;
	members: Member[];
	total: number;
	limit: number;
	offset: number;
}

export async function getCohortMembers(
	orgSlug: string,
	projectSlug: string,
	cohortId: string,
	page: number = 1,
	pageSize: number = 50
): Promise<PaginatedResponse<Member>> {
	const offset = (page - 1) * pageSize;
	const params = new URLSearchParams({
		limit: pageSize.toString(),
		offset: offset.toString()
	});
	const response = await api<MembersApiResponse>(`${getCohortBasePath(orgSlug, projectSlug)}/${cohortId}/members?${params}`);

	// Transform backend response to frontend format
	return {
		data: response.members || [],
		total: response.total,
		page: page,
		page_size: pageSize
	};
}

export async function getUserCohorts(orgSlug: string, projectSlug: string, userId: string): Promise<UserCohort[]> {
	return api<UserCohort[]>(`${getUserBasePath(orgSlug, projectSlug)}/${userId}/cohorts`);
}

export async function checkUserMembership(
	orgSlug: string,
	projectSlug: string,
	cohortId: string,
	userId: string
): Promise<{ is_member: boolean }> {
	return api<{ is_member: boolean }>(`${getCohortBasePath(orgSlug, projectSlug)}/${cohortId}/check`, {
		method: 'POST',
		body: JSON.stringify({ user_id: userId })
	});
}
