import { api } from './client';
import type { Cohort, CreateCohortRequest, UpdateCohortRequest, CohortStats, RecomputeResponse, RecomputeJob } from './types';

function getBasePath(orgSlug: string, projectSlug: string): string {
	return `/api/v1/organizations/${orgSlug}/projects/${projectSlug}/cohorts`;
}

interface ListCohortsResponse {
	cohorts: Cohort[];
	limit: number;
	offset: number;
}

export async function listCohorts(orgSlug: string, projectSlug: string): Promise<Cohort[]> {
	const response = await api<ListCohortsResponse>(getBasePath(orgSlug, projectSlug));
	return response.cohorts;
}

export async function getCohort(orgSlug: string, projectSlug: string, id: string): Promise<Cohort> {
	return api<Cohort>(`${getBasePath(orgSlug, projectSlug)}/${id}`);
}

export async function createCohort(orgSlug: string, projectSlug: string, data: CreateCohortRequest): Promise<Cohort> {
	return api<Cohort>(getBasePath(orgSlug, projectSlug), {
		method: 'POST',
		body: JSON.stringify(data)
	});
}

export async function updateCohort(orgSlug: string, projectSlug: string, id: string, data: UpdateCohortRequest): Promise<Cohort> {
	return api<Cohort>(`${getBasePath(orgSlug, projectSlug)}/${id}`, {
		method: 'PUT',
		body: JSON.stringify(data)
	});
}

export async function deleteCohort(orgSlug: string, projectSlug: string, id: string): Promise<void> {
	return api<void>(`${getBasePath(orgSlug, projectSlug)}/${id}`, {
		method: 'DELETE'
	});
}

export async function activateCohort(orgSlug: string, projectSlug: string, id: string): Promise<Cohort> {
	return api<Cohort>(`${getBasePath(orgSlug, projectSlug)}/${id}/activate`, {
		method: 'POST'
	});
}

export async function deactivateCohort(orgSlug: string, projectSlug: string, id: string): Promise<Cohort> {
	return api<Cohort>(`${getBasePath(orgSlug, projectSlug)}/${id}/deactivate`, {
		method: 'POST'
	});
}

export async function getCohortStats(orgSlug: string, projectSlug: string, id: string): Promise<CohortStats> {
	return api<CohortStats>(`${getBasePath(orgSlug, projectSlug)}/${id}/stats`);
}

export async function recomputeCohort(orgSlug: string, projectSlug: string, id: string, force?: boolean): Promise<RecomputeResponse> {
	return api<RecomputeResponse>(`${getBasePath(orgSlug, projectSlug)}/${id}/recompute`, {
		method: 'POST',
		body: JSON.stringify({ force: force ?? false })
	});
}

export async function getRecomputeStatus(orgSlug: string, projectSlug: string, cohortId: string, jobId: string): Promise<RecomputeJob> {
	return api<RecomputeJob>(`${getBasePath(orgSlug, projectSlug)}/${cohortId}/recompute/${jobId}`);
}
