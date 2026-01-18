import { api } from './client';
import type { Cohort, CreateCohortRequest, UpdateCohortRequest, CohortStats } from './types';

const BASE_PATH = '/api/v1/cohorts';

interface ListCohortsResponse {
	cohorts: Cohort[];
	limit: number;
	offset: number;
}

export async function listCohorts(): Promise<Cohort[]> {
	const response = await api<ListCohortsResponse>(BASE_PATH);
	return response.cohorts;
}

export async function getCohort(id: string): Promise<Cohort> {
	return api<Cohort>(`${BASE_PATH}/${id}`);
}

export async function createCohort(data: CreateCohortRequest): Promise<Cohort> {
	return api<Cohort>(BASE_PATH, {
		method: 'POST',
		body: JSON.stringify(data)
	});
}

export async function updateCohort(id: string, data: UpdateCohortRequest): Promise<Cohort> {
	return api<Cohort>(`${BASE_PATH}/${id}`, {
		method: 'PUT',
		body: JSON.stringify(data)
	});
}

export async function deleteCohort(id: string): Promise<void> {
	return api<void>(`${BASE_PATH}/${id}`, {
		method: 'DELETE'
	});
}

export async function activateCohort(id: string): Promise<Cohort> {
	return api<Cohort>(`${BASE_PATH}/${id}/activate`, {
		method: 'POST'
	});
}

export async function deactivateCohort(id: string): Promise<Cohort> {
	return api<Cohort>(`${BASE_PATH}/${id}/deactivate`, {
		method: 'POST'
	});
}

export async function getCohortStats(id: string): Promise<CohortStats> {
	return api<CohortStats>(`${BASE_PATH}/${id}/stats`);
}
