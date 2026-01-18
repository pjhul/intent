import { api } from './client';
import type { Organization, CreateOrganizationRequest, UpdateOrganizationRequest } from './types';

const BASE_PATH = '/api/v1/organizations';

interface ListOrganizationsResponse {
	organizations: Organization[];
	limit: number;
	offset: number;
}

export async function listOrganizations(): Promise<Organization[]> {
	const response = await api<ListOrganizationsResponse>(BASE_PATH);
	return response.organizations;
}

export async function getOrganization(slug: string): Promise<Organization> {
	return api<Organization>(`${BASE_PATH}/${slug}`);
}

export async function createOrganization(data: CreateOrganizationRequest): Promise<Organization> {
	return api<Organization>(BASE_PATH, {
		method: 'POST',
		body: JSON.stringify(data)
	});
}

export async function updateOrganization(slug: string, data: UpdateOrganizationRequest): Promise<Organization> {
	return api<Organization>(`${BASE_PATH}/${slug}`, {
		method: 'PUT',
		body: JSON.stringify(data)
	});
}

export async function deleteOrganization(slug: string): Promise<void> {
	return api<void>(`${BASE_PATH}/${slug}`, {
		method: 'DELETE'
	});
}
