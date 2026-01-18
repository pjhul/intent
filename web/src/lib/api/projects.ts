import { api } from './client';
import type { Project, CreateProjectRequest, UpdateProjectRequest } from './types';

function getBasePath(orgSlug: string): string {
	return `/api/v1/organizations/${orgSlug}/projects`;
}

interface ListProjectsResponse {
	projects: Project[];
	limit: number;
	offset: number;
}

export async function listProjects(orgSlug: string): Promise<Project[]> {
	const response = await api<ListProjectsResponse>(getBasePath(orgSlug));
	return response.projects;
}

export async function getProject(orgSlug: string, projectSlug: string): Promise<Project> {
	return api<Project>(`${getBasePath(orgSlug)}/${projectSlug}`);
}

export async function createProject(orgSlug: string, data: CreateProjectRequest): Promise<Project> {
	return api<Project>(getBasePath(orgSlug), {
		method: 'POST',
		body: JSON.stringify(data)
	});
}

export async function updateProject(orgSlug: string, projectSlug: string, data: UpdateProjectRequest): Promise<Project> {
	return api<Project>(`${getBasePath(orgSlug)}/${projectSlug}`, {
		method: 'PUT',
		body: JSON.stringify(data)
	});
}

export async function deleteProject(orgSlug: string, projectSlug: string): Promise<void> {
	return api<void>(`${getBasePath(orgSlug)}/${projectSlug}`, {
		method: 'DELETE'
	});
}
