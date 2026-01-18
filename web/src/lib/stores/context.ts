import { writable, derived } from 'svelte/store';
import type { Organization, Project } from '$lib/api/types';

// Current organization store
function createOrganizationStore() {
	const { subscribe, set, update } = writable<Organization | null>(null);

	return {
		subscribe,
		set,
		update,
		clear() {
			set(null);
		}
	};
}

// Current project store
function createProjectStore() {
	const { subscribe, set, update } = writable<Project | null>(null);

	return {
		subscribe,
		set,
		update,
		clear() {
			set(null);
		}
	};
}

export const currentOrganization = createOrganizationStore();
export const currentProject = createProjectStore();

// Derived store for checking if we have both org and project context
export const hasContext = derived(
	[currentOrganization, currentProject],
	([$org, $project]) => $org !== null && $project !== null
);

// Derived store for the current context slugs
export const contextSlugs = derived(
	[currentOrganization, currentProject],
	([$org, $project]) => ({
		orgSlug: $org?.slug ?? null,
		projectSlug: $project?.slug ?? null
	})
);

// Helper to clear all context
export function clearContext() {
	currentOrganization.clear();
	currentProject.clear();
}
