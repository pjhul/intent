import { writable, derived } from 'svelte/store';
import type { Cohort, CohortStatus } from '$lib/api/types';
import { listCohorts } from '$lib/api/cohorts';

function createCohortsStore() {
	const { subscribe, set, update } = writable<Cohort[]>([]);

	return {
		subscribe,
		set,
		async load() {
			try {
				const cohorts = await listCohorts();
				set(cohorts);
				return cohorts;
			} catch (error) {
				console.error('Failed to load cohorts:', error);
				throw error;
			}
		},
		add(cohort: Cohort) {
			update((cohorts) => [...cohorts, cohort]);
		},
		updateCohort(id: string, updatedCohort: Cohort) {
			update((cohorts) => cohorts.map((c) => (c.id === id ? updatedCohort : c)));
		},
		remove(id: string) {
			update((cohorts) => cohorts.filter((c) => c.id !== id));
		},
		clear() {
			set([]);
		}
	};
}

export const cohorts = createCohortsStore();

export const cohortsByStatus = derived(cohorts, ($cohorts) => {
	const grouped: Record<CohortStatus, Cohort[]> = {
		active: [],
		inactive: [],
		draft: []
	};

	const list = Array.isArray($cohorts) ? $cohorts : [];
	for (const cohort of list) {
		if (cohort?.status && grouped[cohort.status]) {
			grouped[cohort.status].push(cohort);
		}
	}

	return grouped;
});

export const cohortCount = derived(cohorts, ($cohorts) =>
	Array.isArray($cohorts) ? $cohorts.length : 0
);
