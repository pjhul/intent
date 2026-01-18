import { writable, get } from 'svelte/store';
import type { MembershipChange } from '$lib/api/types';
import { getApiBase } from '$lib/api/client';

export const membershipChanges = writable<MembershipChange[]>([]);
export const isConnected = writable(false);

let eventSource: EventSource | null = null;

export function connectSSE(cohortIds?: string[]): () => void {
	if (eventSource) {
		eventSource.close();
	}

	const params = cohortIds?.map((id) => `cohort_id=${id}`).join('&') || '';
	const url = `${getApiBase()}/api/v1/stream/cohort-changes${params ? `?${params}` : ''}`;

	eventSource = new EventSource(url);

	eventSource.onopen = () => {
		isConnected.set(true);
	};

	eventSource.onerror = () => {
		isConnected.set(false);
	};

	eventSource.addEventListener('membership_change', (e) => {
		try {
			const change = JSON.parse(e.data) as MembershipChange;
			membershipChanges.update((list) => [change, ...list].slice(0, 100));
		} catch (error) {
			console.error('Failed to parse SSE message:', error);
		}
	});

	return () => {
		if (eventSource) {
			eventSource.close();
			eventSource = null;
			isConnected.set(false);
		}
	};
}

export function disconnectSSE(): void {
	if (eventSource) {
		eventSource.close();
		eventSource = null;
		isConnected.set(false);
	}
}

export function clearChanges(): void {
	membershipChanges.set([]);
}
