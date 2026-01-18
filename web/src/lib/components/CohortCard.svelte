<script lang="ts">
	import type { Cohort } from '$lib/api/types';
	import StatusBadge from './StatusBadge.svelte';
	import { formatDistanceToNow } from 'date-fns';

	export let cohort: Cohort;
	export let memberCount: number | undefined = undefined;

	$: updatedAgo = formatDistanceToNow(new Date(cohort.updated_at), { addSuffix: true });
</script>

<a
	href="/cohorts/{cohort.id}"
	class="card p-4 hover:border-blue-300 hover:shadow-md transition-all block"
>
	<div class="flex items-start justify-between gap-4">
		<div class="flex-1 min-w-0">
			<div class="flex items-center gap-2">
				<h3 class="text-lg font-semibold text-gray-900 truncate">{cohort.name}</h3>
				<StatusBadge status={cohort.status} />
			</div>
			{#if cohort.description}
				<p class="mt-1 text-sm text-gray-500 line-clamp-2">{cohort.description}</p>
			{/if}
		</div>
	</div>

	<div class="mt-4 flex items-center gap-4 text-sm text-gray-500">
		<div class="flex items-center gap-1">
			<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
				<path
					stroke-linecap="round"
					stroke-linejoin="round"
					stroke-width="2"
					d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"
				/>
			</svg>
			{#if memberCount !== undefined}
				<span>{memberCount.toLocaleString()} members</span>
			{:else}
				<span>-- members</span>
			{/if}
		</div>
		<div class="flex items-center gap-1">
			<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
				<path
					stroke-linecap="round"
					stroke-linejoin="round"
					stroke-width="2"
					d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"
				/>
			</svg>
			<span>Updated {updatedAgo}</span>
		</div>
		<div class="flex items-center gap-1">
			<span class="text-gray-400">v{cohort.version}</span>
		</div>
	</div>
</a>
