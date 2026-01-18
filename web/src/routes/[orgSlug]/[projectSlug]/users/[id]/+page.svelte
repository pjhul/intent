<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import { getUserCohorts } from '$lib/api/membership';
	import { toasts } from '$lib/stores/toast';
	import type { UserCohort } from '$lib/api/types';
	import { format } from 'date-fns';

	$: orgSlug = $page.params.orgSlug;
	$: projectSlug = $page.params.projectSlug;
	$: userId = decodeURIComponent($page.params.id);

	let cohorts: UserCohort[] = [];
	let loading = true;
	let error: string | null = null;

	async function loadUserCohorts() {
		loading = true;
		error = null;
		try {
			cohorts = await getUserCohorts(orgSlug, projectSlug, userId);
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load user cohorts';
		} finally {
			loading = false;
		}
	}

	onMount(loadUserCohorts);
</script>

<svelte:head>
	<title>User {userId} | Cohort Manager</title>
</svelte:head>

<div class="p-6 max-w-4xl mx-auto">
	<!-- Breadcrumb -->
	<div class="flex items-center gap-2 text-sm text-gray-500 mb-6">
		<a href="/{orgSlug}/{projectSlug}" class="hover:text-gray-700">Dashboard</a>
		<span>/</span>
		<a href="/{orgSlug}/{projectSlug}/users/lookup" class="hover:text-gray-700">User Lookup</a>
		<span>/</span>
		<span class="truncate max-w-xs">{userId}</span>
	</div>

	<div class="card p-6">
		<div class="flex items-start justify-between mb-6">
			<div>
				<h1 class="text-2xl font-bold text-gray-900">User Details</h1>
				<p class="mt-1 text-sm text-gray-500 font-mono break-all">{userId}</p>
			</div>
			<a href="/{orgSlug}/{projectSlug}/users/lookup" class="btn btn-secondary">
				<svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						stroke-width="2"
						d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
					/>
				</svg>
				New Search
			</a>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<svg class="animate-spin h-8 w-8 text-blue-600" fill="none" viewBox="0 0 24 24">
					<circle
						class="opacity-25"
						cx="12"
						cy="12"
						r="10"
						stroke="currentColor"
						stroke-width="4"
					/>
					<path
						class="opacity-75"
						fill="currentColor"
						d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
					/>
				</svg>
			</div>
		{:else if error}
			<div class="p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">
				{error}
			</div>
		{:else}
			<h2 class="text-lg font-semibold text-gray-900 mb-4">
				Cohort Memberships
				<span class="text-gray-400 font-normal">({cohorts.length})</span>
			</h2>

			{#if cohorts.length === 0}
				<div class="text-center py-8 bg-gray-50 rounded-lg">
					<svg
						class="mx-auto h-12 w-12 text-gray-400"
						fill="none"
						stroke="currentColor"
						viewBox="0 0 24 24"
					>
						<path
							stroke-linecap="round"
							stroke-linejoin="round"
							stroke-width="2"
							d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"
						/>
					</svg>
					<p class="mt-2 text-gray-500">This user is not a member of any cohorts.</p>
				</div>
			{:else}
				<div class="overflow-hidden rounded-lg border border-gray-200">
					<table class="min-w-full divide-y divide-gray-200">
						<thead class="bg-gray-50">
							<tr>
								<th
									class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
								>
									Cohort
								</th>
								<th
									class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
								>
									Joined At
								</th>
							</tr>
						</thead>
						<tbody class="bg-white divide-y divide-gray-200">
							{#each cohorts as cohort}
								<tr class="hover:bg-gray-50">
									<td class="px-4 py-3">
										<a
											href="/{orgSlug}/{projectSlug}/cohorts/{cohort.cohort_id}"
											class="text-blue-600 hover:text-blue-800 hover:underline font-medium"
										>
											{cohort.cohort_name}
										</a>
									</td>
									<td class="px-4 py-3 text-sm text-gray-500">
										{format(new Date(cohort.joined_at), 'PPpp')}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	</div>
</div>
