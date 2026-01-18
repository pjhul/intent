<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import CohortCard from '$lib/components/CohortCard.svelte';
	import StatusBadge from '$lib/components/StatusBadge.svelte';
	import { cohorts, cohortsByStatus } from '$lib/stores/cohorts';
	import { membershipChanges, connectSSE, clearChanges } from '$lib/stores/realtime';
	import { toasts } from '$lib/stores/toast';
	import type { CohortStatus } from '$lib/api/types';
	import { formatDistanceToNow } from 'date-fns';

	let loading = true;
	let error: string | null = null;
	let searchQuery = '';
	let statusFilter: CohortStatus | 'all' = 'all';

	let disconnectSSE: (() => void) | null = null;

	$: cohortList = Array.isArray($cohorts) ? $cohorts : [];

	$: filteredCohorts = cohortList.filter((cohort) => {
		const matchesSearch =
			searchQuery === '' ||
			cohort.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
			cohort.description?.toLowerCase().includes(searchQuery.toLowerCase());

		const matchesStatus = statusFilter === 'all' || cohort.status === statusFilter;

		return matchesSearch && matchesStatus;
	});

	$: statusCounts = {
		all: cohortList.length,
		active: $cohortsByStatus.active.length,
		inactive: $cohortsByStatus.inactive.length,
		draft: $cohortsByStatus.draft.length
	};

	onMount(async () => {
		try {
			await cohorts.load();
			disconnectSSE = connectSSE();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load cohorts';
			toasts.error('Failed to load cohorts');
		} finally {
			loading = false;
		}
	});

	onDestroy(() => {
		if (disconnectSSE) {
			disconnectSSE();
		}
	});
</script>

<svelte:head>
	<title>Cohort Dashboard</title>
</svelte:head>

<div class="p-6 max-w-7xl mx-auto">
	<!-- Header -->
	<div class="flex items-center justify-between mb-6">
		<div>
			<h1 class="text-2xl font-bold text-gray-900">Cohorts</h1>
			<p class="text-gray-500 mt-1">Manage your user segments</p>
		</div>
		<a href="/cohorts/new" class="btn btn-primary">
			<svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
				<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
			</svg>
			New Cohort
		</a>
	</div>

	<!-- Stats Cards -->
	<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
		<button
			class="card p-4 text-left transition-all {statusFilter === 'all'
				? 'ring-2 ring-blue-500'
				: 'hover:border-gray-300'}"
			on:click={() => (statusFilter = 'all')}
		>
			<div class="text-sm text-gray-500">Total Cohorts</div>
			<div class="text-2xl font-bold text-gray-900">{statusCounts.all}</div>
		</button>
		<button
			class="card p-4 text-left transition-all {statusFilter === 'active'
				? 'ring-2 ring-green-500'
				: 'hover:border-gray-300'}"
			on:click={() => (statusFilter = 'active')}
		>
			<div class="text-sm text-gray-500">Active</div>
			<div class="text-2xl font-bold text-green-600">{statusCounts.active}</div>
		</button>
		<button
			class="card p-4 text-left transition-all {statusFilter === 'inactive'
				? 'ring-2 ring-gray-500'
				: 'hover:border-gray-300'}"
			on:click={() => (statusFilter = 'inactive')}
		>
			<div class="text-sm text-gray-500">Inactive</div>
			<div class="text-2xl font-bold text-gray-600">{statusCounts.inactive}</div>
		</button>
		<button
			class="card p-4 text-left transition-all {statusFilter === 'draft'
				? 'ring-2 ring-yellow-500'
				: 'hover:border-gray-300'}"
			on:click={() => (statusFilter = 'draft')}
		>
			<div class="text-sm text-gray-500">Draft</div>
			<div class="text-2xl font-bold text-yellow-600">{statusCounts.draft}</div>
		</button>
	</div>

	<div class="flex gap-6">
		<!-- Cohort List -->
		<div class="flex-1">
			<!-- Search -->
			<div class="mb-4">
				<div class="relative">
					<svg
						class="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400"
						fill="none"
						stroke="currentColor"
						viewBox="0 0 24 24"
					>
						<path
							stroke-linecap="round"
							stroke-linejoin="round"
							stroke-width="2"
							d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
						/>
					</svg>
					<input
						type="text"
						class="input pl-10"
						placeholder="Search cohorts..."
						bind:value={searchQuery}
					/>
				</div>
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
			{:else if filteredCohorts.length === 0}
				<div class="text-center py-12">
					{#if cohortList.length === 0}
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
						<h3 class="mt-2 text-sm font-medium text-gray-900">No cohorts</h3>
						<p class="mt-1 text-sm text-gray-500">Get started by creating a new cohort.</p>
						<div class="mt-6">
							<a href="/cohorts/new" class="btn btn-primary"> Create Cohort </a>
						</div>
					{:else}
						<p class="text-gray-500">No cohorts match your search criteria.</p>
					{/if}
				</div>
			{:else}
				<div class="grid gap-4">
					{#each filteredCohorts as cohort (cohort.id)}
						<CohortCard {cohort} />
					{/each}
				</div>
			{/if}
		</div>

		<!-- Activity Feed -->
		<div class="w-80 hidden lg:block">
			<div class="card p-4 sticky top-6">
				<div class="flex items-center justify-between mb-4">
					<h2 class="font-semibold text-gray-900">Recent Activity</h2>
					{#if $membershipChanges.length > 0}
						<button class="text-xs text-gray-500 hover:text-gray-700" on:click={clearChanges}>
							Clear
						</button>
					{/if}
				</div>

				{#if $membershipChanges.length === 0}
					<p class="text-sm text-gray-500 text-center py-4">No recent activity</p>
				{:else}
					<div class="space-y-3 max-h-96 overflow-y-auto">
						{#each $membershipChanges as change}
							<div class="text-sm">
								<div class="flex items-center gap-2">
									<span
										class="w-2 h-2 rounded-full {change.new_status === 1
											? 'bg-green-500'
											: 'bg-red-500'}"
									></span>
									<span class="font-medium truncate">{change.user_id}</span>
								</div>
								<div class="ml-4 text-gray-500">
									{change.new_status === 1 ? 'joined' : 'left'}
									<a
										href="/cohorts/{change.cohort_id}"
										class="text-blue-600 hover:underline"
									>
										{change.cohort_name}
									</a>
								</div>
								<div class="ml-4 text-xs text-gray-400">
									{formatDistanceToNow(new Date(change.changed_at), { addSuffix: true })}
								</div>
							</div>
						{/each}
					</div>
				{/if}
			</div>
		</div>
	</div>
</div>
