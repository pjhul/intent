<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { page } from '$app/stores';
	import { goto } from '$app/navigation';
	import { getCohort, deleteCohort, activateCohort, deactivateCohort, getCohortStats, recomputeCohort, getRecomputeStatus } from '$lib/api/cohorts';
	import { cohorts } from '$lib/stores/cohorts';
	import { membershipChanges, connectSSE, clearChanges } from '$lib/stores/realtime';
	import { toasts } from '$lib/stores/toast';
	import type { Cohort, CohortStats, RecomputeJob } from '$lib/api/types';
	import StatusBadge from '$lib/components/StatusBadge.svelte';
	import MemberList from '$lib/components/MemberList.svelte';
	import { format, formatDistanceToNow } from 'date-fns';

	$: cohortId = $page.params.id;

	let cohort: Cohort | null = null;
	let stats: CohortStats | null = null;
	let loading = true;
	let error: string | null = null;
	let actionLoading = false;
	let showDeleteConfirm = false;

	let disconnectSSE: (() => void) | null = null;

	// Recompute state
	let recomputeJob: RecomputeJob | null = null;
	let recomputePollingInterval: ReturnType<typeof setInterval> | null = null;

	$: relevantChanges = $membershipChanges.filter((c) => c.cohort_id === cohortId);

	async function loadCohort() {
		loading = true;
		error = null;
		try {
			cohort = await getCohort(cohortId);
			stats = await getCohortStats(cohortId);
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load cohort';
		} finally {
			loading = false;
		}
	}

	async function handleActivate() {
		if (!cohort || actionLoading) return;
		actionLoading = true;
		try {
			cohort = await activateCohort(cohort.id);
			cohorts.updateCohort(cohort.id, cohort);
			toasts.success('Cohort activated');
		} catch (e) {
			toasts.error('Failed to activate cohort');
		} finally {
			actionLoading = false;
		}
	}

	async function handleDeactivate() {
		if (!cohort || actionLoading) return;
		actionLoading = true;
		try {
			cohort = await deactivateCohort(cohort.id);
			cohorts.updateCohort(cohort.id, cohort);
			toasts.success('Cohort deactivated');
		} catch (e) {
			toasts.error('Failed to deactivate cohort');
		} finally {
			actionLoading = false;
		}
	}

	async function handleDelete() {
		if (!cohort || actionLoading) return;
		actionLoading = true;
		try {
			await deleteCohort(cohort.id);
			cohorts.remove(cohort.id);
			toasts.success('Cohort deleted');
			goto('/');
		} catch (e) {
			toasts.error('Failed to delete cohort');
		} finally {
			actionLoading = false;
			showDeleteConfirm = false;
		}
	}

	async function handleRecompute() {
		if (!cohort || actionLoading || recomputeJob) return;
		actionLoading = true;
		try {
			const response = await recomputeCohort(cohort.id);
			recomputeJob = {
				id: response.job_id,
				cohort_id: response.cohort_id,
				status: response.status,
				progress: { total_users: 0, processed_users: 0, members_found: 0, members_added: 0, members_removed: 0 },
				started_at: new Date().toISOString()
			};
			startRecomputePolling(response.job_id);
			toasts.success('Recompute started');
		} catch (e) {
			if (e instanceof Error && e.message.includes('already in progress')) {
				toasts.error('Recompute already in progress');
			} else {
				toasts.error('Failed to start recompute');
			}
		} finally {
			actionLoading = false;
		}
	}

	function startRecomputePolling(jobId: string) {
		if (recomputePollingInterval) {
			clearInterval(recomputePollingInterval);
		}
		recomputePollingInterval = setInterval(async () => {
			if (!cohort) return;
			try {
				const job = await getRecomputeStatus(cohort.id, jobId);
				recomputeJob = job;
				if (job.status === 'completed' || job.status === 'failed') {
					stopRecomputePolling();
					if (job.status === 'completed') {
						toasts.success(`Recompute complete: ${job.progress.members_added} added, ${job.progress.members_removed} removed`);
						// Refresh stats after completion
						stats = await getCohortStats(cohort.id);
					} else {
						toasts.error(`Recompute failed: ${job.error || 'Unknown error'}`);
					}
					// Clear job after a delay so user can see final state
					setTimeout(() => {
						recomputeJob = null;
					}, 5000);
				}
			} catch (e) {
				console.error('Failed to poll recompute status:', e);
			}
		}, 2000);
	}

	function stopRecomputePolling() {
		if (recomputePollingInterval) {
			clearInterval(recomputePollingInterval);
			recomputePollingInterval = null;
		}
	}

	onMount(() => {
		loadCohort();
		disconnectSSE = connectSSE([cohortId]);
	});

	onDestroy(() => {
		if (disconnectSSE) {
			disconnectSSE();
		}
		stopRecomputePolling();
	});
</script>

<svelte:head>
	<title>{cohort?.name || 'Cohort'} | Cohort Manager</title>
</svelte:head>

<div class="p-6 max-w-7xl mx-auto">
	{#if loading}
		<div class="flex justify-center py-12">
			<svg class="animate-spin h-8 w-8 text-blue-600" fill="none" viewBox="0 0 24 24">
				<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
				<path
					class="opacity-75"
					fill="currentColor"
					d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
				/>
			</svg>
		</div>
	{:else if error}
		<div class="text-center py-12">
			<div class="p-4 bg-red-50 border border-red-200 rounded-lg text-red-700 inline-block">
				{error}
			</div>
			<div class="mt-4">
				<a href="/" class="btn btn-secondary">Back to Dashboard</a>
			</div>
		</div>
	{:else if cohort}
		<!-- Header -->
		<div class="mb-6">
			<div class="flex items-center gap-2 text-sm text-gray-500 mb-2">
				<a href="/" class="hover:text-gray-700">Dashboard</a>
				<span>/</span>
				<span>{cohort.name}</span>
			</div>

			<div class="flex items-start justify-between">
				<div>
					<div class="flex items-center gap-3">
						<h1 class="text-2xl font-bold text-gray-900">{cohort.name}</h1>
						<StatusBadge status={cohort.status} />
					</div>
					{#if cohort.description}
						<p class="mt-1 text-gray-500">{cohort.description}</p>
					{/if}
				</div>

				<div class="flex items-center gap-2">
					{#if cohort.status === 'active'}
						<button
							class="btn btn-secondary"
							on:click={handleRecompute}
							disabled={actionLoading || recomputeJob !== null}
						>
							{#if recomputeJob && (recomputeJob.status === 'pending' || recomputeJob.status === 'running')}
								<svg class="animate-spin -ml-1 mr-2 h-4 w-4" fill="none" viewBox="0 0 24 24">
									<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
									<path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
								</svg>
								Recomputing...
							{:else}
								Recompute
							{/if}
						</button>
						<button
							class="btn btn-secondary"
							on:click={handleDeactivate}
							disabled={actionLoading}
						>
							Deactivate
						</button>
					{:else}
						<button
							class="btn btn-success"
							on:click={handleActivate}
							disabled={actionLoading}
						>
							Activate
						</button>
					{/if}
					<a href="/cohorts/{cohort.id}/edit" class="btn btn-secondary">Edit</a>
					<button
						class="btn btn-danger"
						on:click={() => (showDeleteConfirm = true)}
						disabled={actionLoading}
					>
						Delete
					</button>
				</div>
			</div>
		</div>

		<!-- Recompute Progress -->
		{#if recomputeJob && (recomputeJob.status === 'pending' || recomputeJob.status === 'running')}
			<div class="mt-4 p-4 bg-blue-50 border border-blue-200 rounded-lg">
				<div class="flex items-center gap-3">
					<svg class="animate-spin h-5 w-5 text-blue-600" fill="none" viewBox="0 0 24 24">
						<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
						<path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
					</svg>
					<div>
						<div class="font-medium text-blue-900">Recomputing membership...</div>
						<div class="text-sm text-blue-700">
							{#if recomputeJob.progress.members_found > 0}
								Found {recomputeJob.progress.members_found.toLocaleString()} matching users
								{#if recomputeJob.progress.members_added > 0 || recomputeJob.progress.members_removed > 0}
									| +{recomputeJob.progress.members_added.toLocaleString()} / -{recomputeJob.progress.members_removed.toLocaleString()}
								{/if}
							{:else}
								Querying events...
							{/if}
						</div>
					</div>
				</div>
			</div>
		{:else if recomputeJob && recomputeJob.status === 'completed'}
			<div class="mt-4 p-4 bg-green-50 border border-green-200 rounded-lg">
				<div class="flex items-center gap-3">
					<svg class="h-5 w-5 text-green-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
						<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
					</svg>
					<div>
						<div class="font-medium text-green-900">Recompute complete</div>
						<div class="text-sm text-green-700">
							Found {recomputeJob.progress.members_found.toLocaleString()} members |
							+{recomputeJob.progress.members_added.toLocaleString()} added |
							-{recomputeJob.progress.members_removed.toLocaleString()} removed
						</div>
					</div>
				</div>
			</div>
		{:else if recomputeJob && recomputeJob.status === 'failed'}
			<div class="mt-4 p-4 bg-red-50 border border-red-200 rounded-lg">
				<div class="flex items-center gap-3">
					<svg class="h-5 w-5 text-red-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
						<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
					</svg>
					<div>
						<div class="font-medium text-red-900">Recompute failed</div>
						<div class="text-sm text-red-700">{recomputeJob.error || 'Unknown error'}</div>
					</div>
				</div>
			</div>
		{/if}

		<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
			<!-- Main Content -->
			<div class="lg:col-span-2 space-y-6">
				<!-- Stats -->
				<div class="card p-4">
					<h2 class="font-semibold text-gray-900 mb-4">Statistics</h2>
					<div class="grid grid-cols-3 gap-4">
						<div>
							<div class="text-sm text-gray-500">Members</div>
							<div class="text-2xl font-bold text-gray-900">
								{stats?.member_count?.toLocaleString() ?? '--'}
							</div>
						</div>
						<div>
							<div class="text-sm text-gray-500">Version</div>
							<div class="text-2xl font-bold text-gray-900">{cohort.version}</div>
						</div>
						<div>
							<div class="text-sm text-gray-500">Last Updated</div>
							<div class="text-lg font-medium text-gray-900">
								{formatDistanceToNow(new Date(cohort.updated_at), { addSuffix: true })}
							</div>
						</div>
					</div>
				</div>

				<!-- Rules -->
				<div class="card p-4">
					<h2 class="font-semibold text-gray-900 mb-4">Rules</h2>
					<div class="bg-gray-50 rounded-lg p-4">
						<div class="text-sm text-gray-500 mb-2">
							Match <span class="font-medium text-gray-700">{cohort.rules.operator}</span> of the following conditions:
						</div>
						{#if cohort.rules.conditions.length === 0}
							<p class="text-gray-500 italic">No conditions defined</p>
						{:else}
							<div class="space-y-2">
								{#each cohort.rules.conditions as condition, i}
									<div class="bg-white p-3 rounded border border-gray-200 text-sm">
										<span class="font-medium text-blue-600">{condition.type}</span>
										{#if condition.event_name}
											- {condition.event_name}
										{/if}
										{#if condition.property_name}
											- {condition.property_name}
										{/if}
										{#if condition.aggregation}
											<span class="text-gray-500">({condition.aggregation})</span>
										{/if}
										{#if condition.operator}
											<span class="font-mono">{condition.operator}</span>
										{/if}
										{#if condition.value !== undefined}
											<span class="font-mono text-green-600">{JSON.stringify(condition.value)}</span>
										{/if}
										{#if condition.time_window}
											<span class="text-gray-500">
												within {condition.time_window.duration || 'specified range'}
											</span>
										{/if}
									</div>
								{/each}
							</div>
						{/if}
					</div>
				</div>

				<!-- Members -->
				<div class="card p-4">
					<MemberList cohortId={cohort.id} />
				</div>
			</div>

			<!-- Sidebar -->
			<div class="space-y-6">
				<!-- Info -->
				<div class="card p-4">
					<h2 class="font-semibold text-gray-900 mb-4">Details</h2>
					<dl class="space-y-3 text-sm">
						<div>
							<dt class="text-gray-500">ID</dt>
							<dd class="font-mono text-gray-900 break-all">{cohort.id}</dd>
						</div>
						<div>
							<dt class="text-gray-500">Created</dt>
							<dd class="text-gray-900">{format(new Date(cohort.created_at), 'PPpp')}</dd>
						</div>
						<div>
							<dt class="text-gray-500">Updated</dt>
							<dd class="text-gray-900">{format(new Date(cohort.updated_at), 'PPpp')}</dd>
						</div>
					</dl>
				</div>

				<!-- Recent Activity -->
				<div class="card p-4">
					<div class="flex items-center justify-between mb-4">
						<h2 class="font-semibold text-gray-900">Recent Activity</h2>
						{#if relevantChanges.length > 0}
							<button class="text-xs text-gray-500 hover:text-gray-700" on:click={clearChanges}>
								Clear
							</button>
						{/if}
					</div>

					{#if relevantChanges.length === 0}
						<p class="text-sm text-gray-500 text-center py-4">No recent activity</p>
					{:else}
						<div class="space-y-3 max-h-64 overflow-y-auto">
							{#each relevantChanges as change}
								<div class="text-sm">
									<div class="flex items-center gap-2">
										<span
											class="w-2 h-2 rounded-full {change.new_status === 1
												? 'bg-green-500'
												: 'bg-red-500'}"
										></span>
										<a
											href="/users/{change.user_id}"
											class="font-medium truncate hover:text-blue-600"
										>
											{change.user_id}
										</a>
									</div>
									<div class="ml-4 text-gray-500">
										{change.new_status === 1 ? 'joined' : 'left'}
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
	{/if}
</div>

<!-- Delete Confirmation Modal -->
{#if showDeleteConfirm}
	<div class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
		<div class="bg-white rounded-lg p-6 max-w-md w-full mx-4">
			<h3 class="text-lg font-semibold text-gray-900">Delete Cohort</h3>
			<p class="mt-2 text-gray-500">
				Are you sure you want to delete "{cohort?.name}"? This action cannot be undone.
			</p>
			<div class="mt-4 flex justify-end gap-3">
				<button
					class="btn btn-secondary"
					on:click={() => (showDeleteConfirm = false)}
					disabled={actionLoading}
				>
					Cancel
				</button>
				<button class="btn btn-danger" on:click={handleDelete} disabled={actionLoading}>
					{#if actionLoading}
						Deleting...
					{:else}
						Delete
					{/if}
				</button>
			</div>
		</div>
	</div>
{/if}
