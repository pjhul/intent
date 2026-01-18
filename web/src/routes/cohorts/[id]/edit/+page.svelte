<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import { goto } from '$app/navigation';
	import { getCohort, updateCohort } from '$lib/api/cohorts';
	import { cohorts } from '$lib/stores/cohorts';
	import { toasts } from '$lib/stores/toast';
	import CohortForm from '$lib/components/CohortForm.svelte';
	import type { Cohort, UpdateCohortRequest } from '$lib/api/types';

	$: cohortId = $page.params.id;

	let cohort: Cohort | null = null;
	let loading = true;
	let saving = false;
	let error: string | null = null;

	async function loadCohort() {
		loading = true;
		error = null;
		try {
			cohort = await getCohort(cohortId);
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load cohort';
		} finally {
			loading = false;
		}
	}

	async function handleSubmit(
		event: CustomEvent<UpdateCohortRequest>
	) {
		if (!cohort) return;
		saving = true;
		try {
			const updated = await updateCohort(cohort.id, event.detail);
			cohorts.updateCohort(cohort.id, updated);
			toasts.success('Cohort updated successfully');
			goto(`/cohorts/${cohort.id}`);
		} catch (e) {
			toasts.error(e instanceof Error ? e.message : 'Failed to update cohort');
		} finally {
			saving = false;
		}
	}

	function handleCancel() {
		goto(`/cohorts/${cohortId}`);
	}

	onMount(loadCohort);
</script>

<svelte:head>
	<title>Edit {cohort?.name || 'Cohort'} | Cohort Manager</title>
</svelte:head>

<div class="p-6 max-w-4xl mx-auto">
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
		<!-- Breadcrumb -->
		<div class="flex items-center gap-2 text-sm text-gray-500 mb-6">
			<a href="/" class="hover:text-gray-700">Dashboard</a>
			<span>/</span>
			<a href="/cohorts/{cohort.id}" class="hover:text-gray-700">{cohort.name}</a>
			<span>/</span>
			<span>Edit</span>
		</div>

		<div class="card p-6">
			<h1 class="text-2xl font-bold text-gray-900 mb-6">Edit Cohort</h1>
			<CohortForm
				{cohort}
				loading={saving}
				submitLabel="Save Changes"
				on:submit={handleSubmit}
				on:cancel={handleCancel}
			/>
		</div>
	{/if}
</div>
