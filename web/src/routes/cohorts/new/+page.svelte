<script lang="ts">
	import { goto } from '$app/navigation';
	import { createCohort } from '$lib/api/cohorts';
	import { cohorts } from '$lib/stores/cohorts';
	import { toasts } from '$lib/stores/toast';
	import CohortForm from '$lib/components/CohortForm.svelte';
	import type { CreateCohortRequest } from '$lib/api/types';

	let loading = false;

	async function handleSubmit(
		event: CustomEvent<CreateCohortRequest>
	) {
		loading = true;
		try {
			const newCohort = await createCohort(event.detail);
			cohorts.add(newCohort);
			toasts.success('Cohort created successfully');
			goto(`/cohorts/${newCohort.id}`);
		} catch (e) {
			toasts.error(e instanceof Error ? e.message : 'Failed to create cohort');
		} finally {
			loading = false;
		}
	}

	function handleCancel() {
		goto('/');
	}
</script>

<svelte:head>
	<title>Create Cohort | Cohort Manager</title>
</svelte:head>

<div class="p-6 max-w-4xl mx-auto">
	<!-- Breadcrumb -->
	<div class="flex items-center gap-2 text-sm text-gray-500 mb-6">
		<a href="/" class="hover:text-gray-700">Dashboard</a>
		<span>/</span>
		<span>New Cohort</span>
	</div>

	<div class="card p-6">
		<h1 class="text-2xl font-bold text-gray-900 mb-6">Create New Cohort</h1>
		<CohortForm
			{loading}
			submitLabel="Create Cohort"
			on:submit={handleSubmit}
			on:cancel={handleCancel}
		/>
	</div>
</div>
