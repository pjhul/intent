<script lang="ts">
	import { createEventDispatcher } from 'svelte';
	import type { Cohort, Rules, CohortStatus } from '$lib/api/types';
	import RuleBuilder from './RuleBuilder.svelte';

	export let cohort: Partial<Cohort> = {};
	export let loading = false;
	export let submitLabel = 'Create Cohort';

	const dispatch = createEventDispatcher<{
		submit: { name: string; description: string; rules: Rules; status: CohortStatus };
		cancel: void;
	}>();

	let name = cohort.name || '';
	let description = cohort.description || '';
	let rules: Rules = cohort.rules || { operator: 'AND', conditions: [] };
	let status: CohortStatus = cohort.status || 'draft';

	function handleSubmit() {
		if (!name.trim()) return;
		dispatch('submit', {
			name: name.trim(),
			description: description.trim(),
			rules,
			status
		});
	}
</script>

<form on:submit|preventDefault={handleSubmit} class="space-y-6">
	<!-- Name -->
	<div>
		<label for="name" class="label">
			Name <span class="text-red-500">*</span>
		</label>
		<input
			id="name"
			type="text"
			class="input"
			placeholder="Enter cohort name"
			bind:value={name}
			required
		/>
	</div>

	<!-- Description -->
	<div>
		<label for="description" class="label">Description</label>
		<textarea
			id="description"
			class="input min-h-[100px]"
			placeholder="Describe this cohort's purpose"
			bind:value={description}
		></textarea>
	</div>

	<!-- Status -->
	<div>
		<label class="label">Initial Status</label>
		<div class="flex gap-4">
			<label class="flex items-center gap-2 cursor-pointer">
				<input
					type="radio"
					name="status"
					value="draft"
					bind:group={status}
					class="text-blue-600 focus:ring-blue-500"
				/>
				<span class="text-sm text-gray-700">Draft</span>
			</label>
			<label class="flex items-center gap-2 cursor-pointer">
				<input
					type="radio"
					name="status"
					value="active"
					bind:group={status}
					class="text-blue-600 focus:ring-blue-500"
				/>
				<span class="text-sm text-gray-700">Active</span>
			</label>
			<label class="flex items-center gap-2 cursor-pointer">
				<input
					type="radio"
					name="status"
					value="inactive"
					bind:group={status}
					class="text-blue-600 focus:ring-blue-500"
				/>
				<span class="text-sm text-gray-700">Inactive</span>
			</label>
		</div>
	</div>

	<!-- Rules -->
	<div class="pt-4 border-t border-gray-200">
		<RuleBuilder bind:rules />
	</div>

	<!-- Actions -->
	<div class="flex items-center justify-end gap-3 pt-4 border-t border-gray-200">
		<button type="button" class="btn btn-secondary" on:click={() => dispatch('cancel')}>
			Cancel
		</button>
		<button type="submit" class="btn btn-primary" disabled={loading || !name.trim()}>
			{#if loading}
				<svg class="animate-spin -ml-1 mr-2 h-4 w-4" fill="none" viewBox="0 0 24 24">
					<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
					<path
						class="opacity-75"
						fill="currentColor"
						d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
					/>
				</svg>
				Saving...
			{:else}
				{submitLabel}
			{/if}
		</button>
	</div>
</form>
