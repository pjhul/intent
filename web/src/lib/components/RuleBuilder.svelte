<script lang="ts">
	import type { Rules, Condition } from '$lib/api/types';
	import ConditionRow from './ConditionRow.svelte';

	export let rules: Rules = {
		operator: 'AND',
		conditions: []
	};

	export let showPreview = true;

	function addCondition() {
		const newCondition: Condition = {
			type: 'event',
			event_name: '',
			operator: 'gte',
			value: 1
		};
		rules = {
			...rules,
			conditions: [...rules.conditions, newCondition]
		};
	}

	function updateCondition(index: number, condition: Condition) {
		const conditions = [...rules.conditions];
		conditions[index] = condition;
		rules = { ...rules, conditions };
	}

	function removeCondition(index: number) {
		rules = {
			...rules,
			conditions: rules.conditions.filter((_, i) => i !== index)
		};
	}

	function setOperator(operator: 'AND' | 'OR') {
		rules = { ...rules, operator };
	}

	$: previewJson = JSON.stringify(rules, null, 2);
</script>

<div class="space-y-4">
	<div class="flex items-center justify-between">
		<h3 class="text-lg font-semibold text-gray-900">Rules</h3>
		<div class="flex items-center gap-2">
			<span class="text-sm text-gray-500">Match</span>
			<div class="inline-flex rounded-md shadow-sm">
				<button
					type="button"
					class="px-3 py-1.5 text-sm font-medium rounded-l-md border {rules.operator === 'AND'
						? 'bg-blue-600 text-white border-blue-600'
						: 'bg-white text-gray-700 border-gray-300 hover:bg-gray-50'}"
					on:click={() => setOperator('AND')}
				>
					ALL
				</button>
				<button
					type="button"
					class="px-3 py-1.5 text-sm font-medium rounded-r-md border-t border-r border-b {rules.operator === 'OR'
						? 'bg-blue-600 text-white border-blue-600'
						: 'bg-white text-gray-700 border-gray-300 hover:bg-gray-50'}"
					on:click={() => setOperator('OR')}
				>
					ANY
				</button>
			</div>
			<span class="text-sm text-gray-500">conditions</span>
		</div>
	</div>

	{#if rules.conditions.length === 0}
		<div class="text-center py-8 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">
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
					d="M12 6v6m0 0v6m0-6h6m-6 0H6"
				/>
			</svg>
			<p class="mt-2 text-sm text-gray-500">No conditions defined</p>
			<button type="button" class="mt-4 btn btn-primary" on:click={addCondition}>
				Add First Condition
			</button>
		</div>
	{:else}
		<div class="space-y-3">
			{#each rules.conditions as condition, index}
				{#if index > 0}
					<div class="flex justify-center">
						<span
							class="px-3 py-1 text-xs font-medium uppercase tracking-wider text-gray-500 bg-gray-100 rounded-full"
						>
							{rules.operator}
						</span>
					</div>
				{/if}
				<ConditionRow
					{condition}
					{index}
					on:update={(e) => updateCondition(index, e.detail)}
					on:remove={() => removeCondition(index)}
				/>
			{/each}
		</div>

		<button type="button" class="btn btn-secondary" on:click={addCondition}>
			<svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
				<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
			</svg>
			Add Condition
		</button>
	{/if}

	{#if showPreview && rules.conditions.length > 0}
		<div class="mt-6">
			<details class="group">
				<summary class="cursor-pointer text-sm font-medium text-gray-700 hover:text-gray-900">
					<span class="group-open:hidden">Show</span>
					<span class="hidden group-open:inline">Hide</span>
					JSON Preview
				</summary>
				<pre
					class="mt-2 p-4 bg-gray-900 text-gray-100 rounded-lg text-sm overflow-x-auto">{previewJson}</pre>
			</details>
		</div>
	{/if}
</div>
