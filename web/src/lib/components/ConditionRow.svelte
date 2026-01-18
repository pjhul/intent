<script lang="ts">
	import { createEventDispatcher } from 'svelte';
	import type { Condition, ConditionType, ComparisonOperator, AggregationType, TimeWindowType, PropertyFilter } from '$lib/api/types';

	export let condition: Condition;
	export let index: number;

	const dispatch = createEventDispatcher<{
		update: Condition;
		remove: void;
	}>();

	const conditionTypes: { value: ConditionType; label: string }[] = [
		{ value: 'event', label: 'Event' },
		{ value: 'property', label: 'User Property' },
		{ value: 'aggregate', label: 'Aggregation' }
	];

	const operators: { value: ComparisonOperator; label: string }[] = [
		{ value: 'eq', label: '=' },
		{ value: 'ne', label: '!=' },
		{ value: 'gt', label: '>' },
		{ value: 'gte', label: '>=' },
		{ value: 'lt', label: '<' },
		{ value: 'lte', label: '<=' },
		{ value: 'in', label: 'in' },
		{ value: 'nin', label: 'not in' }
	];

	const aggregations: { value: AggregationType; label: string }[] = [
		{ value: 'count', label: 'Count' },
		{ value: 'sum', label: 'Sum' },
		{ value: 'avg', label: 'Average' },
		{ value: 'min', label: 'Min' },
		{ value: 'max', label: 'Max' },
		{ value: 'distinct_count', label: 'Distinct Count' }
	];

	const timeWindowTypes: { value: TimeWindowType; label: string }[] = [
		{ value: 'sliding', label: 'Sliding Window' },
		{ value: 'absolute', label: 'Absolute Range' }
	];

	function updateField<K extends keyof Condition>(field: K, value: Condition[K]) {
		const updated = { ...condition, [field]: value };
		dispatch('update', updated);
	}

	function updateTimeWindow(field: string, value: string) {
		const timeWindow = condition.time_window || { type: 'sliding' as TimeWindowType };
		const updated = { ...condition, time_window: { ...timeWindow, [field]: value } };
		dispatch('update', updated);
	}

	function toggleTimeWindow() {
		if (condition.time_window) {
			const { time_window, ...rest } = condition;
			dispatch('update', rest as Condition);
		} else {
			dispatch('update', { ...condition, time_window: { type: 'sliding', duration: '7d' } });
		}
	}

	function addPropertyFilter() {
		const filters = condition.property_filters || [];
		const newFilter: PropertyFilter = { property: '', operator: 'eq', value: '' };
		dispatch('update', { ...condition, property_filters: [...filters, newFilter] });
	}

	function updatePropertyFilter(filterIndex: number, field: keyof PropertyFilter, value: unknown) {
		const filters = [...(condition.property_filters || [])];
		filters[filterIndex] = { ...filters[filterIndex], [field]: value };
		dispatch('update', { ...condition, property_filters: filters });
	}

	function removePropertyFilter(filterIndex: number) {
		const filters = (condition.property_filters || []).filter((_, i) => i !== filterIndex);
		dispatch('update', { ...condition, property_filters: filters.length > 0 ? filters : undefined });
	}

	function handleTypeChange(e: Event) {
		const target = e.target as HTMLSelectElement;
		updateField('type', target.value as ConditionType);
	}

	function handleAggregationChange(e: Event) {
		const target = e.target as HTMLSelectElement;
		updateField('aggregation', target.value as AggregationType);
	}

	function handleOperatorChange(e: Event) {
		const target = e.target as HTMLSelectElement;
		updateField('operator', target.value as ComparisonOperator);
	}

	function handleFilterOperatorChange(filterIndex: number, e: Event) {
		const target = e.target as HTMLSelectElement;
		updatePropertyFilter(filterIndex, 'operator', target.value);
	}
</script>

<div class="bg-gray-50 rounded-lg p-4 border border-gray-200">
	<div class="flex items-start justify-between gap-4">
		<span class="text-sm font-medium text-gray-500">Condition {index + 1}</span>
		<button
			type="button"
			class="text-gray-400 hover:text-red-500 transition-colors"
			on:click={() => dispatch('remove')}
			aria-label="Remove condition"
		>
			<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
				<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
			</svg>
		</button>
	</div>

	<div class="mt-3 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
		<!-- Condition Type -->
		<div>
			<label class="label">Type</label>
			<select
				class="input"
				value={condition.type}
				on:change={handleTypeChange}
			>
				{#each conditionTypes as type}
					<option value={type.value}>{type.label}</option>
				{/each}
			</select>
		</div>

		<!-- Event Name (for event and aggregate types) -->
		{#if condition.type === 'event' || condition.type === 'aggregate'}
			<div>
				<label class="label">Event Name</label>
				<input
					type="text"
					class="input"
					placeholder="e.g., page_view"
					value={condition.event_name || ''}
					on:input={(e) => updateField('event_name', e.currentTarget.value)}
				/>
			</div>
		{/if}

		<!-- Property Name (for property type) -->
		{#if condition.type === 'property'}
			<div>
				<label class="label">Property Name</label>
				<input
					type="text"
					class="input"
					placeholder="e.g., subscription_tier"
					value={condition.property_name || ''}
					on:input={(e) => updateField('property_name', e.currentTarget.value)}
				/>
			</div>
		{/if}

		<!-- Aggregation (for aggregate type) -->
		{#if condition.type === 'aggregate'}
			<div>
				<label class="label">Aggregation</label>
				<select
					class="input"
					value={condition.aggregation || 'count'}
					on:change={handleAggregationChange}
				>
					{#each aggregations as agg}
						<option value={agg.value}>{agg.label}</option>
					{/each}
				</select>
			</div>

			{#if condition.aggregation && condition.aggregation !== 'count'}
				<div>
					<label class="label">Aggregation Field</label>
					<input
						type="text"
						class="input"
						placeholder="e.g., amount"
						value={condition.aggregation_field || ''}
						on:input={(e) => updateField('aggregation_field', e.currentTarget.value)}
					/>
				</div>
			{/if}
		{/if}

		<!-- Operator -->
		<div>
			<label class="label">Operator</label>
			<select
				class="input"
				value={condition.operator || 'eq'}
				on:change={handleOperatorChange}
			>
				{#each operators as op}
					<option value={op.value}>{op.label}</option>
				{/each}
			</select>
		</div>

		<!-- Value -->
		<div>
			<label class="label">Value</label>
			<input
				type="text"
				class="input"
				placeholder="Value"
				value={condition.value ?? ''}
				on:input={(e) => {
					const val = e.currentTarget.value;
					const numVal = Number(val);
					updateField('value', isNaN(numVal) ? val : numVal);
				}}
			/>
		</div>
	</div>

	<!-- Time Window Toggle -->
	<div class="mt-4">
		<label class="flex items-center gap-2 cursor-pointer">
			<input
				type="checkbox"
				class="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
				checked={!!condition.time_window}
				on:change={toggleTimeWindow}
			/>
			<span class="text-sm text-gray-700">Add time window</span>
		</label>
	</div>

	<!-- Time Window Configuration -->
	{#if condition.time_window}
		<div class="mt-3 p-3 bg-white rounded border border-gray-200">
			<div class="grid grid-cols-1 md:grid-cols-3 gap-4">
				<div>
					<label class="label">Window Type</label>
					<select
						class="input"
						value={condition.time_window.type}
						on:change={(e) => updateTimeWindow('type', e.currentTarget.value)}
					>
						{#each timeWindowTypes as twType}
							<option value={twType.value}>{twType.label}</option>
						{/each}
					</select>
				</div>

				{#if condition.time_window.type === 'sliding'}
					<div>
						<label class="label">Duration</label>
						<input
							type="text"
							class="input"
							placeholder="e.g., 7d, 24h, 30m"
							value={condition.time_window.duration || ''}
							on:input={(e) => updateTimeWindow('duration', e.currentTarget.value)}
						/>
					</div>
				{:else}
					<div>
						<label class="label">Start</label>
						<input
							type="datetime-local"
							class="input"
							value={condition.time_window.start || ''}
							on:input={(e) => updateTimeWindow('start', e.currentTarget.value)}
						/>
					</div>
					<div>
						<label class="label">End</label>
						<input
							type="datetime-local"
							class="input"
							value={condition.time_window.end || ''}
							on:input={(e) => updateTimeWindow('end', e.currentTarget.value)}
						/>
					</div>
				{/if}
			</div>
		</div>
	{/if}

	<!-- Property Filters (for event and aggregate types) -->
	{#if condition.type === 'event' || condition.type === 'aggregate'}
		<div class="mt-4">
			<div class="flex items-center justify-between">
				<label class="text-sm font-medium text-gray-700">Property Filters</label>
				<button
					type="button"
					class="text-sm text-blue-600 hover:text-blue-700"
					on:click={addPropertyFilter}
				>
					+ Add Filter
				</button>
			</div>

			{#if condition.property_filters && condition.property_filters.length > 0}
				<div class="mt-2 space-y-2">
					{#each condition.property_filters as filter, filterIndex}
						<div class="flex items-center gap-2 p-2 bg-white rounded border border-gray-200">
							<input
								type="text"
								class="input flex-1"
								placeholder="Property"
								value={filter.property}
								on:input={(e) => updatePropertyFilter(filterIndex, 'property', e.currentTarget.value)}
							/>
							<select
								class="input w-24"
								value={filter.operator}
								on:change={(e) => updatePropertyFilter(filterIndex, 'operator', e.currentTarget.value)}
							>
								{#each operators as op}
									<option value={op.value}>{op.label}</option>
								{/each}
							</select>
							<input
								type="text"
								class="input flex-1"
								placeholder="Value"
								value={filter.value ?? ''}
								on:input={(e) => updatePropertyFilter(filterIndex, 'value', e.currentTarget.value)}
							/>
							<button
								type="button"
								class="p-1 text-gray-400 hover:text-red-500"
								on:click={() => removePropertyFilter(filterIndex)}
							>
								<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
									<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
								</svg>
							</button>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	{/if}
</div>
