<script lang="ts">
	import type { Member } from '$lib/api/types';
	import { getCohortMembers } from '$lib/api/membership';
	import { format } from 'date-fns';

	export let cohortId: string;

	let members: Member[] = [];
	let loading = false;
	let error: string | null = null;
	let page = 1;
	let pageSize = 20;
	let total = 0;

	$: hasMore = members.length < total;
	$: totalPages = Math.ceil(total / pageSize);

	async function loadMembers() {
		loading = true;
		error = null;
		try {
			const response = await getCohortMembers(cohortId, page, pageSize);
			members = response.data || [];
			total = response.total;
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load members';
		} finally {
			loading = false;
		}
	}

	async function loadMore() {
		if (loading || !hasMore) return;
		page++;
		loading = true;
		try {
			const response = await getCohortMembers(cohortId, page, pageSize);
			members = [...members, ...(response.data || [])];
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load more members';
			page--;
		} finally {
			loading = false;
		}
	}

	// Load on mount
	loadMembers();
</script>

<div class="space-y-4">
	<div class="flex items-center justify-between">
		<h3 class="text-lg font-semibold text-gray-900">Members</h3>
		<span class="text-sm text-gray-500">{total.toLocaleString()} total</span>
	</div>

	{#if error}
		<div class="p-4 bg-red-50 border border-red-200 rounded-md text-red-700 text-sm">
			{error}
		</div>
	{/if}

	<div class="overflow-hidden rounded-lg border border-gray-200">
		<table class="min-w-full divide-y divide-gray-200">
			<thead class="bg-gray-50">
				<tr>
					<th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
						User ID
					</th>
					<th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
						Joined At
					</th>
				</tr>
			</thead>
			<tbody class="bg-white divide-y divide-gray-200">
				{#if members.length === 0 && !loading}
					<tr>
						<td colspan="2" class="px-4 py-8 text-center text-gray-500">
							No members in this cohort yet
						</td>
					</tr>
				{/if}
				{#each members as member}
					<tr class="hover:bg-gray-50">
						<td class="px-4 py-3 text-sm font-mono text-gray-900">
							<a href="/users/{member.user_id}" class="hover:text-blue-600 hover:underline">
								{member.user_id}
							</a>
						</td>
						<td class="px-4 py-3 text-sm text-gray-500">
							{format(new Date(member.joined_at), 'PPpp')}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>

	{#if hasMore}
		<div class="flex justify-center">
			<button class="btn btn-secondary" on:click={loadMore} disabled={loading}>
				{#if loading}
					Loading...
				{:else}
					Load More
				{/if}
			</button>
		</div>
	{/if}

	{#if loading && members.length === 0}
		<div class="flex justify-center py-8">
			<svg class="animate-spin h-8 w-8 text-blue-600" fill="none" viewBox="0 0 24 24">
				<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
				<path
					class="opacity-75"
					fill="currentColor"
					d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
				/>
			</svg>
		</div>
	{/if}
</div>
