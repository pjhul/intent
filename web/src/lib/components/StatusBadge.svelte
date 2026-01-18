<script lang="ts">
	import type { CohortStatus } from '$lib/api/types';

	export let status: CohortStatus;

	const styles: Record<CohortStatus, string> = {
		active: 'bg-green-100 text-green-800',
		inactive: 'bg-gray-100 text-gray-800',
		draft: 'bg-yellow-100 text-yellow-800'
	};

	const labels: Record<CohortStatus, string> = {
		active: 'Active',
		inactive: 'Inactive',
		draft: 'Draft'
	};
</script>

<span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium {styles[status]}">
	{labels[status]}
</span>
