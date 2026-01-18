<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/stores';

	$: orgSlug = $page.params.orgSlug;
	$: projectSlug = $page.params.projectSlug;

	let userId = '';

	function handleSubmit() {
		if (userId.trim()) {
			goto(`/${orgSlug}/${projectSlug}/users/${encodeURIComponent(userId.trim())}`);
		}
	}
</script>

<svelte:head>
	<title>User Lookup | Cohort Manager</title>
</svelte:head>

<div class="p-6 max-w-2xl mx-auto">
	<div class="card p-6">
		<h1 class="text-2xl font-bold text-gray-900 mb-2">User Lookup</h1>
		<p class="text-gray-500 mb-6">Search for a user to see which cohorts they belong to.</p>

		<form on:submit|preventDefault={handleSubmit} class="space-y-4">
			<div>
				<label for="userId" class="label">User ID</label>
				<input
					id="userId"
					type="text"
					class="input"
					placeholder="Enter user ID"
					bind:value={userId}
					autofocus
				/>
			</div>

			<button type="submit" class="btn btn-primary w-full" disabled={!userId.trim()}>
				<svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						stroke-width="2"
						d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
					/>
				</svg>
				Look Up User
			</button>
		</form>
	</div>
</div>
