<script lang="ts">
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';
	import { listOrganizations, createOrganization } from '$lib/api/organizations';
	import type { Organization } from '$lib/api/types';
	import { toasts } from '$lib/stores/toast';

	let organizations: Organization[] = [];
	let loading = true;
	let error: string | null = null;
	let showCreateForm = false;
	let creating = false;

	let newOrg = {
		name: '',
		slug: '',
		description: ''
	};

	onMount(async () => {
		try {
			organizations = await listOrganizations();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load organizations';
			toasts.error('Failed to load organizations');
		} finally {
			loading = false;
		}
	});

	function generateSlug(name: string): string {
		return name
			.toLowerCase()
			.replace(/[^a-z0-9]+/g, '-')
			.replace(/^-|-$/g, '');
	}

	$: if (newOrg.name && !newOrg.slug) {
		newOrg.slug = generateSlug(newOrg.name);
	}

	async function handleCreateOrg() {
		if (!newOrg.name || !newOrg.slug) {
			toasts.error('Name and slug are required');
			return;
		}

		creating = true;
		try {
			const org = await createOrganization(newOrg);
			organizations = [...organizations, org];
			showCreateForm = false;
			newOrg = { name: '', slug: '', description: '' };
			toasts.success('Organization created successfully');
			goto(`/${org.slug}`);
		} catch (e) {
			toasts.error(e instanceof Error ? e.message : 'Failed to create organization');
		} finally {
			creating = false;
		}
	}
</script>

<svelte:head>
	<title>Select Organization</title>
</svelte:head>

<div class="p-6 max-w-4xl mx-auto">
	<div class="flex items-center justify-between mb-8">
		<div>
			<h1 class="text-2xl font-bold text-gray-900">Organizations</h1>
			<p class="text-gray-500 mt-1">Select an organization to continue</p>
		</div>
		<button class="btn btn-primary" on:click={() => (showCreateForm = true)}>
			<svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
				<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
			</svg>
			New Organization
		</button>
	</div>

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
		<div class="p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">
			{error}
		</div>
	{:else if organizations.length === 0 && !showCreateForm}
		<div class="text-center py-12">
			<svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
				<path
					stroke-linecap="round"
					stroke-linejoin="round"
					stroke-width="2"
					d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4"
				/>
			</svg>
			<h3 class="mt-2 text-sm font-medium text-gray-900">No organizations</h3>
			<p class="mt-1 text-sm text-gray-500">Get started by creating your first organization.</p>
			<div class="mt-6">
				<button class="btn btn-primary" on:click={() => (showCreateForm = true)}>
					Create Organization
				</button>
			</div>
		</div>
	{:else}
		<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
			{#each organizations as org}
				<a
					href="/{org.slug}"
					class="card p-6 hover:border-blue-500 hover:shadow-md transition-all"
				>
					<h3 class="font-semibold text-lg text-gray-900">{org.name}</h3>
					<p class="text-sm text-gray-500 mt-1">{org.slug}</p>
					{#if org.description}
						<p class="text-sm text-gray-600 mt-2">{org.description}</p>
					{/if}
				</a>
			{/each}
		</div>
	{/if}

	{#if showCreateForm}
		<div class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
			<div class="bg-white rounded-lg p-6 w-full max-w-md">
				<h2 class="text-lg font-semibold mb-4">Create Organization</h2>
				<form on:submit|preventDefault={handleCreateOrg} class="space-y-4">
					<div>
						<label for="name" class="block text-sm font-medium text-gray-700">Name</label>
						<input
							type="text"
							id="name"
							class="input mt-1"
							bind:value={newOrg.name}
							placeholder="My Organization"
							required
						/>
					</div>
					<div>
						<label for="slug" class="block text-sm font-medium text-gray-700">Slug</label>
						<input
							type="text"
							id="slug"
							class="input mt-1"
							bind:value={newOrg.slug}
							placeholder="my-organization"
							required
						/>
					</div>
					<div>
						<label for="description" class="block text-sm font-medium text-gray-700"
							>Description (optional)</label
						>
						<textarea
							id="description"
							class="input mt-1"
							bind:value={newOrg.description}
							placeholder="Optional description..."
							rows="2"
						/>
					</div>
					<div class="flex justify-end gap-3">
						<button
							type="button"
							class="btn btn-secondary"
							on:click={() => (showCreateForm = false)}
						>
							Cancel
						</button>
						<button type="submit" class="btn btn-primary" disabled={creating}>
							{creating ? 'Creating...' : 'Create'}
						</button>
					</div>
				</form>
			</div>
		</div>
	{/if}
</div>
