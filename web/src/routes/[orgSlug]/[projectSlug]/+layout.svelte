<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import { getOrganization } from '$lib/api/organizations';
	import { getProject } from '$lib/api/projects';
	import { currentOrganization, currentProject, clearContext } from '$lib/stores/context';
	import { isConnected } from '$lib/stores/realtime';
	import { cohorts } from '$lib/stores/cohorts';

	$: orgSlug = $page.params.orgSlug;
	$: projectSlug = $page.params.projectSlug;
	$: currentPath = $page.url.pathname;

	let loading = true;
	let error: string | null = null;

	onMount(async () => {
		await loadContext();
		return () => {
			clearContext();
			cohorts.clear();
		};
	});

	async function loadContext() {
		loading = true;
		error = null;
		try {
			const [org, project] = await Promise.all([
				getOrganization(orgSlug),
				getProject(orgSlug, projectSlug)
			]);
			currentOrganization.set(org);
			currentProject.set(project);
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load context';
		} finally {
			loading = false;
		}
	}

	function isActive(path: string): boolean {
		const basePath = `/${orgSlug}/${projectSlug}`;
		if (path === basePath || path === basePath + '/') {
			return currentPath === basePath || currentPath === basePath + '/';
		}
		return currentPath.startsWith(path);
	}
</script>

{#if loading}
	<div class="flex items-center justify-center h-screen">
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
	<div class="p-6">
		<div class="p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">
			{error}
		</div>
		<div class="mt-4">
			<a href="/{orgSlug}" class="text-blue-600 hover:underline">&larr; Back to projects</a>
		</div>
	</div>
{:else}
	<div class="min-h-screen flex">
		<!-- Sidebar -->
		<aside class="w-64 bg-gray-900 text-white flex flex-col">
			<div class="p-4 border-b border-gray-800">
				<div class="text-xs text-gray-400 uppercase tracking-wide">Organization</div>
				<a href="/{orgSlug}" class="text-sm font-medium hover:text-gray-300">
					{$currentOrganization?.name ?? orgSlug}
				</a>
				<div class="mt-2 text-xs text-gray-400 uppercase tracking-wide">Project</div>
				<div class="text-lg font-bold">{$currentProject?.name ?? projectSlug}</div>
			</div>

			<nav class="flex-1 p-4">
				<ul class="space-y-2">
					<li>
						<a
							href="/{orgSlug}/{projectSlug}"
							class="flex items-center gap-3 px-3 py-2 rounded-md transition-colors {isActive(
								`/${orgSlug}/${projectSlug}`
							) && !currentPath.includes('/cohorts/') && !currentPath.includes('/users/')
								? 'bg-gray-800 text-white'
								: 'text-gray-300 hover:bg-gray-800 hover:text-white'}"
						>
							<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									stroke-width="2"
									d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6"
								/>
							</svg>
							Dashboard
						</a>
					</li>
					<li>
						<a
							href="/{orgSlug}/{projectSlug}/cohorts/new"
							class="flex items-center gap-3 px-3 py-2 rounded-md transition-colors {isActive(
								`/${orgSlug}/${projectSlug}/cohorts/new`
							)
								? 'bg-gray-800 text-white'
								: 'text-gray-300 hover:bg-gray-800 hover:text-white'}"
						>
							<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									stroke-width="2"
									d="M12 4v16m8-8H4"
								/>
							</svg>
							New Cohort
						</a>
					</li>
					<li>
						<a
							href="/{orgSlug}/{projectSlug}/users/lookup"
							class="flex items-center gap-3 px-3 py-2 rounded-md transition-colors {isActive(
								`/${orgSlug}/${projectSlug}/users`
							)
								? 'bg-gray-800 text-white'
								: 'text-gray-300 hover:bg-gray-800 hover:text-white'}"
						>
							<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									stroke-width="2"
									d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
								/>
							</svg>
							User Lookup
						</a>
					</li>
				</ul>
			</nav>

			<!-- Back to org link -->
			<div class="p-4 border-t border-gray-800">
				<a
					href="/{orgSlug}"
					class="flex items-center gap-2 text-sm text-gray-400 hover:text-white"
				>
					<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
						<path
							stroke-linecap="round"
							stroke-linejoin="round"
							stroke-width="2"
							d="M11 17l-5-5m0 0l5-5m-5 5h12"
						/>
					</svg>
					Change Project
				</a>
			</div>

			<!-- Connection Status -->
			<div class="p-4 border-t border-gray-800">
				<div class="flex items-center gap-2 text-sm">
					<span class="w-2 h-2 rounded-full {$isConnected ? 'bg-green-500' : 'bg-red-500'}"></span>
					<span class="text-gray-400">
						{$isConnected ? 'Connected' : 'Disconnected'}
					</span>
				</div>
			</div>
		</aside>

		<!-- Main Content -->
		<main class="flex-1 overflow-auto">
			<slot />
		</main>
	</div>
{/if}
