<script lang="ts">
	import '../app.css';
	import Toast from '$lib/components/Toast.svelte';
	import { toasts } from '$lib/stores/toast';
	import { isConnected } from '$lib/stores/realtime';
	import { page } from '$app/stores';

	$: currentPath = $page.url.pathname;

	function isActive(path: string): boolean {
		if (path === '/') {
			return currentPath === '/';
		}
		return currentPath.startsWith(path);
	}
</script>

<div class="min-h-screen flex">
	<!-- Sidebar -->
	<aside class="w-64 bg-gray-900 text-white flex flex-col">
		<div class="p-4 border-b border-gray-800">
			<h1 class="text-xl font-bold">Cohort Manager</h1>
		</div>

		<nav class="flex-1 p-4">
			<ul class="space-y-2">
				<li>
					<a
						href="/"
						class="flex items-center gap-3 px-3 py-2 rounded-md transition-colors {isActive('/')
							? 'bg-gray-800 text-white'
							: 'text-gray-300 hover:bg-gray-800 hover:text-white'}"
					>
						<svg
							class="w-5 h-5"
							fill="none"
							stroke="currentColor"
							viewBox="0 0 24 24"
						>
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
						href="/cohorts/new"
						class="flex items-center gap-3 px-3 py-2 rounded-md transition-colors {isActive(
							'/cohorts/new'
						)
							? 'bg-gray-800 text-white'
							: 'text-gray-300 hover:bg-gray-800 hover:text-white'}"
					>
						<svg
							class="w-5 h-5"
							fill="none"
							stroke="currentColor"
							viewBox="0 0 24 24"
						>
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
						href="/users/lookup"
						class="flex items-center gap-3 px-3 py-2 rounded-md transition-colors {isActive(
							'/users'
						)
							? 'bg-gray-800 text-white'
							: 'text-gray-300 hover:bg-gray-800 hover:text-white'}"
					>
						<svg
							class="w-5 h-5"
							fill="none"
							stroke="currentColor"
							viewBox="0 0 24 24"
						>
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

		<!-- Connection Status -->
		<div class="p-4 border-t border-gray-800">
			<div class="flex items-center gap-2 text-sm">
				<span
					class="w-2 h-2 rounded-full {$isConnected ? 'bg-green-500' : 'bg-red-500'}"
				></span>
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

<!-- Toast Container -->
<div class="fixed bottom-4 right-4 z-50 space-y-2">
	{#each $toasts as toast (toast.id)}
		<Toast {toast} on:dismiss={() => toasts.remove(toast.id)} />
	{/each}
</div>
