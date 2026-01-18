<script lang="ts">
	import '../app.css';
	import Toast from '$lib/components/Toast.svelte';
	import { toasts } from '$lib/stores/toast';
</script>

<div class="min-h-screen bg-gray-50">
	<slot />
</div>

<!-- Toast Container -->
<div class="fixed bottom-4 right-4 z-50 space-y-2">
	{#each $toasts as toast (toast.id)}
		<Toast {toast} on:dismiss={() => toasts.remove(toast.id)} />
	{/each}
</div>
