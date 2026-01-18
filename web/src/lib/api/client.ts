// Use empty string for relative URLs (goes through Vite proxy in dev)
// Set VITE_API_URL for production or direct backend access
const API_BASE = import.meta.env.VITE_API_URL || '';

export class ApiError extends Error {
	constructor(
		public status: number,
		message: string
	) {
		super(message);
		this.name = 'ApiError';
	}
}

export async function api<T>(path: string, options?: RequestInit): Promise<T> {
	const res = await fetch(`${API_BASE}${path}`, {
		...options,
		headers: {
			'Content-Type': 'application/json',
			...options?.headers
		}
	});

	if (!res.ok) {
		const text = await res.text();
		throw new ApiError(res.status, text || `Request failed with status ${res.status}`);
	}

	const contentType = res.headers.get('content-type');
	if (contentType && contentType.includes('application/json')) {
		return res.json();
	}

	return {} as T;
}

export function getApiBase(): string {
	return API_BASE;
}
