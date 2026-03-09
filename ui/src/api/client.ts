// Base HTTP client with AbortController support

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api/v1';

export interface RequestOptions {
  method?: 'GET' | 'POST' | 'PUT' | 'DELETE';
  body?: unknown;
  signal?: AbortSignal;
  headers?: Record<string, string>;
}

export class ApiError extends Error {
  status?: number;
  response?: unknown;

  constructor(
    message: string,
    status?: number,
    response?: unknown
  ) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.response = response;
  }
}

async function request<T>(
  endpoint: string,
  options: RequestOptions = {}
): Promise<T> {
  const { method = 'GET', body, signal, headers = {} } = options;

  const config: RequestInit = {
    method,
    signal,
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
  };

  if (body) {
    config.body = JSON.stringify(body);
  }

  const url = `${API_BASE_URL}${endpoint}`;

  try {
    const response = await fetch(url, config);

    if (!response.ok) {
      let errorMessage = `HTTP ${response.status}: ${response.statusText}`;
      try {
        const errorData = await response.json();
        if (errorData.error) {
          errorMessage = errorData.error;
        }
      } catch {
        // If error response is not JSON, use default message
      }
      throw new ApiError(errorMessage, response.status);
    }

    // Check if response is JSON
    const contentType = response.headers.get('content-type');
    if (contentType && contentType.includes('application/json')) {
      return await response.json();
    }

    // For non-JSON responses, return empty object
    return {} as T;
  } catch (error) {
    if (error instanceof ApiError) {
      throw error;
    }

    // Handle network errors and aborted requests
    if (error instanceof Error) {
      if (error.name === 'AbortError') {
        throw new ApiError('Request cancelled', 0);
      }
      throw new ApiError(error.message);
    }

    throw new ApiError('Unknown error occurred');
  }
}

export const api = {
  get: <T>(endpoint: string, signal?: AbortSignal): Promise<T> =>
    request<T>(endpoint, { method: 'GET', signal }),

  post: <T>(
    endpoint: string,
    body?: unknown,
    signal?: AbortSignal
  ): Promise<T> => request<T>(endpoint, { method: 'POST', body, signal }),

  put: <T>(
    endpoint: string,
    body?: unknown,
    signal?: AbortSignal
  ): Promise<T> => request<T>(endpoint, { method: 'PUT', body, signal }),

  delete: <T>(endpoint: string, signal?: AbortSignal): Promise<T> =>
    request<T>(endpoint, { method: 'DELETE', signal }),
};
