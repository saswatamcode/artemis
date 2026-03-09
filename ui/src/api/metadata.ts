// Metadata API endpoints with caching

import { api } from './client';
import type {
  MetadataAttributeKeysResponse,
  MetadataAttributeValuesResponse,
  MetadataParams,
} from './types';

// Simple in-memory cache with TTL
interface CacheEntry<T> {
  data: T;
  timestamp: number;
}

const cache = new Map<string, CacheEntry<unknown>>();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

function getCacheKey(endpoint: string, params?: MetadataParams): string {
  const paramStr = params ? JSON.stringify(params) : '';
  return `${endpoint}:${paramStr}`;
}

function getFromCache<T>(key: string): T | null {
  const entry = cache.get(key) as CacheEntry<T> | undefined;
  if (!entry) return null;

  const now = Date.now();
  if (now - entry.timestamp > CACHE_TTL) {
    cache.delete(key);
    return null;
  }

  return entry.data;
}

function setInCache<T>(key: string, data: T): void {
  cache.set(key, {
    data,
    timestamp: Date.now(),
  });
}

export function clearMetadataCache(): void {
  cache.clear();
}

export async function fetchAttributeKeys(
  params?: MetadataParams,
  signal?: AbortSignal
): Promise<MetadataAttributeKeysResponse> {
  const cacheKey = getCacheKey('/metadata/attribute_keys', params);

  // Check cache first
  const cached = getFromCache<MetadataAttributeKeysResponse>(cacheKey);
  if (cached) {
    return cached;
  }

  // Build query string
  const queryParams = new URLSearchParams();
  if (params?.start) queryParams.set('start', params.start);
  if (params?.end) queryParams.set('end', params.end);

  const endpoint = `/metadata/attribute_keys${
    queryParams.toString() ? `?${queryParams.toString()}` : ''
  }`;

  const response = await api.get<MetadataAttributeKeysResponse>(
    endpoint,
    signal
  );

  // Cache the response
  setInCache(cacheKey, response);

  return response;
}

export async function fetchAttributeValues(
  key: string,
  params?: MetadataParams,
  signal?: AbortSignal
): Promise<MetadataAttributeValuesResponse> {
  const cacheKey = getCacheKey(`/metadata/attribute_values/${key}`, params);

  // Check cache first
  const cached = getFromCache<MetadataAttributeValuesResponse>(cacheKey);
  if (cached) {
    return cached;
  }

  // Build query string
  const queryParams = new URLSearchParams();
  queryParams.set('key', key);
  if (params?.start) queryParams.set('start', params.start);
  if (params?.end) queryParams.set('end', params.end);
  if (params?.limit !== undefined)
    queryParams.set('limit', params.limit.toString());

  const endpoint = `/metadata/attribute_values?${queryParams.toString()}`;

  const response = await api.get<MetadataAttributeValuesResponse>(
    endpoint,
    signal
  );

  // Cache the response
  setInCache(cacheKey, response);

  return response;
}
