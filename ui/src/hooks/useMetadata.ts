// Hook for fetching metadata with caching

import { useState, useEffect } from 'react';
import { fetchAttributeKeys, fetchAttributeValues } from '../api/metadata';
import { useAbortController } from './useAbortController';
import type { MetadataParams } from '../api/types';

export function useAttributeKeys(params?: MetadataParams) {
  const [keys, setKeys] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const { getSignal } = useAbortController();

  useEffect(() => {
    const loadKeys = async () => {
      setLoading(true);
      setError(null);

      try {
        const response = await fetchAttributeKeys(params, getSignal());
        setKeys(response.data.attributeKeys);
      } catch (err) {
        if (err instanceof Error && err.name !== 'AbortError') {
          setError(err);
        }
      } finally {
        setLoading(false);
      }
    };

    loadKeys();
  }, [params?.start, params?.end]); // Re-fetch when time range changes

  return { keys, loading, error };
}

export function useAttributeValues(key: string | null, params?: MetadataParams) {
  const [values, setValues] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [limited, setLimited] = useState(false);
  const { getSignal } = useAbortController();

  useEffect(() => {
    if (!key) {
      setValues([]);
      setLimited(false);
      return;
    }

    const loadValues = async () => {
      setLoading(true);
      setError(null);

      try {
        const response = await fetchAttributeValues(key, params, getSignal());
        setValues(response.data.values);
        setLimited(response.data.limited);
      } catch (err) {
        if (err instanceof Error && err.name !== 'AbortError') {
          setError(err);
        }
      } finally {
        setLoading(false);
      }
    };

    loadValues();
  }, [key, params?.start, params?.end, params?.limit]);

  return { values, loading, error, limited };
}
