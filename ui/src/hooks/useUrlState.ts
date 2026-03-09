// Hook for synchronizing state with URL query parameters

import { useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';

export function useUrlState<T extends Record<string, string | undefined>>(
  defaultValues: T
): [T, (updates: Partial<T>) => void] {
  const [searchParams, setSearchParams] = useSearchParams();

  // Parse current state from URL
  const state = { ...defaultValues };
  for (const key in defaultValues) {
    const value = searchParams.get(key);
    if (value !== null) {
      state[key] = value as T[Extract<keyof T, string>];
    }
  }

  // Update URL with new state
  const updateState = useCallback(
    (updates: Partial<T>) => {
      setSearchParams((prev) => {
        const newParams = new URLSearchParams(prev);

        for (const key in updates) {
          const value = updates[key];
          if (value !== undefined && value !== '') {
            newParams.set(key, value);
          } else {
            newParams.delete(key);
          }
        }

        return newParams;
      });
    },
    [setSearchParams]
  );

  return [state, updateState];
}
