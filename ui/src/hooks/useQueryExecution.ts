// Hook for executing queries with debouncing

import { useState, useCallback, useRef, useEffect } from 'react';
import { executeQueryRange } from '../api/queryRange';
import { useAbortController } from './useAbortController';
import type { QueryRangeParams, QueryRangeResponse } from '../api/types';

export interface QueryExecutionState {
  data: QueryRangeResponse | null;
  loading: boolean;
  error: Error | null;
}

export function useQueryExecution(debounceMs: number = 250) {
  const [state, setState] = useState<QueryExecutionState>({
    data: null,
    loading: false,
    error: null,
  });

  const { getSignal } = useAbortController();
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Clean up debounce timer on unmount
  useEffect(() => {
    return () => {
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
    };
  }, []);

  const execute = useCallback(
    (params: QueryRangeParams, immediate: boolean = false) => {
      // Clear existing debounce timer
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }

      const doExecute = async () => {
        setState({ data: null, loading: true, error: null });

        try {
          const response = await executeQueryRange(params, getSignal());
          setState({ data: response, loading: false, error: null });
        } catch (err) {
          if (err instanceof Error && err.name !== 'AbortError') {
            setState({ data: null, loading: false, error: err });
          }
        }
      };

      if (immediate) {
        doExecute();
      } else {
        debounceTimerRef.current = setTimeout(doExecute, debounceMs);
      }
    },
    [debounceMs, getSignal]
  );

  const reset = useCallback(() => {
    setState({ data: null, loading: false, error: null });
  }, []);

  return { ...state, execute, reset };
}
