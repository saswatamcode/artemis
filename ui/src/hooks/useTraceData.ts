// Hook for fetching and building trace trees

import { useState, useEffect } from 'react';
import { fetchTrace } from '../api/queryTrace';
import { buildSpanTree } from '../utils/traceTreeBuilder';
import { useAbortController } from './useAbortController';
import type { TraceParams } from '../api/types';
import type { TraceTree } from '../utils/traceTreeBuilder';

export interface TraceDataState {
  tree: TraceTree | null;
  loading: boolean;
  error: Error | null;
  traceID: string | null;
}

export function useTraceData(params: TraceParams | null) {
  const [state, setState] = useState<TraceDataState>({
    tree: null,
    loading: false,
    error: null,
    traceID: null,
  });

  const { getSignal } = useAbortController();

  useEffect(() => {
    if (!params?.traceID) {
      setState({
        tree: null,
        loading: false,
        error: null,
        traceID: null,
      });
      return;
    }

    const loadTrace = async () => {
      setState((prev) => ({
        ...prev,
        loading: true,
        error: null,
        traceID: params.traceID,
      }));

      try {
        const response = await fetchTrace(params, getSignal());
        const tree = buildSpanTree(response.data.spans);

        setState({
          tree,
          loading: false,
          error: null,
          traceID: params.traceID,
        });
      } catch (err) {
        if (err instanceof Error && err.name !== 'AbortError') {
          setState({
            tree: null,
            loading: false,
            error: err,
            traceID: params.traceID,
          });
        }
      }
    };

    loadTrace();
  }, [params?.traceID, params?.start, params?.end]);

  return state;
}
