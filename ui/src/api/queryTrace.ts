// Query trace API endpoint

import { api } from './client';
import type { TraceResponse, TraceParams } from './types';

export async function fetchTrace(
  params: TraceParams,
  signal?: AbortSignal
): Promise<TraceResponse> {
  // Build query string
  const queryParams = new URLSearchParams();
  queryParams.set('traceID', params.traceID);

  if (params.start) {
    queryParams.set('start', params.start);
  }

  if (params.end) {
    queryParams.set('end', params.end);
  }

  const endpoint = `/query/trace?${queryParams.toString()}`;

  return api.get<TraceResponse>(endpoint, signal);
}
