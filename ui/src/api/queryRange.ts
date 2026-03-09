// Query range API endpoint

import { api } from './client';
import type { QueryRangeResponse, QueryRangeParams } from './types';

export async function executeQueryRange(
  params: QueryRangeParams,
  signal?: AbortSignal
): Promise<QueryRangeResponse> {
  // Build query string
  const queryParams = new URLSearchParams();
  queryParams.set('query', params.query);
  queryParams.set('start', params.start);
  queryParams.set('end', params.end);

  if (params.step) {
    queryParams.set('step', params.step);
  }

  if (params.exemplars !== undefined) {
    queryParams.set('exemplars', params.exemplars.toString());
  }

  if (params.exemplar_strategy) {
    queryParams.set('exemplar_strategy', params.exemplar_strategy);
  }

  const endpoint = `/query_range?${queryParams.toString()}`;

  return api.get<QueryRangeResponse>(endpoint, signal);
}
