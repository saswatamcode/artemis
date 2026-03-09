// API response types matching backend queryapi models

// Metadata responses
export interface MetadataAttributeKeysResponse {
  status: string;
  data: {
    attributeKeys: string[];
    count: number;
  };
}

export interface MetadataAttributeValuesResponse {
  status: string;
  data: {
    attributeKey: string;
    values: string[];
    count: number;
    limited: boolean;
  };
}

// Query range response (Prometheus-compatible)
export interface QueryRangeResponse {
  status: string;
  data: QueryRangeData;
}

export interface QueryRangeData {
  resultType: 'vector' | 'matrix' | 'scalar';
  result: PrometheusResult[];
  stats?: QueryStats;
}

export interface QueryStats {
  spansScanned: number;
  blocksScanned: number;
  executionTime: string;
}

// Prometheus result formats
export interface PrometheusResult {
  metric: Record<string, string>;
  values?: [number, string][]; // [timestamp, value] pairs for matrix
  value?: [number, string]; // [timestamp, value] for vector
  exemplars?: Exemplar[]; // Optional trace exemplars
}

// Exemplar attached to metric data points
export interface Exemplar {
  timestamp: number; // Unix seconds
  traceID: string;
  spanID: string;
  duration: number; // Nanoseconds
  labels: Record<string, string>;
}

// Trace detail response
export interface TraceResponse {
  status: string;
  data: TraceData;
}

export interface TraceData {
  traceID: string;
  spans: SpanDetail[];
  spanCount: number;
}

export interface SpanDetail {
  spanID: string;
  name: string;
  startTime: number; // Unix nanoseconds
  endTime: number; // Unix nanoseconds
  duration: number; // Nanoseconds
  serviceName: string;
  attributes: Record<string, string>;
  parentSpanID?: string;
}

// Span tree node for UI rendering
export interface SpanNode extends SpanDetail {
  children: SpanNode[];
  depth: number;
}

// Error response
export interface ErrorResponse {
  status: 'error';
  errorType: string;
  error: string;
}

// Query parameters
export interface QueryRangeParams {
  query: string;
  start: string; // Unix timestamp or RFC3339
  end: string;
  step?: string; // e.g., "15s", "1m"
  exemplars?: number;
  exemplar_strategy?: 'slowest' | 'fastest' | 'random';
}

export interface MetadataParams {
  key?: string; // For attribute_values endpoint
  start?: string;
  end?: string;
  limit?: number;
}

export interface TraceParams {
  traceID: string;
  start?: string;
  end?: string;
}
