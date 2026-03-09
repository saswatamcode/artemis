// Duration formatting utilities for trace spans

export function formatNanoDuration(nanos: number): string {
  if (nanos < 1000) {
    return `${nanos}ns`;
  }
  if (nanos < 1000000) {
    return `${(nanos / 1000).toFixed(2)}μs`;
  }
  if (nanos < 1000000000) {
    return `${(nanos / 1000000).toFixed(2)}ms`;
  }
  return `${(nanos / 1000000000).toFixed(2)}s`;
}

export function formatDurationCompact(nanos: number): string {
  if (nanos < 1000) {
    return `${nanos}ns`;
  }
  if (nanos < 1000000) {
    return `${Math.round(nanos / 1000)}μs`;
  }
  if (nanos < 1000000000) {
    return `${Math.round(nanos / 1000000)}ms`;
  }
  return `${(nanos / 1000000000).toFixed(1)}s`;
}

export function nanosToMillis(nanos: number): number {
  return nanos / 1000000;
}

export function millisToNanos(millis: number): number {
  return millis * 1000000;
}
