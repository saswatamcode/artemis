// Time parsing and formatting utilities

import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import utc from 'dayjs/plugin/utc';

dayjs.extend(relativeTime);
dayjs.extend(utc);

export function parseTime(input: string): Date {
  // Try parsing as unix timestamp (seconds or nanoseconds)
  const num = Number(input);
  if (!isNaN(num)) {
    // If number > 1e12, assume nanoseconds, otherwise seconds
    if (num > 1e12) {
      return new Date(num / 1e6); // Convert nanoseconds to milliseconds
    }
    return new Date(num * 1000); // Convert seconds to milliseconds
  }

  // Try parsing as RFC3339 or ISO string
  const date = new Date(input);
  if (!isNaN(date.getTime())) {
    return date;
  }

  throw new Error(`Invalid time format: ${input}`);
}

export function formatTimestamp(
  timestamp: number,
  format: 'short' | 'long' | 'relative' = 'short'
): string {
  const date = dayjs(timestamp);

  switch (format) {
    case 'short':
      return date.format('HH:mm:ss');
    case 'long':
      return date.format('YYYY-MM-DD HH:mm:ss');
    case 'relative':
      return date.fromNow();
    default:
      return date.format();
  }
}

export function formatTimeRange(start: Date, end: Date): string {
  const duration = end.getTime() - start.getTime();
  return `${dayjs(start).format('HH:mm:ss')} - ${dayjs(end).format('HH:mm:ss')} (${formatDuration(duration)})`;
}

export function formatDuration(durationMs: number): string {
  if (durationMs < 1000) {
    return `${Math.round(durationMs)}ms`;
  }
  if (durationMs < 60000) {
    return `${(durationMs / 1000).toFixed(1)}s`;
  }
  if (durationMs < 3600000) {
    return `${(durationMs / 60000).toFixed(1)}m`;
  }
  return `${(durationMs / 3600000).toFixed(1)}h`;
}

export function getTimeRangePresets(): Array<{
  label: string;
  value: () => { start: Date; end: Date };
}> {
  const now = new Date();

  return [
    {
      label: 'Last 5 minutes',
      value: () => ({
        start: new Date(now.getTime() - 5 * 60 * 1000),
        end: now,
      }),
    },
    {
      label: 'Last 15 minutes',
      value: () => ({
        start: new Date(now.getTime() - 15 * 60 * 1000),
        end: now,
      }),
    },
    {
      label: 'Last 30 minutes',
      value: () => ({
        start: new Date(now.getTime() - 30 * 60 * 1000),
        end: now,
      }),
    },
    {
      label: 'Last 1 hour',
      value: () => ({
        start: new Date(now.getTime() - 60 * 60 * 1000),
        end: now,
      }),
    },
    {
      label: 'Last 3 hours',
      value: () => ({
        start: new Date(now.getTime() - 3 * 60 * 60 * 1000),
        end: now,
      }),
    },
    {
      label: 'Last 6 hours',
      value: () => ({
        start: new Date(now.getTime() - 6 * 60 * 60 * 1000),
        end: now,
      }),
    },
    {
      label: 'Last 12 hours',
      value: () => ({
        start: new Date(now.getTime() - 12 * 60 * 60 * 1000),
        end: now,
      }),
    },
    {
      label: 'Last 24 hours',
      value: () => ({
        start: new Date(now.getTime() - 24 * 60 * 60 * 1000),
        end: now,
      }),
    },
  ];
}

export function toUnixTimestamp(date: Date): string {
  return Math.floor(date.getTime() / 1000).toString();
}

export function toUnixNanoseconds(date: Date): string {
  return (date.getTime() * 1e6).toString();
}
