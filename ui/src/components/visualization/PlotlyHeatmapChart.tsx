import { useMemo } from 'react';
import Plot from 'react-plotly.js';
import { Box, Text, Paper } from '@mantine/core';
import type { QueryRangeData } from '../../api/types';

interface PlotlyHeatmapChartProps {
  data: QueryRangeData;
  onExemplarClick?: (traceID: string) => void;
}

interface HeatmapCell {
  time: number; // Unix timestamp (seconds)
  bucket: number; // Duration bucket index
  count: number;
}

interface Exemplar {
  time: number;
  duration: number;
  traceID: string;
  spanID: string;
}

// Calculate which bucket a duration falls into (matches backend)
const calculateDurationBucket = (durationNs: number): number => {
  if (durationNs <= 0) return 0;
  return Math.floor(Math.log2(durationNs));
};

// Format duration in nanoseconds to human-readable string
const formatDuration = (ns: number): string => {
  if (ns < 1000) return `${ns}ns`;
  if (ns < 1_000_000) return `${(ns / 1000).toFixed(1)}µs`;
  if (ns < 1_000_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`;
  return `${(ns / 1_000_000_000).toFixed(2)}s`;
};

// Get duration range for a bucket
const getDurationRange = (bucket: number): string => {
  const start = Math.pow(2, bucket);
  const end = Math.pow(2, bucket + 1);
  return `${formatDuration(start)} - ${formatDuration(end)}`;
};

export function PlotlyHeatmapChart({ data, onExemplarClick }: PlotlyHeatmapChartProps) {
  // Extract heatmap cells from data
  // Each series represents one duration bucket with values over time
  const cells = useMemo(() => {
    const cells: HeatmapCell[] = [];

    for (const series of data.result || []) {
      const durationBucket = parseInt(series.metric.duration_bucket || '0', 10);

      // Each value is [timestamp, count]
      if (series.values && durationBucket !== undefined) {
        for (const [timestamp, countStr] of series.values) {
          const count = parseFloat(countStr);
          if (count > 0) {
            cells.push({
              time: timestamp, // Already in seconds
              bucket: durationBucket,
              count,
            });
          }
        }
      }
    }

    console.log('[PlotlyHeatmap] Parsed cells:', cells.length);
    return cells;
  }, [data.result]);

  // Extract exemplars
  const exemplars = useMemo(() => {
    const exs: Exemplar[] = [];

    for (const series of data.result || []) {
      if (series.exemplars) {
        for (const ex of series.exemplars) {
          exs.push({
            time: ex.timestamp,
            duration: ex.duration,
            traceID: ex.traceID,
            spanID: ex.spanID,
          });
        }
      }
    }

    console.log(`[PlotlyHeatmap] Found ${exs.length} exemplars`);
    return exs;
  }, [data.result]);

  // Build heatmap data structure
  const plotData = useMemo(() => {
    if (cells.length === 0) return null;

    // Get unique times and buckets
    const times = [...new Set(cells.map((c) => c.time))].sort((a, b) => a - b);
    const buckets = [...new Set(cells.map((c) => c.bucket))].sort((a, b) => a - b);

    // Build 2D matrix for heatmap (buckets × times)
    const z: number[][] = [];
    const text: string[][] = [];

    for (let i = buckets.length - 1; i >= 0; i--) {
      const bucket = buckets[i];
      const row: number[] = [];
      const textRow: string[] = [];

      for (const time of times) {
        const cell = cells.find((c) => c.time === time && c.bucket === bucket);
        const count = cell?.count || 0;
        row.push(count);
        textRow.push(
          `Time: ${new Date(time * 1000).toLocaleString()}<br>` +
            `Bucket: ${getDurationRange(bucket)}<br>` +
            `Count: ${count}`
        );
      }

      z.push(row);
      text.push(textRow);
    }

    // Format time labels
    const xLabels = times.map((t) => {
      const date = new Date(t * 1000);
      return date.toLocaleTimeString();
    });

    // Format bucket labels (reversed for display)
    const yLabels = buckets.slice().reverse().map((b) => getDurationRange(b));

    console.log('[PlotlyHeatmap] Grid:', {
      timeSteps: times.length,
      durationBuckets: buckets.length,
      totalCells: cells.length,
    });

    return { z, text, xLabels, yLabels, times, buckets };
  }, [cells]);

  if (!plotData) {
    return (
      <Box p="xl" style={{ textAlign: 'center' }}>
        <Text c="dimmed">No heatmap data to display</Text>
      </Box>
    );
  }

  const { z, text, xLabels, yLabels, times, buckets } = plotData;

  // Prepare exemplar scatter points
  // Map exemplars to their actual time labels and bucket labels
  const exemplarTrace = exemplars.length > 0 ? {
    x: exemplars.map((ex) => {
      // Find the closest time in our time buckets
      const closestTimeIdx = times.findIndex((t, idx) => {
        if (idx === times.length - 1) return true;
        const nextT = times[idx + 1];
        return ex.time >= t && ex.time < nextT;
      });
      // Return the x label (time string)
      return xLabels[closestTimeIdx >= 0 ? closestTimeIdx : 0];
    }),
    y: exemplars.map((ex) => {
      const bucket = calculateDurationBucket(ex.duration);
      // Find this bucket in our sorted buckets array
      const bucketIdx = buckets.indexOf(bucket);
      if (bucketIdx === -1) {
        console.warn('[PlotlyHeatmap] Exemplar bucket not found:', bucket, 'for duration:', ex.duration);
        return yLabels[0]; // Fallback to first bucket
      }
      // Return the y label (duration range string) - reversed
      const reversedIdx = buckets.length - 1 - bucketIdx;
      return yLabels[reversedIdx];
    }),
    mode: 'markers' as const,
    type: 'scatter' as const,
    marker: {
      size: 10,
      color: '#ef4444',
      symbol: 'circle',
      line: {
        color: '#fff',
        width: 2,
      },
    },
    text: exemplars.map((ex) =>
      `Duration: ${formatDuration(ex.duration)}<br>` +
      `Bucket: ${calculateDurationBucket(ex.duration)}<br>` +
      `Click to view trace`
    ),
    hovertemplate: '%{text}<extra></extra>',
    name: 'Exemplars',
    showlegend: false,
  } : null;

  return (
    <Paper p="md" withBorder shadow="sm">
      <Plot
        data={[
          {
            z,
            text,
            x: xLabels,
            y: yLabels,
            type: 'heatmap',
            colorscale: [
              [0, '#eff6ff'],
              [0.2, '#bfdbfe'],
              [0.4, '#60a5fa'],
              [0.6, '#3b82f6'],
              [0.8, '#2563eb'],
              [1, '#1e40af'],
            ],
            hovertemplate: '%{text}<extra></extra>',
            showscale: true,
            colorbar: {
              title: 'Count',
              thickness: 15,
              len: 0.7,
            },
          },
          ...(exemplarTrace ? [exemplarTrace] : []),
        ]}
        layout={{
          title: 'Latency Heatmap',
          xaxis: {
            title: 'Time',
            side: 'bottom',
            tickangle: -45,
          },
          yaxis: {
            title: 'Duration',
            autorange: true,
          },
          hovermode: 'closest',
          width: 1200,
          height: 600,
          margin: {
            l: 150,
            r: 100,
            t: 50,
            b: 100,
          },
        }}
        config={{
          displayModeBar: true,
          displaylogo: false,
          modeBarButtonsToRemove: ['lasso2d', 'select2d'],
        }}
        onClick={(event: any) => {
          if (!onExemplarClick || !event.points || event.points.length === 0) return;

          const point = event.points[0];

          // Check if this is an exemplar click (scatter trace)
          if (point.data.type === 'scatter') {
            const pointIndex = point.pointIndex;
            const exemplar = exemplars[pointIndex];
            if (exemplar) {
              onExemplarClick(exemplar.traceID);
            }
          }
        }}
      />

      <Box mt="md">
        <Text size="xs" c="dimmed" ta="center">
          {cells.length} cells across {times.length} time steps × {buckets.length} duration buckets
          {exemplars.length > 0 && ` • ${exemplars.length} exemplars (red dots - click to view trace)`}
        </Text>
      </Box>
    </Paper>
  );
}
