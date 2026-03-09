// Heatmap visualization for duration distribution over time

import { Box, Text, Paper } from '@mantine/core';
import { useEffect, useRef, useMemo, useState } from 'react';
import type { QueryRangeData } from '../../api/types';

interface HeatmapChartProps {
  data: QueryRangeData;
  onExemplarClick?: (traceID: string) => void;
}

// Duration bucket bounds (exponential scale, matches backend calculation)
const getDurationBounds = (bucket: number): [number, number] => {
  const start = Math.pow(2, bucket);
  const end = Math.pow(2, bucket + 1);
  return [start, end];
};

// Calculate which bucket a duration falls into (matches backend algorithm)
// Backend uses: bits.Len64(uint64(durationNs)) - 1
// Which is equivalent to: floor(log2(durationNs)) for powers of 2, but slightly different for non-powers
// bits.Len64 returns the number of bits needed to represent the number
const calculateDurationBucket = (durationNs: number): number => {
  if (durationNs <= 0) return 0;
  // JavaScript equivalent of bits.Len64(x) - 1
  // Math.floor(Math.log2(x)) is close but not exact for all numbers
  // bits.Len64 = position of highest set bit = floor(log2(x)) + 1 for x > 0
  // So bits.Len64(x) - 1 = floor(log2(x))
  return Math.floor(Math.log2(durationNs));
};

const formatDuration = (ns: number): string => {
  if (ns < 1000) return `${ns}ns`;
  if (ns < 1000000) return `${(ns / 1000).toFixed(0)}µs`;
  if (ns < 1000000000) return `${(ns / 1000000).toFixed(0)}ms`;
  return `${(ns / 1000000000).toFixed(1)}s`;
};

export function HeatmapChart({ data, onExemplarClick }: HeatmapChartProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [hoveredExemplar, setHoveredExemplar] = useState<{
    traceID: string;
    spanID: string;
    duration: number;
    x: number;
    y: number;
  } | null>(null);

  // Parse heatmap data from matrix format
  const heatmapData = useMemo(() => {
    if (!data.result || data.result.length === 0) {
      return null;
    }

    // Parse data: each series has duration_bucket label
    const cells: Array<{
      time: number;
      bucket: number;
      count: number;
      range: string;
    }> = [];

    for (const series of data.result) {
      const durationBucket = parseInt(series.metric.duration_bucket || '0');
      const durationRange = series.metric.duration_range || '';

      if (series.values) {
        for (const [timestamp, countStr] of series.values) {
          cells.push({
            time: timestamp,
            bucket: durationBucket,
            count: parseFloat(countStr),
            range: durationRange,
          });
        }
      }
    }

    if (cells.length === 0) return null;

    // Get time range and bucket range
    const times = [...new Set(cells.map((c) => c.time))].sort((a, b) => a - b);
    const buckets = [...new Set(cells.map((c) => c.bucket))].sort((a, b) => a - b);
    const maxCount = Math.max(...cells.map((c) => c.count));

    // Build grid
    const grid = new Map<string, number>();
    for (const cell of cells) {
      grid.set(`${cell.time}_${cell.bucket}`, cell.count);
    }

    // Debug logging
    console.log('[Heatmap] Grid info:', {
      timeSteps: times.length,
      durationBuckets: buckets.length,
      bucketRange: `${buckets[0]} to ${buckets[buckets.length - 1]}`,
      totalCells: cells.length,
      maxCount,
    });

    return { cells, times, buckets, maxCount, grid };
  }, [data.result]);

  // Extract exemplars
  const exemplars = useMemo(() => {
    const exs: Array<{
      time: number;
      duration: number;
      traceID: string;
      spanID: string;
    }> = [];

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

    // Debug logging
    if (exs.length > 0) {
      console.log(`[Heatmap] Found ${exs.length} exemplars`);
      const sample = exs[0];
      console.log('[Heatmap] Sample exemplar:', {
        duration: sample.duration,
        durationMs: (sample.duration / 1_000_000).toFixed(2) + 'ms',
        calculatedBucket: calculateDurationBucket(sample.duration),
        traceID: sample.traceID.slice(0, 16) + '...',
      });
      const buckets = exs.map(ex => calculateDurationBucket(ex.duration));
      console.log('[Heatmap] Exemplar buckets (unique):', Array.from(new Set(buckets)).sort((a, b) => a - b));
      console.log('[Heatmap] Duration range:', {
        min: (Math.min(...exs.map(e => e.duration)) / 1_000_000).toFixed(2) + 'ms',
        max: (Math.max(...exs.map(e => e.duration)) / 1_000_000).toFixed(2) + 'ms',
      });
    }

    return exs;
  }, [data.result]);

  // Draw heatmap
  useEffect(() => {
    if (!heatmapData || !canvasRef.current) return;

    const canvas = canvasRef.current;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    // Use device pixel ratio for crisp rendering on retina displays
    const dpr = window.devicePixelRatio || 1;
    const displayWidth = 1200;
    const displayHeight = 500;

    // Set actual canvas size (accounting for device pixel ratio)
    canvas.width = displayWidth * dpr;
    canvas.height = displayHeight * dpr;

    // Scale context to match device pixel ratio
    ctx.scale(dpr, dpr);

    const width = displayWidth;
    const height = displayHeight;
    const { times, buckets, grid, maxCount } = heatmapData;

    const marginLeft = 80;
    const marginRight = 40;
    const marginTop = 40;
    const marginBottom = 80;

    const chartWidth = width - marginLeft - marginRight;
    const chartHeight = height - marginTop - marginBottom;

    const cellWidth = chartWidth / times.length;
    const cellHeight = chartHeight / buckets.length;

    // Clear
    ctx.clearRect(0, 0, width, height);

    // Background
    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0, 0, width, height);

    // Draw cells
    for (let i = 0; i < times.length; i++) {
      for (let j = 0; j < buckets.length; j++) {
        const time = times[i];
        const bucket = buckets[j];
        const count = grid.get(`${time}_${bucket}`) || 0;

        if (count === 0) continue;

        const x = marginLeft + i * cellWidth;
        const y = marginTop + (buckets.length - j - 1) * cellHeight; // Reverse Y (smallest at bottom)

        // Color intensity based on count (blue gradient)
        const intensity = count / maxCount;
        const blue = Math.round(59 + intensity * 196); // 59 to 255
        const green = Math.round(130 + intensity * 125); // 130 to 255
        ctx.fillStyle = `rgb(${59 + Math.round(intensity * 50)}, ${green}, ${blue})`;
        ctx.fillRect(x, y, cellWidth, cellHeight);

        // Border
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.3)';
        ctx.strokeRect(x, y, cellWidth, cellHeight);
      }
    }

    // Draw exemplars as red dots
    if (exemplars.length > 0) {
      for (const ex of exemplars) {
        // Find time index (find closest time bucket)
        let timeIdx = -1;
        let minTimeDiff = Infinity;
        for (let i = 0; i < times.length; i++) {
          const diff = Math.abs(times[i] - ex.time);
          if (diff < minTimeDiff) {
            minTimeDiff = diff;
            timeIdx = i;
          }
        }
        if (timeIdx === -1) continue;

        // Calculate which duration bucket this exemplar belongs to
        const calculatedBucket = calculateDurationBucket(ex.duration);

        // Find this bucket in our buckets array
        const bucketIdx = buckets.indexOf(calculatedBucket);
        if (bucketIdx === -1) continue;

        const x = marginLeft + timeIdx * cellWidth + cellWidth / 2;
        const y = marginTop + (buckets.length - bucketIdx - 1) * cellHeight + cellHeight / 2;

        // Check if this exemplar is hovered
        const isHovered = hoveredExemplar?.traceID === ex.traceID;
        const radius = isHovered ? 5 : 3; // Smaller default, larger on hover

        // Draw red dot with white border
        ctx.fillStyle = '#ef4444';
        ctx.beginPath();
        ctx.arc(x, y, radius, 0, Math.PI * 2);
        ctx.fill();

        // White border for visibility
        ctx.strokeStyle = '#fff';
        ctx.lineWidth = isHovered ? 2.5 : 1.5;
        ctx.stroke();

        // Add outer glow on hover
        if (isHovered) {
          ctx.strokeStyle = 'rgba(239, 68, 68, 0.3)';
          ctx.lineWidth = 4;
          ctx.beginPath();
          ctx.arc(x, y, radius + 2, 0, Math.PI * 2);
          ctx.stroke();
        }
      }
    }

    // Y-axis labels (duration buckets)
    ctx.fillStyle = '#666';
    ctx.font = '11px sans-serif';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';

    const step = Math.max(1, Math.floor(buckets.length / 10)); // Show max 10 labels
    for (let j = 0; j < buckets.length; j += step) {
      const bucket = buckets[j];
      const [start] = getDurationBounds(bucket);
      const y = marginTop + (buckets.length - j - 1) * cellHeight + cellHeight / 2;
      ctx.fillText(formatDuration(start), marginLeft - 10, y);
    }

    // Y-axis title
    ctx.save();
    ctx.translate(20, marginTop + chartHeight / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.textAlign = 'center';
    ctx.font = 'bold 12px sans-serif';
    ctx.fillStyle = '#333';
    ctx.fillText('Duration', 0, 0);
    ctx.restore();

    // X-axis labels (time)
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';

    const timeStep = Math.max(1, Math.floor(times.length / 8)); // Show max 8 labels
    for (let i = 0; i < times.length; i += timeStep) {
      const time = times[i];
      const x = marginLeft + i * cellWidth + cellWidth / 2;
      const date = new Date(time * 1000);
      const label = date.toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit',
      });
      ctx.fillText(label, x, height - marginBottom + 10);
    }

    // X-axis title
    ctx.textAlign = 'center';
    ctx.font = 'bold 12px sans-serif';
    ctx.fillStyle = '#333';
    ctx.fillText('Time', marginLeft + chartWidth / 2, height - 20);

    // Title
    ctx.textAlign = 'center';
    ctx.font = 'bold 14px sans-serif';
    ctx.fillStyle = '#333';
    ctx.fillText('Latency Heatmap', width / 2, 20);

    // Legend
    if (exemplars.length > 0) {
      ctx.textAlign = 'left';
      ctx.font = '11px sans-serif';
      ctx.fillStyle = '#ef4444';
      ctx.beginPath();
      ctx.arc(width - marginRight - 60, 20, 4, 0, Math.PI * 2);
      ctx.fill();
      ctx.strokeStyle = '#fff';
      ctx.lineWidth = 2;
      ctx.stroke();
      ctx.fillStyle = '#666';
      ctx.fillText(`${exemplars.length} exemplars`, width - marginRight - 50, 24);
    }
  }, [heatmapData, exemplars, hoveredExemplar]);

  // Handle mouse move for hover effects
  const handleMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (!heatmapData || exemplars.length === 0) return;

    const rect = canvasRef.current?.getBoundingClientRect();
    if (!rect) return;

    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;

    // Scale to canvas coordinates
    const scaleX = 1200 / rect.width;
    const scaleY = 500 / rect.height;
    const canvasX = x * scaleX;
    const canvasY = y * scaleY;

    // Heatmap layout constants
    const marginLeft = 80;
    const marginTop = 40;
    const marginRight = 40;
    const marginBottom = 80;
    const chartWidth = 1200 - marginLeft - marginRight;
    const chartHeight = 500 - marginTop - marginBottom;

    const { times, buckets } = heatmapData;
    const cellWidth = chartWidth / times.length;
    const cellHeight = chartHeight / buckets.length;

    // Find hovered exemplar
    let hoveredEx: typeof hoveredExemplar = null;
    const hoverRadius = 15; // Larger hover detection radius

    for (const ex of exemplars) {
      let timeIdx = -1;
      let minTimeDiff = Infinity;
      for (let i = 0; i < times.length; i++) {
        const diff = Math.abs(times[i] - ex.time);
        if (diff < minTimeDiff) {
          minTimeDiff = diff;
          timeIdx = i;
        }
      }
      if (timeIdx === -1) continue;

      const calculatedBucket = calculateDurationBucket(ex.duration);
      const bucketIdx = buckets.indexOf(calculatedBucket);
      if (bucketIdx === -1) continue;

      const exX = marginLeft + timeIdx * cellWidth + cellWidth / 2;
      const exY = marginTop + (buckets.length - bucketIdx - 1) * cellHeight + cellHeight / 2;

      const dist = Math.sqrt(Math.pow(canvasX - exX, 2) + Math.pow(canvasY - exY, 2));
      if (dist < hoverRadius) {
        hoveredEx = {
          traceID: ex.traceID,
          spanID: ex.spanID,
          duration: ex.duration,
          x: e.clientX,
          y: e.clientY,
        };
        break;
      }
    }

    setHoveredExemplar(hoveredEx);
  };

  if (!heatmapData) {
    return (
      <Box p="xl" style={{ textAlign: 'center' }}>
        <Text c="dimmed">No heatmap data to display</Text>
        <Text size="sm" c="dimmed" mt="xs">
          Use query: heatmap(&#123;selector&#125;)
        </Text>
      </Box>
    );
  }

  return (
    <Paper p="md" withBorder shadow="sm" style={{ backgroundColor: '#fafafa', position: 'relative' }}>
      <canvas
        ref={canvasRef}
        style={{
          width: '100%',
          height: 'auto',
          display: 'block',
          cursor: hoveredExemplar ? 'pointer' : 'default',
        }}
        onMouseMove={handleMouseMove}
        onMouseLeave={() => setHoveredExemplar(null)}
        onClick={(e) => {
          if (!onExemplarClick || exemplars.length === 0 || !heatmapData) return;

          // Get click coordinates relative to canvas
          const rect = canvasRef.current?.getBoundingClientRect();
          if (!rect) return;

          const x = e.clientX - rect.left;
          const y = e.clientY - rect.top;

          // Scale to canvas coordinates
          const scaleX = 1200 / rect.width;
          const scaleY = 500 / rect.height;
          const canvasX = x * scaleX;
          const canvasY = y * scaleY;

          // Heatmap layout constants (must match drawing code)
          const marginLeft = 80;
          const marginTop = 40;
          const marginRight = 40;
          const marginBottom = 80;
          const chartWidth = 1200 - marginLeft - marginRight;
          const chartHeight = 500 - marginTop - marginBottom;

          const { times, buckets } = heatmapData;
          const cellWidth = chartWidth / times.length;
          const cellHeight = chartHeight / buckets.length;

          // Find clicked exemplar (within 10px radius)
          let clickedExemplar: typeof exemplars[0] | null = null;
          let minDist = 10; // 10px click tolerance

          for (const ex of exemplars) {
            // Find positions (same logic as drawing)
            let timeIdx = -1;
            let minTimeDiff = Infinity;
            for (let i = 0; i < times.length; i++) {
              const diff = Math.abs(times[i] - ex.time);
              if (diff < minTimeDiff) {
                minTimeDiff = diff;
                timeIdx = i;
              }
            }
            if (timeIdx === -1) continue;

            const calculatedBucket = calculateDurationBucket(ex.duration);
            const bucketIdx = buckets.indexOf(calculatedBucket);
            if (bucketIdx === -1) continue;

            const exX = marginLeft + timeIdx * cellWidth + cellWidth / 2;
            const exY = marginTop + (buckets.length - bucketIdx - 1) * cellHeight + cellHeight / 2;

            // Calculate distance from click to exemplar
            const dist = Math.sqrt(Math.pow(canvasX - exX, 2) + Math.pow(canvasY - exY, 2));
            if (dist < minDist) {
              minDist = dist;
              clickedExemplar = ex;
            }
          }

          if (clickedExemplar) {
            onExemplarClick(clickedExemplar.traceID);
          }
        }}
      />
      {/* Hover tooltip */}
      {hoveredExemplar && (
        <Box
          style={{
            position: 'fixed',
            left: hoveredExemplar.x + 15,
            top: hoveredExemplar.y - 10,
            backgroundColor: 'rgba(255, 255, 255, 0.98)',
            border: '1px solid #d0d0d0',
            borderRadius: '6px',
            padding: '8px 12px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            pointerEvents: 'none',
            zIndex: 1000,
            maxWidth: '300px',
          }}
        >
          <Text size="sm" fw={600} c="#333" mb={4}>
            Duration: {formatDuration(hoveredExemplar.duration)}
          </Text>
          <Text size="xs" c="#3b82f6" fw={500} style={{ fontFamily: 'monospace' }}>
            Click to view trace →
          </Text>
          <Text
            size="xs"
            c="dimmed"
            mt={4}
            style={{
              fontFamily: 'monospace',
              fontSize: '9px',
              wordBreak: 'break-all',
            }}
          >
            {hoveredExemplar.traceID}
          </Text>
        </Box>
      )}

      <Box mt="md">
        <Text size="xs" c="dimmed" ta="center">
          {heatmapData.times.length} time buckets × {heatmapData.buckets.length} duration buckets
          {exemplars.length > 0 && ` • Hover over red dots to see trace info`}
        </Text>
      </Box>
    </Paper>
  );
}
