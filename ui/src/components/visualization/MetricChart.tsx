// Metric chart using Recharts

import {
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Scatter,
  ComposedChart,
} from 'recharts';
import { Box, Text } from '@mantine/core';
import { useMemo, useState } from 'react';
import { formatTimestamp } from '../../utils/timeFormatting';
import type { QueryRangeData } from '../../api/types';

interface MetricChartProps {
  data: QueryRangeData;
  onExemplarClick?: (traceID: string) => void;
}

export function MetricChart({ data, onExemplarClick }: MetricChartProps) {
  // Track which series are visible (all visible by default)
  const [visibleSeries, setVisibleSeries] = useState<Set<string>>(new Set());

  if (!data.result || data.result.length === 0) {
    return (
      <Box p="xl" style={{ textAlign: 'center' }}>
        <Text c="dimmed">No data to display</Text>
      </Box>
    );
  }

  // Memoize chart data transformation to prevent recalculation on every render
  const { chartData, seriesIds, seriesIdMap, exemplarData } = useMemo(() => {
    const chartData: Array<{ timestamp: number; [key: string]: number }> = [];
    const seriesMap = new Map<number, { timestamp: number; [key: string]: number }>();
    const seriesIdMap = new Map<string, string>();
    let seriesCounter = 0;
    const exemplarData: Array<{ timestamp: number; value: number; traceID: string }> = [];

    for (const series of data.result) {
      // Create full label for data key
      const fullLabel = Object.entries(series.metric).length > 0
        ? Object.entries(series.metric)
            .map(([key, value]) => `${key}="${value}"`)
            .join(', ')
        : 'value';

      // Create short ID for display
      const seriesId = `series-${seriesCounter++}`;
      seriesIdMap.set(seriesId, fullLabel);

      if (series.values) {
        for (const [timestamp, value] of series.values) {
          const ts = timestamp * 1000; // Convert to milliseconds
          if (!seriesMap.has(ts)) {
            seriesMap.set(ts, { timestamp: ts });
          }
          const point = seriesMap.get(ts)!;
          point[seriesId] = parseFloat(value);
        }
      }

      // Extract exemplars from this series
      // Match them to rate values at those timestamps
      if (series.exemplars && series.exemplars.length > 0) {
        for (const exemplar of series.exemplars) {
          // Find the rate value at this timestamp from the series
          const matchingValue = series.values?.find(([ts]) => ts === exemplar.timestamp);
          if (matchingValue) {
            exemplarData.push({
              timestamp: exemplar.timestamp * 1000, // Convert to milliseconds
              value: parseFloat(matchingValue[1]), // Use the rate value as Y coordinate
              traceID: exemplar.traceID,
            });
          }
        }
      }
    }

    const sortedData = Array.from(seriesMap.values()).sort(
      (a, b) => a.timestamp - b.timestamp
    );

    chartData.push(...sortedData);

    return {
      chartData,
      seriesIds: Array.from(seriesIdMap.keys()),
      seriesIdMap,
      exemplarData,
    };
  }, [data.result]);

  // Initialize visible series when seriesIds change
  const allVisible = visibleSeries.size === 0;
  const toggleSeries = (seriesId: string) => {
    setVisibleSeries((prev) => {
      const next = new Set(prev);
      if (next.has(seriesId)) {
        next.delete(seriesId);
      } else {
        next.add(seriesId);
      }
      return next;
    });
  };

  // Blue-based color palette (professional and calm)
  const colors = [
    '#3b82f6', // Blue
    '#06b6d4', // Cyan
    '#0ea5e9', // Sky
    '#6366f1', // Indigo
    '#8b5cf6', // Purple
    '#a855f7', // Violet
    '#2563eb', // Deep blue
    '#0284c7', // Deep cyan
    '#1d4ed8', // Navy
    '#4f46e5', // Deep indigo
    '#7c3aed', // Deep purple
    '#60a5fa', // Light blue
    '#22d3ee', // Light cyan
    '#818cf8', // Light indigo
    '#a78bfa', // Light purple
    '#1e40af', // Dark blue
  ];

  // Custom tooltip to show full labels and trace IDs
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      // Check if this is an exemplar (has traceID in payload)
      const exemplar = payload.find((p: any) => p.payload?.traceID);

      return (
        <Box
          p="md"
          style={{
            backgroundColor: 'rgba(255, 255, 255, 0.98)',
            border: '1px solid #d0d0d0',
            borderRadius: '6px',
            boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
            maxWidth: '500px',
          }}
        >
          <Text size="sm" fw={600} mb="xs" c="#333">
            {formatTimestamp(label, 'long')}
          </Text>
          {exemplar && (
            <Box
              mt="xs"
              p="sm"
              style={{
                backgroundColor: '#eff6ff',
                borderRadius: '4px',
                borderLeft: '3px solid #3b82f6',
              }}
            >
              <Text size="sm" fw={700} c="#333" mb={6}>
                {exemplar.value?.toFixed(6)}
              </Text>
              <Text size="xs" c="#3b82f6" fw={500} mb={6}>
                📍 Click to view trace
              </Text>
              <Text size="xs" c="dimmed" style={{ fontFamily: 'monospace', fontSize: '10px', wordBreak: 'break-all' }}>
                {exemplar.payload.traceID}
              </Text>
            </Box>
          )}
          {!exemplar && payload.map((entry: any, index: number) => (
            <Box key={index} mt={index > 0 ? 'xs' : 0}>
              <Box style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <Box
                  style={{
                    width: '12px',
                    height: '12px',
                    backgroundColor: entry.color,
                    borderRadius: '2px',
                    flexShrink: 0,
                  }}
                />
                <Box style={{ flex: 1, minWidth: 0 }}>
                  <Text
                    size="xs"
                    c="dimmed"
                    style={{
                      fontFamily: 'monospace',
                      fontSize: '10px',
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                    }}
                    title={seriesIdMap.get(entry.dataKey)}
                  >
                    {seriesIdMap.get(entry.dataKey)}
                  </Text>
                  <Text size="sm" fw={600} style={{ color: entry.color }}>
                    {entry.value.toFixed(6)}
                  </Text>
                </Box>
              </Box>
            </Box>
          ))}
        </Box>
      );
    }
    return null;
  };

  // Custom exemplar dot with click handler
  const ExemplarDot = (props: any) => {
    const { cx, cy, payload } = props;
    if (!payload || !payload.traceID) return null;

    return (
      <circle
        cx={cx}
        cy={cy}
        r={6}
        fill="#ef4444"
        stroke="#fff"
        strokeWidth={2}
        style={{ cursor: 'pointer' }}
        onClick={() => {
          if (onExemplarClick) {
            onExemplarClick(payload.traceID);
          }
        }}
        onMouseEnter={(e) => {
          e.currentTarget.setAttribute('r', '8');
        }}
        onMouseLeave={(e) => {
          e.currentTarget.setAttribute('r', '6');
        }}
      >
        <title>Click to view trace: {payload.traceID}</title>
      </circle>
    );
  };

  return (
    <Box>
      <ResponsiveContainer width="100%" height={500}>
        <ComposedChart
          data={chartData}
          margin={{ top: 20, right: 30, left: 20, bottom: 20 }}
        >
          <CartesianGrid
            strokeDasharray="3 3"
            stroke="#e0e0e0"
            vertical={false}
          />
          <XAxis
            dataKey="timestamp"
            tickFormatter={(value) => formatTimestamp(value, 'short')}
            type="number"
            domain={['dataMin', 'dataMax']}
            tick={{ fontSize: 12, fill: '#666' }}
            stroke="#999"
          />
          <YAxis
            yAxisId="left"
            domain={[0, 'auto']}
            tick={{ fontSize: 12, fill: '#666' }}
            stroke="#999"
            width={60}
          />
          <Tooltip
            content={<CustomTooltip />}
            cursor={{ stroke: '#999', strokeWidth: 1, strokeDasharray: '5 5' }}
          />
          {seriesIds.map((seriesId, index) => {
            const isVisible = allVisible || visibleSeries.has(seriesId);
            if (!isVisible) return null;

            return (
              <Area
                key={seriesId}
                yAxisId="left"
                type="monotone"
                dataKey={seriesId}
                stroke={colors[index % colors.length]}
                fill={colors[index % colors.length]}
                fillOpacity={0.2}
                strokeWidth={2}
                name={seriesIdMap.get(seriesId)}
                activeDot={{ r: 4, strokeWidth: 0 }}
                dot={false}
              />
            );
          })}
          {exemplarData.length > 0 && (
            <Scatter
              yAxisId="left"
              data={exemplarData}
              dataKey="value"
              fill="#ef4444"
              shape={<ExemplarDot />}
            />
          )}
        </ComposedChart>
      </ResponsiveContainer>

      {/* Professional legend below chart */}
      {seriesIds.length > 0 && (
        <Box
          mt="lg"
          p="md"
          style={{
            maxHeight: '200px',
            overflowY: 'auto',
            borderTop: '2px solid #e8e8e8',
            backgroundColor: '#fafafa',
            borderRadius: '0 0 4px 4px',
          }}
        >
          <Box style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
            <Box>
              <Text size="sm" fw={600} c="#555">
                Series ({seriesIds.length})
              </Text>
              <Text size="xs" c="dimmed" mt={2}>
                Click to toggle visibility
              </Text>
            </Box>
            {exemplarData.length > 0 && (
              <Box style={{ display: 'flex', alignItems: 'center', gap: '6px', backgroundColor: '#fff', padding: '4px 8px', borderRadius: '4px', border: '1px solid #e0e0e0' }}>
                <Box
                  style={{
                    width: '10px',
                    height: '10px',
                    backgroundColor: '#ef4444',
                    borderRadius: '50%',
                    border: '2px solid #fff',
                    boxShadow: '0 0 0 1px #ef4444',
                  }}
                />
                <Text size="xs" c="#666" fw={500}>
                  {exemplarData.length} exemplar{exemplarData.length !== 1 ? 's' : ''} (click to view trace)
                </Text>
              </Box>
            )}
          </Box>
          <Box style={{ display: 'flex', flexWrap: 'wrap', gap: '10px' }}>
            {seriesIds.map((seriesId, index) => {
              const isVisible = allVisible || visibleSeries.has(seriesId);
              return (
                <Box
                  key={seriesId}
                  px="sm"
                  py={6}
                  onClick={() => toggleSeries(seriesId)}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '8px',
                    backgroundColor: isVisible ? '#fff' : '#f5f5f5',
                    borderRadius: '4px',
                    border: isVisible ? `2px solid ${colors[index % colors.length]}` : '2px solid #e0e0e0',
                    fontSize: '11px',
                    fontFamily: 'monospace',
                    transition: 'all 0.2s',
                    cursor: 'pointer',
                    opacity: isVisible ? 1 : 0.5,
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.transform = 'translateY(-2px)';
                    e.currentTarget.style.boxShadow = '0 2px 8px rgba(0,0,0,0.1)';
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.transform = 'translateY(0)';
                    e.currentTarget.style.boxShadow = 'none';
                  }}
                >
                  <Box
                    style={{
                      width: '3px',
                      height: '16px',
                      backgroundColor: colors[index % colors.length],
                      borderRadius: '2px',
                      flexShrink: 0,
                      opacity: isVisible ? 1 : 0.3,
                    }}
                  />
                  <Text
                    size="xs"
                    c={isVisible ? '#444' : '#999'}
                    style={{
                      maxWidth: '400px',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                      textDecoration: isVisible ? 'none' : 'line-through',
                    }}
                    title={seriesIdMap.get(seriesId)}
                  >
                    {seriesIdMap.get(seriesId)}
                  </Text>
                </Box>
              );
            })}
          </Box>
        </Box>
      )}
    </Box>
  );
}
