import { useMemo, useState } from 'react';
import Plot from 'react-plotly.js';
import { Box, Text, Paper, Group } from '@mantine/core';
import type { QueryRangeData } from '../../api/types';

interface PlotlyMetricChartProps {
  data: QueryRangeData;
  onExemplarClick?: (traceID: string) => void;
  startTime: Date;
  endTime: Date;
}

export function PlotlyMetricChart({ data, onExemplarClick, startTime, endTime }: PlotlyMetricChartProps) {
  const [selectedSeries, setSelectedSeries] = useState<Set<number>>(new Set());
  const [isHovering, setIsHovering] = useState(false);

  // Transform data to Plotly format
  const { traces, exemplarTraces, seriesLabels } = useMemo(() => {
    if (!data.result || data.result.length === 0) {
      return { traces: [], exemplarTraces: [], seriesLabels: [] };
    }

    const colors = [
      '#3b82f6', '#06b6d4', '#0ea5e9', '#6366f1', '#8b5cf6',
      '#a855f7', '#2563eb', '#0284c7', '#1d4ed8', '#4f46e5',
      '#7c3aed', '#60a5fa', '#22d3ee', '#818cf8', '#a78bfa',
    ];

    const traces: any[] = [];
    const exemplarTraces: any[] = [];
    const seriesLabels: string[] = [];

    data.result.forEach((series, index) => {
      // Create series label
      const label = Object.entries(series.metric).length > 0
        ? Object.entries(series.metric)
            .map(([k, v]) => `${k}="${v}"`)
            .join(', ')
        : 'value';

      seriesLabels.push(label);

      // Extract time series data
      const x: number[] = [];
      const y: number[] = [];

      if (series.values) {
        for (const [timestamp, value] of series.values) {
          x.push(timestamp * 1000); // Convert to milliseconds
          y.push(parseFloat(value));
        }
      }

      // Create line trace
      traces.push({
        x,
        y,
        type: 'scatter',
        mode: 'lines',
        name: label,
        line: {
          color: colors[index % colors.length],
          width: 2,
        },
        fill: 'tozeroy',
        fillcolor: colors[index % colors.length] + '20', // 20 = ~12% opacity
        hovertemplate:
          '<b>%{fullData.name}</b><br>' +
          'Time: %{x|%Y-%m-%d %H:%M:%S}<br>' +
          'Value: %{y:.6f}<br>' +
          '<extra></extra>',
        visible: true, // Will be updated based on selection
      });

      // Extract exemplars for this series
      if (series.exemplars && series.exemplars.length > 0) {
        const exX: number[] = [];
        const exY: number[] = [];
        const exText: string[] = [];
        const exTraceIDs: string[] = [];

        for (const ex of series.exemplars) {
          // Find matching value at this timestamp
          const matchingValue = series.values?.find(([ts]) => ts === ex.timestamp);
          if (matchingValue) {
            exX.push(ex.timestamp * 1000); // Convert to ms
            exY.push(parseFloat(matchingValue[1]));
            exText.push(
              `Duration: ${(ex.duration / 1_000_000).toFixed(2)}ms<br>` +
              `Click to view trace`
            );
            exTraceIDs.push(ex.traceID);
          }
        }

        if (exX.length > 0) {
          exemplarTraces.push({
            x: exX,
            y: exY,
            type: 'scatter',
            mode: 'markers',
            name: `${label} (exemplars)`,
            marker: {
              size: 10,
              color: '#ef4444',
              symbol: 'circle',
              line: {
                color: '#fff',
                width: 2,
              },
            },
            text: exText,
            hovertemplate: '%{text}<extra></extra>',
            showlegend: false,
            customdata: exTraceIDs, // Store trace IDs for click handling
            visible: true, // Will be updated based on parent series visibility
            seriesIndex: index, // Track which series this belongs to
          });
        }
      }
    });

    return { traces, exemplarTraces, seriesLabels };
  }, [data.result]);

  // Toggle series visibility
  const toggleSeries = (index: number) => {
    setSelectedSeries((prev) => {
      const next = new Set(prev);
      if (next.has(index)) {
        next.delete(index);
        // If removing the last selected, show all
        if (next.size === 0) {
          return new Set();
        }
      } else {
        // If clicking when all are visible, show only this one
        if (prev.size === 0) {
          next.add(index);
        } else {
          next.add(index);
        }
      }
      return next;
    });
  };

  // Determine which traces to show
  const allVisible = selectedSeries.size === 0;
  const visibleTraces = useMemo(() => {
    // Update trace visibility based on selection
    const seriesTraces = traces.map((trace, i) => ({
      ...trace,
      visible: (allVisible || selectedSeries.has(i)) as any,
    }));

    // Update exemplar visibility and opacity based on their parent series and hover state
    const exemplarTracesWithVisibility = exemplarTraces.map((trace) => ({
      ...trace,
      visible: (allVisible || selectedSeries.has(trace.seriesIndex)) as any,
      marker: {
        ...trace.marker,
        opacity: isHovering ? 1 : 0,
      },
    }));

    return [...seriesTraces, ...exemplarTracesWithVisibility];
  }, [traces, exemplarTraces, selectedSeries, allVisible, isHovering]);

  if (traces.length === 0) {
    return (
      <Box p="xl" style={{ textAlign: 'center' }}>
        <Text c="dimmed">No data to display</Text>
      </Box>
    );
  }

  const colors = [
    '#3b82f6', '#06b6d4', '#0ea5e9', '#6366f1', '#8b5cf6',
    '#a855f7', '#2563eb', '#0284c7', '#1d4ed8', '#4f46e5',
    '#7c3aed', '#60a5fa', '#22d3ee', '#818cf8', '#a78bfa',
  ];

  return (
    <Box>
      <Paper p="md" withBorder shadow="sm">
        <Plot
          key={`plot-${Array.from(selectedSeries).sort().join('-')}`}
          data={visibleTraces}
          layout={{
            title: undefined,
            xaxis: {
              type: 'date',
              title: undefined,
              showgrid: true,
              gridcolor: '#e8e8e8',
              zeroline: false,
              range: [startTime.getTime(), endTime.getTime()], // Set explicit range
            },
            yaxis: {
              title: undefined,
              showgrid: true,
              gridcolor: '#e8e8e8',
              zeroline: true,
              tickformat: '.2s', // SI prefix formatting
              hoverformat: '.6f',
            },
            hovermode: 'closest',
            showlegend: false,
            margin: {
              l: 70,
              r: 30,
              t: 30,
              b: 50,
            },
            height: 500,
            plot_bgcolor: '#fafafa',
            paper_bgcolor: '#fff',
          }}
          config={{
            displayModeBar: true,
            displaylogo: false,
            modeBarButtonsToRemove: ['lasso2d', 'select2d'],
            toImageButtonOptions: {
              format: 'png',
              filename: 'metrics',
            },
          }}
          style={{ width: '100%' }}
          useResizeHandler={true}
          onClick={(event: any) => {
            if (!onExemplarClick || !event.points || event.points.length === 0) return;

            const point = event.points[0];

            // Check if this is an exemplar click
            if (point.data.mode === 'markers' && point.data.customdata) {
              const traceID = point.data.customdata[point.pointIndex];
              if (traceID) {
                onExemplarClick(traceID);
              }
            }
          }}
          onHover={() => setIsHovering(true)}
          onUnhover={() => setIsHovering(false)}
        />
      </Paper>

      {/* Interactive Legend */}
      {traces.length > 0 && (
        <Paper
          mt="lg"
          p="md"
          withBorder
          style={{
            maxHeight: '200px',
            overflowY: 'auto',
            backgroundColor: '#fafafa',
          }}
        >
          <Box style={{ marginBottom: '12px' }}>
            <Group justify="space-between">
              <Box>
                <Text size="sm" fw={600} c="#555">
                  Series ({traces.length})
                </Text>
                <Text size="xs" c="dimmed" mt={2}>
                  Click to toggle visibility
                </Text>
              </Box>
              {exemplarTraces.length > 0 && (
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
                    {exemplarTraces.reduce((sum, t) => sum + t.x.length, 0)} exemplar(s)
                  </Text>
                </Box>
              )}
            </Group>
          </Box>

          <Box style={{ display: 'flex', flexWrap: 'wrap', gap: '10px' }}>
            {seriesLabels.map((label, index) => {
              const isVisible = allVisible || selectedSeries.has(index);
              return (
                <Box
                  key={index}
                  px="sm"
                  py={6}
                  onClick={() => toggleSeries(index)}
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
                    title={label}
                  >
                    {label}
                  </Text>
                </Box>
              );
            })}
          </Box>
        </Paper>
      )}
    </Box>
  );
}
