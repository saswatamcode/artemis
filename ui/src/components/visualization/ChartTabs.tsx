// Tab switcher for chart and table views

import { Tabs } from '@mantine/core';
import { IconChartLine, IconTable, IconChartDots } from '@tabler/icons-react';
import { useMemo } from 'react';
import { PlotlyMetricChart } from './PlotlyMetricChart';
import { MetricTable } from './MetricTable';
import { PlotlyHeatmapChart } from './PlotlyHeatmapChart';
import type { QueryRangeData } from '../../api/types';

interface ChartTabsProps {
  data: QueryRangeData;
  onExemplarClick?: (traceID: string) => void;
  startTime: Date;
  endTime: Date;
}

export function ChartTabs({ data, onExemplarClick, startTime, endTime }: ChartTabsProps) {
  // Detect if this is heatmap data
  const isHeatmap = useMemo(() => {
    if (!data.result || data.result.length === 0) return false;
    // Heatmap series have duration_bucket label
    return data.result.some((series) => series.metric.duration_bucket !== undefined);
  }, [data.result]);

  return (
    <Tabs defaultValue={isHeatmap ? 'heatmap' : 'graph'}>
      <Tabs.List>
        {isHeatmap && (
          <Tabs.Tab value="heatmap" leftSection={<IconChartDots size={16} />}>
            Heatmap
          </Tabs.Tab>
        )}
        {!isHeatmap && (
          <Tabs.Tab value="graph" leftSection={<IconChartLine size={16} />}>
            Graph
          </Tabs.Tab>
        )}
        <Tabs.Tab value="table" leftSection={<IconTable size={16} />}>
          Table
        </Tabs.Tab>
      </Tabs.List>

      {isHeatmap && (
        <Tabs.Panel value="heatmap" pt="md">
          <PlotlyHeatmapChart
            data={data}
            onExemplarClick={onExemplarClick}
          />
        </Tabs.Panel>
      )}

      {!isHeatmap && (
        <Tabs.Panel value="graph" pt="md">
          <PlotlyMetricChart
            data={data}
            onExemplarClick={onExemplarClick}
            startTime={startTime}
            endTime={endTime}
          />
        </Tabs.Panel>
      )}

      <Tabs.Panel value="table" pt="md">
        <MetricTable data={data} />
      </Tabs.Panel>
    </Tabs>
  );
}
