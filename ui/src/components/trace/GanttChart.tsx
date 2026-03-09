// Gantt chart with virtualized span list and timeline

import { Box, Group, Paper } from '@mantine/core';
import { Virtuoso } from 'react-virtuoso';
import { SpanRow } from './SpanRow';
import { Timeline } from './Timeline';
import { flattenSpanTree } from '../../utils/traceTreeBuilder';
import { useTrace } from '../../contexts/TraceContext';
import type { TraceTree } from '../../utils/traceTreeBuilder';

interface GanttChartProps {
  tree: TraceTree;
}

export function GanttChart({ tree }: GanttChartProps) {
  const { state, selectSpan, toggleCollapse } = useTrace();

  const flatSpans = flattenSpanTree(tree.roots, state.collapsedSpans);

  return (
    <Paper
      withBorder
      shadow="sm"
      style={{
        borderRadius: '8px',
        overflow: 'hidden',
      }}
    >
      <Group gap={0} wrap="nowrap" align="stretch">
        {/* Left pane: Span list */}
        <Box
          style={{
            width: '40%',
            minWidth: 300,
            borderRight: '2px solid #e8e8e8',
            overflow: 'hidden',
            backgroundColor: '#fafafa',
          }}
        >
          <Virtuoso
            data={flatSpans}
            totalCount={flatSpans.length}
            itemContent={(_index, span) => (
              <SpanRow
                span={span}
                isCollapsed={state.collapsedSpans.has(span.spanID)}
                isSelected={state.selectedSpanID === span.spanID}
                onToggleCollapse={() => toggleCollapse(span.spanID)}
                onSelect={() => selectSpan(span.spanID)}
              />
            )}
            style={{ height: 600 }}
          />
        </Box>

        {/* Right pane: Timeline */}
        <Box
          style={{
            flex: 1,
            overflow: 'auto',
            backgroundColor: '#ffffff',
          }}
        >
          <Timeline
            spans={flatSpans}
            minTime={tree.minTime}
            maxTime={tree.maxTime}
            selectedSpanID={state.selectedSpanID}
            onSpanClick={selectSpan}
          />
        </Box>
      </Group>
    </Paper>
  );
}
