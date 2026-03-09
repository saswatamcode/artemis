// Main trace view container

import { Stack, Group, Text, Button, Paper, Box } from '@mantine/core';
import { IconArrowsMaximize, IconArrowsMinimize, IconClock, IconServer } from '@tabler/icons-react';
import { GanttChart } from './GanttChart';
import { SpanDetails } from './SpanDetails';
import { MiniTimeline } from './MiniTimeline';
import { useTrace } from '../../contexts/TraceContext';
import { formatNanoDuration } from '../../utils/durationFormatting';
import type { TraceTree } from '../../utils/traceTreeBuilder';

interface TraceViewProps {
  traceID: string;
  tree: TraceTree;
}

export function TraceView({ traceID, tree }: TraceViewProps) {
  const { state, selectSpan, collapseAll, expandAll } = useTrace();

  const selectedSpan = state.selectedSpanID
    ? tree.spanMap.get(state.selectedSpanID)
    : null;

  const totalSpans = Array.from(tree.spanMap.values()).length;

  // Get unique services
  const services = new Set(
    Array.from(tree.spanMap.values()).map((s) => s.serviceName)
  );

  // Get root span
  const rootSpan = tree.roots[0];

  return (
    <Stack gap="lg">
      {/* Trace Summary Card */}
      <Paper
        shadow="sm"
        p="xl"
        withBorder
        style={{
          background: 'linear-gradient(135deg, #3b82f6 0%, #2563eb 100%)',
          color: 'white',
        }}
      >
        <Stack gap="md">
          <Group justify="space-between" align="start">
            <Stack gap="xs">
              <Text size="xs" opacity={0.9} tt="uppercase" fw={600}>
                Trace ID
              </Text>
              <Text
                size="lg"
                fw={700}
                style={{ fontFamily: 'monospace', letterSpacing: '0.5px' }}
              >
                {traceID}
              </Text>
              {rootSpan && (
                <Text size="sm" opacity={0.95} fw={500}>
                  {rootSpan.name}
                </Text>
              )}
            </Stack>

            <Group>
              <Button
                variant="white"
                size="xs"
                leftSection={<IconArrowsMinimize size={14} />}
                onClick={collapseAll}
              >
                Collapse All
              </Button>
              <Button
                variant="white"
                size="xs"
                leftSection={<IconArrowsMaximize size={14} />}
                onClick={expandAll}
              >
                Expand All
              </Button>
            </Group>
          </Group>

          <Group gap="xl">
            <Group gap="xs">
              <IconClock size={18} opacity={0.9} />
              <Box>
                <Text size="xs" opacity={0.8}>
                  Duration
                </Text>
                <Text size="md" fw={600}>
                  {formatNanoDuration(tree.duration)}
                </Text>
              </Box>
            </Group>

            <Group gap="xs">
              <Box>
                <Text size="xs" opacity={0.8}>
                  Spans
                </Text>
                <Text size="md" fw={600}>
                  {totalSpans}
                </Text>
              </Box>
            </Group>

            <Group gap="xs">
              <IconServer size={18} opacity={0.9} />
              <Box>
                <Text size="xs" opacity={0.8}>
                  Services
                </Text>
                <Text size="md" fw={600}>
                  {services.size}
                </Text>
              </Box>
            </Group>
          </Group>

          {/* Mini timeline in summary */}
          <Box
            mt="md"
            p="md"
            style={{
              backgroundColor: 'rgba(255, 255, 255, 0.15)',
              borderRadius: '8px',
              backdropFilter: 'blur(10px)',
            }}
          >
            <Text size="xs" mb="sm" opacity={0.9} fw={600}>
              TRACE OVERVIEW
            </Text>
            <MiniTimeline
              spans={Array.from(tree.spanMap.values())}
              minTime={tree.minTime}
              maxTime={tree.maxTime}
            />
          </Box>
        </Stack>
      </Paper>

      {/* Gantt chart */}
      <GanttChart tree={tree} />

      {/* Span details drawer */}
      <SpanDetails
        span={selectedSpan || null}
        opened={!!selectedSpan}
        onClose={() => selectSpan(null)}
      />
    </Stack>
  );
}
