// Span details drawer with attributes

import { Drawer, Stack, Text, Table, Group, Badge, Code } from '@mantine/core';
import { formatNanoDuration } from '../../utils/durationFormatting';
import { formatTimestamp } from '../../utils/timeFormatting';
import type { SpanNode } from '../../api/types';

interface SpanDetailsProps {
  span: SpanNode | null;
  opened: boolean;
  onClose: () => void;
}

export function SpanDetails({ span, opened, onClose }: SpanDetailsProps) {
  if (!span) return null;

  const hasError = span.attributes['error'] === 'true' ||
                   span.attributes['otel.status_code'] === 'ERROR';

  return (
    <Drawer
      opened={opened}
      onClose={onClose}
      position="right"
      size="lg"
      title={
        <Group>
          <Text fw={600}>Span Details</Text>
          {hasError && <Badge color="red">Error</Badge>}
        </Group>
      }
    >
      <Stack gap="md">
        {/* Basic info */}
        <Stack gap="xs">
          <Text size="sm" fw={600}>
            Span Name
          </Text>
          <Code block>{span.name}</Code>
        </Stack>

        <Stack gap="xs">
          <Text size="sm" fw={600}>
            Service
          </Text>
          <Text size="sm">{span.serviceName}</Text>
        </Stack>

        <Stack gap="xs">
          <Text size="sm" fw={600}>
            Duration
          </Text>
          <Text size="sm" ff="monospace">
            {formatNanoDuration(span.duration)}
          </Text>
        </Stack>

        <Stack gap="xs">
          <Text size="sm" fw={600}>
            Start Time
          </Text>
          <Text size="sm" ff="monospace">
            {formatTimestamp(span.startTime / 1e6, 'long')}
          </Text>
        </Stack>

        <Stack gap="xs">
          <Text size="sm" fw={600}>
            End Time
          </Text>
          <Text size="sm" ff="monospace">
            {formatTimestamp(span.endTime / 1e6, 'long')}
          </Text>
        </Stack>

        {/* IDs */}
        <Stack gap="xs">
          <Text size="sm" fw={600}>
            Span ID
          </Text>
          <Code block>{span.spanID}</Code>
        </Stack>

        {span.parentSpanID && (
          <Stack gap="xs">
            <Text size="sm" fw={600}>
              Parent Span ID
            </Text>
            <Code block>{span.parentSpanID}</Code>
          </Stack>
        )}

        {/* Attributes */}
        <Stack gap="xs">
          <Text size="sm" fw={600}>
            Attributes
          </Text>
          {Object.keys(span.attributes).length > 0 ? (
            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Key</Table.Th>
                  <Table.Th>Value</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {Object.entries(span.attributes)
                  .sort(([a], [b]) => a.localeCompare(b))
                  .map(([key, value]) => (
                    <Table.Tr key={key}>
                      <Table.Td>
                        <Text size="sm" ff="monospace">
                          {key}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="sm" ff="monospace">
                          {value}
                        </Text>
                      </Table.Td>
                    </Table.Tr>
                  ))}
              </Table.Tbody>
            </Table>
          ) : (
            <Text size="sm" c="dimmed">
              No attributes
            </Text>
          )}
        </Stack>
      </Stack>
    </Drawer>
  );
}
