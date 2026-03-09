// Tabular metric view

import { Table, ScrollArea, Text, Box } from '@mantine/core';
import { formatTimestamp } from '../../utils/timeFormatting';
import type { QueryRangeData } from '../../api/types';

interface MetricTableProps {
  data: QueryRangeData;
}

export function MetricTable({ data }: MetricTableProps) {
  if (!data.result || data.result.length === 0) {
    return (
      <Box p="xl" style={{ textAlign: 'center' }}>
        <Text c="dimmed">No data to display</Text>
      </Box>
    );
  }

  return (
    <ScrollArea h={400}>
      <Table striped highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Metric Labels</Table.Th>
            <Table.Th>Timestamp</Table.Th>
            <Table.Th>Value</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {data.result.flatMap((series, seriesIndex) => {
            const labels = Object.entries(series.metric)
              .map(([key, value]) => `${key}="${value}"`)
              .join(', ');

            if (series.values) {
              return series.values.map(([timestamp, value], valueIndex) => (
                <Table.Tr key={`${seriesIndex}-${valueIndex}`}>
                  <Table.Td>
                    <Text size="sm" ff="monospace">
                      {labels || '<no labels>'}
                    </Text>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm">
                      {formatTimestamp(timestamp * 1000, 'long')}
                    </Text>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm" ff="monospace">
                      {parseFloat(value).toFixed(4)}
                    </Text>
                  </Table.Td>
                </Table.Tr>
              ));
            }

            if (series.value) {
              const [timestamp, value] = series.value;
              return (
                <Table.Tr key={seriesIndex}>
                  <Table.Td>
                    <Text size="sm" ff="monospace">
                      {labels || '<no labels>'}
                    </Text>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm">
                      {formatTimestamp(timestamp * 1000, 'long')}
                    </Text>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm" ff="monospace">
                      {parseFloat(value).toFixed(4)}
                    </Text>
                  </Table.Td>
                </Table.Tr>
              );
            }

            return [];
          })}
        </Table.Tbody>
      </Table>
    </ScrollArea>
  );
}
