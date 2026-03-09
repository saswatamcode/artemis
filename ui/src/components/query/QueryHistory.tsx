// Query history sidebar

import { ScrollArea, Text, ActionIcon, Group, Code, Stack } from '@mantine/core';
import { IconX } from '@tabler/icons-react';

interface QueryHistoryProps {
  queries: string[];
  onSelect: (query: string) => void;
  onClear?: () => void;
}

export function QueryHistory({
  queries,
  onSelect,
  onClear,
}: QueryHistoryProps) {
  if (queries.length === 0) {
    return (
      <Text c="dimmed" size="sm" p="md">
        No query history yet
      </Text>
    );
  }

  return (
    <Stack gap="xs">
      <Group justify="space-between" px="md" pt="sm">
        <Text size="sm" fw={600}>
          Recent Queries
        </Text>
        {onClear && (
          <ActionIcon
            variant="subtle"
            size="sm"
            onClick={onClear}
            title="Clear history"
          >
            <IconX size={14} />
          </ActionIcon>
        )}
      </Group>
      <ScrollArea h={300}>
        <Stack gap="xs" p="md" pt={0}>
          {queries.map((query, index) => (
            <Code
              key={index}
              block
              style={{
                cursor: 'pointer',
                fontSize: '12px',
                maxWidth: '100%',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
              onClick={() => onSelect(query)}
            >
              {query}
            </Code>
          ))}
        </Stack>
      </ScrollArea>
    </Stack>
  );
}
