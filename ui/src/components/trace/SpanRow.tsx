// Individual span row in the gantt chart

import { Group, Text, ActionIcon, Box } from '@mantine/core';
import { IconChevronRight, IconChevronDown } from '@tabler/icons-react';
import { formatDurationCompact } from '../../utils/durationFormatting';
import { getServiceColor } from '../../utils/colorUtils';
import type { SpanNode } from '../../api/types';

interface SpanRowProps {
  span: SpanNode;
  isCollapsed: boolean;
  isSelected: boolean;
  onToggleCollapse: () => void;
  onSelect: () => void;
}

export function SpanRow({
  span,
  isCollapsed,
  isSelected,
  onToggleCollapse,
  onSelect,
}: SpanRowProps) {
  const hasChildren = span.children.length > 0;
  const serviceColor = getServiceColor(span.serviceName);

  return (
    <Box
      style={{
        display: 'flex',
        alignItems: 'center',
        padding: '4px 8px',
        cursor: 'pointer',
        backgroundColor: isSelected ? '#e3f2fd' : 'transparent',
        borderLeft: `3px solid ${serviceColor}`,
        transition: 'background-color 0.2s',
      }}
      onClick={onSelect}
      onMouseEnter={(e) => {
        if (!isSelected) {
          e.currentTarget.style.backgroundColor = '#f5f5f5';
        }
      }}
      onMouseLeave={(e) => {
        if (!isSelected) {
          e.currentTarget.style.backgroundColor = 'transparent';
        }
      }}
    >
      <Group gap={4} wrap="nowrap" style={{ flex: 1, minWidth: 0 }}>
        {/* Indentation based on depth */}
        <Box style={{ width: span.depth * 20 }} />

        {/* Collapse/expand button */}
        {hasChildren ? (
          <ActionIcon
            size="xs"
            variant="subtle"
            onClick={(e) => {
              e.stopPropagation();
              onToggleCollapse();
            }}
          >
            {isCollapsed ? (
              <IconChevronRight size={14} />
            ) : (
              <IconChevronDown size={14} />
            )}
          </ActionIcon>
        ) : (
          <Box style={{ width: 22 }} />
        )}

        {/* Service name badge */}
        <Box
          style={{
            backgroundColor: serviceColor,
            color: 'white',
            padding: '2px 6px',
            borderRadius: 4,
            fontSize: 11,
            fontWeight: 600,
            whiteSpace: 'nowrap',
          }}
        >
          {span.serviceName}
        </Box>

        {/* Span name */}
        <Text
          size="sm"
          truncate
          style={{ flex: 1, minWidth: 0, fontFamily: 'monospace' }}
        >
          {span.name}
        </Text>

        {/* Duration */}
        <Text size="xs" c="dimmed" style={{ whiteSpace: 'nowrap' }}>
          {formatDurationCompact(span.duration)}
        </Text>
      </Group>
    </Box>
  );
}
