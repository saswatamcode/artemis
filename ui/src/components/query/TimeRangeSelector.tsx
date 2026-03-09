// Time range selector with presets and custom picker

import { Group, Button, Menu } from '@mantine/core';
import { DateTimePicker } from '@mantine/dates';
import { IconClock, IconChevronDown } from '@tabler/icons-react';
import { getTimeRangePresets } from '../../utils/timeFormatting';
import '@mantine/dates/styles.css';

interface TimeRangeSelectorProps {
  startTime: Date;
  endTime: Date;
  onChange: (start: Date, end: Date) => void;
}

export function TimeRangeSelector({
  startTime,
  endTime,
  onChange,
}: TimeRangeSelectorProps) {
  const presets = getTimeRangePresets();

  return (
    <Group gap="xs">
      <Menu shadow="md" width={200}>
        <Menu.Target>
          <Button
            leftSection={<IconClock size={16} />}
            rightSection={<IconChevronDown size={16} />}
            variant="default"
            size="sm"
          >
            Quick Range
          </Button>
        </Menu.Target>

        <Menu.Dropdown>
          <Menu.Label>Time Range Presets</Menu.Label>
          {presets.map((preset) => (
            <Menu.Item
              key={preset.label}
              onClick={() => {
                const range = preset.value();
                onChange(range.start, range.end);
              }}
            >
              {preset.label}
            </Menu.Item>
          ))}
        </Menu.Dropdown>
      </Menu>

      <DateTimePicker
        label="Start Time"
        value={startTime}
        onChange={(value) => {
          if (value) onChange(value, endTime);
        }}
        size="sm"
        style={{ flex: 1 }}
      />

      <DateTimePicker
        label="End Time"
        value={endTime}
        onChange={(value) => {
          if (value) onChange(startTime, value);
        }}
        size="sm"
        style={{ flex: 1 }}
      />
    </Group>
  );
}
