// Main application layout with Mantine AppShell

import { AppShell as MantineAppShell, Group, Title, Text } from '@mantine/core';
import { IconActivity } from '@tabler/icons-react';
import type { ReactNode } from 'react';

interface AppShellProps {
  children: ReactNode;
}

export function AppShell({ children }: AppShellProps) {
  return (
    <MantineAppShell header={{ height: 60 }} padding="md">
      <MantineAppShell.Header>
        <Group h="100%" px="md" justify="space-between">
          <Group>
            <IconActivity size={28} stroke={2} />
            <div>
              <Title order={3}>Artemis</Title>
              <Text size="xs" c="dimmed">
                Trace Database Query Interface
              </Text>
            </div>
          </Group>
        </Group>
      </MantineAppShell.Header>

      <MantineAppShell.Main>{children}</MantineAppShell.Main>
    </MantineAppShell>
  );
}
