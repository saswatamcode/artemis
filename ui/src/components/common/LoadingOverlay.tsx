// Loading overlay component

import { Center, Loader, Text, Stack } from '@mantine/core';

interface LoadingOverlayProps {
  message?: string;
}

export function LoadingOverlay({ message }: LoadingOverlayProps) {
  return (
    <Center h="100%">
      <Stack align="center" gap="md">
        <Loader size="lg" />
        {message && <Text c="dimmed">{message}</Text>}
      </Stack>
    </Center>
  );
}
