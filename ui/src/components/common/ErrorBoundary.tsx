// Error boundary for catching React errors

import { Component, type ReactNode } from 'react';
import { Alert, Button, Container, Stack, Title } from '@mantine/core';

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: unknown) {
    console.error('ErrorBoundary caught an error:', error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <Container size="md" py="xl">
          <Stack gap="md">
            <Title order={2}>Something went wrong</Title>
            <Alert color="red" title="Error">
              {this.state.error?.message || 'An unexpected error occurred'}
            </Alert>
            <Button
              onClick={() => {
                this.setState({ hasError: false, error: null });
                window.location.href = '/';
              }}
            >
              Go to Home
            </Button>
          </Stack>
        </Container>
      );
    }

    return this.props.children;
  }
}
