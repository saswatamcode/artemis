// Trace detail page with gantt chart

import { Container, Alert, Button, Group } from '@mantine/core';
import { useParams, useNavigate } from 'react-router-dom';
import { IconArrowLeft } from '@tabler/icons-react';
import { TraceProvider } from '../contexts/TraceContext';
import { TraceView } from '../components/trace/TraceView';
import { LoadingOverlay } from '../components/common/LoadingOverlay';
import { useTraceData } from '../hooks/useTraceData';

function TraceContent() {
  const { traceID } = useParams<{ traceID: string }>();
  const navigate = useNavigate();

  const { tree, loading, error } = useTraceData(
    traceID ? { traceID } : null
  );

  if (loading) {
    return (
      <Container size="xl" style={{ height: 400 }}>
        <LoadingOverlay message="Loading trace..." />
      </Container>
    );
  }

  if (error) {
    return (
      <Container size="xl">
        <Alert color="red" title="Error Loading Trace">
          {error.message}
        </Alert>
      </Container>
    );
  }

  if (!tree || !traceID) {
    return (
      <Container size="xl">
        <Alert color="yellow" title="No Trace Found">
          Trace not found or invalid trace ID.
        </Alert>
      </Container>
    );
  }

  return (
    <Container size="xl">
      <Group mb="md">
        <Button
          variant="default"
          size="sm"
          leftSection={<IconArrowLeft size={16} />}
          onClick={() => navigate('/')}
        >
          Back to Metrics
        </Button>
      </Group>
      <TraceView traceID={traceID} tree={tree} />
    </Container>
  );
}

export function TracePage() {
  return (
    <TraceProvider>
      <TraceContent />
    </TraceProvider>
  );
}
