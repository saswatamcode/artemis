// Metrics query and visualization page

import { Container, Stack, Paper } from '@mantine/core';
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '../contexts/QueryContext';
import { QueryPanel } from '../components/query/QueryPanel';
import { ChartTabs } from '../components/visualization/ChartTabs';
import { LoadingOverlay } from '../components/common/LoadingOverlay';

export function MetricsPage() {
  const { state } = useQuery();
  const [hasResults, setHasResults] = useState(false);
  const navigate = useNavigate();

  const handleExemplarClick = (traceID: string) => {
    navigate(`/trace/${traceID}`);
  };

  return (
    <Container size="xl">
      <Stack gap="md">
        <QueryPanel onResultsChange={setHasResults} />

        {state.loading && (
          <Paper p="xl" withBorder>
            <LoadingOverlay message="Executing query..." />
          </Paper>
        )}

        {!state.loading && hasResults && state.result && (
          <Paper p="md" withBorder>
            <ChartTabs
              data={state.result.data}
              onExemplarClick={handleExemplarClick}
              startTime={state.startTime}
              endTime={state.endTime}
            />
          </Paper>
        )}
      </Stack>
    </Container>
  );
}
