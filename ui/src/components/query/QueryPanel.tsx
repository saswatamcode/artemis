// Main query panel orchestrator

import { Paper, Stack, Group, Button, TextInput, Alert } from '@mantine/core';
import { IconPlayerPlay, IconHistory } from '@tabler/icons-react';
import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { QueryEditor } from './QueryEditor';
import { TimeRangeSelector } from './TimeRangeSelector';
import { QueryHistory } from './QueryHistory';
import { useQuery } from '../../contexts/QueryContext';
import { useQueryExecution } from '../../hooks/useQueryExecution';
import { toUnixTimestamp } from '../../utils/timeFormatting';

interface QueryPanelProps {
  onResultsChange?: (hasResults: boolean) => void;
}

export function QueryPanel({ onResultsChange }: QueryPanelProps) {
  const {
    state,
    setQuery,
    setTimeRange,
    setStep,
    setResult,
    setLoading,
    setError,
    addToHistory,
    getHistory,
  } = useQuery();

  const [searchParams, setSearchParams] = useSearchParams();
  const [showHistory, setShowHistory] = useState(false);
  const queryExecution = useQueryExecution(250);

  const [initialized, setInitialized] = useState(false);

  // Initialize from URL on mount
  useEffect(() => {
    if (!initialized) {
      const queryParam = searchParams.get('query');
      const rangeParam = searchParams.get('range'); // e.g., "1h", "6h", "24h"

      if (queryParam) {
        setQuery(queryParam);
      }

      if (rangeParam) {
        const now = new Date();
        let duration = 60 * 60 * 1000; // Default 1h

        if (rangeParam.endsWith('m')) {
          duration = parseInt(rangeParam) * 60 * 1000;
        } else if (rangeParam.endsWith('h')) {
          duration = parseInt(rangeParam) * 60 * 60 * 1000;
        } else if (rangeParam.endsWith('d')) {
          duration = parseInt(rangeParam) * 24 * 60 * 60 * 1000;
        }

        setTimeRange(new Date(now.getTime() - duration), now);
      }

      setInitialized(true);
    }
  }, [initialized, searchParams, setQuery, setTimeRange]);

  // Update URL when query changes (but not during initialization)
  useEffect(() => {
    if (initialized && state.query) {
      const newParams = new URLSearchParams();
      newParams.set('query', state.query);

      // Calculate range string (e.g., "1h")
      const duration = state.endTime.getTime() - state.startTime.getTime();
      const hours = Math.round(duration / (60 * 60 * 1000));
      if (hours > 0) {
        newParams.set('range', `${hours}h`);
      }

      setSearchParams(newParams, { replace: true });
    }
  }, [initialized, state.query, state.startTime, state.endTime, setSearchParams]);

  const handleExecute = () => {
    if (!state.query.trim()) return;

    // Always use "now" as end time when executing
    const now = new Date();
    const duration = state.endTime.getTime() - state.startTime.getTime();
    const adjustedStart = new Date(now.getTime() - duration);

    // Update the time range in context to show "now"
    setTimeRange(adjustedStart, now);

    // Keep exemplar count low for performance
    const isHeatmapQuery = state.query.trim().startsWith('heatmap(');
    const exemplarCount = isHeatmapQuery ? 5 : 3;

    const params = {
      query: state.query,
      start: toUnixTimestamp(adjustedStart),
      end: toUnixTimestamp(now),
      step: state.step,
      exemplars: exemplarCount, // Reduced for better query performance
      exemplar_strategy: 'slowest' as const, // Default to slowest for debugging
    };

    queryExecution.execute(params, true);
    addToHistory(state.query);
  };

  // Update context when execution state changes
  useEffect(() => {
    setLoading(queryExecution.loading);
    setResult(queryExecution.data);
    setError(queryExecution.error);

    if (onResultsChange) {
      onResultsChange(!!queryExecution.data);
    }
  }, [queryExecution.loading, queryExecution.data, queryExecution.error, setLoading, setResult, setError, onResultsChange]);

  return (
    <Paper shadow="sm" p="md" withBorder>
      <Stack gap="md">
        <QueryEditor
          value={state.query}
          onChange={setQuery}
          onExecute={handleExecute}
        />

        <Group justify="space-between">
          <TimeRangeSelector
            startTime={state.startTime}
            endTime={state.endTime}
            onChange={setTimeRange}
          />

          <Group gap="xs">
            <TextInput
              label="Step"
              value={state.step}
              onChange={(e) => setStep(e.currentTarget.value)}
              placeholder="auto"
              description="Empty = auto"
              size="sm"
              style={{ width: 120 }}
            />

            <Button
              leftSection={<IconHistory size={16} />}
              variant="default"
              size="sm"
              onClick={() => setShowHistory(!showHistory)}
              style={{ marginTop: 20 }}
            >
              History
            </Button>

            <Button
              leftSection={<IconPlayerPlay size={16} />}
              onClick={handleExecute}
              loading={queryExecution.loading}
              disabled={!state.query.trim()}
              size="sm"
              style={{ marginTop: 20 }}
            >
              Execute
            </Button>
          </Group>
        </Group>

        {queryExecution.error && (
          <Alert color="red" title="Query Error">
            {queryExecution.error.message}
          </Alert>
        )}

        {showHistory && (
          <QueryHistory
            queries={getHistory()}
            onSelect={(query) => {
              setQuery(query);
              setShowHistory(false);
            }}
            onClear={() => {
              localStorage.removeItem('artemis_query_history');
              setShowHistory(false);
            }}
          />
        )}
      </Stack>
    </Paper>
  );
}
