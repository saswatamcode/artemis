// Query panel state context

import { createContext, useContext, useState, type ReactNode } from 'react';
import type { QueryRangeResponse } from '../api/types';

export interface QueryState {
  query: string;
  startTime: Date;
  endTime: Date;
  step: string;
  result: QueryRangeResponse | null;
  loading: boolean;
  error: Error | null;
}

interface QueryContextValue {
  state: QueryState;
  setQuery: (query: string) => void;
  setTimeRange: (start: Date, end: Date) => void;
  setStep: (step: string) => void;
  setResult: (result: QueryRangeResponse | null) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: Error | null) => void;
  addToHistory: (query: string) => void;
  getHistory: () => string[];
}

const QueryContext = createContext<QueryContextValue | null>(null);

const HISTORY_KEY = 'artemis_query_history';
const MAX_HISTORY = 20;

export function QueryProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<QueryState>({
    query: '',
    startTime: new Date(Date.now() - 60 * 60 * 1000), // Last 1 hour
    endTime: new Date(),
    step: '', // Empty = auto-calculate on backend
    result: null,
    loading: false,
    error: null,
  });

  const setQuery = (query: string) => {
    setState((prev) => ({ ...prev, query }));
  };

  const setTimeRange = (start: Date, end: Date) => {
    setState((prev) => ({ ...prev, startTime: start, endTime: end }));
  };

  const setStep = (step: string) => {
    setState((prev) => ({ ...prev, step }));
  };

  const setResult = (result: QueryRangeResponse | null) => {
    setState((prev) => ({ ...prev, result }));
  };

  const setLoading = (loading: boolean) => {
    setState((prev) => ({ ...prev, loading }));
  };

  const setError = (error: Error | null) => {
    setState((prev) => ({ ...prev, error }));
  };

  const addToHistory = (query: string) => {
    if (!query.trim()) return;

    try {
      const history = getHistory();
      const filtered = history.filter((q) => q !== query);
      const newHistory = [query, ...filtered].slice(0, MAX_HISTORY);
      localStorage.setItem(HISTORY_KEY, JSON.stringify(newHistory));
    } catch (err) {
      console.error('Failed to save query history:', err);
    }
  };

  const getHistory = (): string[] => {
    try {
      const stored = localStorage.getItem(HISTORY_KEY);
      if (stored) {
        return JSON.parse(stored);
      }
    } catch (err) {
      console.error('Failed to load query history:', err);
    }
    return [];
  };

  return (
    <QueryContext.Provider
      value={{
        state,
        setQuery,
        setTimeRange,
        setStep,
        setResult,
        setLoading,
        setError,
        addToHistory,
        getHistory,
      }}
    >
      {children}
    </QueryContext.Provider>
  );
}

export function useQuery() {
  const context = useContext(QueryContext);
  if (!context) {
    throw new Error('useQuery must be used within QueryProvider');
  }
  return context;
}
