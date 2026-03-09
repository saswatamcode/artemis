// Trace view state context

import { createContext, useContext, useState, type ReactNode } from 'react';

export interface TraceState {
  selectedSpanID: string | null;
  collapsedSpans: Set<string>;
  searchTerm: string;
}

interface TraceContextValue {
  state: TraceState;
  selectSpan: (spanID: string | null) => void;
  toggleCollapse: (spanID: string) => void;
  collapseAll: () => void;
  expandAll: () => void;
  setSearchTerm: (term: string) => void;
}

const TraceContext = createContext<TraceContextValue | null>(null);

export function TraceProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<TraceState>({
    selectedSpanID: null,
    collapsedSpans: new Set(),
    searchTerm: '',
  });

  const selectSpan = (spanID: string | null) => {
    setState((prev) => ({ ...prev, selectedSpanID: spanID }));
  };

  const toggleCollapse = (spanID: string) => {
    setState((prev) => {
      const newCollapsed = new Set(prev.collapsedSpans);
      if (newCollapsed.has(spanID)) {
        newCollapsed.delete(spanID);
      } else {
        newCollapsed.add(spanID);
      }
      return { ...prev, collapsedSpans: newCollapsed };
    });
  };

  const collapseAll = () => {
    setState((prev) => ({ ...prev, collapsedSpans: new Set() }));
  };

  const expandAll = () => {
    setState((prev) => ({ ...prev, collapsedSpans: new Set() }));
  };

  const setSearchTerm = (term: string) => {
    setState((prev) => ({ ...prev, searchTerm: term }));
  };

  return (
    <TraceContext.Provider
      value={{
        state,
        selectSpan,
        toggleCollapse,
        collapseAll,
        expandAll,
        setSearchTerm,
      }}
    >
      {children}
    </TraceContext.Provider>
  );
}

export function useTrace() {
  const context = useContext(TraceContext);
  if (!context) {
    throw new Error('useTrace must be used within TraceProvider');
  }
  return context;
}
