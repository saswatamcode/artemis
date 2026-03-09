// CodeMirror query editor with autocomplete

import { useCallback, useEffect, useMemo } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { autocompletion, type CompletionContext } from '@codemirror/autocomplete';
import { EditorView } from '@codemirror/view';
import { useAttributeKeys } from '../../hooks/useMetadata';

interface QueryEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute?: () => void;
  height?: string;
}

export function QueryEditor({
  value,
  onChange,
  onExecute,
  height = '40px',
}: QueryEditorProps) {
  const { keys: attributeKeys } = useAttributeKeys();

  // Autocomplete function
  const autocompleteFunction = useCallback(
    (context: CompletionContext) => {
      const word = context.matchBefore(/\w*/);
      if (!word || (word.from === word.to && !context.explicit)) {
        return null;
      }

      const options = attributeKeys.map((key) => ({
        label: key,
        type: 'variable',
      }));

      // Add common PromQL functions
      const functions = [
        'rate',
        'sum',
        'avg',
        'min',
        'max',
        'count',
        'histogram_quantile',
        'increase',
      ].map((fn) => ({
        label: fn,
        type: 'function',
      }));

      return {
        from: word.from,
        options: [...functions, ...options],
      };
    },
    [attributeKeys]
  );

  const extensions = useMemo(
    () => [
      autocompletion({ override: [autocompleteFunction] }),
      EditorView.theme({
        '&': {
          fontSize: '14px',
        },
        '.cm-content': {
          fontFamily: 'ui-monospace, SFMono-Regular, Monaco, Consolas, monospace',
        },
        '.cm-scroller': {
          overflow: 'auto',
        },
      }),
    ],
    [autocompleteFunction]
  );

  // Handle keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Execute query on Enter (without Shift)
      if (e.key === 'Enter' && !e.shiftKey && onExecute) {
        e.preventDefault();
        onExecute();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onExecute]);

  return (
    <CodeMirror
      value={value}
      height={height}
      extensions={extensions}
      onChange={onChange}
      placeholder="Enter PromQL query (e.g., rate(http_requests_total[5m]))"
    />
  );
}
