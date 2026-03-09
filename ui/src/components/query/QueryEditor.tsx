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
  const { keys: attributeKeys, loading, error } = useAttributeKeys();

  // Debug logging
  useEffect(() => {
    console.log('[QueryEditor] Attribute keys:', attributeKeys);
    console.log('[QueryEditor] Loading:', loading);
    console.log('[QueryEditor] Error:', error);
  }, [attributeKeys, loading, error]);

  // Context-aware autocomplete function
  const autocompleteFunction = useCallback(
    (context: CompletionContext) => {
      const line = context.state.doc.lineAt(context.pos);
      const textBefore = line.text.slice(0, context.pos - line.from);

      console.log('[Autocomplete] Triggered at position:', context.pos);
      console.log('[Autocomplete] Text before cursor:', textBefore);
      console.log('[Autocomplete] Available keys:', attributeKeys);

      // Check if we're inside braces {...}
      const openBraces = (textBefore.match(/{/g) || []).length;
      const closeBraces = (textBefore.match(/}/g) || []).length;
      const insideBraces = openBraces > closeBraces;

      // Check if we're after an equals sign (for label values)
      const afterEquals = /(\w+)\s*=\s*["']?\w*$/.test(textBefore);

      console.log('[Autocomplete] Inside braces:', insideBraces);
      console.log('[Autocomplete] After equals:', afterEquals);

      // Match current word being typed
      const word = context.matchBefore(/[\w."-]*/);
      if (!word || (word.from === word.to && !context.explicit)) {
        console.log('[Autocomplete] No word match, returning null');
        return null;
      }

      let options: Array<{ label: string; type: string; apply?: string }> = [];

      if (insideBraces && !afterEquals) {
        // Inside braces: suggest label names
        console.log('[Autocomplete] Suggesting attribute keys');
        options = attributeKeys.map((key) => ({
          label: key,
          type: 'variable',
          apply: key + '="',
        }));
      } else if (insideBraces && afterEquals) {
        // After equals: suggest label values (would need API call per key)
        // For now, just show placeholder
        console.log('[Autocomplete] Suggesting label values placeholder');
        options = [
          { label: '"value"', type: 'text' },
        ];
      } else {
        // Outside braces: suggest functions
        console.log('[Autocomplete] Suggesting functions');
        const functions = [
          'rate',
          'heatmap',
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
          apply: fn + '(',
        }));

        options = functions;
      }

      console.log('[Autocomplete] Options:', options);

      if (options.length === 0) {
        return null;
      }

      return {
        from: word.from,
        options,
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
