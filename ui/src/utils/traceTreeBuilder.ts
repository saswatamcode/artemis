// Trace tree builder - converts flat span array to hierarchical tree

import type { SpanDetail, SpanNode } from '../api/types';

export interface TraceTree {
  roots: SpanNode[];
  spanMap: Map<string, SpanNode>;
  minTime: number;
  maxTime: number;
  duration: number;
}

export function buildSpanTree(spans: SpanDetail[]): TraceTree {
  if (spans.length === 0) {
    return {
      roots: [],
      spanMap: new Map(),
      minTime: 0,
      maxTime: 0,
      duration: 0,
    };
  }

  // First pass: Create SpanNode for each span and build lookup map
  const spanMap = new Map<string, SpanNode>();
  for (const span of spans) {
    spanMap.set(span.spanID, {
      ...span,
      children: [],
      depth: 0,
    });
  }

  // Second pass: Build parent-child relationships
  const roots: SpanNode[] = [];
  for (const node of spanMap.values()) {
    if (node.parentSpanID) {
      const parent = spanMap.get(node.parentSpanID);
      if (parent) {
        parent.children.push(node);
      } else {
        // Orphan span (parent not in this trace) - treat as root
        roots.push(node);
      }
    } else {
      // Root span (no parent)
      roots.push(node);
    }
  }

  // Sort children by start time
  for (const node of spanMap.values()) {
    node.children.sort((a, b) => a.startTime - b.startTime);
  }

  // Sort roots by start time
  roots.sort((a, b) => a.startTime - b.startTime);

  // Third pass: Calculate depth for each node
  function calculateDepth(node: SpanNode, depth: number) {
    node.depth = depth;
    for (const child of node.children) {
      calculateDepth(child, depth + 1);
    }
  }

  for (const root of roots) {
    calculateDepth(root, 0);
  }

  // Calculate trace bounds
  let minTime = Infinity;
  let maxTime = -Infinity;

  for (const span of spans) {
    if (span.startTime < minTime) {
      minTime = span.startTime;
    }
    if (span.endTime > maxTime) {
      maxTime = span.endTime;
    }
  }

  const duration = maxTime - minTime;

  return {
    roots,
    spanMap,
    minTime,
    maxTime,
    duration,
  };
}

// Flatten tree for virtualized rendering (depth-first traversal)
export function flattenSpanTree(
  roots: SpanNode[],
  collapsedSpans: Set<string> = new Set()
): SpanNode[] {
  const result: SpanNode[] = [];

  function traverse(node: SpanNode) {
    result.push(node);

    // Only traverse children if not collapsed
    if (!collapsedSpans.has(node.spanID)) {
      for (const child of node.children) {
        traverse(child);
      }
    }
  }

  for (const root of roots) {
    traverse(root);
  }

  return result;
}

// Find span by ID in tree
export function findSpanInTree(
  roots: SpanNode[],
  spanID: string
): SpanNode | null {
  function search(nodes: SpanNode[]): SpanNode | null {
    for (const node of nodes) {
      if (node.spanID === spanID) {
        return node;
      }
      const found = search(node.children);
      if (found) {
        return found;
      }
    }
    return null;
  }

  return search(roots);
}

// Get all ancestor span IDs
export function getAncestorSpanIDs(
  spanMap: Map<string, SpanNode>,
  spanID: string
): string[] {
  const ancestors: string[] = [];
  let currentSpan = spanMap.get(spanID);

  while (currentSpan?.parentSpanID) {
    ancestors.push(currentSpan.parentSpanID);
    currentSpan = spanMap.get(currentSpan.parentSpanID);
  }

  return ancestors;
}
