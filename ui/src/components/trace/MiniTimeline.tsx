// Mini timeline overview for trace

import { useEffect, useRef } from 'react';
import { Box } from '@mantine/core';
import { getServiceColor } from '../../utils/colorUtils';
import type { SpanNode } from '../../api/types';

interface MiniTimelineProps {
  spans: SpanNode[];
  minTime: number;
  maxTime: number;
}

const HEIGHT = 50;

export function MiniTimeline({ spans, minTime, maxTime }: MiniTimelineProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    // Use device pixel ratio for sharp rendering
    const dpr = window.devicePixelRatio || 1;
    const displayWidth = 1200;
    const displayHeight = HEIGHT;

    canvas.width = displayWidth * dpr;
    canvas.height = displayHeight * dpr;
    ctx.scale(dpr, dpr);

    const width = displayWidth;
    const height = displayHeight;
    const duration = maxTime - minTime;

    // Find max depth for vertical scaling
    const maxDepth = Math.max(...spans.map((s) => s.depth || 0), 0);
    const rowHeight = maxDepth > 0 ? (height - 16) / (maxDepth + 1) : height - 16;

    // Clear canvas
    ctx.clearRect(0, 0, width, height);

    // Draw background (transparent for blue card)
    ctx.fillStyle = 'rgba(255, 255, 255, 0.2)';
    ctx.fillRect(0, 0, width, height);

    // Draw spans as hierarchical bars (like miniature gantt chart)
    spans.forEach((span) => {
      const startRatio = (span.startTime - minTime) / duration;
      const durationRatio = span.duration / duration;

      const x = startRatio * width;
      const barWidth = Math.max(durationRatio * width, 1);

      // Position based on depth (hierarchical structure)
      const depth = span.depth || 0;
      const y = 8 + depth * rowHeight;
      const barHeight = Math.min(rowHeight * 0.7, 4); // Thin bars, max 4px

      const color = getServiceColor(span.serviceName);
      ctx.fillStyle = color;
      ctx.globalAlpha = 0.85;
      ctx.fillRect(x, y, Math.max(barWidth, 0.5), barHeight);
      ctx.globalAlpha = 1;
    });

    // Draw border (subtle white)
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.4)';
    ctx.lineWidth = 2;
    ctx.strokeRect(0, 0, width, height);
  }, [spans, minTime, maxTime]);

  return (
    <Box>
      <canvas
        ref={canvasRef}
        style={{
          width: '100%',
          height: HEIGHT,
          display: 'block',
          borderRadius: '4px',
        }}
      />
    </Box>
  );
}
