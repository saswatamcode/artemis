// Canvas timeline bars for span visualization

import { useEffect, useRef } from 'react';
import { getServiceColor, ERROR_COLOR } from '../../utils/colorUtils';
import type { SpanNode } from '../../api/types';

interface TimelineProps {
  spans: SpanNode[];
  minTime: number;
  maxTime: number;
  selectedSpanID: string | null;
  onSpanClick?: (spanID: string) => void;
}

const ROW_HEIGHT = 32;
const PADDING = 4;

export function Timeline({
  spans,
  minTime,
  maxTime,
  selectedSpanID,
  onSpanClick,
}: TimelineProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const width = canvas.width;
    const height = canvas.height;
    const duration = maxTime - minTime;

    // Clear canvas
    ctx.clearRect(0, 0, width, height);

    // Draw spans
    spans.forEach((span, index) => {
      const y = index * ROW_HEIGHT;

      // Calculate x position and width based on time
      const startRatio = (span.startTime - minTime) / duration;
      const durationRatio = span.duration / duration;

      const x = startRatio * width;
      const barWidth = Math.max(durationRatio * width, 2); // Minimum 2px width

      // Determine color
      const hasError = span.attributes['error'] === 'true' ||
                       span.attributes['otel.status_code'] === 'ERROR';
      const color = hasError ? ERROR_COLOR : getServiceColor(span.serviceName);

      // Draw bar
      ctx.fillStyle = color;
      ctx.fillRect(
        x,
        y + PADDING,
        barWidth,
        ROW_HEIGHT - PADDING * 2
      );

      // Highlight selected span
      if (span.spanID === selectedSpanID) {
        ctx.strokeStyle = '#2563eb';
        ctx.lineWidth = 2;
        ctx.strokeRect(
          x - 1,
          y + PADDING - 1,
          barWidth + 2,
          ROW_HEIGHT - PADDING * 2 + 2
        );
      }
    });
  }, [spans, minTime, maxTime, selectedSpanID]);

  const handleClick = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (!onSpanClick) return;

    const canvas = canvasRef.current;
    if (!canvas) return;

    const rect = canvas.getBoundingClientRect();
    const y = e.clientY - rect.top;
    const rowIndex = Math.floor(y / ROW_HEIGHT);

    if (rowIndex >= 0 && rowIndex < spans.length) {
      onSpanClick(spans[rowIndex].spanID);
    }
  };

  return (
    <canvas
      ref={canvasRef}
      width={800}
      height={spans.length * ROW_HEIGHT}
      onClick={handleClick}
      style={{
        width: '100%',
        height: spans.length * ROW_HEIGHT,
        cursor: 'pointer',
      }}
    />
  );
}
