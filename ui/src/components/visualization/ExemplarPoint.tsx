// Exemplar point marker for metric charts
// Note: Exemplar support is not yet implemented in the backend
// This component is a placeholder for future implementation

import { useNavigate } from 'react-router-dom';
import type { Exemplar } from '../../api/types';

interface ExemplarPointProps {
  exemplar: Exemplar;
  x: number;
  y: number;
}

export function ExemplarPoint({ exemplar, x, y }: ExemplarPointProps) {
  const navigate = useNavigate();

  const handleClick = () => {
    navigate(`/trace/${exemplar.traceID}`);
  };

  return (
    <circle
      cx={x}
      cy={y}
      r={4}
      fill="#ef4444"
      stroke="white"
      strokeWidth={2}
      onClick={handleClick}
      style={{ cursor: 'pointer' }}
    >
      <title>{`Trace: ${exemplar.traceID} (${exemplar.duration}ns)`}</title>
    </circle>
  );
}
