// Service-based color generation utilities

// Generate a consistent color for a service name using a hash function
function hashCode(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return Math.abs(hash);
}

// HSL to RGB conversion
function hslToRgb(h: number, s: number, l: number): [number, number, number] {
  const c = (1 - Math.abs(2 * l - 1)) * s;
  const x = c * (1 - Math.abs(((h / 60) % 2) - 1));
  const m = l - c / 2;

  let r = 0,
    g = 0,
    b = 0;
  if (h < 60) {
    [r, g, b] = [c, x, 0];
  } else if (h < 120) {
    [r, g, b] = [x, c, 0];
  } else if (h < 180) {
    [r, g, b] = [0, c, x];
  } else if (h < 240) {
    [r, g, b] = [0, x, c];
  } else if (h < 300) {
    [r, g, b] = [x, 0, c];
  } else {
    [r, g, b] = [c, 0, x];
  }

  return [
    Math.round((r + m) * 255),
    Math.round((g + m) * 255),
    Math.round((b + m) * 255),
  ];
}

const colorCache = new Map<string, string>();

export function getServiceColor(serviceName: string): string {
  if (colorCache.has(serviceName)) {
    return colorCache.get(serviceName)!;
  }

  const hash = hashCode(serviceName);
  const hue = hash % 360;
  const saturation = 0.7;
  const lightness = 0.5;

  const [r, g, b] = hslToRgb(hue, saturation, lightness);
  const color = `rgb(${r}, ${g}, ${b})`;

  colorCache.set(serviceName, color);
  return color;
}

export function getServiceColorWithAlpha(
  serviceName: string,
  alpha: number
): string {
  const hash = hashCode(serviceName);
  const hue = hash % 360;
  const saturation = 0.7;
  const lightness = 0.5;

  const [r, g, b] = hslToRgb(hue, saturation, lightness);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

// Predefined color for error spans
export const ERROR_COLOR = '#ef4444'; // Red
export const WARNING_COLOR = '#f59e0b'; // Orange
export const SUCCESS_COLOR = '#3b82f6'; // Blue
