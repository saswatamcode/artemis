import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('errors');

// Test configuration
export const options = {
  stages: [
    { duration: '30s', target: 50 },  // Ramp up to 10 VUs over 30s
    { duration: '1m', target: 50 },   // Stay at 10 VUs for 1 minute
    { duration: '30s', target: 150 },  // Ramp up to 20 VUs
    { duration: '2m', target: 150 },   // Stay at 20 VUs for 2 minutes
    { duration: '30s', target: 0 },   // Ramp down to 0
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // 95% of requests should be below 500ms
    errors: ['rate<0.1'],              // Error rate should be below 10%
  },
};

// Generate random hex string
function randomHex(length) {
  const chars = '0123456789abcdef';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

// Generate trace and span IDs
function generateTraceID() {
  return randomHex(32); // 16 bytes = 32 hex chars
}

function generateSpanID() {
  return randomHex(16); // 8 bytes = 16 hex chars
}

// Generate current timestamp in nanoseconds
function nowNano() {
  return Date.now() * 1000000; // Convert milliseconds to nanoseconds
}

// Create OTLP trace payload
function createOTLPTrace(serviceName, spanName, numSpans = 3) {
  const traceID = generateTraceID();
  const now = nowNano();
  const spanDuration = 100000000; // 100ms in nanoseconds

  const spans = [];
  let parentSpanID = null;

  for (let i = 0; i < numSpans; i++) {
    const spanID = generateSpanID();
    const span = {
      traceId: traceID,
      spanId: spanID,
      name: `${spanName}-${i}`,
      kind: i === 0 ? 1 : 2, // SPAN_KIND_SERVER or SPAN_KIND_CLIENT
      startTimeUnixNano: (now + i * 10000000).toString(),
      endTimeUnixNano: (now + i * 10000000 + spanDuration).toString(),
      attributes: [
        {
          key: 'http.method',
          value: { stringValue: 'POST' }
        },
        {
          key: 'http.status_code',
          value: { intValue: '200' }
        },
        {
          key: 'span.index',
          value: { intValue: i.toString() }
        }
      ]
    };

    if (parentSpanID) {
      span.parentSpanId = parentSpanID;
    }

    parentSpanID = spanID;
    spans.push(span);
  }

  return {
    resourceSpans: [
      {
        resource: {
          attributes: [
            {
              key: 'service.name',
              value: { stringValue: serviceName }
            },
            {
              key: 'service.version',
              value: { stringValue: '1.0.0' }
            },
            {
              key: 'deployment.environment',
              value: { stringValue: 'k6-load-test' }
            }
          ]
        },
        scopeSpans: [
          {
            scope: {
              name: 'k6-load-test',
              version: '1.0.0'
            },
            spans: spans
          }
        ]
      }
    ]
  };
}

// Main test function
export default function () {
  const otlpEndpoint = __ENV.OTLP_ENDPOINT || 'http://otel-collector:4318';
  const url = `${otlpEndpoint}/v1/traces`;

  // Generate random service name from a pool
  const services = [
    'k6-frontend',
    'k6-api-gateway',
    'k6-auth-service',
    'k6-payment-service',
    'k6-inventory-service',
    'k6-notification-service',
  ];
  const serviceName = services[Math.floor(Math.random() * services.length)];

  // Create trace with 3-5 spans
  const numSpans = Math.floor(Math.random() * 3) + 3; // 3-5 spans
  const trace = createOTLPTrace(serviceName, 'operation', numSpans);

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    timeout: '10s',
  };

  // Send OTLP trace
  const res = http.post(url, JSON.stringify(trace), params);

  // Check response
  const success = check(res, {
    'status is 200': (r) => r.status === 200,
    'response time < 500ms': (r) => r.timings.duration < 500,
  });

  errorRate.add(!success);

  // Brief sleep to simulate realistic load pattern
  sleep(Math.random() * 0.5); // Random sleep 0-500ms
}
