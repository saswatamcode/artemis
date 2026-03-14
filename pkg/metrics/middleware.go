package metrics

import (
	"net/http"
	"strconv"
	"time"
)

// responseWriter wraps http.ResponseWriter to capture status code and response size
type responseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.bytesWritten += int64(n)
	return n, err
}

// HTTPMiddleware returns middleware that instruments HTTP handlers with Prometheus metrics
func HTTPMiddleware(apiName string, m *APIMetrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if m == nil {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()

			// Wrap response writer to capture status code and size
			rw := &responseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK, // default if WriteHeader is not called
			}

			// Serve the request
			next.ServeHTTP(rw, r)

			// Record metrics
			duration := time.Since(start).Seconds()
			status := strconv.Itoa(rw.statusCode)

			// Get request size
			requestSize := r.ContentLength
			if requestSize < 0 {
				requestSize = 0
			}

			m.RecordHTTPRequest(
				apiName,
				r.Method,
				r.URL.Path,
				status,
				duration,
				requestSize,
				rw.bytesWritten,
			)
		})
	}
}
