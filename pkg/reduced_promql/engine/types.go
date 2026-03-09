package engine

import (
	"fmt"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// ResultType indicates the type of query result
type ResultType string

const (
	// ResultTypeSpans - query returns spans (VectorSelector, MatrixSelector)
	ResultTypeSpans ResultType = "spans"
	// ResultTypeVector - query returns instant vector (rate, aggregations)
	ResultTypeVector ResultType = "vector"
	// ResultTypeMatrix - query returns range matrix (heatmap)
	ResultTypeMatrix ResultType = "matrix"
	// ResultTypeScalar - query returns single scalar value (some histogram_quantile cases)
	ResultTypeScalar ResultType = "scalar"
)

// QueryStats contains statistics about query execution.
type QueryStats struct {
	SpansScanned  int64         // Total spans scanned from blocks
	BlocksScanned int64         // Total blocks scanned
	Duration      time.Duration // Total query execution time
}

// String returns a human-readable summary of query statistics.
func (s QueryStats) String() string {
	return fmt.Sprintf("scanned %d spans across %d blocks in %v",
		s.SpansScanned, s.BlocksScanned, s.Duration)
}

// QueryResult contains query results and execution statistics.
// The result type depends on the query:
//   - Selector queries → Spans
//   - rate(), sum(), etc → Vector
//   - heatmap() → Matrix
//   - histogram_quantile() → Scalar or Vector
type QueryResult struct {
	// Type indicates which field contains the result
	Type ResultType

	// Result data (only one will be populated based on Type)
	Spans  []*span.Span  // For ResultTypeSpans
	Vector InstantVector // For ResultTypeVector
	Matrix Matrix        // For ResultTypeMatrix
	Scalar *ScalarResult // For ResultTypeScalar

	// Execution statistics
	Stats QueryStats
}

// Sample represents a single metric sample at a point in time
type Sample struct {
	Metric map[string]string // Label set
	Value  float64           // Sample value
	Time   time.Time         // Sample timestamp
}

// InstantVector is a set of samples at a single point in time (or across time for aggregations)
// Used for: rate(), sum(), avg(), count(), etc.
type InstantVector []Sample

// MatrixSample represents a time series with multiple values
type MatrixSample struct {
	Metric map[string]string // Label set
	Values []SamplePair      // Time series data
}

// SamplePair is a timestamp-value pair
type SamplePair struct {
	Time  time.Time
	Value float64
}

// Matrix is a set of time series
// Used for: heatmap() where we have multiple dimensions (time × duration)
type Matrix []MatrixSample

// ScalarResult is a single scalar value
type ScalarResult struct {
	Value float64
	Time  time.Time
}

// ToPrometheusJSON converts the result to Prometheus-compatible JSON format
func (r *QueryResult) ToPrometheusJSON() map[string]interface{} {
	result := map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"resultType": string(r.Type),
		},
	}

	data := result["data"].(map[string]interface{})

	switch r.Type {
	case ResultTypeVector:
		// Convert InstantVector to Prometheus format
		samples := make([]map[string]interface{}, len(r.Vector))
		for i, s := range r.Vector {
			samples[i] = map[string]interface{}{
				"metric": s.Metric,
				"value":  []interface{}{s.Time.Unix(), fmt.Sprintf("%.15g", s.Value)},
			}
		}
		data["result"] = samples

	case ResultTypeMatrix:
		// Convert Matrix to Prometheus format
		series := make([]map[string]interface{}, len(r.Matrix))
		for i, m := range r.Matrix {
			values := make([][]interface{}, len(m.Values))
			for j, v := range m.Values {
				values[j] = []interface{}{v.Time.Unix(), fmt.Sprintf("%.15g", v.Value)}
			}
			series[i] = map[string]interface{}{
				"metric": m.Metric,
				"values": values,
			}
		}
		data["result"] = series

	case ResultTypeScalar:
		// Scalar result
		data["result"] = []interface{}{r.Scalar.Time.Unix(), fmt.Sprintf("%.15g", r.Scalar.Value)}

	case ResultTypeSpans:
		// For spans, we don't have a Prometheus format
		// Return basic info
		data["result"] = map[string]interface{}{
			"span_count": len(r.Spans),
			"message":    "Use trace API endpoints for span data",
		}
	}

	return result
}
