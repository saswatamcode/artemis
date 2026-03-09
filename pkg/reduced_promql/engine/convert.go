package engine

import (
	"strconv"
	"time"

	"github.com/saswatamcode/artemis/pkg/reduced_promql/parser"
	"github.com/saswatamcode/artemis/pkg/span"
)

// determineResultType determines what type of result a query will produce
// based on the AST expression type
func determineResultType(expr parser.Expr) ResultType {
	switch e := expr.(type) {
	case *parser.VectorSelector:
		return ResultTypeSpans

	case *parser.MatrixSelector:
		return ResultTypeSpans

	case *parser.Call:
		switch e.Func {
		case "rate":
			return ResultTypeMatrix // rate() returns range vectors (time series)
		case "histogram_quantile":
			return ResultTypeScalar
		case "heatmap":
			return ResultTypeMatrix
		}

	case *parser.Aggregation:
		// All aggregations return vectors
		return ResultTypeVector
	}

	// Default to spans
	return ResultTypeSpans
}

// convertSpansToVector converts synthetic spans (from rate, aggregations) to InstantVector
//
// Expected span format from operators:
//   - rate(): Tags["rate"], Tags["bucket_time"], Tags["count"]
//   - aggregations: Tags["agg_value"], Tags["agg_op"], plus grouping labels
func convertSpansToVector(spans []*span.Span) InstantVector {
	vector := make(InstantVector, 0, len(spans))

	for _, sp := range spans {
		sample := Sample{
			Metric: make(map[string]string),
		}

		// Extract value from tags
		if rateVal, ok := sp.Tags["rate"]; ok {
			// Rate result
			if val, err := strconv.ParseFloat(rateVal, 64); err == nil {
				sample.Value = val
			}
			if bucketTime, ok := sp.Tags["bucket_time"]; ok {
				if ts, err := strconv.ParseInt(bucketTime, 10, 64); err == nil {
					sample.Time = time.Unix(0, ts)
				}
			}

			// Copy all non-synthetic tags as metric labels
			// Exclude synthetic tags: rate, bucket_time, count
			for k, v := range sp.Tags {
				if k != "rate" && k != "bucket_time" && k != "count" {
					sample.Metric[k] = v
				}
			}

		} else if aggVal, ok := sp.Tags["agg_value"]; ok {
			// Aggregation result
			if val, err := strconv.ParseFloat(aggVal, 64); err == nil {
				sample.Value = val
			}
			sample.Time = sp.StartTime

			// Copy all tags except agg_value and agg_op as metric labels
			for k, v := range sp.Tags {
				if k != "agg_value" && k != "agg_op" {
					sample.Metric[k] = v
				}
			}
		}

		vector = append(vector, sample)
	}

	return vector
}

// convertRateSpansToMatrix converts rate() synthetic spans to Matrix (time series)
func convertRateSpansToMatrix(spans []*span.Span) Matrix {
	// Group spans by metric labels to create separate time series
	seriesMap := make(map[string]*MatrixSample)

	for _, sp := range spans {
		// Extract value and time
		var value float64
		var timestamp time.Time

		if rateVal, ok := sp.Tags["rate"]; ok {
			if val, err := strconv.ParseFloat(rateVal, 64); err == nil {
				value = val
			}
		}

		if bucketTime, ok := sp.Tags["bucket_time"]; ok {
			if ts, err := strconv.ParseInt(bucketTime, 10, 64); err == nil {
				timestamp = time.Unix(0, ts)
			}
		}

		// Build metric labels (excluding synthetic tags)
		metric := make(map[string]string)
		for k, v := range sp.Tags {
			if k != "rate" && k != "bucket_time" && k != "count" {
				metric[k] = v
			}
		}

		// Create a key for this series based on metric labels
		metricKey := ""
		if len(metric) > 0 {
			// Use sorted keys for consistent grouping
			keys := make([]string, 0, len(metric))
			for k := range metric {
				keys = append(keys, k)
			}
			// Sort keys for stable ordering
			for i := 0; i < len(keys)-1; i++ {
				for j := i + 1; j < len(keys); j++ {
					if keys[i] > keys[j] {
						keys[i], keys[j] = keys[j], keys[i]
					}
				}
			}
			for _, k := range keys {
				metricKey += k + "=" + metric[k] + ","
			}
		}

		// Get or create series for this metric
		series, exists := seriesMap[metricKey]
		if !exists {
			series = &MatrixSample{
				Metric: metric,
				Values: make([]SamplePair, 0),
			}
			seriesMap[metricKey] = series
		}

		// Add data point to series
		series.Values = append(series.Values, SamplePair{
			Time:  timestamp,
			Value: value,
		})
	}

	// Convert map to slice and sort values by time
	matrix := make(Matrix, 0, len(seriesMap))
	for _, series := range seriesMap {
		// Sort values by time
		// (rate operator doesn't guarantee order)
		for i := 0; i < len(series.Values)-1; i++ {
			for j := i + 1; j < len(series.Values); j++ {
				if series.Values[i].Time.After(series.Values[j].Time) {
					series.Values[i], series.Values[j] = series.Values[j], series.Values[i]
				}
			}
		}
		matrix = append(matrix, *series)
	}

	return matrix
}

// convertSpansToMatrix converts synthetic spans to Matrix
// This is a dispatcher that routes to the appropriate converter based on span format:
//   - Rate data (has "rate" tag) → convertRateSpansToMatrix
//   - Heatmap data (has "duration_bucket" tag) → convertHeatmapSpansToMatrix
func convertSpansToMatrix(spans []*span.Span) Matrix {
	if len(spans) == 0 {
		return Matrix{}
	}

	// Check first span to determine format
	firstSpan := spans[0]

	// Rate data has "rate" and "bucket_time" tags
	if _, hasRate := firstSpan.Tags["rate"]; hasRate {
		return convertRateSpansToMatrix(spans)
	}

	// Heatmap data has "duration_bucket" tag
	if _, hasDurationBucket := firstSpan.Tags["duration_bucket"]; hasDurationBucket {
		return convertHeatmapSpansToMatrix(spans)
	}

	// Fallback to empty matrix
	return Matrix{}
}

// convertHeatmapSpansToMatrix converts synthetic spans (from heatmap) to Matrix
//
// Expected span format from heatmap operator:
//   - Tags["time_bucket"] - timestamp (int64)
//   - Tags["duration_bucket"] - duration bucket index (0-63)
//   - Tags["count"] - number of spans in this cell
//   - Tags["duration_range"] - human-readable duration range
func convertHeatmapSpansToMatrix(spans []*span.Span) Matrix {
	// Group spans by time_bucket to create time series
	seriesMap := make(map[string]*MatrixSample)

	for _, sp := range spans {
		timeBucket := sp.Tags["time_bucket"]
		durationBucket := sp.Tags["duration_bucket"]
		countStr := sp.Tags["count"]
		durationRange := sp.Tags["duration_range"]

		// Parse values
		var timestamp time.Time
		if ts, err := strconv.ParseInt(timeBucket, 10, 64); err == nil {
			timestamp = time.Unix(0, ts)
		}

		var count float64
		if c, err := strconv.ParseFloat(countStr, 64); err == nil {
			count = c
		}

		// Create metric labels
		metricKey := "duration_bucket=" + durationBucket
		series, exists := seriesMap[metricKey]
		if !exists {
			series = &MatrixSample{
				Metric: map[string]string{
					"duration_bucket": durationBucket,
					"duration_range":  durationRange,
				},
				Values: make([]SamplePair, 0),
			}
			seriesMap[metricKey] = series
		}

		// Add data point
		series.Values = append(series.Values, SamplePair{
			Time:  timestamp,
			Value: count,
		})
	}

	// Convert map to slice
	matrix := make(Matrix, 0, len(seriesMap))
	for _, series := range seriesMap {
		matrix = append(matrix, *series)
	}

	return matrix
}

// convertSpansToScalar converts synthetic spans (from histogram_quantile) to ScalarResult
//
// Expected span format from histogram_quantile operator:
//   - Tags["quantile"] - requested quantile (e.g., "0.95")
//   - Tags["quantile_value"] - calculated duration in nanoseconds
//   - Tags["total_spans"] - total number of spans
func convertSpansToScalar(spans []*span.Span) *ScalarResult {
	if len(spans) == 0 {
		return &ScalarResult{
			Value: 0,
			Time:  time.Now(),
		}
	}

	// histogram_quantile should return a single span
	sp := spans[0]

	var value float64
	if quantileValue, ok := sp.Tags["quantile_value"]; ok {
		if val, err := strconv.ParseFloat(quantileValue, 64); err == nil {
			value = val
		}
	}

	return &ScalarResult{
		Value: value,
		Time:  sp.StartTime,
	}
}
