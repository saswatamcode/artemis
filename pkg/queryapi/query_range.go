package queryapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/saswatamcode/artemis/pkg/reduced_promql/engine"
	"github.com/saswatamcode/artemis/pkg/reduced_promql/parser"
	"github.com/saswatamcode/artemis/pkg/span"
)

// handleQueryRange executes metric queries with optional trace exemplars
// GET/POST /api/v1/query_range?query=Q&start=X&end=Y&step=S&exemplars=N&exemplar_strategy=S
//
// This is a unified endpoint that returns both metric data AND sample trace IDs:
//   - Metric data: Prometheus-compatible time series (vector/matrix/scalar)
//   - Exemplars: Sample trace IDs per time step for drilling down into traces
//
// Parameters:
//   - query: The reduced PromQL query (required)
//   - start: Start time (unix epoch seconds/nanos or RFC3339)
//   - end: End time (unix epoch seconds/nanos or RFC3339)
//   - step: Time series granularity (e.g., "15s", "1m") - optional
//   - exemplars: Max number of exemplars per time step (optional, default 0 = no exemplars)
//   - exemplar_strategy: Selection strategy - "slowest", "fastest", "random" (optional, default "slowest")
//
// Returns: Prometheus-compatible JSON with metric data + embedded exemplars
func (s *Server) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	queryStr := q.Get("query")
	if queryStr == "" {
		respondError(w, http.StatusBadRequest, "query parameter 'query' is required")
		return
	}

	// Parse time range
	start, end, err := parseTimeRange(q.Get("start"), q.Get("end"))
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Parse optional step parameter
	stepStr := q.Get("step")
	if stepStr == "" {
		// Default step based on time range
		stepStr = calculateDefaultStep(start, end)
	}

	// Parse step duration
	stepDuration, err := time.ParseDuration(stepStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid step parameter: %v", err))
		return
	}

	// Parse optional exemplars parameter
	exemplars := 0
	if exemplarsStr := q.Get("exemplars"); exemplarsStr != "" {
		if e, err := strconv.Atoi(exemplarsStr); err == nil && e > 0 {
			exemplars = e
			// Cap at 100 per step to prevent excessive queries
			if exemplars > 100 {
				exemplars = 100
			}
		}
	}

	// Parse optional exemplar strategy
	exemplarStrategy := q.Get("exemplar_strategy")
	if exemplarStrategy == "" {
		exemplarStrategy = "slowest" // Default to slowest (most useful for debugging)
	}

	requestStart := time.Now()
	s.logger.Info("Processing query_range request",
		slog.String("query", queryStr),
		slog.Time("start", start),
		slog.Time("end", end),
		slog.String("step", stepStr),
		slog.Duration("step_duration", stepDuration),
		slog.Int("exemplars", exemplars),
		slog.String("exemplar_strategy", exemplarStrategy))

	// Execute metric query using reduced PromQL engine
	opts := &engine.QueryOptions{
		StartTime:   start,
		EndTime:     end,
		Step:        stepDuration,
		Context:     r.Context(),
		UseSnapshot: true, // Enable MVCC snapshot isolation
		Limit:       0,    // No limit for metrics queries
	}

	result, err := s.queryEngine.Execute(queryStr, opts)
	if err != nil {
		s.logger.Error("Query execution failed",
			slog.String("query", queryStr),
			slog.String("error", err.Error()))
		respondError(w, http.StatusBadRequest, "query execution failed: "+err.Error())
		return
	}

	s.logger.Debug("Query executed successfully",
		slog.String("query", queryStr),
		slog.String("result_type", string(result.Type)),
		slog.String("stats", result.Stats.String()))

	// For span queries (VectorSelector), convert to time series by counting spans per time step
	// This enables queries like {name="promqlExec"} to show span counts over time (like Prometheus metrics)
	if result.Type == engine.ResultTypeSpans {
		conversionStart := time.Now()
		s.logger.Debug("Converting span query to time series",
			slog.Int("span_count", len(result.Spans)))

		// Convert spans to time series by bucketing and counting
		matrix := convertSpansToTimeSeries(result.Spans, start, end, stepDuration)
		result.Type = engine.ResultTypeMatrix
		result.Matrix = matrix

		s.logger.Debug("Span to timeseries conversion complete",
			slog.Duration("conversion_duration", time.Since(conversionStart)),
			slog.Int("series_count", len(matrix)))
	}

	// Convert to Prometheus-compatible JSON format
	prometheusJSON := result.ToPrometheusJSON()

	// Add query stats to the response
	data := prometheusJSON["data"].(map[string]interface{})
	data["stats"] = &QueryStats{
		SpansScanned:  result.Stats.SpansScanned,
		BlocksScanned: result.Stats.BlocksScanned,
		ExecutionTime: result.Stats.Duration.String(),
	}

	// Add exemplar support if requested
	if exemplars > 0 && result.Type == engine.ResultTypeMatrix {
		exemplarStart := time.Now()
		s.logger.Debug("Fetching exemplars",
			slog.Int("count", exemplars),
			slog.String("strategy", exemplarStrategy))

		// Extract selector and range from the query
		selector, rangeDuration, hasRange, err := extractSelectorAndRange(queryStr)
		if err != nil {
			s.logger.Warn("Failed to extract selector for exemplars",
				slog.String("query", queryStr),
				slog.String("error", err.Error()))
		} else {
			// Fetch exemplars for each time step
			exemplarMap, err := s.fetchExemplars(r.Context(), selector, start, end, stepDuration, rangeDuration, hasRange, exemplars, exemplarStrategy)
			if err != nil {
				s.logger.Warn("Failed to fetch exemplars",
					slog.String("error", err.Error()))
			} else {
				// Attach exemplars to the response
				s.attachExemplarsToResponse(data, exemplarMap)
				s.logger.Debug("Exemplars fetched",
					slog.Duration("exemplar_duration", time.Since(exemplarStart)),
					slog.Int("exemplar_count", len(exemplarMap)),
					slog.Bool("has_range", hasRange),
					slog.Duration("range", rangeDuration))
			}
		}
	}

	s.logger.Info("Query_range request completed",
		slog.String("query", queryStr),
		slog.Duration("total_duration", time.Since(requestStart)),
		slog.Duration("query_exec_duration", result.Stats.Duration),
		slog.Int64("spans_scanned", result.Stats.SpansScanned))

	respondJSON(w, prometheusJSON)
}

// extractSelectorAndRange extracts the base selector and range duration from a query expression.
// For example, from "rate({name="promqlExec"}[5m])" it extracts:
//   - selector: "{name="promqlExec"}"
//   - rangeDuration: 5m
//   - hasRange: true
// For "{name="foo"}" it extracts:
//   - selector: "{name="foo"}"
//   - rangeDuration: 0
//   - hasRange: false
func extractSelectorAndRange(queryStr string) (selector string, rangeDuration time.Duration, hasRange bool, err error) {
	expr, parseErr := parser.Parse(queryStr)
	if parseErr != nil {
		return "", 0, false, fmt.Errorf("failed to parse query: %w", parseErr)
	}

	// Walk the AST to find the first VectorSelector or MatrixSelector
	var findSelector func(parser.Expr) bool
	findSelector = func(e parser.Expr) bool {
		switch n := e.(type) {
		case *parser.VectorSelector:
			selector = n.String()
			hasRange = false
			return true
		case *parser.MatrixSelector:
			// MatrixSelector has a Vector field which is a VectorSelector
			selector = n.Vector.String()
			// Parse range duration from RangeStr (e.g., "5m")
			rangeDuration, err = time.ParseDuration(n.RangeStr)
			if err != nil {
				err = fmt.Errorf("failed to parse range duration %q: %w", n.RangeStr, err)
				return false
			}
			hasRange = true
			return true
		case *parser.Call:
			// Check function arguments
			for _, arg := range n.Args {
				if findSelector(arg) {
					return true
				}
			}
		case *parser.Aggregation:
			// Check aggregation expression
			return findSelector(n.Expr)
		}
		return false
	}

	if !findSelector(expr) {
		return "", 0, false, fmt.Errorf("no selector found in query")
	}

	if err != nil {
		return "", 0, false, err
	}

	return selector, rangeDuration, hasRange, nil
}

// fetchExemplars queries actual spans and assigns exemplars to time steps.
//
// Two strategies:
//  1. Lookback window (hasRange=true): For rate({...}[5m]), each step gets exemplars from
//     its lookback window [step-range, step], matching the rate operator's evaluation logic.
//  2. Direct bucketing (hasRange=false): For {name="foo"}, heatmap(), etc., each step gets
//     exemplars from spans that fall in that time bucket.
func (s *Server) fetchExemplars(ctx context.Context, selector string, start, end time.Time, step time.Duration, rangeDuration time.Duration, hasRange bool, count int, strategy string) (map[int64][]Exemplar, error) {
	// Query spans ONCE for the entire time range
	// For lookback queries, extend start time to capture spans in first step's lookback window
	queryStart := start
	if hasRange && rangeDuration > 0 {
		queryStart = start.Add(-rangeDuration)
	}

	// Keep exemplar fetch lightweight - only get what we need
	// Limit = count * 30 (buffer for bucketing across time steps)
	// This prevents slow queries while still providing good exemplar coverage
	limit := count * 30
	if limit < 50 {
		limit = 50 // Minimum
	}
	if limit > 500 {
		limit = 500 // Hard cap for performance
	}

	queryStr := selector
	opts := &engine.QueryOptions{
		StartTime:   queryStart,
		EndTime:     end,
		Context:     ctx,
		UseSnapshot: true,
		Limit:       limit,
	}

	result, err := s.queryEngine.Execute(queryStr, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to query spans for exemplars: %w", err)
	}

	if result.Type != engine.ResultTypeSpans || len(result.Spans) == 0 {
		return make(map[int64][]Exemplar), nil
	}

	// Sort spans for deterministic ordering (by StartTime, then SpanID)
	// This ensures we always get the same exemplars for the same query
	sort.Slice(result.Spans, func(i, j int) bool {
		if !result.Spans[i].StartTime.Equal(result.Spans[j].StartTime) {
			return result.Spans[i].StartTime.Before(result.Spans[j].StartTime)
		}
		return result.Spans[i].SpanID < result.Spans[j].SpanID
	})

	// Choose strategy based on query type
	if hasRange {
		// Strategy 1: Lookback window (for rate(), etc.)
		// Each step gets exemplars from spans in [step-range, step]
		return s.fetchExemplarsWithLookback(result.Spans, start, end, step, rangeDuration, count, strategy)
	}

	// Strategy 2: Direct bucketing (for selectors, heatmap, etc.)
	// Each step gets exemplars from spans that fall in that time bucket
	return s.fetchExemplarsWithBucketing(result.Spans, start, end, step, count, strategy)
}

// fetchExemplarsWithLookback implements lookback window strategy for range queries like rate().
// For each step, it selects exemplars from the sliding lookback window [step-range, step).
// This matches the exact evaluation logic of the rate operator.
//
// Boundary semantics: [windowStart, windowEnd) - inclusive start, exclusive end.
// This prevents double-counting spans that fall exactly on step boundaries.
func (s *Server) fetchExemplarsWithLookback(spans []*span.Span, start, end time.Time, step time.Duration, rangeDuration time.Duration, count int, strategy string) (map[int64][]Exemplar, error) {
	exemplarMap := make(map[int64][]Exemplar)

	// Iterate over each step in the evaluation range
	for stepTime := start; stepTime.Before(end) || stepTime.Equal(end); stepTime = stepTime.Add(step) {
		// Calculate lookback window for this step: [windowStart, windowEnd)
		windowStart := stepTime.Add(-rangeDuration)
		windowEnd := stepTime

		// Filter spans that fall within this lookback window
		var windowSpans []*span.Span
		for _, sp := range spans {
			// Use [windowStart, windowEnd) semantics (inclusive start, exclusive end)
			// This matches Prometheus convention and prevents double-counting
			if !sp.StartTime.Before(windowStart) && sp.StartTime.Before(windowEnd) {
				windowSpans = append(windowSpans, sp)
			}
		}

		// Select exemplars from this window
		if len(windowSpans) > 0 {
			exemplars := selectExemplars(windowSpans, count, strategy)
			if len(exemplars) > 0 {
				exemplarMap[stepTime.Unix()] = exemplars
			}
		}
	}

	return exemplarMap, nil
}

// fetchExemplarsWithBucketing implements direct bucketing strategy for non-range queries.
// Spans are assigned to buckets based on where their timestamp falls.
// This is used for selector queries {name="foo"}, heatmap(), etc.
//
// Boundary semantics: Spans in [start, end) are assigned to buckets.
// Each bucket covers [bucketTime, bucketTime+step).
func (s *Server) fetchExemplarsWithBucketing(spans []*span.Span, start, end time.Time, step time.Duration, count int, strategy string) (map[int64][]Exemplar, error) {
	// Group spans by time step bucket - OPTIMIZED with direct bucket calculation
	bucketMap := make(map[int64][]*span.Span)
	stepNanos := step.Nanoseconds()
	startNanos := start.UnixNano()
	endNanos := end.UnixNano()

	for _, sp := range spans {
		spanNanos := sp.StartTime.UnixNano()

		// Use [start, end) range semantics (inclusive start, exclusive end)
		if spanNanos >= startNanos && spanNanos < endNanos {
			// Calculate bucket directly instead of iterating through all steps
			// This is O(n) instead of O(n*m) - HUGE performance improvement!
			bucketOffset := (spanNanos - startNanos) / stepNanos
			bucketTime := start.Add(time.Duration(bucketOffset) * step)

			// Double-check bucket is within range (should always be true due to above check)
			if !bucketTime.After(end) {
				bucket := bucketTime.Unix()
				bucketMap[bucket] = append(bucketMap[bucket], sp)
			}
		}
	}

	// Select exemplars from each bucket
	exemplarMap := make(map[int64][]Exemplar)
	for bucket, bucketSpans := range bucketMap {
		if len(bucketSpans) > 0 {
			exemplars := selectExemplars(bucketSpans, count, strategy)
			if len(exemplars) > 0 {
				exemplarMap[bucket] = exemplars
			}
		}
	}

	return exemplarMap, nil
}

// attachExemplarsToResponse attaches exemplars to the Prometheus JSON response
func (s *Server) attachExemplarsToResponse(data map[string]interface{}, exemplarMap map[int64][]Exemplar) {
	// The data structure is:
	// data["result"] = []map[string]interface{}{
	//   {
	//     "metric": {...},
	//     "values": [[timestamp, value], ...]
	//   },
	//   ...
	// }
	//
	// We need to add exemplars to each series:
	// {
	//   "metric": {...},
	//   "values": [[timestamp, value], ...],
	//   "exemplars": [
	//     {"timestamp": 123, "value": 456, "traceID": "abc"},
	//     ...
	//   ]
	// }

	result, ok := data["result"].([]map[string]interface{})
	if !ok {
		return
	}

	// For each series, attach exemplars that match the time range
	for _, series := range result {
		values, ok := series["values"].([][]interface{})
		if !ok || len(values) == 0 {
			continue
		}

		// Check if this is a heatmap series (has duration_bucket metric)
		metric, _ := series["metric"].(map[string]interface{})
		durationBucketStr, isHeatmap := metric["duration_bucket"].(string)
		var minDuration, maxDuration int64
		if isHeatmap {
			// Parse bucket index and calculate duration range
			var bucketIdx int64
			fmt.Sscanf(durationBucketStr, "%d", &bucketIdx)
			minDuration, maxDuration = getDurationRangeForBucket(int32(bucketIdx))
		}

		// Collect all exemplars for this series' time range
		var allExemplars []map[string]interface{}
		for _, v := range values {
			if len(v) < 2 {
				continue
			}
			timestamp, ok := v[0].(int64)
			if !ok {
				continue
			}

			// Find exemplars for this timestamp
			if exemplars, exists := exemplarMap[timestamp]; exists {
				for _, ex := range exemplars {
					// For heatmap series, only include exemplars with durations in this bucket's range
					if isHeatmap {
						if ex.Duration < minDuration || ex.Duration >= maxDuration {
							continue // Skip exemplars outside this duration bucket
						}
					}

					allExemplars = append(allExemplars, map[string]interface{}{
						"timestamp": timestamp, // Use step bucket timestamp for alignment
						"duration":  ex.Duration,
						"traceID":   ex.TraceID,
						"spanID":    ex.SpanID,
					})
				}
			}
		}

		if len(allExemplars) > 0 {
			series["exemplars"] = allExemplars
		}
	}
}

// getDurationRangeForBucket returns the min (inclusive) and max (exclusive) duration
// in nanoseconds for a given duration bucket index.
// Bucket N contains durations in range [2^N, 2^(N+1)).
// For example, bucket 20 = [2^20, 2^21) = [1048576, 2097152) nanoseconds ≈ [1ms, 2ms).
func getDurationRangeForBucket(bucket int32) (minDuration int64, maxDuration int64) {
	if bucket < 0 {
		return 0, 0
	}
	if bucket >= 62 {
		// Bucket 62 and above - use max int64 as upper bound
		minDuration = int64(1) << uint(bucket)
		maxDuration = 9223372036854775807 // math.MaxInt64
		return minDuration, maxDuration
	}
	minDuration = int64(1) << uint(bucket)
	maxDuration = int64(1) << uint(bucket+1)
	return minDuration, maxDuration
}

// convertSpansToTimeSeries converts raw spans to a time series by counting spans per time bucket.
// This enables selector queries like {name="promqlExec"} to return count-over-time metrics,
// similar to how Prometheus handles metric selectors.
//
// Boundary semantics: Spans in [start, end) are assigned to buckets.
// Each bucket covers [bucketTime, bucketTime+step).
func convertSpansToTimeSeries(spans []*span.Span, start, end time.Time, step time.Duration) engine.Matrix {
	if len(spans) == 0 {
		return engine.Matrix{}
	}

	// Group spans by label set and time bucket
	type seriesKey struct {
		labels string
		bucket int64
	}

	bucketCounts := make(map[seriesKey]int)
	labelSets := make(map[string]map[string]string)

	startNanos := start.UnixNano()
	endNanos := end.UnixNano()
	stepNanos := step.Nanoseconds()

	for _, sp := range spans {
		spanNanos := sp.StartTime.UnixNano()

		// Use [start, end) range semantics (inclusive start, exclusive end)
		// This prevents double-counting spans at exact boundaries
		if spanNanos < startNanos || spanNanos >= endNanos {
			continue // Skip spans outside query range
		}

		// Build label set from span (excluding internal fields)
		labels := make(map[string]string)

		// Add tags first (filter out span-specific identifiers)
		for k, v := range sp.Tags {
			if k != "parent_span_id" && k != "trace_id" && k != "span_id" {
				labels[k] = v
			}
		}

		// Override with top-level span fields (these take precedence)
		// This ensures sp.Name is used even if tags contain a "name" key
		labels["__name"] = sp.Name  // Use __name to avoid conflicts with tag "name"
		labels["service_name"] = sp.ServiceName

		// Create stable label key
		labelKey := makeStableLabelKey(labels)

		// Calculate which step bucket this span falls into (O(1) instead of O(steps))
		// CRITICAL PERFORMANCE FIX: This was O(n*m) before!
		bucketIndex := (spanNanos - startNanos) / stepNanos
		bucketTime := start.Add(time.Duration(bucketIndex) * step)

		key := seriesKey{
			labels: labelKey,
			bucket: bucketTime.Unix(),
		}
		bucketCounts[key]++

		// Store label set
		if _, exists := labelSets[labelKey]; !exists {
			labelSets[labelKey] = labels
		}
	}

	// Convert to matrix format: group by label set, create time series
	seriesMap := make(map[string]*engine.MatrixSample)

	for key, count := range bucketCounts {
		if _, exists := seriesMap[key.labels]; !exists {
			seriesMap[key.labels] = &engine.MatrixSample{
				Metric: labelSets[key.labels],
				Values: make([]engine.SamplePair, 0),
			}
		}

		seriesMap[key.labels].Values = append(seriesMap[key.labels].Values, engine.SamplePair{
			Time:  time.Unix(key.bucket, 0),
			Value: float64(count),
		})
	}

	// Convert to matrix and sort values by time
	matrix := make(engine.Matrix, 0, len(seriesMap))
	for _, series := range seriesMap {
		// Sort values by time
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

// makeStableLabelKey creates a deterministic key from a label set
func makeStableLabelKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	// Sort keys
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}

	// Bubble sort
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	// Build key using strings.Builder for efficiency
	var result strings.Builder
	for _, k := range keys {
		result.WriteString(k)
		result.WriteString("=")
		result.WriteString(labels[k])
		result.WriteString(",")
	}
	return result.String()
}
