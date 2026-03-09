package executor

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/saswatamcode/artemis/pkg/reduced_promql/parser"
	"github.com/saswatamcode/artemis/pkg/span"
)

// AggregationOperator implements aggregation operations with grouping.
//
// Supported aggregations:
//   - sum: Sum of span counts per group
//   - avg: Average of span counts per group
//   - min: Minimum value per group (e.g., min duration)
//   - max: Maximum value per group (e.g., max duration)
//   - count: Count of spans per group
//
// Grouping:
//   - by (label1, label2): Include only specified labels in grouping key
//   - without (label1): Exclude specified labels from grouping key
//
// Algorithm:
//  1. Consume all batches from input
//  2. For each span, build grouping key from tags
//  3. Update aggregate state for each group
//  4. Emit one synthetic span per group with aggregated value
//
// Grouping key construction:
//   - For "by (service_name, status)": key = "service_name=api,status=200"
//   - For "without (trace_id)": key = all tags except trace_id
//   - Tags are sorted alphabetically for consistent hashing
//
// Output format:
//   - One span per group
//   - Tags contain grouping labels + aggregated value
//   - Tags["agg_value"] = aggregated value
//   - Tags["agg_op"] = aggregation operation (sum, avg, etc.)
//
// Example:
//
//	Input: sum by (service_name) ({job="app"})
//	Output:
//	  {service_name="api", agg_value="150", agg_op="sum"}
//	  {service_name="db", agg_value="80", agg_op="sum"}
type AggregationOperator struct {
	op       string           // "sum", "avg", "min", "max", "count"
	grouping *parser.Grouping // by/without clause (may be nil)
	input    BatchIterator
	ctx      *ExecutionContext
	consumed bool

	// Aggregation state: grouping key → aggregate state
	groups map[string]*AggregateState

	// Result state
	resultBatch []*span.Span
	resultIdx   int
}

// AggregateState holds the state for a single aggregation group.
type AggregateState struct {
	count int64             // Number of spans in this group
	sum   float64           // Sum of values (for sum, avg)
	min   float64           // Minimum value
	max   float64           // Maximum value
	tags  map[string]string // Grouping tags for this group
}

// NewAggregationOperator creates a new aggregation operator.
func NewAggregationOperator(op string, grouping *parser.Grouping, input BatchIterator, execCtx *ExecutionContext) *AggregationOperator {
	return &AggregationOperator{
		op:       op,
		grouping: grouping,
		input:    input,
		ctx:      execCtx,
		groups:   make(map[string]*AggregateState),
	}
}

// Next returns the next batch of aggregation results.
//
// On first call, consumes all input and calculates aggregates.
// Returns results as synthetic spans (one per group).
func (a *AggregationOperator) Next() ([]*span.Span, error) {
	// First call: consume input and aggregate
	if !a.consumed {
		if err := a.consumeAndAggregate(); err != nil {
			return nil, err
		}
		a.consumed = true
	}

	// Return results (single batch)
	if a.resultIdx >= len(a.resultBatch) {
		return nil, nil // Exhausted
	}

	// Return all results at once
	results := a.resultBatch[a.resultIdx:]
	a.resultIdx = len(a.resultBatch)
	return results, nil
}

// consumeAndAggregate consumes all input and calculates aggregates.
func (a *AggregationOperator) consumeAndAggregate() error {
	// Consume all input batches
	for {
		batch, err := a.input.Next()
		if err != nil {
			return fmt.Errorf("aggregation: error reading input: %w", err)
		}
		if batch == nil {
			break // Input exhausted
		}

		// Aggregate each span
		for _, sp := range batch {
			// Build grouping key
			key, groupTags := a.buildGroupingKey(sp)

			// Get or create aggregate state
			state, ok := a.groups[key]
			if !ok {
				state = &AggregateState{
					min:  math.MaxFloat64,
					max:  -math.MaxFloat64,
					tags: groupTags,
				}
				a.groups[key] = state
			}

			// Update aggregate state
			a.updateState(state, sp)
		}
	}

	// Convert aggregates to result spans
	a.resultBatch = make([]*span.Span, 0, len(a.groups))

	for _, state := range a.groups {
		// Calculate final aggregated value
		var aggValue float64
		switch a.op {
		case "sum":
			aggValue = state.sum
		case "avg":
			if state.count > 0 {
				aggValue = state.sum / float64(state.count)
			}
		case "min":
			aggValue = state.min
		case "max":
			aggValue = state.max
		case "count":
			aggValue = float64(state.count)
		}

		// Create synthetic span with aggregated value
		tags := make(map[string]string, len(state.tags)+2)
		for k, v := range state.tags {
			tags[k] = v
		}
		tags["agg_value"] = fmt.Sprintf("%f", aggValue)
		tags["agg_op"] = a.op

		syntheticSpan := &span.Span{
			TraceID:     "aggregation_result",
			SpanID:      fmt.Sprintf("agg_%s", buildKeyFromTags(state.tags)),
			Name:        a.op,
			ServiceName: "query_engine",
			Tags:        tags,
		}

		a.resultBatch = append(a.resultBatch, syntheticSpan)
	}

	return nil
}

// buildGroupingKey constructs a grouping key and tag set from a span.
func (a *AggregationOperator) buildGroupingKey(sp *span.Span) (string, map[string]string) {
	groupTags := make(map[string]string)

	// Collect all tags (including top-level fields)
	allTags := make(map[string]string)
	for k, v := range sp.Tags {
		allTags[k] = v
	}
	// Add top-level fields
	if sp.ServiceName != "" {
		allTags["service_name"] = sp.ServiceName
	}
	if sp.Name != "" {
		allTags["name"] = sp.Name
	}
	allTags["trace_id"] = sp.TraceID
	allTags["span_id"] = sp.SpanID

	// Apply grouping logic
	if a.grouping == nil {
		// No grouping: single group
		// Include all tags
		groupTags = allTags
	} else if a.grouping.Kind == "by" {
		// Include only specified labels
		for _, key := range a.grouping.Keys {
			if val, ok := allTags[key]; ok {
				groupTags[key] = val
			}
		}
	} else if a.grouping.Kind == "without" {
		// Exclude specified labels
		excluded := make(map[string]bool)
		for _, key := range a.grouping.Keys {
			excluded[key] = true
		}
		for k, v := range allTags {
			if !excluded[k] {
				groupTags[k] = v
			}
		}
	}

	// Build key from tags
	key := buildKeyFromTags(groupTags)
	return key, groupTags
}

// buildKeyFromTags builds a stable string key from tags.
func buildKeyFromTags(tags map[string]string) string {
	if len(tags) == 0 {
		return "no_group"
	}

	// Sort keys for consistent hashing
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build key: "key1=val1,key2=val2,..."
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(tags[k])
	}
	return b.String()
}

// updateState updates aggregate state with a span.
func (a *AggregationOperator) updateState(state *AggregateState, sp *span.Span) {
	state.count++

	// Extract value based on aggregation type
	// For now, we'll use span count (1.0 per span)
	// Future: could aggregate duration, custom metrics, etc.
	value := 1.0

	// For duration-based aggregations
	if a.op == "min" || a.op == "max" {
		// Use span duration for min/max
		value = float64(sp.GetDuration())
	}

	// Update aggregates
	state.sum += value
	if value < state.min {
		state.min = value
	}
	if value > state.max {
		state.max = value
	}
}

// Close releases resources held by the operator.
func (a *AggregationOperator) Close() error {
	if a.input != nil {
		return a.input.Close()
	}
	return nil
}
