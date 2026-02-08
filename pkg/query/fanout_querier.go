package query

import (
	"sync"

	"github.com/saswatamcode/artemis/pkg/span"
)

// FanoutQuerier fans out queries to multiple queriers in parallel
// It queries the head block and persisted blocks concurrently, then merges results
type FanoutQuerier struct {
	queriers []Querier
}

// NewFanoutQuerier creates a new fanout querier
// Queriers are queried in the order provided, with results merged in that order
func NewFanoutQuerier(queriers ...Querier) *FanoutQuerier {
	return &FanoutQuerier{
		queriers: queriers,
	}
}

// Select queries spans using matchers across all queriers in parallel
func (q *FanoutQuerier) Select(matchers ...*Matcher) (*SelectResult, error) {
	return q.SelectWithTimeRange(nil, matchers...)
}

// SelectWithTimeRange queries spans within a time range across all queriers in parallel
// Results from all queriers are merged together
func (q *FanoutQuerier) SelectWithTimeRange(timeRange *TimeRange, matchers ...*Matcher) (*SelectResult, error) {
	if len(q.queriers) == 0 {
		return &SelectResult{Spans: make([]*span.Span, 0)}, nil
	}

	// If only one querier, no need for parallelization
	if len(q.queriers) == 1 {
		return q.queriers[0].SelectWithTimeRange(timeRange, matchers...)
	}

	// Query all queriers in parallel
	type querierResult struct {
		result *SelectResult
		err    error
		index  int // Track order for consistent merging
	}

	resultCh := make(chan querierResult, len(q.queriers))
	var wg sync.WaitGroup

	for i, querier := range q.queriers {
		wg.Add(1)
		go func(idx int, qr Querier) {
			defer wg.Done()
			res, err := qr.SelectWithTimeRange(timeRange, matchers...)
			resultCh <- querierResult{result: res, err: err, index: idx}
		}(i, querier)
	}

	// Wait for all queries to complete
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results in order
	// We must drain the channel completely even if there's an error
	// to avoid goroutine leaks (goroutines waiting to send on the channel)
	results := make([]*SelectResult, len(q.queriers))
	var firstError error
	for res := range resultCh {
		if res.err != nil && firstError == nil {
			firstError = res.err
		}
		results[res.index] = res.result
	}

	// Return error if any querier failed
	if firstError != nil {
		return nil, firstError
	}

	// Merge results
	return mergeResults(results), nil
}

// mergeResults merges multiple SelectResult into a single result
// Results are merged in the order provided (persisted blocks first, then head)
// Deduplicates spans by span ID to handle cases where a span was flushed during query
func mergeResults(results []*SelectResult) *SelectResult {
	// Calculate total capacity
	totalSpans := 0
	for _, res := range results {
		if res != nil {
			totalSpans += len(res.Spans)
		}
	}

	merged := &SelectResult{
		Spans: make([]*span.Span, 0, totalSpans),
	}

	// Use a map to deduplicate by span ID
	// This handles the case where a span was flushed from head to persisted block during query
	seen := make(map[string]bool, totalSpans)

	// Append spans in order, deduplicating by span ID
	for _, res := range results {
		if res != nil {
			for _, sp := range res.Spans {
				if !seen[sp.SpanID] {
					seen[sp.SpanID] = true
					merged.Spans = append(merged.Spans, sp)
				}
			}
		}
	}

	return merged
}
