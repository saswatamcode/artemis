package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/reduced_promql/executor"
	"github.com/saswatamcode/artemis/pkg/reduced_promql/parser"
	"github.com/saswatamcode/artemis/pkg/reduced_promql/planner"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// Engine is the main query execution engine for reduced PromQL.
//
// It orchestrates the complete query pipeline:
//  1. Parse query string to AST
//  2. Plan query (AST → Physical Plan)
//  3. Build iterator tree from physical plan
//  4. Execute query and collect results
//  5. Return results with statistics
//
// The engine integrates with Artemis infrastructure:
//   - Uses IsolationCoordinator for buffer pooling and MVCC
//   - Queries blocks via Block interface (HeadBlock, ArrowBlock, ParquetBlock)
//   - Supports cancellation via context.Context
//   - Collects execution statistics (spans scanned, blocks scanned, duration)
type Engine struct {
	// Function to get current blocks (called on each query to get latest)
	blockGetter func() []block.Block
	isolation   *storage.IsolationCoordinator
}

// NewEngine creates a new query engine.
//
// Parameters:
//   - blockGetter: Function that returns current blocks (called on each query for latest data)
//   - isolation: IsolationCoordinator for buffer pooling and MVCC
//
// The blockGetter function should be lightweight (e.g., db.GetBlocks()) and will be called
// on each query to ensure we always query the latest block set.
func NewEngine(blockGetter func() []block.Block, isolation *storage.IsolationCoordinator) *Engine {
	return &Engine{
		blockGetter: blockGetter,
		isolation:   isolation,
	}
}

// Execute executes a reduced PromQL query and returns results.
//
// The query string is parsed, planned, and executed. Results are converted
// to the appropriate format based on query type:
//   - Selectors → Spans
//   - rate(), aggregations → InstantVector (Prometheus metrics)
//   - histogram_quantile() → Scalar
//   - heatmap() → Matrix
//
// Example queries:
//   - {service_name="api"} → Spans
//   - rate({service_name="api"}[5m]) → InstantVector
//   - histogram_quantile(0.95, {service_name="api"}) → Scalar
//   - sum by (service_name) ({job="app"}) → InstantVector
//
// Parameters:
//   - queryStr: Reduced PromQL query string
//   - opts: Query options (time range, context)
//
// Returns:
//   - QueryResult with appropriate result type and statistics
//   - Error if parsing, planning, or execution fails
func (e *Engine) Execute(queryStr string, opts *QueryOptions) (*QueryResult, error) {
	startTime := time.Now()

	// 1. Parse query
	expr, err := parser.Parse(queryStr)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// 2. Normalize AST for consistent execution
	expr = parser.Normalize(expr)

	// 3. Determine result type from AST
	resultType := determineResultType(expr)

	// 4. Get current blocks (dynamic - always queries latest)
	blocks := e.blockGetter()

	// 5. Plan query
	queryPlanner := planner.NewPlanner(blocks)
	plan, err := queryPlanner.Plan(expr)
	if err != nil {
		return nil, fmt.Errorf("planning error: %w", err)
	}

	// 6. Create execution context
	execCtx := &executor.ExecutionContext{
		StartTime: opts.StartTime,
		EndTime:   opts.EndTime,
		Step:      opts.Step,
		Isolation: e.isolation,
		Blocks:    blocks,
		Ctx:       opts.Context,
	}

	// If snapshot isolation is requested, capture snapshot
	if opts.UseSnapshot {
		execCtx.Snapshot = e.isolation.BeginQuery()
	}

	// 7. Build iterator tree
	iterator, err := plan.Build(execCtx)
	if err != nil {
		return nil, fmt.Errorf("build error: %w", err)
	}
	defer iterator.Close()

	// 8. Execute and collect raw results (still as spans from operators)
	var rawSpans []*span.Span

	for {
		batch, err := iterator.Next()
		if err != nil {
			return nil, fmt.Errorf("execution error: %w", err)
		}
		if batch == nil {
			break // Exhausted
		}

		// Collect results
		rawSpans = append(rawSpans, batch...)

		// Check cancellation
		select {
		case <-execCtx.Ctx.Done():
			return nil, fmt.Errorf("query cancelled: %w", execCtx.Ctx.Err())
		default:
		}

		// Apply result limit if specified
		if opts.Limit > 0 && len(rawSpans) >= opts.Limit {
			rawSpans = rawSpans[:opts.Limit]
			break
		}
	}

	// 9. Convert to appropriate result format based on query type
	result := &QueryResult{
		Type: resultType,
		Stats: QueryStats{
			SpansScanned:  execCtx.SpansScanned(),
			BlocksScanned: execCtx.BlocksScanned(),
			Duration:      time.Since(startTime),
		},
	}

	switch resultType {
	case ResultTypeSpans:
		result.Spans = rawSpans

	case ResultTypeVector:
		result.Vector = convertSpansToVector(rawSpans)

	case ResultTypeMatrix:
		result.Matrix = convertSpansToMatrix(rawSpans)

	case ResultTypeScalar:
		result.Scalar = convertSpansToScalar(rawSpans)
	}

	return result, nil
}

// ExecuteAsync executes a query asynchronously and streams results via a channel.
//
// This is useful for long-running queries or streaming results to a client.
// Results are sent as they're produced (per batch).
//
// The returned channel will be closed when the query completes or errors.
// The error channel will receive at most one error before being closed.
//
// Example usage:
//
//	resultCh, errCh := engine.ExecuteAsync(query, opts)
//	for batch := range resultCh {
//	    // Process batch
//	}
//	if err := <-errCh; err != nil {
//	    // Handle error
//	}
func (e *Engine) ExecuteAsync(queryStr string, opts *QueryOptions) (<-chan []*span.Span, <-chan error) {
	resultCh := make(chan []*span.Span, 10) // Buffer some batches
	errCh := make(chan error, 1)

	go func() {
		defer close(resultCh)
		defer close(errCh)

		// Parse and plan
		expr, err := parser.Parse(queryStr)
		if err != nil {
			errCh <- fmt.Errorf("parse error: %w", err)
			return
		}

		expr = parser.Normalize(expr)

		// Get current blocks
		blocks := e.blockGetter()

		queryPlanner := planner.NewPlanner(blocks)
		plan, err := queryPlanner.Plan(expr)
		if err != nil {
			errCh <- fmt.Errorf("planning error: %w", err)
			return
		}

		// Create execution context
		execCtx := &executor.ExecutionContext{
			StartTime: opts.StartTime,
			EndTime:   opts.EndTime,
			Isolation: e.isolation,
			Blocks:    blocks,
			Ctx:       opts.Context,
		}

		if opts.UseSnapshot {
			execCtx.Snapshot = e.isolation.BeginQuery()
		}

		// Build iterator
		iterator, err := plan.Build(execCtx)
		if err != nil {
			errCh <- fmt.Errorf("build error: %w", err)
			return
		}
		defer iterator.Close()

		// Stream results
		totalResults := 0
		for {
			batch, err := iterator.Next()
			if err != nil {
				errCh <- fmt.Errorf("execution error: %w", err)
				return
			}
			if batch == nil {
				break
			}

			// Send batch
			select {
			case resultCh <- batch:
				totalResults += len(batch)
			case <-execCtx.Ctx.Done():
				errCh <- fmt.Errorf("query cancelled: %w", execCtx.Ctx.Err())
				return
			}

			// Apply result limit
			if opts.Limit > 0 && totalResults >= opts.Limit {
				break
			}
		}
	}()

	return resultCh, errCh
}

// QueryOptions specifies options for query execution.
type QueryOptions struct {
	// Time range for the query
	StartTime time.Time
	EndTime   time.Time

	// Step interval for range queries (e.g., rate())
	// If zero, a default will be calculated based on query duration
	Step time.Duration

	// Context for cancellation and timeouts
	Context context.Context

	// Use MVCC snapshot isolation
	// If true, the query will see a consistent snapshot of data
	UseSnapshot bool

	// Maximum number of results to return (0 = unlimited)
	Limit int
}

