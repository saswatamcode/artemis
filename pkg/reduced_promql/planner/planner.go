package planner

import (
	"fmt"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/reduced_promql/parser"
)

// Planner converts AST expressions to physical query execution plans.
//
// Planning pipeline:
//  1. AST validation (already done by parser.Validate)
//  2. AST → Logical plan conversion
//  3. Logical plan optimization
//  4. Logical → Physical plan conversion
//  5. Physical plan optimization
//
// The planner makes decisions about:
//   - Which blocks to scan
//   - Whether to use parallel or sequential scanning
//   - How to order operations for efficiency
//   - Which indexes to use
type Planner struct {
	blocks []block.Block
}

// NewPlanner creates a new query planner.
func NewPlanner(blocks []block.Block) *Planner {
	return &Planner{
		blocks: blocks,
	}
}

// Plan converts an AST expression to a physical query plan.
func (p *Planner) Plan(expr parser.Expr) (PhysicalPlan, error) {
	// 1. Validate AST (defensive check, parser should have done this)
	if err := parser.Validate(expr); err != nil {
		return nil, fmt.Errorf("invalid AST: %w", err)
	}

	// 2. Build logical plan
	logical, err := p.buildLogicalPlan(expr)
	if err != nil {
		return nil, fmt.Errorf("failed to build logical plan: %w", err)
	}

	// 3. Optimize logical plan
	logical = optimizeLogical(logical)

	// 4. Generate physical plan
	physical, err := p.buildPhysicalPlan(logical)
	if err != nil {
		return nil, fmt.Errorf("failed to build physical plan: %w", err)
	}

	// 5. Optimize physical plan
	physical = optimizePhysical(physical)

	return physical, nil
}

// buildLogicalPlan converts an AST expression to a logical plan.
func (p *Planner) buildLogicalPlan(expr parser.Expr) (LogicalPlan, error) {
	switch e := expr.(type) {
	case *parser.VectorSelector:
		return &LogicalScan{
			Metric:   e.Metric,
			Matchers: e.Matchers,
		}, nil

	case *parser.MatrixSelector:
		// Parse range duration
		lookback, err := parseDuration(e.RangeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid range duration %q: %w", e.RangeStr, err)
		}

		// Convert nested vector selector
		scan, err := p.buildLogicalPlan(e.Vector)
		if err != nil {
			return nil, err
		}

		logicalScan, ok := scan.(*LogicalScan)
		if !ok {
			return nil, fmt.Errorf("matrix selector must contain vector selector, got %T", scan)
		}

		return &LogicalRangeScan{
			Scan:     logicalScan,
			Lookback: lookback,
		}, nil

	case *parser.Call:
		return p.buildLogicalCall(e)

	case *parser.Aggregation:
		// Convert input expression
		input, err := p.buildLogicalPlan(e.Expr)
		if err != nil {
			return nil, err
		}

		return &LogicalAggregation{
			Op:       e.Op,
			Grouping: e.Grouping,
			Input:    input,
		}, nil

	case parser.Scalar:
		// Scalars don't have a standalone logical plan
		return nil, fmt.Errorf("scalar values cannot be top-level expressions")

	default:
		return nil, fmt.Errorf("unknown expression type: %T", expr)
	}
}

// buildLogicalCall converts a Call AST node to a logical plan.
func (p *Planner) buildLogicalCall(call *parser.Call) (LogicalPlan, error) {
	switch call.Func {
	case "rate":
		// rate() requires a range selector
		if len(call.Args) != 1 {
			return nil, fmt.Errorf("rate() expects 1 argument, got %d", len(call.Args))
		}

		input, err := p.buildLogicalPlan(call.Args[0])
		if err != nil {
			return nil, err
		}

		rangeScan, ok := input.(*LogicalRangeScan)
		if !ok {
			return nil, fmt.Errorf("rate() requires a range selector, got %T", input)
		}

		return &LogicalRate{
			Input: rangeScan,
		}, nil

	case "histogram_quantile":
		// histogram_quantile(quantile, input)
		if len(call.Args) != 2 {
			return nil, fmt.Errorf("histogram_quantile() expects 2 arguments, got %d", len(call.Args))
		}

		// First argument must be a scalar
		scalar, ok := call.Args[0].(parser.Scalar)
		if !ok {
			return nil, fmt.Errorf("histogram_quantile() first argument must be scalar, got %T", call.Args[0])
		}

		// Convert input expression
		input, err := p.buildLogicalPlan(call.Args[1])
		if err != nil {
			return nil, err
		}

		return &LogicalHistogramQuantile{
			Quantile: scalar.Val,
			Input:    input,
		}, nil

	case "heatmap":
		// heatmap() requires a vector selector
		if len(call.Args) != 1 {
			return nil, fmt.Errorf("heatmap() expects 1 argument, got %d", len(call.Args))
		}

		input, err := p.buildLogicalPlan(call.Args[0])
		if err != nil {
			return nil, err
		}

		scan, ok := input.(*LogicalScan)
		if !ok {
			return nil, fmt.Errorf("heatmap() requires a vector selector, got %T", input)
		}

		return &LogicalHeatmap{
			Input: scan,
		}, nil

	default:
		return nil, fmt.Errorf("unknown function: %s", call.Func)
	}
}

// buildPhysicalPlan converts a logical plan to a physical plan.
func (p *Planner) buildPhysicalPlan(logical LogicalPlan) (PhysicalPlan, error) {
	switch l := logical.(type) {
	case *LogicalScan:
		// Convert matchers
		matchers, err := convertMatchers(l.Matchers)
		if err != nil {
			return nil, err
		}

		// If a metric name is specified (e.g., prometheus{}), convert it to a service_name matcher
		// In tracing, the "metric name" concept maps to service_name
		if l.Metric != nil && *l.Metric != "" {
			// Create a service_name matcher from the metric name
			metricMatcher, err := query.NewMatcher(query.MatchEqual, "service_name", *l.Metric)
			if err != nil {
				return nil, fmt.Errorf("failed to create metric matcher: %w", err)
			}
			// Prepend to matchers (most selective first)
			matchers = append([]*query.Matcher{metricMatcher}, matchers...)
		}

		// Decide on parallelism: use parallel scan if multiple blocks
		parallel := len(p.blocks) > 1

		return &PhysicalScan{
			blocks:   p.blocks,
			matchers: matchers,
			parallel: parallel,
		}, nil

	case *LogicalRangeScan:
		// Convert underlying scan
		scan, err := p.buildPhysicalPlan(l.Scan)
		if err != nil {
			return nil, err
		}

		physicalScan, ok := scan.(*PhysicalScan)
		if !ok {
			return nil, fmt.Errorf("range scan must contain scan, got %T", scan)
		}

		return &PhysicalRangeScan{
			scan:     physicalScan,
			lookback: l.Lookback,
		}, nil

	case *LogicalRate:
		// Convert input range scan
		rangeScan, err := p.buildPhysicalPlan(l.Input)
		if err != nil {
			return nil, err
		}

		physicalRangeScan, ok := rangeScan.(*PhysicalRangeScan)
		if !ok {
			return nil, fmt.Errorf("rate input must be range scan, got %T", rangeScan)
		}

		return &PhysicalRate{
			input: physicalRangeScan,
		}, nil

	case *LogicalHistogramQuantile:
		// Convert input
		input, err := p.buildPhysicalPlan(l.Input)
		if err != nil {
			return nil, err
		}

		return &PhysicalHistogramQuantile{
			quantile: l.Quantile,
			input:    input,
		}, nil

	case *LogicalHeatmap:
		// Convert input scan
		scan, err := p.buildPhysicalPlan(l.Input)
		if err != nil {
			return nil, err
		}

		physicalScan, ok := scan.(*PhysicalScan)
		if !ok {
			return nil, fmt.Errorf("heatmap input must be scan, got %T", scan)
		}

		return &PhysicalHeatmap{
			input: physicalScan,
		}, nil

	case *LogicalAggregation:
		// Convert input
		input, err := p.buildPhysicalPlan(l.Input)
		if err != nil {
			return nil, err
		}

		return &PhysicalAggregation{
			op:       l.Op,
			grouping: l.Grouping,
			input:    input,
		}, nil

	default:
		return nil, fmt.Errorf("unknown logical plan type: %T", logical)
	}
}

// parseDuration parses a PromQL duration string like "5m", "1h", "30s".
func parseDuration(s string) (time.Duration, error) {
	// PromQL durations: ms, s, m, h, d, w, y
	// Go time.ParseDuration supports: ns, us/µs, ms, s, m, h
	// We need to handle d, w, y manually

	if len(s) < 2 {
		return 0, fmt.Errorf("invalid duration: %s", s)
	}

	// Try Go's built-in parser first (handles ms, s, m, h)
	d, err := time.ParseDuration(s)
	if err == nil {
		return d, nil
	}

	// Handle PromQL-specific units: d, w, y
	unit := s[len(s)-1:]
	valueStr := s[:len(s)-1]

	var multiplier time.Duration
	switch unit {
	case "d":
		multiplier = 24 * time.Hour
	case "w":
		multiplier = 7 * 24 * time.Hour
	case "y":
		multiplier = 365 * 24 * time.Hour
	default:
		return 0, fmt.Errorf("invalid duration unit: %s", unit)
	}

	// Parse the numeric value
	var value float64
	_, err = fmt.Sscanf(valueStr, "%f", &value)
	if err != nil {
		return 0, fmt.Errorf("invalid duration value: %s", valueStr)
	}

	return time.Duration(value * float64(multiplier)), nil
}
