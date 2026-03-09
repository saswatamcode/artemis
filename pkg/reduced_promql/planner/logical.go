package planner

import (
	"time"

	"github.com/saswatamcode/artemis/pkg/reduced_promql/parser"
)

// LogicalPlan represents a logical query plan before optimization and physical planning.
// Logical plans are tree structures that mirror the AST but with validated semantics.
type LogicalPlan interface {
	logicalPlan()
}

// LogicalScan represents a vector selector (instant query).
// Example: {service_name="api"}
type LogicalScan struct {
	Metric   *string               // Optional metric name
	Matchers []parser.LabelMatcher // Label matchers
}

func (*LogicalScan) logicalPlan() {}

// LogicalRangeScan represents a matrix selector (range query).
// Example: {service_name="api"}[5m]
type LogicalRangeScan struct {
	Scan     *LogicalScan  // Underlying vector selector
	Lookback time.Duration // Range duration (e.g., 5m)
}

func (*LogicalRangeScan) logicalPlan() {}

// LogicalRate represents the rate() function.
// Example: rate({service_name="api"}[5m])
type LogicalRate struct {
	Input *LogicalRangeScan // Must be a range selector
}

func (*LogicalRate) logicalPlan() {}

// LogicalHistogramQuantile represents the histogram_quantile() function.
// Example: histogram_quantile(0.95, {service_name="api"})
type LogicalHistogramQuantile struct {
	Quantile float64     // Quantile value (e.g., 0.95 for p95)
	Input    LogicalPlan // Input expression
}

func (*LogicalHistogramQuantile) logicalPlan() {}

// LogicalHeatmap represents the heatmap() function.
// Example: heatmap({service_name="api"})
type LogicalHeatmap struct {
	Input *LogicalScan // Must be a vector selector (enforced by grammar)
}

func (*LogicalHeatmap) logicalPlan() {}

// LogicalAggregation represents an aggregation operation.
// Example: sum by (service_name) ({job="app"})
type LogicalAggregation struct {
	Op       string           // "sum", "avg", "min", "max", "count"
	Grouping *parser.Grouping // Optional grouping clause (by/without)
	Input    LogicalPlan      // Input expression
}

func (*LogicalAggregation) logicalPlan() {}
