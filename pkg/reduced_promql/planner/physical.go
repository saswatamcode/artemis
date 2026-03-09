package planner

import (
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/reduced_promql/executor"
	"github.com/saswatamcode/artemis/pkg/reduced_promql/parser"
)

// PhysicalPlan represents a physical query execution plan.
// Physical plans contain concrete execution decisions like parallelism,
// algorithm choice, and resource allocation.
//
// Each physical plan can build an iterator tree for execution.
type PhysicalPlan interface {
	// Build constructs an iterator tree from this physical plan.
	Build(ctx *executor.ExecutionContext) (executor.BatchIterator, error)
}

// PhysicalScan represents a physical scan operation.
// Decides whether to use parallel or sequential scanning based on block count.
type PhysicalScan struct {
	blocks   []block.Block
	matchers query.Matchers
	parallel bool // Use parallel scan if true
}

func (p *PhysicalScan) Build(ctx *executor.ExecutionContext) (executor.BatchIterator, error) {
	// TODO: Implement parallel scan operator
	// For now, always use sequential scan
	return executor.NewScanOperator(p.blocks, p.matchers, ctx), nil
}

// PhysicalRangeScan represents a physical range scan operation.
type PhysicalRangeScan struct {
	scan     *PhysicalScan
	lookback time.Duration
}

func (p *PhysicalRangeScan) Build(ctx *executor.ExecutionContext) (executor.BatchIterator, error) {
	// Build underlying scan
	scanIter, err := p.scan.Build(ctx)
	if err != nil {
		return nil, err
	}

	// Wrap with range scan operator
	return executor.NewRangeScanOperator(scanIter, p.lookback, ctx), nil
}

// PhysicalRate represents a physical rate calculation.
type PhysicalRate struct {
	input *PhysicalRangeScan
}

func (p *PhysicalRate) Build(ctx *executor.ExecutionContext) (executor.BatchIterator, error) {
	// Build input range scan
	inputIter, err := p.input.Build(ctx)
	if err != nil {
		return nil, err
	}

	// Wrap with rate operator, passing the lookback duration from the range scan
	return executor.NewRateOperator(inputIter, p.input.lookback, ctx), nil
}

// PhysicalHistogramQuantile represents a physical histogram quantile calculation.
type PhysicalHistogramQuantile struct {
	quantile float64
	input    PhysicalPlan
}

func (p *PhysicalHistogramQuantile) Build(ctx *executor.ExecutionContext) (executor.BatchIterator, error) {
	// Build input
	inputIter, err := p.input.Build(ctx)
	if err != nil {
		return nil, err
	}

	// Wrap with histogram quantile operator
	return executor.NewHistogramQuantileOperator(p.quantile, inputIter, ctx), nil
}

// PhysicalHeatmap represents a physical heatmap calculation.
type PhysicalHeatmap struct {
	input *PhysicalScan
}

func (p *PhysicalHeatmap) Build(ctx *executor.ExecutionContext) (executor.BatchIterator, error) {
	// Build input scan
	inputIter, err := p.input.Build(ctx)
	if err != nil {
		return nil, err
	}

	// Wrap with heatmap operator
	return executor.NewHeatmapOperator(inputIter, ctx), nil
}

// PhysicalAggregation represents a physical aggregation operation.
type PhysicalAggregation struct {
	op       string
	grouping *parser.Grouping
	input    PhysicalPlan
}

func (p *PhysicalAggregation) Build(ctx *executor.ExecutionContext) (executor.BatchIterator, error) {
	// Build input
	inputIter, err := p.input.Build(ctx)
	if err != nil {
		return nil, err
	}

	// Wrap with aggregation operator
	return executor.NewAggregationOperator(p.op, p.grouping, inputIter, ctx), nil
}
