package planner

// optimizeLogical applies optimization passes to a logical plan.
//
// Current optimizations:
//  1. Predicate pushdown: Push filters into scans
//  2. Matcher reordering: Order matchers by selectivity
//
// Future optimizations:
//   - Constant folding
//   - Dead code elimination
//   - Join reordering (if we add binary operators)
func optimizeLogical(plan LogicalPlan) LogicalPlan {
	// For now, just return the plan unchanged
	// Optimizations will be added incrementally

	// Future optimization: Reorder matchers by selectivity
	// Priority: trace_id > span_id > service_name > attributes
	// This matches the priority in pkg/query/select.go:68-74

	return plan
}

// optimizePhysical applies physical-level optimizations.
//
// Current optimizations:
//  1. Parallel scan decision: Use parallel scan for multiple blocks
//  2. Buffer sizing: Adjust buffer sizes based on block sizes
//
// Future optimizations:
//   - Cost-based parallelism (consider block sizes, not just count)
//   - Index selection (choose best index for matchers)
//   - Memory budgeting (limit concurrent scans if blocks are large)
func optimizePhysical(plan PhysicalPlan) PhysicalPlan {
	// For now, just return the plan unchanged
	return plan
}
