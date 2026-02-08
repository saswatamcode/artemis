package query

import (
	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// SQLQuerier provides SQL-based querying interface with fanout pattern
//
// The actual implementation depends on build tags:
// - Default (no build tag): Returns "unimplemented" errors
// - With 'duckdb' build tag: Uses DuckDB for parallel SQL queries over blocks
//
// Fanout Architecture (DuckDB implementation):
// - Queries are automatically parallelized across all block files
// - DuckDB's UNION ALL creates a fanout view over Arrow IPC (L0) and Parquet (L1+) files
// - Query execution is distributed to each block file independently
// - Results are merged efficiently by DuckDB's query engine
//
// To build with DuckDB support (requires CGO):
//
//	go build -tags duckdb
type SQLQuerier interface {
	// Close releases resources held by the querier
	Close() error

	// LoadBlocks loads blocks into the querier for SQL queries using fanout pattern
	// Creates a "spans" table/view that fans out queries to all blocks in parallel
	//
	// For DuckDB implementation:
	// - Groups blocks by type (Arrow L0, Parquet L1+)
	// - Creates a UNION ALL view that DuckDB parallelizes automatically
	// - Each SQL query will fan out to all block files concurrently
	LoadBlocks(head *storage.ArrowStorage, blocks []block.Block) error

	// SelectSQL executes a SQL query using the fanout pattern
	// The query is automatically distributed across all loaded blocks in parallel
	// The query should select from the "spans" table
	//
	// Fanout behavior (DuckDB):
	// - Query is parsed by DuckDB's planner
	// - Automatically fans out to all block files in parallel
	// - Each block is queried independently
	// - Results are merged and returned
	//
	// Example queries:
	//   - SELECT * FROM spans WHERE service_name = 'my-service' LIMIT 100
	//   - SELECT service_name, COUNT(*) FROM spans GROUP BY service_name
	//   - SELECT AVG(duration) FROM spans WHERE start_time > 1234567890
	SelectSQL(query string) (*SQLResult, error)
}

// SQLResult represents the result of a SQL query
type SQLResult struct {
	Columns []string         // Column names
	Spans   []*span.Span     // Parsed spans (if query returns full span records)
	Rows    []map[string]any // Generic rows (for aggregations, projections, etc.)
}

// IsSpanResult returns true if the result contains full span records
func (r *SQLResult) IsSpanResult() bool {
	return len(r.Spans) > 0
}

// RowCount returns the number of rows in the result
func (r *SQLResult) RowCount() int {
	if r.IsSpanResult() {
		return len(r.Spans)
	}
	return len(r.Rows)
}
