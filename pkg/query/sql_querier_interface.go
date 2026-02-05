package query

import (
	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// SQLQuerier provides SQL-based querying interface
// The actual implementation depends on build tags:
// - Default (no build tag): Returns "unimplemented" errors
// - With 'duckdb' build tag: Uses DuckDB for SQL queries over Arrow IPC and Parquet files
//
// To build with DuckDB support (requires CGO):
//
//	go build -tags duckdb
type SQLQuerier interface {
	// Close releases resources held by the querier
	Close() error

	// LoadBlocks loads blocks into the querier for SQL queries
	// Creates a "spans" table/view that can be queried with SQL
	LoadBlocks(head *storage.ArrowStorage, blocks []block.Block) error

	// SelectSQL executes a SQL query and returns results
	// The query should select from the "spans" table
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
