//go:build duckdb

package query

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// duckDBQuerier provides SQL-based querying using DuckDB with fanout pattern
// It queries Arrow IPC files and Parquet files directly from disk in parallel
//
// Architecture:
// - Groups blocks by type (Arrow L0, Parquet L1+)
// - Creates a UNION ALL view across all block files
// - DuckDB's query engine automatically parallelizes reads across files (fanout)
// - Each SQL query fans out to all blocks, with DuckDB merging results
//
// This implementation is only available when built with the 'duckdb' build tag
type duckDBQuerier struct {
	db     *sql.DB
	conn   *sql.Conn
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSQLQuerier creates a new SQL querier with DuckDB backend
// This implementation is only available when built with the 'duckdb' build tag
// Creates an in-mem duckdb database.
func NewSQLQuerier() (SQLQuerier, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	conn, err := db.Conn(ctx)
	if err != nil {
		// Connection creation failed, cancel context then close db
		cancel()
		db.Close()
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	// Install and load the arrow extension for reading Arrow IPC files
	if _, err := conn.ExecContext(ctx, "INSTALL arrow FROM community; LOAD arrow;"); err != nil {
		// FIX: Close connection before canceling context to avoid delays
		conn.Close()
		cancel()
		db.Close()
		return nil, fmt.Errorf("failed to load arrow extension: %w", err)
	}

	sq := &duckDBQuerier{
		db:     db,
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
	}

	return sq, nil
}

// Close releases resources held by the SQL querier
func (sq *duckDBQuerier) Close() error {
	sq.cancel()

	var firstErr error

	// FIX: Capture and return connection close errors
	if sq.conn != nil {
		if err := sq.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if sq.db != nil {
		if err := sq.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// LoadBlocks loads blocks into DuckDB using fanout pattern
//
// Fanout Strategy:
// 1. Groups blocks by type (Arrow L0 vs Parquet L1+) for optimized reads
// 2. Creates SELECT queries for each block file (persisted blocks only)
// 3. Combines all sources with UNION ALL into a single "spans" view
// 4. DuckDB's query engine parallelizes reads across all files automatically
//
// When a SQL query is executed:
// - DuckDB fans out the query to all block files in parallel
// - Each file is read independently using Arrow/Parquet readers
// - Results are merged automatically by DuckDB's UNION ALL implementation
// - This achieves similar parallelization to our Go-based FanoutQuerier
//
// Note: Head block (in-memory) is not yet supported - only persisted blocks
func (sq *duckDBQuerier) LoadBlocks(head *storage.ArrowStorage, blocks []block.Block) error {
	var sources []string

	// Phase 1: Group blocks by storage format for optimized query planning
	// This allows DuckDB to use format-specific optimizations
	arrowBlocks := make([]block.Block, 0)
	parquetBlocks := make([]block.Block, 0)

	for _, blk := range blocks {
		meta := blk.Meta()
		if meta.Level() == 0 {
			arrowBlocks = append(arrowBlocks, blk)
		} else {
			parquetBlocks = append(parquetBlocks, blk)
		}
	}

	// Phase 2: Create SELECT queries for Arrow L0 blocks
	// DuckDB will parallelize reads across these files
	// Transform uint64 IDs to hex VARCHAR format for compatibility
	for _, blk := range arrowBlocks {
		arrowPath := filepath.Join(blk.Dir(), "spans.arrow")
		sources = append(sources, fmt.Sprintf(`
			SELECT
				printf('%%016x%%016x', trace_id_hi, trace_id_lo) AS trace_id,
				printf('%%016x', span_id) AS span_id,
				CASE WHEN parent_span_id = 0 THEN NULL ELSE printf('%%016x', parent_span_id) END AS parent_span_id,
				name,
				start_time,
				end_time,
				duration,
				service_name,
				tags
			FROM read_arrow('%s')
		`, arrowPath))
	}

	// Phase 3: Create SELECT queries for Parquet L1+ blocks
	// DuckDB will parallelize reads across these files
	// Transform uint64 IDs to hex VARCHAR format for compatibility
	for _, blk := range parquetBlocks {
		parquetPath := filepath.Join(blk.Dir(), "spans.parquet")
		sources = append(sources, fmt.Sprintf(`
			SELECT
				printf('%%016x%%016x', trace_id_hi, trace_id_lo) AS trace_id,
				printf('%%016x', span_id) AS span_id,
				CASE WHEN parent_span_id = 0 THEN NULL ELSE printf('%%016x', parent_span_id) END AS parent_span_id,
				name,
				start_time,
				end_time,
				duration,
				service_name,
				tags
			FROM read_parquet('%s')
		`, parquetPath))
	}

	// TODO: Phase 4: Add head block (in-memory Arrow records) if present
	// This can be implemented using DuckDB's Arrow integration or by
	// writing head records to a temporary Arrow IPC file
	_ = head

	// Phase 5: Create unified "spans" view with UNION ALL
	// This is the fanout merge point - DuckDB handles parallel execution
	if len(sources) > 0 {
		// UNION ALL creates a fanout view that queries all blocks in parallel
		// DuckDB's query planner will optimize this for parallel execution
		unionQuery := "CREATE OR REPLACE VIEW spans AS " + sources[0]
		for i := 1; i < len(sources); i++ {
			unionQuery += " UNION ALL " + sources[i]
		}

		if _, err := sq.conn.ExecContext(sq.ctx, unionQuery); err != nil {
			return fmt.Errorf("failed to create spans view: %w", err)
		}
	} else {
		// Create an empty table with the correct schema when there are no blocks
		// This allows queries to work even when the database is empty
		emptyTableQuery := `
			CREATE OR REPLACE TABLE spans (
				trace_id VARCHAR,
				span_id VARCHAR,
				parent_span_id VARCHAR,
				name VARCHAR,
				start_time BIGINT,
				end_time BIGINT,
				duration BIGINT,
				service_name VARCHAR,
				tags MAP(VARCHAR, VARCHAR)
			)
		`
		if _, err := sq.conn.ExecContext(sq.ctx, emptyTableQuery); err != nil {
			return fmt.Errorf("failed to create empty spans table: %w", err)
		}
	}

	return nil
}

// SelectSQL executes a SQL query using the fanout pattern
//
// Execution Flow:
// 1. Query is sent to DuckDB's query engine
// 2. DuckDB's planner detects the UNION ALL view over multiple files
// 3. Query is automatically fanned out to all block files in parallel
// 4. Each block is queried independently (Arrow/Parquet readers)
// 5. Results are merged by DuckDB and returned
//
// This achieves the same parallelization as our Go-based FanoutQuerier,
// but leverages DuckDB's native query engine for better performance.
//
// The query should select from the "spans" table/view
//
// Example queries:
//   - SELECT * FROM spans WHERE service_name = 'my-service' LIMIT 100
//   - SELECT * FROM spans WHERE start_time >= 1234567890 AND start_time <= 1234567900
//   - SELECT trace_id, COUNT(*) FROM spans GROUP BY trace_id
func (sq *duckDBQuerier) SelectSQL(query string) (*SQLResult, error) {
	rows, err := sq.conn.QueryContext(sq.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Check if this is a span query (has all span columns) or an aggregation
	isSpanQuery := sq.isSpanQuery(columns)

	result := &SQLResult{
		Columns: columns,
	}

	if isSpanQuery {
		spans, err := sq.parseSpans(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to parse spans: %w", err)
		}
		result.Spans = spans
	} else {
		genericRows, err := sq.parseGenericRows(rows, columns)
		if err != nil {
			return nil, fmt.Errorf("failed to parse rows: %w", err)
		}
		result.Rows = genericRows
	}

	return result, nil
}

// isSpanQuery checks if the query result has all span columns
func (sq *duckDBQuerier) isSpanQuery(columns []string) bool {
	requiredColumns := map[string]bool{
		"trace_id":       false,
		"span_id":        false,
		"parent_span_id": false,
		"name":           false,
		"start_time":     false,
		"end_time":       false,
		"duration":       false,
		"service_name":   false,
		"tags":           false,
	}

	for _, col := range columns {
		if _, exists := requiredColumns[col]; exists {
			requiredColumns[col] = true
		}
	}

	for _, found := range requiredColumns {
		if !found {
			return false
		}
	}

	return true
}

// parseSpans parses SQL result rows into span objects
func (sq *duckDBQuerier) parseSpans(rows *sql.Rows) ([]*span.Span, error) {
	var spans []*span.Span

	for rows.Next() {
		var (
			traceID      string
			spanID       string
			parentSpanID sql.NullString
			name         string
			startTime    int64
			endTime      int64
			duration     int64
			serviceName  string
			tagsRaw      interface{} // Scan as interface{} first, then convert
		)

		err := rows.Scan(
			&traceID,
			&spanID,
			&parentSpanID,
			&name,
			&startTime,
			&endTime,
			&duration,
			&serviceName,
			&tagsRaw,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert DuckDB MAP to Go map[string]string
		tags := make(map[string]string)
		if tagsRaw != nil {
			if tagsMap, ok := tagsRaw.(map[string]interface{}); ok {
				for k, v := range tagsMap {
					if vStr, ok := v.(string); ok {
						tags[k] = vStr
					}
				}
			}
		}

		sp := &span.Span{
			TraceID:     traceID,
			SpanID:      spanID,
			Name:        name,
			StartTime:   time.Unix(0, startTime),
			EndTime:     time.Unix(0, endTime),
			Duration:    duration,
			ServiceName: serviceName,
			Tags:        tags,
		}

		if parentSpanID.Valid {
			sp.ParentSpanID = parentSpanID.String
		}

		spans = append(spans, sp)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return spans, nil
}

// parseGenericRows parses SQL result rows into generic row maps
func (sq *duckDBQuerier) parseGenericRows(rows *sql.Rows, columns []string) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	columnCount := len(columns)
	values := make([]interface{}, columnCount)
	valuePtrs := make([]interface{}, columnCount)
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return result, nil
}
