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

// SQLQuerier provides SQL-based querying using DuckDB
// It can query Arrow IPC files and Parquet files directly from disk
type SQLQuerier struct {
	db     *sql.DB
	conn   *sql.Conn
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSQLQuerier creates a new SQL querier with DuckDB backend
func NewSQLQuerier() (*SQLQuerier, error) {
	// Create in-memory DuckDB database
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	conn, err := db.Conn(ctx)
	if err != nil {
		cancel()
		db.Close()
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	sq := &SQLQuerier{
		db:     db,
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
	}

	return sq, nil
}

// Close releases resources held by the SQL querier
func (sq *SQLQuerier) Close() error {
	sq.cancel()
	if sq.conn != nil {
		sq.conn.Close()
	}
	if sq.db != nil {
		return sq.db.Close()
	}
	return nil
}

// LoadBlocks loads blocks into DuckDB for querying
// Creates a unified "spans" table from all blocks (Arrow IPC and Parquet)
func (sq *SQLQuerier) LoadBlocks(head *storage.ArrowStorage, blocks []block.Block) error {
	// Create a view that unions all data sources
	var sources []string

	// Load persisted blocks (Arrow L0 and Parquet L1+)
	for _, blk := range blocks {
		meta := blk.Meta()
		blockDir := blk.Dir()

		if meta.Level() == 0 {
			// L0 blocks are Arrow IPC files
			arrowPath := filepath.Join(blockDir, "spans.arrow")
			sources = append(sources, fmt.Sprintf("SELECT * FROM read_ipc('%s')", arrowPath))
		} else {
			// L1+ blocks are Parquet files
			parquetPath := filepath.Join(blockDir, "spans.parquet")
			sources = append(sources, fmt.Sprintf("SELECT * FROM read_parquet('%s')", parquetPath))
		}
	}

	// TODO: Load head block (in-memory Arrow records) if present
	// For now, we skip the head block and only query persisted blocks
	// This can be implemented later by writing head records to a temporary file
	// or using DuckDB's Arrow integration
	_ = head

	// Create unified view
	if len(sources) > 0 {
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

// SelectSQL executes a SQL query and returns results as spans
// The query should select from the "spans" table/view
//
// Example queries:
//   - SELECT * FROM spans WHERE service_name = 'my-service' LIMIT 100
//   - SELECT * FROM spans WHERE start_time >= 1234567890 AND start_time <= 1234567900
//   - SELECT trace_id, COUNT(*) FROM spans GROUP BY trace_id
func (sq *SQLQuerier) SelectSQL(query string) (*SQLResult, error) {
	rows, err := sq.conn.QueryContext(sq.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column information
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
		// Parse results as spans
		spans, err := sq.parseSpans(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to parse spans: %w", err)
		}
		result.Spans = spans
	} else {
		// Parse results as generic rows
		genericRows, err := sq.parseGenericRows(rows, columns)
		if err != nil {
			return nil, fmt.Errorf("failed to parse rows: %w", err)
		}
		result.Rows = genericRows
	}

	return result, nil
}

// isSpanQuery checks if the query result has all span columns
func (sq *SQLQuerier) isSpanQuery(columns []string) bool {
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

	// Check if all required columns are present
	for _, found := range requiredColumns {
		if !found {
			return false
		}
	}

	return true
}

// parseSpans parses SQL result rows into span objects
func (sq *SQLQuerier) parseSpans(rows *sql.Rows) ([]*span.Span, error) {
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
			tags         map[string]string // DuckDB MAP type
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
			&tags,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
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
func (sq *SQLQuerier) parseGenericRows(rows *sql.Rows, columns []string) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	// Create slice of interface{} for scanning
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

// SQLResult represents the result of a SQL query
type SQLResult struct {
	Columns []string                 // Column names
	Spans   []*span.Span             // Parsed spans (if query returns full span records)
	Rows    []map[string]interface{} // Generic rows (for aggregations, projections, etc.)
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

// SelectSQLFromBlocks is a convenience function that creates a SQL querier,
// loads blocks, executes a query, and returns results
func SelectSQLFromBlocks(head *storage.ArrowStorage, blocks []block.Block, query string) (*SQLResult, error) {
	sq, err := NewSQLQuerier()
	if err != nil {
		return nil, fmt.Errorf("failed to create SQL querier: %w", err)
	}
	defer sq.Close()

	if err := sq.LoadBlocks(head, blocks); err != nil {
		return nil, fmt.Errorf("failed to load blocks: %w", err)
	}

	return sq.SelectSQL(query)
}
