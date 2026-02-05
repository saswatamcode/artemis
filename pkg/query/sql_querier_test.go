//go:build duckdb

package query

import (
	"testing"
)

func TestSQLQuerier_Basic(t *testing.T) {
	// Test basic SQL querier creation and closure
	sq, err := NewSQLQuerier()
	if err != nil {
		t.Fatalf("failed to create SQL querier: %v", err)
	}
	defer sq.Close()

	// Verify the querier was created successfully by loading empty blocks
	err = sq.LoadBlocks(nil, nil)
	if err != nil {
		t.Fatalf("failed to load empty blocks: %v", err)
	}
}

func TestSQLQuerier_EmptyQuery(t *testing.T) {
	sq, err := NewSQLQuerier()
	if err != nil {
		t.Fatalf("failed to create SQL querier: %v", err)
	}
	defer sq.Close()

	// Test loading empty blocks
	err = sq.LoadBlocks(nil, nil)
	if err != nil {
		t.Fatalf("failed to load empty blocks: %v", err)
	}
}

func TestSQLQuerier_BasicSQL(t *testing.T) {
	sq, err := NewSQLQuerier()
	if err != nil {
		t.Fatalf("failed to create SQL querier: %v", err)
	}
	defer sq.Close()

	// Load empty blocks to create the spans table
	err = sq.LoadBlocks(nil, nil)
	if err != nil {
		t.Fatalf("failed to load empty blocks: %v", err)
	}

	// Test a simple SQL query
	// This tests that DuckDB is working
	result, err := sq.SelectSQL("SELECT COUNT(*) as count FROM spans")
	if err != nil {
		t.Fatalf("failed to execute simple query: %v", err)
	}

	if result.RowCount() != 1 {
		t.Fatalf("expected 1 row, got %d", result.RowCount())
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row in result, got %d", len(result.Rows))
	}
}

// Note: More comprehensive tests would require:
// 1. Creating test block directories with Arrow IPC files
// 2. Creating test block directories with Parquet files
// 3. Testing SelectSQL with actual span data
// 4. Testing different SQL queries (filters, aggregations, etc.)
//
// These integration tests should be added once the querier is integrated
// with the rest of the system.
