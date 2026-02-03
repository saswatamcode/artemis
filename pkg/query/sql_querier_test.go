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

	// Verify the querier was created successfully
	if sq.db == nil {
		t.Fatal("database connection is nil")
	}
	if sq.conn == nil {
		t.Fatal("connection is nil")
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

	// Test a simple SQL query (without loading any blocks)
	// This tests that DuckDB is working
	rows, err := sq.conn.QueryContext(sq.ctx, "SELECT 1 as test")
	if err != nil {
		t.Fatalf("failed to execute simple query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}

	var result int
	if err := rows.Scan(&result); err != nil {
		t.Fatalf("failed to scan result: %v", err)
	}

	if result != 1 {
		t.Fatalf("expected 1, got %d", result)
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
