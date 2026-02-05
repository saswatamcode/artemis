//go:build !duckdb

package query

import (
	"errors"
	"testing"
)

func TestSQLQuerier_NotSupported(t *testing.T) {
	// When built without the duckdb tag, the SQL querier should be created
	// but all operations should return ErrSQLNotSupported
	sq, err := NewSQLQuerier()
	if err != nil {
		t.Fatalf("failed to create SQL querier: %v", err)
	}
	defer sq.Close()

	// Test that LoadBlocks returns the not supported error
	err = sq.LoadBlocks(nil, nil)
	if !errors.Is(err, ErrSQLNotSupported) {
		t.Fatalf("expected ErrSQLNotSupported, got: %v", err)
	}

	// Test that SelectSQL returns the not supported error
	_, err = sq.SelectSQL("SELECT * FROM spans")
	if !errors.Is(err, ErrSQLNotSupported) {
		t.Fatalf("expected ErrSQLNotSupported, got: %v", err)
	}

	// Close should always work
	err = sq.Close()
	if err != nil {
		t.Fatalf("Close() should not return error: %v", err)
	}
}
