//go:build !duckdb

package query

import (
	"errors"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// ErrSQLNotSupported is returned when SQL querying is not available
// This happens when the binary is built without the 'duckdb' build tag
var ErrSQLNotSupported = errors.New("SQL querying not supported: build with -tags duckdb to enable")

// defaultSQLQuerier is a no-op implementation that returns errors
// This is used when the binary is built without DuckDB support (no CGO required)
type defaultSQLQuerier struct{}

// NewSQLQuerier creates a new SQL querier
// Without the 'duckdb' build tag, this returns a no-op implementation
// that returns ErrSQLNotSupported for all operations
func NewSQLQuerier() (SQLQuerier, error) {
	return &defaultSQLQuerier{}, nil
}

// Close is a no-op for the default implementation
func (q *defaultSQLQuerier) Close() error {
	return nil
}

// LoadBlocks returns an error indicating SQL is not supported
func (q *defaultSQLQuerier) LoadBlocks(head *storage.ArrowStorage, blocks []block.Block) error {
	return ErrSQLNotSupported
}

// SelectSQL returns an error indicating SQL is not supported
func (q *defaultSQLQuerier) SelectSQL(query string) (*SQLResult, error) {
	return nil, ErrSQLNotSupported
}
