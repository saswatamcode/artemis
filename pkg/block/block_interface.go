package block

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/saswatamcode/artemis/pkg/index"
	"github.com/saswatamcode/artemis/pkg/span"
)

// Block is a unified interface for querying span data across different storage formats:
// - HeadBlock: In-memory Arrow (mutable, active ingestion, no directory)
// - ArrowBlock: Arrow IPC files on disk (immutable L0 blocks from flushed head)
// - ParquetBlock: Parquet files (immutable L1+ blocks from compaction)
//
// All block types support the same query operations, but implement them differently:
// - HeadBlock: Direct memory access to ArrowStorage, always has index, thread-safe
// - ArrowBlock: Loads entire Arrow IPC file into memory, indexed record lookups
// - ParquetBlock: Page-level Parquet reads using OffsetIndex for minimal I/O
type Block interface {
	// Meta returns the block metadata
	// For HeadBlock, metadata is computed dynamically
	// For disk blocks, metadata is loaded from meta.json
	Meta() *BlockMeta

	// Dir returns the directory path of this block
	// Returns empty string "" for HeadBlock (no directory)
	// Returns absolute path for disk-based blocks
	Dir() string

	// Index returns the block's index (may be nil if not loaded)
	Index() *index.Index

	// HasIndex returns true if the block has an index loaded
	HasIndex() bool

	// Close releases resources held by this block
	Close() error

	// GetSpanByID retrieves a single span by ID using the index
	GetSpanByID(spanID string) (*span.Span, error)

	// GetSpansBatch efficiently retrieves multiple spans by ID
	GetSpansBatch(spanIDs []string) ([]*span.Span, error)

	// ReadAll reads all spans from the block (for full scans)
	ReadAll() ([]*span.Span, error)

	// GetTraceByID retrieves all spans for a given trace ID
	GetTraceByID(traceID string) ([]*span.Span, error)

	// GetSpansByTag retrieves all spans that have a specific tag key-value pair
	GetSpansByTag(tagKey, tagValue string) ([]*span.Span, error)
}

// LoadBlock loads a block from disk, automatically detecting whether it's
// an Arrow IPC block (L0) or Parquet block (L1+)
func LoadBlock(dir string) (Block, error) {
	// Read metadata to determine block type
	metaPath := filepath.Join(dir, metaFilename)
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta BlockMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// L0 blocks are Arrow IPC, L1+ are Parquet
	if meta.Level() == 0 {
		return NewArrowBlock(dir)
	}

	return NewParquetBlock(dir)
}
