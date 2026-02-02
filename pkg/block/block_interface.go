package block

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/saswatamcode/artemis/pkg/index"
)

// Block is the interface that all block types must implement
// Provides a unified API for querying both Arrow (L0) and Parquet (L1+) blocks
type Block interface {
	// Meta returns the block metadata
	Meta() *BlockMeta

	// Dir returns the directory path of this block
	Dir() string

	// Index returns the block's index (may be nil if not loaded)
	Index() *index.Index

	// HasIndex returns true if the block has an index loaded
	HasIndex() bool

	// Close releases resources held by this block
	Close() error

	// Records returns Arrow records (only for Arrow blocks, nil for Parquet)
	Records() []arrow.Record

	// Schema returns the Arrow schema (only for Arrow blocks, nil for Parquet)
	Schema() *arrow.Schema
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
