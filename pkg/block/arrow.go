package block

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/saswatamcode/artemis/pkg/index"
	"github.com/saswatamcode/artemis/pkg/span"
)

const (
	metaFilename  = "meta.json"
	dataFilename  = "spans.arrow"
	indexFilename = "index.json"
)

// ArrowBlock represents an Arrow IPC block (L0 blocks)
type ArrowBlock struct {
	meta    *BlockMeta
	dir     string
	records []arrow.Record
	mem     memory.Allocator
	schema  *arrow.Schema
	index   *index.Index
}

// NewArrowBlock creates a new Arrow block from disk
func NewArrowBlock(dir string) (*ArrowBlock, error) {
	metaPath := filepath.Join(dir, metaFilename)
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta BlockMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	ab := &ArrowBlock{
		meta: &meta,
		dir:  dir,
		mem:  memory.NewGoAllocator(),
	}

	if err := ab.loadRecords(); err != nil {
		return nil, fmt.Errorf("failed to load records: %w", err)
	}

	// Load index if it exists
	if err := ab.loadIndex(); err != nil {
		// Index is optional - if it doesn't exist, queries will be slower but still work
		slog.Default().Warn("failed to load index for Arrow block",
			slog.String("block_dir", dir),
			slog.String("error", err.Error()))
	}

	return ab, nil
}

// loadRecords loads Arrow records from the IPC file
func (ab *ArrowBlock) loadRecords() error {
	dataPath := filepath.Join(ab.dir, dataFilename)
	f, err := os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer f.Close()

	reader, err := ipc.NewFileReader(f, ipc.WithAllocator(ab.mem))
	if err != nil {
		return fmt.Errorf("failed to create IPC reader: %w", err)
	}
	defer reader.Close()

	ab.schema = reader.Schema()
	ab.records = make([]arrow.Record, reader.NumRecords())

	for i := 0; i < reader.NumRecords(); i++ {
		rec, err := reader.Record(i)
		if err != nil {
			return fmt.Errorf("failed to read record %d: %w", i, err)
		}
		rec.Retain() // Retain the record so it's not freed when reader closes
		ab.records[i] = rec
	}

	return nil
}

// loadIndex loads the index from disk
func (ab *ArrowBlock) loadIndex() error {
	indexPath := filepath.Join(ab.dir, indexFilename)
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		return fmt.Errorf("index file not found")
	}

	indexData, err := os.ReadFile(indexPath)
	if err != nil {
		return fmt.Errorf("failed to read index file: %w", err)
	}

	var serialized index.SerializedIndex
	if err := json.Unmarshal(indexData, &serialized); err != nil {
		return fmt.Errorf("failed to unmarshal index: %w", err)
	}

	ab.index = index.NewIndexFromSerialized(&serialized)
	return nil
}

// Meta returns the block metadata
func (ab *ArrowBlock) Meta() *BlockMeta {
	return ab.meta
}

// Records returns all Arrow records in this block
func (ab *ArrowBlock) Records() []arrow.Record {
	return ab.records
}

// Schema returns the Arrow schema
func (ab *ArrowBlock) Schema() *arrow.Schema {
	return ab.schema
}

// Index returns the block's index (may be nil if not loaded)
func (ab *ArrowBlock) Index() *index.Index {
	return ab.index
}

// HasIndex returns true if the block has an index loaded
func (ab *ArrowBlock) HasIndex() bool {
	return ab.index != nil
}

// Dir returns the directory path of this block
func (ab *ArrowBlock) Dir() string {
	return ab.dir
}

// Close releases resources held by this block
func (ab *ArrowBlock) Close() error {
	for _, rec := range ab.records {
		rec.Release()
	}
	ab.records = nil
	return nil
}

// extractSpanFromArrowRecord extracts a span from an Arrow record
func extractSpanFromArrowRecord(record arrow.Record, rowIndex int) (*span.Span, error) {
	if rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("invalid row index %d", rowIndex)
	}

	sp := &span.Span{}

	sp.TraceID = record.Column(0).(*array.String).Value(rowIndex)

	sp.SpanID = record.Column(1).(*array.String).Value(rowIndex)

	parentCol := record.Column(2).(*array.String)
	if !parentCol.IsNull(rowIndex) {
		sp.ParentSpanID = parentCol.Value(rowIndex)
	}

	sp.Name = record.Column(3).(*array.String).Value(rowIndex)

	sp.StartTime = time.Unix(0, record.Column(4).(*array.Int64).Value(rowIndex))

	sp.EndTime = time.Unix(0, record.Column(5).(*array.Int64).Value(rowIndex))

	sp.Duration = record.Column(6).(*array.Int64).Value(rowIndex)

	sp.ServiceName = record.Column(7).(*array.String).Value(rowIndex)

	tagsCol := record.Column(8).(*array.Map)
	if !tagsCol.IsNull(rowIndex) {
		sp.Tags = make(map[string]string)

		offset := tagsCol.Offsets()[rowIndex]
		nextOffset := tagsCol.Offsets()[rowIndex+1]

		keys := tagsCol.Keys().(*array.String)
		items := tagsCol.Items().(*array.String)

		for i := int(offset); i < int(nextOffset); i++ {
			key := keys.Value(i)
			value := items.Value(i)
			sp.Tags[key] = value
		}
	}

	return sp, nil
}

// FlushBlock flushes an in-memory block to disk as Arrow IPC
// Uses atomic write with temporary directory to prevent corruption
func FlushBlock(dir string, meta *BlockMeta, records []arrow.Record, schema *arrow.Schema, idx *index.Index) error {
	// CRITICAL: Write to temporary directory first for atomicity
	// This prevents corruption if system crashes during write
	tmpDir := dir + ".tmp"

	// Clean up any existing temp directory
	os.RemoveAll(tmpDir)

	// Create temporary block directory
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp block directory: %w", err)
	}

	metaPath := filepath.Join(tmpDir, metaFilename)
	metaData, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	if err := atomicWriteFile(metaPath, metaData); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	if idx != nil {
		indexPath := filepath.Join(tmpDir, indexFilename)
		serialized := idx.Serialize()
		indexData, err := json.MarshalIndent(serialized, "", "  ")
		if err != nil {
			os.RemoveAll(tmpDir)
			return fmt.Errorf("failed to marshal index: %w", err)
		}
		if err := atomicWriteFile(indexPath, indexData); err != nil {
			os.RemoveAll(tmpDir)
			return fmt.Errorf("failed to write index: %w", err)
		}
	}

	dataPath := filepath.Join(tmpDir, dataFilename)
	f, err := os.Create(dataPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to create data file: %w", err)
	}

	writer, err := ipc.NewFileWriter(f, ipc.WithSchema(schema))
	if err != nil {
		f.Close()
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to create IPC writer: %w", err)
	}

	for _, rec := range records {
		if err := writer.Write(rec); err != nil {
			writer.Close()
			f.Close()
			os.RemoveAll(tmpDir)
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		f.Close()
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// CRITICAL: Fsync data file before closing
	if err := f.Sync(); err != nil {
		f.Close()
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to sync data file: %w", err)
	}
	f.Close()

	// Fsync the directory to ensure all files are persisted
	if err := fsyncDir(tmpDir); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to sync temp directory: %w", err)
	}

	// ATOMIC: Rename temporary directory to final location
	if err := os.Rename(tmpDir, dir); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("failed to rename block directory: %w", err)
	}

	// Fsync parent directory for rename atomicity
	parentDir := filepath.Dir(dir)
	if err := fsyncDir(parentDir); err != nil {
		// Log but don't fail - rename is already complete
		slog.Default().Warn("failed to sync parent directory",
			slog.String("error", err.Error()))
	}

	return nil
}

// atomicWriteFile writes data to a file with fsync for durability
func atomicWriteFile(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return err
	}

	return f.Sync()
}

// fsyncDir fsyncs a directory
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// DeleteBlock deletes a block directory and all its contents
func DeleteBlock(dir string) error {
	return os.RemoveAll(dir)
}
