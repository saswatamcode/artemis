package compactor

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/oklog/ulid/v2"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/index"
	"github.com/saswatamcode/artemis/pkg/span"
)

// Compactor handles Prometheus-style multi-level block compaction
type Compactor struct {
	baseDir      string
	levelConfigs map[int]*LevelConfig
	mem          memory.Allocator
}

// NewCompactor creates a new compactor with default level configs
func NewCompactor(baseDir string) *Compactor {
	return &Compactor{
		baseDir:      baseDir,
		levelConfigs: DefaultLevelConfigs(),
		mem:          memory.NewGoAllocator(),
	}
}

// NewCompactorWithConfigs creates a new compactor with custom level configs
func NewCompactorWithConfigs(baseDir string, levelConfigs map[int]*LevelConfig) *Compactor {
	return &Compactor{
		baseDir:      baseDir,
		levelConfigs: levelConfigs,
		mem:          memory.NewGoAllocator(),
	}
}

// GetBaseDir returns the base directory for compacted blocks
func (c *Compactor) GetBaseDir() string {
	return c.baseDir
}

// Plan creates a compaction plan for blocks at a given level
func (c *Compactor) Plan(blocks []block.Block, level int) *CompactionPlan {
	cfg := c.levelConfigs[level]
	if cfg == nil {
		return nil
	}

	// Filter blocks by level
	var levelBlocks []block.Block
	for _, blk := range blocks {
		if blk.Meta().Level() == level {
			levelBlocks = append(levelBlocks, blk)
		}
	}

	if len(levelBlocks) < cfg.MinBlocks {
		return nil // Not enough blocks to compact
	}

	// Sort by creation time to find oldest
	sort.Slice(levelBlocks, func(i, j int) bool {
		return levelBlocks[i].Meta().CreatedAt.Before(levelBlocks[j].Meta().CreatedAt)
	})

	// Check if oldest block is old enough
	oldestAge := time.Since(levelBlocks[0].Meta().CreatedAt)
	if !cfg.ShouldCompact(len(levelBlocks), oldestAge) {
		return nil
	}

	// Select blocks to compact
	return &CompactionPlan{
		Level:   level,
		Blocks:  levelBlocks,
		Sources: extractULIDs(levelBlocks),
	}
}

// CompactionPlan describes which blocks to compact
type CompactionPlan struct {
	Level   int
	Blocks  []block.Block
	Sources []ulid.ULID
}

// Compact compacts blocks from one level to the next
// For L0, reads Arrow IPC and writes Parquet
// For L1+, reads Parquet and writes larger Parquet blocks
func (c *Compactor) Compact(plan *CompactionPlan) (*block.BlockMeta, error) {
	allSpans, minTime, maxTime, err := c.collectSpans(plan.Blocks)
	if err != nil {
		return nil, err
	}

	// Sort spans by start time
	sort.Slice(allSpans, func(i, j int) bool {
		return allSpans[i].StartTime.Before(allSpans[j].StartTime)
	})

	// Determine WAL segment range from source blocks
	// The compacted block should cover the union of all source block WAL segments
	minWALSegment := -1
	maxWALSegment := -1
	for _, blk := range plan.Blocks {
		meta := blk.Meta()
		if minWALSegment == -1 || meta.MinWALSegment < minWALSegment {
			minWALSegment = meta.MinWALSegment
		}
		if meta.MaxWALSegment > maxWALSegment {
			maxWALSegment = meta.MaxWALSegment
		}
	}

	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	blockID := ulid.MustNew(ulid.Timestamp(time.Now()), entropy)

	compactionMeta := &block.CompactionMeta{
		Level:       plan.Level + 1,
		Sources:     plan.Sources,
		CompactedAt: time.Now(),
	}

	meta := &block.BlockMeta{
		ULID:          blockID,
		MinTime:       minTime,
		MaxTime:       maxTime,
		SpanCount:     int64(len(allSpans)),
		Version:       1,
		CreatedAt:     time.Now(),
		Compaction:    compactionMeta,
		MinWALSegment: minWALSegment,
		MaxWALSegment: maxWALSegment,
	}

	idx := c.buildIndex(allSpans)

	// Write as Parquet (all compacted blocks are Parquet, even from L0)
	blockDir := filepath.Join(c.baseDir, blockID.String())
	if err := block.WriteParquetBlock(blockDir, meta, allSpans, idx); err != nil {
		return nil, fmt.Errorf("failed to write parquet block: %w", err)
	}

	return meta, nil
}

// collectSpans collects all spans from a list of blocks (Arrow or Parquet)
func (c *Compactor) collectSpans(blocks []block.Block) ([]*span.Span, int64, int64, error) {
	var allSpans []*span.Span
	var minTime int64 = -1
	var maxTime int64

	for _, blk := range blocks {
		meta := blk.Meta()

		if minTime == -1 || meta.MinTime < minTime {
			minTime = meta.MinTime
		}
		if meta.MaxTime > maxTime {
			maxTime = meta.MaxTime
		}

		// Check if this is a Parquet block (Records() returns nil)
		if blk.Records() == nil {
			pblk, err := block.NewParquetBlock(blk.Dir())
			if err != nil {
				return nil, 0, 0, fmt.Errorf("failed to open parquet block: %w", err)
			}
			defer pblk.Close()

			spans, err := pblk.ReadAll()
			if err != nil {
				return nil, 0, 0, fmt.Errorf("failed to read parquet block: %w", err)
			}
			allSpans = append(allSpans, spans...)
		} else {
			// This is an Arrow IPC block (L0)
			records := blk.Records()
			for _, record := range records {
				for row := 0; row < int(record.NumRows()); row++ {
					sp, err := extractSpanFromRecord(record, row)
					if err != nil {
						continue
					}
					allSpans = append(allSpans, sp)
				}
			}
		}
	}

	return allSpans, minTime, maxTime, nil
}

// buildIndex builds an index for spans
// Groups spans into row groups of 1024 spans each for Parquet
func (c *Compactor) buildIndex(spans []*span.Span) *index.Index {
	idx := index.NewIndex()

	const rowGroupSize = 1024
	for i, sp := range spans {
		recordIdx := i / rowGroupSize // Which row group
		rowIdx := i % rowGroupSize    // Row within row group
		idx.AddSpan(sp, recordIdx, rowIdx)
	}

	return idx
}

// extractSpanFromRecord extracts a span from an Arrow record
func extractSpanFromRecord(record arrow.Record, rowIndex int) (*span.Span, error) {
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

// extractULIDs extracts ULIDs from a list of blocks
func extractULIDs(blocks []block.Block) []ulid.ULID {
	ulids := make([]ulid.ULID, len(blocks))
	for i, blk := range blocks {
		ulids[i] = blk.Meta().ULID
	}
	return ulids
}
