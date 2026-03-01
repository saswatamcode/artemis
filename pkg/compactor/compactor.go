package compactor

import (
	"fmt"
	"log/slog"
	"math/rand"
	"path/filepath"
	"sort"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/index"
	"github.com/saswatamcode/artemis/pkg/span"
)

// Compactor handles Prometheus-style multi-level block compaction
type Compactor struct {
	baseDir      string
	levelConfigs map[int]*LevelConfig
}

// NewCompactor creates a new compactor with default level configs
func NewCompactor(baseDir string) *Compactor {
	return &Compactor{
		baseDir:      baseDir,
		levelConfigs: DefaultLevelConfigs(),
	}
}

// NewCompactorWithConfigs creates a new compactor with custom level configs
func NewCompactorWithConfigs(baseDir string, levelConfigs map[int]*LevelConfig) *Compactor {
	return &Compactor{
		baseDir:      baseDir,
		levelConfigs: levelConfigs,
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

	// Collect links from all source blocks
	allLinks, err := c.collectLinks(plan.Blocks)
	if err != nil {
		return nil, err
	}

	// Sort spans by start time
	sort.Slice(allSpans, func(i, j int) bool {
		return allSpans[i].StartTime.Before(allSpans[j].StartTime)
	})

	// Determine WAL segment range from source blocks
	// The compacted block should cover the union of all source block WAL segments
	// IMPORTANT: Validate that source blocks have contiguous or overlapping ranges
	minWALSegment := -1
	maxWALSegment := -1
	segmentRanges := make([][2]int, 0, len(plan.Blocks))

	for _, blk := range plan.Blocks {
		meta := blk.Meta()
		segmentRanges = append(segmentRanges, [2]int{meta.MinWALSegment, meta.MaxWALSegment})

		if minWALSegment == -1 || meta.MinWALSegment < minWALSegment {
			minWALSegment = meta.MinWALSegment
		}
		if meta.MaxWALSegment > maxWALSegment {
			maxWALSegment = meta.MaxWALSegment
		}
	}

	// Validate WAL segment continuity
	// Check if all segments from minWALSegment to maxWALSegment are covered
	if minWALSegment != -1 && maxWALSegment != -1 {
		covered := make([]bool, maxWALSegment-minWALSegment+1)
		for _, r := range segmentRanges {
			for seg := r[0]; seg <= r[1]; seg++ {
				if seg >= minWALSegment && seg <= maxWALSegment {
					covered[seg-minWALSegment] = true
				}
			}
		}

		// Check for gaps
		for i, isCovered := range covered {
			if !isCovered {
				return nil, fmt.Errorf("WAL segment gap detected: segment %d not covered by any source block (compacting blocks with non-contiguous WAL segments)", minWALSegment+i)
			}
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

	// Write links to Parquet if any links were collected
	if len(allLinks) > 0 {
		slog.Default().Info("writing parquet links",
			slog.String("block_dir", blockDir),
			slog.Int("link_count", len(allLinks)))
		if err := block.WriteParquetLinks(blockDir, allLinks); err != nil {
			return nil, fmt.Errorf("failed to write parquet links: %w", err)
		}
		slog.Default().Info("successfully wrote parquet links file")
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

		// Use the unified ReadAll() method - works for both Arrow and Parquet blocks
		spans, err := blk.ReadAll()
		if err != nil {
			return nil, 0, 0, fmt.Errorf("failed to read block %s: %w", blk.Dir(), err)
		}
		allSpans = append(allSpans, spans...)
	}

	return allSpans, minTime, maxTime, nil
}

// collectLinks collects all links from a list of blocks (Arrow or Parquet)
func (c *Compactor) collectLinks(blocks []block.Block) ([]*span.SpanLink, error) {
	var allLinks []*span.SpanLink

	for _, blk := range blocks {
		slog.Default().Info("processing block for links",
			slog.String("block_dir", blk.Dir()),
			slog.String("block_type", fmt.Sprintf("%T", blk)),
			slog.Int("level", blk.Meta().Level()))

		// Try to read links directly from ArrowBlock or ParquetBlock
		switch b := blk.(type) {
		case *block.ArrowBlock:
			slog.Default().Info("matched ArrowBlock")
			links, err := b.ReadAllLinks()
			if err != nil {
				slog.Default().Warn("failed to read links from Arrow block",
					slog.String("block_dir", blk.Dir()),
					slog.String("error", err.Error()))
				continue
			}
			if links != nil && len(links) > 0 {
				slog.Default().Info("collected links from Arrow block",
					slog.String("block_dir", blk.Dir()),
					slog.Int("link_count", len(links)))
				allLinks = append(allLinks, links...)
			} else {
				slog.Default().Info("Arrow block has no links",
					slog.String("block_dir", blk.Dir()))
			}
		case *block.ParquetBlock:
			slog.Default().Info("matched ParquetBlock")
			links, err := b.ReadAllLinks()
			if err != nil {
				slog.Default().Warn("failed to read links from Parquet block",
					slog.String("block_dir", blk.Dir()),
					slog.String("error", err.Error()))
				continue
			}
			if links != nil && len(links) > 0 {
				slog.Default().Info("collected links from Parquet block",
					slog.String("block_dir", blk.Dir()),
					slog.Int("link_count", len(links)))
				allLinks = append(allLinks, links...)
			} else {
				slog.Default().Info("Parquet block has no links",
					slog.String("block_dir", blk.Dir()))
			}
		default:
			slog.Default().Warn("block type doesn't support links",
				slog.String("block_dir", blk.Dir()),
				slog.String("block_type", fmt.Sprintf("%T", blk)))
		}
	}

	slog.Default().Info("total links collected for compaction",
		slog.Int("total_links", len(allLinks)))

	return allLinks, nil
}

// buildIndex builds an index for spans
// Groups spans into row groups of 1024 spans each for Parquet
func (c *Compactor) buildIndex(spans []*span.Span) *index.Index {
	idx := index.NewIndex()

	const rowGroupSize = 1024
	for i, sp := range spans {
		recordIdx := i / rowGroupSize           // Which row group
		rowIdx := i % rowGroupSize              // Row within row group
		idx.AddSpan(sp, recordIdx, rowIdx, nil) // No attrRef for in-memory compaction
	}

	return idx
}

// extractULIDs extracts ULIDs from a list of blocks
func extractULIDs(blocks []block.Block) []ulid.ULID {
	ulids := make([]ulid.ULID, len(blocks))
	for i, blk := range blocks {
		ulids[i] = blk.Meta().ULID
	}
	return ulids
}
