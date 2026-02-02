package block

import (
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

// CompactionLevel represents the compaction level of a block (similar to Prometheus)
// Level 1 = initial blocks from head (typically 2h)
// Level 2+ = compacted blocks with increasing time ranges
type CompactionLevel int

// CompactionMeta contains metadata about a compacted block
type CompactionMeta struct {
	Level       int         `json:"level"`        // Compaction level (1 = from head, 2+ = compacted)
	Sources     []ulid.ULID `json:"sources"`      // ULIDs of source blocks
	CompactedAt time.Time   `json:"compacted_at"` // When this block was created by compaction
}

// BlockMeta contains metadata for a block
type BlockMeta struct {
	ULID          ulid.ULID       `json:"ulid"`                 // Unique identifier for the block
	MinTime       int64           `json:"min_time"`             // Minimum timestamp in the block (nanoseconds)
	MaxTime       int64           `json:"max_time"`             // Maximum timestamp in the block (nanoseconds)
	SpanCount     int64           `json:"span_count"`           // Number of spans in the block
	Version       int             `json:"version"`              // Block format version
	CreatedAt     time.Time       `json:"created_at"`           // When the block was created
	Compaction    *CompactionMeta `json:"compaction,omitempty"` // Compaction metadata (nil for L0)
	MinWALSegment int             `json:"min_wal_segment"`      // Lowest WAL segment index included in this block
	MaxWALSegment int             `json:"max_wal_segment"`      // Highest WAL segment index included in this block
}

// BlockStats holds statistics about a block
type BlockStats struct {
	SpanCount     int64
	RecordBatches int
	MinTime       time.Time
	MaxTime       time.Time
	SizeBytes     int64
}

// String returns a string representation of the block
func (m *BlockMeta) String() string {
	levelStr := "L0 (head)"
	if m.Compaction != nil {
		duration := m.Duration()
		levelStr = fmt.Sprintf("L%d (%s)", m.Compaction.Level, formatDuration(duration))
	}

	return fmt.Sprintf("Block{ULID: %s, Level: %s, MinTime: %s, MaxTime: %s, Spans: %d}",
		m.ULID.String(),
		levelStr,
		time.Unix(0, m.MinTime).Format(time.RFC3339),
		time.Unix(0, m.MaxTime).Format(time.RFC3339),
		m.SpanCount)
}

// formatDuration formats a duration in a human-readable way
func formatDuration(d time.Duration) string {
	hours := d.Hours()
	if hours < 1 {
		return fmt.Sprintf("%.0fm", d.Minutes())
	} else if hours < 24 {
		return fmt.Sprintf("%.0fh", hours)
	} else {
		days := hours / 24
		return fmt.Sprintf("%.0fd", days)
	}
}

// Duration returns the time range covered by this block
func (m *BlockMeta) Duration() time.Duration {
	return time.Duration(m.MaxTime - m.MinTime)
}

// Contains returns true if the given timestamp falls within this block's time range
func (m *BlockMeta) Contains(ts int64) bool {
	return ts >= m.MinTime && ts <= m.MaxTime
}

// Overlaps returns true if this block's time range overlaps with another block
func (m *BlockMeta) Overlaps(other *BlockMeta) bool {
	return m.MinTime <= other.MaxTime && m.MaxTime >= other.MinTime
}

// Level returns the compaction level of this block (0 for head blocks, 1+ for compacted)
func (m *BlockMeta) Level() int {
	if m.Compaction == nil {
		return 0
	}
	return m.Compaction.Level
}
