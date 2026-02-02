package compactor

import (
	"time"
)

// LevelConfig holds configuration for compaction at each level
type LevelConfig struct {
	// TargetDuration is the target time range for blocks at this level
	TargetDuration time.Duration

	// MinBlockAge is how long to wait before compacting blocks to next level
	MinBlockAge time.Duration

	// MinBlocks is minimum number of blocks to trigger compaction
	MinBlocks int
}

// DefaultLevelConfigs returns default Prometheus-style compaction configuration
// Level 0: Blocks from head (no compaction) - variable duration based on head flush settings
// Level 1: 2h blocks (first compaction from L0)
// Level 2: 4h blocks (compact 2x 2h blocks)
// Level 3: 8h blocks (compact 2x 4h blocks)
// Level 4: 2d (48h) blocks (compact 6x 8h blocks)
// Level 5: 14d (336h) blocks (compact 7x 2d blocks)
func DefaultLevelConfigs() map[int]*LevelConfig {
	return map[int]*LevelConfig{
		0: {
			TargetDuration: 0, // Variable for L0 (from head)
			MinBlockAge:    10 * time.Minute,
			MinBlocks:      2, // Compact at least 2 L0 blocks
		},
		1: {
			TargetDuration: 2 * time.Hour,
			MinBlockAge:    2 * time.Hour,
			MinBlocks:      2, // Compact 2+ blocks
		},
		2: {
			TargetDuration: 4 * time.Hour,
			MinBlockAge:    4 * time.Hour,
			MinBlocks:      2,
		},
		3: {
			TargetDuration: 8 * time.Hour,
			MinBlockAge:    8 * time.Hour,
			MinBlocks:      2,
		},
		4: {
			TargetDuration: 48 * time.Hour, // 2 days
			MinBlockAge:    48 * time.Hour,
			MinBlocks:      2,
		},
		5: {
			TargetDuration: 336 * time.Hour, // 14 days
			MinBlockAge:    336 * time.Hour,
			MinBlocks:      2,
		},
	}
}

// ShouldCompact determines if blocks at this level should be compacted
func (cfg *LevelConfig) ShouldCompact(blockCount int, oldestBlockAge time.Duration) bool {
	return blockCount >= cfg.MinBlocks && oldestBlockAge >= cfg.MinBlockAge
}
