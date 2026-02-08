package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	checkpointFile = "checkpoint.json"
)

// CheckpointMetadata tracks which WAL segments have been deleted
type CheckpointMetadata struct {
	MaxDeletedSegment int       `json:"max_deleted_segment"` // Highest WAL segment index that's been deleted
	Timestamp         time.Time `json:"timestamp"`           // When this checkpoint was created
	DeletedCount      int       `json:"deleted_count"`       // Number of segments deleted in this checkpoint
}

// WriteCheckpointMetadata writes checkpoint metadata to disk
// This tracks which WAL segments have been deleted (because they're in blocks)
// Uses atomic write-and-fsync to ensure durability before WAL segments are deleted
func WriteCheckpointMetadata(dir string, maxDeletedSegment int, deletedCount int) error {
	metadata := CheckpointMetadata{
		MaxDeletedSegment: maxDeletedSegment,
		Timestamp:         time.Now(),
		DeletedCount:      deletedCount,
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint metadata: %w", err)
	}

	checkpointPath := filepath.Join(dir, checkpointFile)
	tempPath := checkpointPath + ".tmp"

	// Write to temporary file first
	f, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp checkpoint: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write checkpoint data: %w", err)
	}

	// CRITICAL: Fsync before rename to ensure data is on disk
	// This prevents data loss if system crashes after WAL deletion
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to sync checkpoint: %w", err)
	}
	f.Close()

	if err := os.Rename(tempPath, checkpointPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename checkpoint: %w", err)
	}

	// CRITICAL: Fsync parent directory for rename atomicity
	// Without this, the rename might not be durable and checkpoint could be lost on crash
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("failed to open dir for fsync after checkpoint rename: %w", err)
	}
	defer dirFile.Close()

	if err := dirFile.Sync(); err != nil {
		return fmt.Errorf("failed to fsync directory after checkpoint rename: %w", err)
	}

	return nil
}

// ReadCheckpointMetadata reads checkpoint metadata from disk
// Returns nil if no checkpoint exists
func ReadCheckpointMetadata(dir string) (*CheckpointMetadata, error) {
	checkpointPath := filepath.Join(dir, checkpointFile)

	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read checkpoint metadata: %w", err)
	}

	var metadata CheckpointMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint metadata: %w", err)
	}

	// Validate checkpoint metadata for sanity
	// Corrupted checkpoint could cause incorrect WAL replay behavior
	if err := validateCheckpointMetadata(&metadata); err != nil {
		return nil, fmt.Errorf("invalid checkpoint metadata: %w", err)
	}

	return &metadata, nil
}

// validateCheckpointMetadata validates checkpoint metadata values
func validateCheckpointMetadata(m *CheckpointMetadata) error {
	if m.MaxDeletedSegment < 0 {
		return fmt.Errorf("MaxDeletedSegment is negative: %d", m.MaxDeletedSegment)
	}

	if m.DeletedCount < 0 {
		return fmt.Errorf("DeletedCount is negative: %d", m.DeletedCount)
	}

	// DeletedCount should be MaxDeletedSegment + 1 (segments 0 through MaxDeletedSegment)
	expectedCount := m.MaxDeletedSegment + 1
	if m.DeletedCount != expectedCount {
		return fmt.Errorf("DeletedCount (%d) inconsistent with MaxDeletedSegment (%d), expected %d",
			m.DeletedCount, m.MaxDeletedSegment, expectedCount)
	}

	// Timestamp shouldn't be in the future (allowing 1 minute clock skew)
	if m.Timestamp.After(time.Now().Add(1 * time.Minute)) {
		return fmt.Errorf("Timestamp is in the future: %v", m.Timestamp)
	}

	return nil
}

// DeleteWALSegments deletes WAL segments up to and including the given index
// CRITICAL: Fsyncs the parent directory after deletion to ensure durability
// This ensures deleted segments don't reappear after a crash
func DeleteWALSegments(dir string, maxIndex int) error {
	for i := 0; i <= maxIndex; i++ {
		segmentPath := filepath.Join(dir, fmt.Sprintf("%06d.wal", i))
		if err := os.Remove(segmentPath); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
	}

	// CRITICAL: Fsync parent directory to make deletions durable
	// Without this, deleted segments could reappear after crash recovery
	// The checkpoint metadata says they're deleted, but filesystem hasn't persisted the removal
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("failed to open dir for fsync: %w", err)
	}
	defer dirFile.Close()

	if err := dirFile.Sync(); err != nil {
		return fmt.Errorf("failed to fsync directory after WAL deletion: %w", err)
	}

	return nil
}
