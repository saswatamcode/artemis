package wal

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// ReplayStats tracks statistics during WAL replay
type ReplayStats struct {
	TotalSegments     int             // Total number of segments to replay
	ProcessedSegments int             // Segments processed so far
	TotalRecords      int64           // Total records processed
	TotalSpans        int64           // Total spans replayed
	CorruptedRecords  int64           // Number of corrupted records encountered
	SkippedRecords    int64           // Number of records skipped
	StartTime         time.Time       // When replay started
	LastSegmentTime   time.Time       // When last segment was processed
	SegmentDurations  []time.Duration // Duration per segment
	Errors            []ReplayError   // Errors encountered during replay
}

// ReplayError represents an error encountered during replay
type ReplayError struct {
	Segment     string    // Segment file where error occurred
	Offset      int64     // Offset in the segment
	RecordIndex int64     // Record index
	Err         error     // The actual error
	Timestamp   time.Time // When the error occurred
}

// ReplayProgress represents the current state of replay
type ReplayProgress struct {
	Stage              string  // Current stage (e.g., "checkpoint", "wal")
	CurrentSegment     string  // Current segment being processed
	SegmentProgress    float64 // Progress within current segment (0-1)
	TotalProgress      float64 // Overall progress (0-1)
	ProcessedRecords   int64   // Records processed so far
	ProcessedSpans     int64   // Spans processed so far
	ElapsedTime        time.Duration
	EstimatedRemaining time.Duration
}

// ReplayCallback is called periodically during replay to report progress
type ReplayCallback func(progress ReplayProgress)

// ReplayOptions configures WAL replay behavior
type ReplayOptions struct {
	ProgressCallback ReplayCallback // ProgressCallback is called periodically to report progress
	ProgressInterval int64          // ProgressInterval controls how often progress is reported, reported every N records
	StopOnError      bool           // StopOnError controls whether to stop on first error or continue
	SkipCorrupted    bool           // SkipCorrupted controls whether to skip corrupted records
}

// DefaultReplayOptions returns default replay options
func DefaultReplayOptions() *ReplayOptions {
	return &ReplayOptions{
		ProgressInterval: 1000,  // Report every 1000 records
		StopOnError:      false, // Continue on errors by default
		SkipCorrupted:    true,  // Skip corrupted records by default
	}
}

// Replay replays the WAL with enhanced progress tracking and error handling
func (r *Reader) Replay(callback func(*span.Span) error, opts *ReplayOptions) (*ReplayStats, error) {
	if opts == nil {
		opts = DefaultReplayOptions()
	}

	stats := &ReplayStats{
		StartTime: time.Now(),
		Errors:    make([]ReplayError, 0),
	}

	filteredSegments, err := listFilteredWALSegments(r.dir)
	if err != nil {
		return stats, fmt.Errorf("failed to list WAL segments: %w", err)
	}

	if len(filteredSegments) > 0 {
		r.logger.Info("replaying WAL segments", "count", len(filteredSegments))
	}

	stats.TotalSegments = len(filteredSegments)

	// Replay each segment
	for i, segmentFile := range filteredSegments {
		stats.ProcessedSegments = i
		segmentPath := r.dir + "/" + segmentFile

		if opts.ProgressCallback != nil {
			progress := float64(i) / float64(len(filteredSegments))
			opts.ProgressCallback(ReplayProgress{
				Stage:            "wal",
				CurrentSegment:   segmentFile,
				TotalProgress:    progress,
				ProcessedRecords: stats.TotalRecords,
				ProcessedSpans:   stats.TotalSpans,
				ElapsedTime:      time.Since(stats.StartTime),
			})
		}

		segmentStart := time.Now()
		if err := r.replaySegmentWithStats(segmentPath, callback, opts, stats); err != nil {
			if opts.StopOnError {
				return stats, fmt.Errorf("segment %s replay failed: %w", segmentFile, err)
			}
		}
		stats.SegmentDurations = append(stats.SegmentDurations, time.Since(segmentStart))
		stats.LastSegmentTime = time.Now()
		stats.ProcessedSegments = i + 1
	}

	if opts.ProgressCallback != nil {
		opts.ProgressCallback(ReplayProgress{
			Stage:            "complete",
			TotalProgress:    1.0,
			ProcessedRecords: stats.TotalRecords,
			ProcessedSpans:   stats.TotalSpans,
			ElapsedTime:      time.Since(stats.StartTime),
		})
	}

	return stats, nil
}

// replaySegmentWithStats replays a single segment with statistics tracking
func (r *Reader) replaySegmentWithStats(filename string, callback func(*span.Span) error, opts *ReplayOptions, stats *ReplayStats) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	recordIndex := int64(0)

	for {
		crc, _, recordType, data, offset, err := readRecordFull(reader)
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}

			// Handle corrupted record
			replayErr := ReplayError{
				Segment:     filename,
				Offset:      offset,
				RecordIndex: recordIndex,
				Err:         err,
				Timestamp:   time.Now(),
			}
			stats.Errors = append(stats.Errors, replayErr)
			stats.CorruptedRecords++

			if opts.SkipCorrupted {
				stats.SkippedRecords++
				continue
			}

			if opts.StopOnError {
				return fmt.Errorf("corrupted record at offset %d: %w", offset, err)
			}
			continue
		}

		if !verifyCRC(crc, data) {
			replayErr := ReplayError{
				Segment:     filename,
				Offset:      offset,
				RecordIndex: recordIndex,
				Err:         fmt.Errorf("CRC mismatch"),
				Timestamp:   time.Now(),
			}
			stats.Errors = append(stats.Errors, replayErr)
			stats.CorruptedRecords++

			if opts.SkipCorrupted {
				stats.SkippedRecords++
				continue
			}

			if opts.StopOnError {
				return fmt.Errorf("CRC mismatch at offset %d", offset)
			}
			continue
		}

		stats.TotalRecords++
		recordIndex++

		if RecordType(recordType) == RecordTypeSpan {
			s, err := unmarshalSpan(data)
			if err != nil {
				replayErr := ReplayError{
					Segment:     filename,
					Offset:      offset,
					RecordIndex: recordIndex,
					Err:         fmt.Errorf("unmarshal span: %w", err),
					Timestamp:   time.Now(),
				}
				stats.Errors = append(stats.Errors, replayErr)

				if opts.StopOnError {
					return err
				}
				continue
			}

			if err := callback(s); err != nil {
				return fmt.Errorf("callback error: %w", err)
			}

			stats.TotalSpans++
		}

		if opts.ProgressCallback != nil && stats.TotalRecords%opts.ProgressInterval == 0 {
			opts.ProgressCallback(ReplayProgress{
				Stage:            "wal",
				CurrentSegment:   filename,
				ProcessedRecords: stats.TotalRecords,
				ProcessedSpans:   stats.TotalSpans,
				ElapsedTime:      time.Since(stats.StartTime),
			})
		}
	}
}

// listFilteredWALSegments returns WAL segment filenames that should be replayed,
// taking checkpoint metadata into account. Returns segment names (not full paths).
func listFilteredWALSegments(dir string) ([]string, error) {
	startSegment := 0

	metadata, err := ReadCheckpointMetadata(dir)
	if err == nil && metadata != nil {
		startSegment = metadata.MaxDeletedSegment + 1
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var segments []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) >= 4 && name[len(name)-4:] == ".wal" {
			var segmentIndex int
			if _, err := fmt.Sscanf(name, "%06d.wal", &segmentIndex); err == nil {
				if segmentIndex >= startSegment {
					segments = append(segments, name)
				}
			}
		}
	}

	return segments, nil
}
