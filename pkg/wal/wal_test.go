package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

func TestWAL_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	defer w.Close()

	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
		Tags: map[string]string{
			"key": "value",
		},
	}

	err = w.WriteSpan(sp)
	if err != nil {
		t.Fatalf("WriteSpan() error = %v", err)
	}

	// Close and reopen to read
	w.Close()

	reader := NewReader(tmpDir, nil)
	spans := []span.Span{}
	_, err = reader.Replay(func(s *span.Span) error {
		spans = append(spans, *s)
		return nil
	}, DefaultReplayOptions())

	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	if len(spans) != 1 {
		t.Errorf("Replay() returned %d spans, want 1", len(spans))
	}

	if spans[0].SpanID != "span-1" {
		t.Errorf("Replayed span ID = %s, want span-1", spans[0].SpanID)
	}
}

func TestWAL_MultipleSpans(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}

	for i := range 100 {
		sp := &span.Span{
			TraceID:     "trace-1",
			SpanID:      fmt.Sprintf("span-%d", i),
			Name:        "operation",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		if err := w.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan() error = %v", err)
		}
	}

	w.Close()

	reader := NewReader(tmpDir, nil)
	count := 0
	_, err = reader.Replay(func(s *span.Span) error {
		count++
		return nil
	}, DefaultReplayOptions())

	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	if count != 100 {
		t.Errorf("Replayed %d spans, want 100", count)
	}
}

func TestWAL_SegmentRotation(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}

	// Write enough spans to potentially trigger rotation
	// (though in practice the segment size is large)
	for i := range 1000 {
		sp := &span.Span{
			TraceID:     "trace-1",
			SpanID:      fmt.Sprintf("span-%d", i),
			Name:        "operation-with-long-name-to-increase-size",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service-with-long-name",
			Tags: map[string]string{
				"tag1": "value1-very-long-value-to-increase-size",
				"tag2": "value2-very-long-value-to-increase-size",
			},
		}
		if err := w.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan() error = %v", err)
		}
	}

	segmentIndex := w.SegmentIndex()
	w.Close()

	reader := NewReader(tmpDir, nil)
	count := 0
	_, err = reader.Replay(func(s *span.Span) error {
		count++
		return nil
	}, DefaultReplayOptions())

	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	if count != 1000 {
		t.Errorf("Replayed %d spans, want 1000", count)
	}

	t.Logf("Created %d segments", segmentIndex+1)
}

func TestWAL_EmptyDir(t *testing.T) {
	tmpDir := t.TempDir()

	reader := NewReader(tmpDir, nil)
	count := 0
	stats, err := reader.Replay(func(s *span.Span) error {
		count++
		return nil
	}, DefaultReplayOptions())

	if err != nil {
		t.Errorf("Replay() on empty dir error = %v, want nil", err)
	}

	if count != 0 {
		t.Errorf("Replayed %d spans from empty dir, want 0", count)
	}

	if stats.ProcessedSegments != 0 {
		t.Errorf("Processed %d segments from empty dir, want 0", stats.ProcessedSegments)
	}
}

func TestWAL_CorruptionHandling(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}

	for i := range 10 {
		sp := &span.Span{
			SpanID:      fmt.Sprintf("span-%d", i),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		w.WriteSpan(sp)
	}

	w.Close()

	// Corrupt the WAL file by appending garbage
	segmentPath := filepath.Join(tmpDir, "000000.wal")
	f, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open segment: %v", err)
	}
	f.Write([]byte("CORRUPT DATA"))
	f.Close()

	// Try to replay with skip corrupted option
	reader := NewReader(tmpDir, nil)
	opts := DefaultReplayOptions()
	opts.SkipCorrupted = true

	count := 0
	stats, err := reader.Replay(func(s *span.Span) error {
		count++
		return nil
	}, opts)

	// Should not fail completely
	if err != nil {
		t.Logf("Replay() error = %v (expected for corrupted data)", err)
	}

	// Should still get most spans
	if count < 5 {
		t.Errorf("Replayed %d spans, want at least 5", count)
	}

	if stats.CorruptedRecords == 0 {
		t.Error("Should have detected corrupted records")
	}
}

func TestWAL_SegmentIndexContinuesAfterRestart(t *testing.T) {
	tmpDir := t.TempDir()

	// First WAL instance - creates segment 000000.wal
	w1, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}

	// Write some spans
	for i := range 10 {
		sp := &span.Span{
			SpanID:      fmt.Sprintf("span-%d", i),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		if err := w1.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan() error = %v", err)
		}
	}

	firstIndex := w1.SegmentIndex()
	w1.Close()

	// Verify segment file exists
	segmentPath := filepath.Join(tmpDir, fmt.Sprintf("%06d.wal", firstIndex))
	if _, err := os.Stat(segmentPath); os.IsNotExist(err) {
		t.Fatalf("Expected segment %06d.wal to exist", firstIndex)
	}

	// Second WAL instance - should continue from next index
	w2, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() after restart error = %v", err)
	}

	secondIndex := w2.SegmentIndex()
	w2.Close()

	// Segment index should be firstIndex + 1
	expectedIndex := firstIndex + 1
	if secondIndex != expectedIndex {
		t.Errorf("After restart, segment index = %d, want %d", secondIndex, expectedIndex)
	}

	// Verify new segment file was created
	newSegmentPath := filepath.Join(tmpDir, fmt.Sprintf("%06d.wal", secondIndex))
	if _, err := os.Stat(newSegmentPath); os.IsNotExist(err) {
		t.Fatalf("Expected new segment %06d.wal to exist", secondIndex)
	}

	t.Logf("First WAL created segment %06d.wal", firstIndex)
	t.Logf("Second WAL (after restart) created segment %06d.wal", secondIndex)
}

func TestWAL_SegmentIndexAfterDeletion(t *testing.T) {
	tmpDir := t.TempDir()

	// Create initial WAL
	w1, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}
	w1.Close()

	// Create segments 000000.wal, 000001.wal, 000002.wal
	for i := range 3 {
		segmentPath := filepath.Join(tmpDir, fmt.Sprintf("%06d.wal", i))
		if err := os.WriteFile(segmentPath, []byte("dummy"), 0644); err != nil {
			t.Fatalf("Failed to create dummy segment: %v", err)
		}
	}

	// Delete segment 000000.wal (simulate checkpoint cleanup)
	os.Remove(filepath.Join(tmpDir, "000000.wal"))

	// Now we have: 000001.wal, 000002.wal
	// New WAL should start from 000003.wal
	w2, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() after deletion error = %v", err)
	}

	index := w2.SegmentIndex()
	w2.Close()

	// Should be 3 (highest was 2, so next is 3)
	if index != 3 {
		t.Errorf("After deletion, segment index = %d, want 3", index)
	}

	// Verify segment 000003.wal was created
	segmentPath := filepath.Join(tmpDir, "000003.wal")
	if _, err := os.Stat(segmentPath); os.IsNotExist(err) {
		t.Fatalf("Expected segment 000003.wal to exist")
	}

	t.Logf("After deleting segment 0, new WAL created segment %06d.wal", index)
}

func TestReplay_WithCheckpointMetadata(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL and write spans to multiple segments
	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		t.Fatalf("NewWAL() error = %v", err)
	}

	// Write spans to segment 0
	for i := range 10 {
		sp := &span.Span{
			SpanID:      fmt.Sprintf("segment0-span-%d", i),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		if err := w.WriteSpan(sp); err != nil {
			t.Fatalf("WriteSpan() error = %v", err)
		}
	}
	w.Close()

	// Create more segments manually
	for seg := 1; seg <= 3; seg++ {
		w, _ = NewWAL(tmpDir, nil)
		for i := range 10 {
			sp := &span.Span{
				SpanID:      fmt.Sprintf("segment%d-span-%d", seg, i),
				StartTime:   time.Now(),
				EndTime:     time.Now().Add(time.Millisecond),
				ServiceName: "service",
			}
			w.WriteSpan(sp)
		}
		w.Close()
	}

	// Simulate checkpoint: delete segments 0 and 1
	if err := DeleteWALSegments(tmpDir, 1); err != nil {
		t.Fatalf("DeleteWALSegments() error = %v", err)
	}

	// Write checkpoint metadata
	if err := WriteCheckpointMetadata(tmpDir, 1, 2); err != nil {
		t.Fatalf("WriteCheckpointMetadata() error = %v", err)
	}

	// Verify checkpoint metadata exists
	metadata, err := ReadCheckpointMetadata(tmpDir)
	if err != nil {
		t.Fatalf("ReadCheckpointMetadata() error = %v", err)
	}
	if metadata.MaxDeletedSegment != 1 {
		t.Errorf("MaxDeletedSegment = %d, want 1", metadata.MaxDeletedSegment)
	}

	// Replay - should skip segments 0 and 1, only read 2 and 3
	reader := NewReader(tmpDir, nil)
	count := 0
	seenSpans := make(map[string]bool)

	_, err = reader.Replay(func(s *span.Span) error {
		count++
		seenSpans[s.SpanID] = true
		return nil
	}, DefaultReplayOptions())

	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	// Should only see spans from segments 2 and 3 (20 spans total)
	expectedCount := 20
	if count != expectedCount {
		t.Errorf("Replayed %d spans, want %d (only segments 2 and 3)", count, expectedCount)
	}

	// Verify we didn't see spans from deleted segments
	for i := range 10 {
		if seenSpans[fmt.Sprintf("segment0-span-%d", i)] {
			t.Errorf("Should not have replayed span from deleted segment 0")
		}
		if seenSpans[fmt.Sprintf("segment1-span-%d", i)] {
			t.Errorf("Should not have replayed span from deleted segment 1")
		}
	}

	// Verify we did see spans from remaining segments
	for i := range 10 {
		if !seenSpans[fmt.Sprintf("segment2-span-%d", i)] {
			t.Errorf("Should have replayed span from segment 2")
		}
		if !seenSpans[fmt.Sprintf("segment3-span-%d", i)] {
			t.Errorf("Should have replayed span from segment 3")
		}
	}

	t.Logf("Successfully replayed %d spans, skipped deleted segments 0-1", count)
}

func BenchmarkWAL_WriteSpan(b *testing.B) {
	tmpDir := b.TempDir()

	w, err := NewWAL(tmpDir, nil)
	if err != nil {
		b.Fatalf("NewWAL() error = %v", err)
	}
	defer w.Close()

	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
		Tags: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	for b.Loop() {
		w.WriteSpan(sp)
	}
}
