package storage

import (
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// TestMVCCSnapshotIsolation_BeginQuery tests the Prometheus-style snapshot isolation for queries
func TestMVCCSnapshotIsolation_BeginQuery(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Register some initial commits
	ic.AcquireAppendLock()
	seq1 := ic.AssignCommitSequence(100)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(100, seq1, []string{"span1", "span2"})

	ic.AcquireAppendLock()
	seq2 := ic.AssignCommitSequence(101)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(101, seq2, []string{"span3", "span4"})

	// Query captures snapshot (should see 4 spans)
	snapshotSeq := ic.BeginQuery()
	if snapshotSeq != seq2 {
		t.Errorf("Expected snapshot seq %d, got %d", seq2, snapshotSeq)
	}

	// Verify initial spans are visible
	if !ic.IsVisible("span1", snapshotSeq) {
		t.Error("span1 should be visible at snapshot")
	}
	if !ic.IsVisible("span2", snapshotSeq) {
		t.Error("span2 should be visible at snapshot")
	}
	if !ic.IsVisible("span3", snapshotSeq) {
		t.Error("span3 should be visible at snapshot")
	}
	if !ic.IsVisible("span4", snapshotSeq) {
		t.Error("span4 should be visible at snapshot")
	}

	// New transaction commits AFTER snapshot was captured
	ic.AcquireAppendLock()
	seq3 := ic.AssignCommitSequence(102)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(102, seq3, []string{"span5", "span6"})

	// These new spans should NOT be visible at the snapshot
	if ic.IsVisible("span5", snapshotSeq) {
		t.Error("span5 should NOT be visible at snapshot (committed after snapshot)")
	}
	if ic.IsVisible("span6", snapshotSeq) {
		t.Error("span6 should NOT be visible at snapshot (committed after snapshot)")
	}

	// But they should be visible at a NEW snapshot
	newSnapshotSeq := ic.BeginQuery()
	if !ic.IsVisible("span5", newSnapshotSeq) {
		t.Error("span5 should be visible at new snapshot")
	}
	if !ic.IsVisible("span6", newSnapshotSeq) {
		t.Error("span6 should be visible at new snapshot")
	}
}

// TestMVCCSnapshotIsolation_IsVisible tests the lock-free visibility checks
func TestMVCCSnapshotIsolation_IsVisible(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Register commits
	ic.AcquireAppendLock()
	seq1 := ic.AssignCommitSequence(100)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(100, seq1, []string{"span1"})

	ic.AcquireAppendLock()
	seq2 := ic.AssignCommitSequence(101)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(101, seq2, []string{"span2"})

	ic.AcquireAppendLock()
	seq3 := ic.AssignCommitSequence(102)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(102, seq3, []string{"span3"})

	// Check visibility at different snapshots
	// At seq1: only span1 visible
	if !ic.IsVisible("span1", seq1) {
		t.Error("span1 should be visible at seq1")
	}
	if ic.IsVisible("span2", seq1) {
		t.Error("span2 should NOT be visible at seq1 (committed later)")
	}
	if ic.IsVisible("span3", seq1) {
		t.Error("span3 should NOT be visible at seq1 (committed later)")
	}

	// At seq2: span1 and span2 visible
	if !ic.IsVisible("span1", seq2) {
		t.Error("span1 should be visible at seq2")
	}
	if !ic.IsVisible("span2", seq2) {
		t.Error("span2 should be visible at seq2")
	}
	if ic.IsVisible("span3", seq2) {
		t.Error("span3 should NOT be visible at seq2 (committed later)")
	}

	// At seq3: all spans visible
	if !ic.IsVisible("span1", seq3) {
		t.Error("span1 should be visible at seq3")
	}
	if !ic.IsVisible("span2", seq3) {
		t.Error("span2 should be visible at seq3")
	}
	if !ic.IsVisible("span3", seq3) {
		t.Error("span3 should be visible at seq3")
	}

	// Non-existent span
	if ic.IsVisible("nonexistent", seq3) {
		t.Error("nonexistent span should not be visible")
	}
}

// TestMVCCSnapshotIsolation_ConcurrentQueries tests concurrent queries with snapshot isolation
func TestMVCCSnapshotIsolation_ConcurrentQueries(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Register initial commit
	ic.AcquireAppendLock()
	seq1 := ic.AssignCommitSequence(100)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(100, seq1, []string{"span1", "span2"})

	// Query 1 captures snapshot
	query1Snapshot := ic.BeginQuery()

	// New commit happens
	ic.AcquireAppendLock()
	seq2 := ic.AssignCommitSequence(101)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(101, seq2, []string{"span3", "span4"})

	// Query 2 captures snapshot AFTER new commit
	query2Snapshot := ic.BeginQuery()

	// Query 1 should only see span1, span2
	if !ic.IsVisible("span1", query1Snapshot) {
		t.Error("Query 1: span1 should be visible")
	}
	if !ic.IsVisible("span2", query1Snapshot) {
		t.Error("Query 1: span2 should be visible")
	}
	if ic.IsVisible("span3", query1Snapshot) {
		t.Error("Query 1: span3 should NOT be visible (committed after snapshot)")
	}
	if ic.IsVisible("span4", query1Snapshot) {
		t.Error("Query 1: span4 should NOT be visible (committed after snapshot)")
	}

	// Query 2 should see all spans
	if !ic.IsVisible("span1", query2Snapshot) {
		t.Error("Query 2: span1 should be visible")
	}
	if !ic.IsVisible("span2", query2Snapshot) {
		t.Error("Query 2: span2 should be visible")
	}
	if !ic.IsVisible("span3", query2Snapshot) {
		t.Error("Query 2: span3 should be visible")
	}
	if !ic.IsVisible("span4", query2Snapshot) {
		t.Error("Query 2: span4 should be visible")
	}
}

// TestMVCCSnapshotIsolation_IntegrationWithAppender tests MVCC integration with actual appender
func TestMVCCSnapshotIsolation_IntegrationWithAppender(t *testing.T) {
	// Create isolation coordinator
	ic := NewIsolationCoordinator()

	// Create storage
	storage := NewArrowStorage()
	defer storage.Release()

	linkStorage := NewArrowLinkStorage()
	defer linkStorage.Release()

	// Create mock WAL
	wal := newMockWAL()

	// Configure transaction dependencies
	storage.SetTransactionDependencies(ic, linkStorage, wal)

	// Create first transaction and commit
	appender1 := storage.BeginTransaction()
	now := time.Now()
	span1 := &span.Span{
		TraceID:     "00000000000000010000000000000000",
		SpanID:      "0000000000000001",
		Name:        "test1",
		StartTime:   now,
		EndTime:     now.Add(time.Millisecond),
		Duration:    1_000_000,
		ServiceName: "svc1",
	}
	appender1.AddSpan(span1)
	if err := appender1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Query captures snapshot (should see span1)
	snapshotSeq := ic.BeginQuery()

	// Verify span1 is visible
	if !ic.IsVisible(span1.SpanID, snapshotSeq) {
		t.Error("span1 should be visible at snapshot")
	}

	// Create second transaction AFTER snapshot was captured
	appender2 := storage.BeginTransaction()
	span2 := &span.Span{
		TraceID:     "00000000000000010000000000000000",
		SpanID:      "0000000000000002",
		Name:        "test2",
		StartTime:   now,
		EndTime:     now.Add(time.Millisecond),
		Duration:    1_000_000,
		ServiceName: "svc2",
	}
	appender2.AddSpan(span2)
	if err := appender2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	// span2 should NOT be visible at the original snapshot
	if ic.IsVisible(span2.SpanID, snapshotSeq) {
		t.Error("span2 should NOT be visible at snapshot (committed after snapshot)")
	}

	// But span2 should be visible at a NEW snapshot
	newSnapshotSeq := ic.BeginQuery()
	if !ic.IsVisible(span2.SpanID, newSnapshotSeq) {
		t.Error("span2 should be visible at new snapshot")
	}
}

// TestMVCCSnapshotIsolation_EmptySnapshot tests snapshot behavior with no commits
func TestMVCCSnapshotIsolation_EmptySnapshot(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Capture snapshot before any commits
	snapshotSeq := ic.BeginQuery()
	if snapshotSeq != 0 {
		t.Errorf("Expected snapshot seq 0 for empty MVCC, got %d", snapshotSeq)
	}

	// No spans should be visible
	if ic.IsVisible("anyspan", snapshotSeq) {
		t.Error("No spans should be visible at empty snapshot")
	}

	// Register a commit
	ic.AcquireAppendLock()
	seq1 := ic.AssignCommitSequence(100)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(100, seq1, []string{"span1"})

	// Span should NOT be visible at the old snapshot (captured before commit)
	if ic.IsVisible("span1", snapshotSeq) {
		t.Error("span1 should NOT be visible at empty snapshot (committed after snapshot)")
	}

	// Span should be visible at a new snapshot
	newSnapshotSeq := ic.BeginQuery()
	if !ic.IsVisible("span1", newSnapshotSeq) {
		t.Error("span1 should be visible at new snapshot")
	}
}
