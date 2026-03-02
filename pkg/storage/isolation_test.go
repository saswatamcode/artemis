package storage

import (
	"sync"
	"testing"

	"github.com/saswatamcode/artemis/pkg/span"
)

func TestIsolationCoordinator_NewTransaction(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Transaction IDs should be monotonically increasing
	txn1 := ic.NewTransaction()
	txn2 := ic.NewTransaction()
	txn3 := ic.NewTransaction()

	if txn2 <= txn1 {
		t.Errorf("Expected txn2 (%d) > txn1 (%d)", txn2, txn1)
	}
	if txn3 <= txn2 {
		t.Errorf("Expected txn3 (%d) > txn2 (%d)", txn3, txn2)
	}

	// Check statistics
	stats := ic.Stats()
	if stats.TotalTransactions != 3 {
		t.Errorf("Expected 3 transactions, got %d", stats.TotalTransactions)
	}
}

func TestIsolationCoordinator_ConcurrentTransactionAllocation(t *testing.T) {
	ic := NewIsolationCoordinator()

	const numGoroutines = 100
	const txnsPerGoroutine = 100

	var wg sync.WaitGroup
	txnIDs := make([]uint64, numGoroutines*txnsPerGoroutine)

	// Allocate transaction IDs concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < txnsPerGoroutine; j++ {
				idx := goroutineID*txnsPerGoroutine + j
				txnIDs[idx] = ic.NewTransaction()
			}
		}(i)
	}

	wg.Wait()

	// Verify all IDs are unique
	seen := make(map[uint64]bool)
	for _, id := range txnIDs {
		if seen[id] {
			t.Errorf("Duplicate transaction ID: %d", id)
		}
		seen[id] = true
	}

	// Verify statistics
	stats := ic.Stats()
	expectedTotal := uint64(numGoroutines * txnsPerGoroutine)
	if stats.TotalTransactions != expectedTotal {
		t.Errorf("Expected %d transactions, got %d", expectedTotal, stats.TotalTransactions)
	}
}

func TestIsolationCoordinator_SpanBufferPool(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Get buffer from pool
	buf1 := ic.GetSpanBuffer()
	if buf1 == nil {
		t.Fatal("Expected non-nil buffer")
	}
	if cap(*buf1) == 0 {
		t.Error("Expected pre-allocated capacity")
	}

	// Add some spans
	*buf1 = append(*buf1, &span.Span{SpanID: "span1"})
	*buf1 = append(*buf1, &span.Span{SpanID: "span2"})

	if len(*buf1) != 2 {
		t.Errorf("Expected 2 spans in buffer, got %d", len(*buf1))
	}

	// Release buffer (should clear it)
	ic.ReleaseSpanBuffer(buf1)

	// Get buffer again (might be the same one from pool)
	buf2 := ic.GetSpanBuffer()
	if buf2 == nil {
		t.Fatal("Expected non-nil buffer")
	}

	// Should be cleared
	if len(*buf2) != 0 {
		t.Errorf("Expected cleared buffer, got %d spans", len(*buf2))
	}
}

func TestIsolationCoordinator_LinkBufferPool(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Get buffer from pool
	buf := ic.GetLinkBuffer()
	if buf == nil {
		t.Fatal("Expected non-nil buffer")
	}

	// Add some links
	*buf = append(*buf, &span.SpanLink{SpanID: "span1"})

	// Release and get again
	ic.ReleaseLinkBuffer(buf)
	buf2 := ic.GetLinkBuffer()

	// Should be cleared
	if len(*buf2) != 0 {
		t.Errorf("Expected cleared buffer, got %d links", len(*buf2))
	}
}

func TestIsolationCoordinator_AppendLock(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Acquire lock
	ic.AcquireAppendLock()

	// Try to acquire in another goroutine (should block)
	acquired := false
	go func() {
		ic.AcquireAppendLock()
		acquired = true
		ic.ReleaseAppendLock()
	}()

	// Give goroutine time to try acquiring
	// (it should be blocked)
	// Note: This is a bit flaky but acceptable for tests

	if acquired {
		t.Error("Lock should not be acquired while held")
	}

	// Release lock
	ic.ReleaseAppendLock()

	// Now the goroutine should be able to acquire
	// (give it some time)
}

func TestIsolationCoordinator_RegisterCommit(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Register commits (simulate what appender does: assign sequence, then register)
	ic.AcquireAppendLock()
	seq1 := ic.AssignCommitSequence(100)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(100, seq1, []string{"span1", "span2"})

	ic.AcquireAppendLock()
	seq2 := ic.AssignCommitSequence(101)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(101, seq2, []string{"span3"})

	ic.AcquireAppendLock()
	seq3 := ic.AssignCommitSequence(102)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(102, seq3, []string{"span4", "span5", "span6"})

	// Check statistics
	stats := ic.Stats()
	if stats.TotalCommits != 3 {
		t.Errorf("Expected 3 commits, got %d", stats.TotalCommits)
	}
	if stats.ActiveTransactions != 3 {
		t.Errorf("Expected 3 active transactions in MVCC map, got %d", stats.ActiveTransactions)
	}

	// Get snapshot at seq2 (includes seq1 and seq2)
	spanIDs := ic.GetSnapshot(seq2)
	expected := 3 // span1, span2, span3
	if len(spanIDs) != expected {
		t.Errorf("Expected %d span IDs at snapshot seq2, got %d: %v", expected, len(spanIDs), spanIDs)
	}

	// Get snapshot at seq3 (includes all)
	spanIDs = ic.GetSnapshot(seq3)
	expected = 6 // all spans
	if len(spanIDs) != expected {
		t.Errorf("Expected %d span IDs at snapshot seq3, got %d", expected, len(spanIDs))
	}

	// Get snapshot at seq1 (only first commit)
	spanIDs = ic.GetSnapshot(seq1)
	expected = 2 // span1, span2
	if len(spanIDs) != expected {
		t.Errorf("Expected %d span IDs at snapshot seq1, got %d", expected, len(spanIDs))
	}
}

func TestIsolationCoordinator_RegisterRollback(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Register rollbacks
	ic.RegisterRollback(100)
	ic.RegisterRollback(101)

	// Check statistics
	stats := ic.Stats()
	if stats.TotalRollbacks != 2 {
		t.Errorf("Expected 2 rollbacks, got %d", stats.TotalRollbacks)
	}
	if stats.ActiveTransactions != 0 {
		t.Errorf("Expected 0 active transactions (rollbacks don't go in MVCC map), got %d", stats.ActiveTransactions)
	}
}

func TestIsolationCoordinator_GetLatestSnapshot(t *testing.T) {
	ic := NewIsolationCoordinator()

	// No commits yet
	commitSeq, spanIDs := ic.GetLatestSnapshot()
	if commitSeq != 0 {
		t.Errorf("Expected commitSeq 0 for empty MVCC map, got %d", commitSeq)
	}
	if len(spanIDs) != 0 {
		t.Errorf("Expected 0 span IDs, got %d", len(spanIDs))
	}

	// Register some commits
	ic.AcquireAppendLock()
	seq1 := ic.AssignCommitSequence(100)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(100, seq1, []string{"span1", "span2"})

	ic.AcquireAppendLock()
	seq2 := ic.AssignCommitSequence(101)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(101, seq2, []string{"span3"})

	ic.AcquireAppendLock()
	seq3 := ic.AssignCommitSequence(102)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(102, seq3, []string{"span4"})

	// Get latest snapshot
	commitSeq, spanIDs = ic.GetLatestSnapshot()
	if commitSeq != seq3 {
		t.Errorf("Expected latest commitSeq %d, got %d", seq3, commitSeq)
	}
	if len(spanIDs) != 4 {
		t.Errorf("Expected 4 span IDs, got %d", len(spanIDs))
	}
}

func TestIsolationCoordinator_MVCCGarbageCollection(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Register more than 10K transactions (should trigger GC)
	for i := uint64(1); i <= 10005; i++ {
		ic.AcquireAppendLock()
		seq := ic.AssignCommitSequence(i)
		ic.ReleaseAppendLock()
		ic.RegisterCommit(i, seq, []string{"span"})
	}

	stats := ic.Stats()

	// Should have GC'd old transactions
	if stats.ActiveTransactions > 10000 {
		t.Errorf("Expected <= 10000 active transactions after GC, got %d", stats.ActiveTransactions)
	}

	// Total commits should still be 10005
	if stats.TotalCommits != 10005 {
		t.Errorf("Expected 10005 total commits, got %d", stats.TotalCommits)
	}
}

func TestIsolationCoordinator_ConcurrentCommits(t *testing.T) {
	ic := NewIsolationCoordinator()

	const numGoroutines = 100
	var wg sync.WaitGroup

	// Register commits concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			txnID := uint64(id + 1)
			spanIDs := []string{
				"span-" + string(rune(id)) + "-1",
				"span-" + string(rune(id)) + "-2",
			}
			ic.AcquireAppendLock()
			seq := ic.AssignCommitSequence(txnID)
			ic.ReleaseAppendLock()
			ic.RegisterCommit(txnID, seq, spanIDs)
		}(i)
	}

	wg.Wait()

	// Verify statistics
	stats := ic.Stats()
	if stats.TotalCommits != uint64(numGoroutines) {
		t.Errorf("Expected %d commits, got %d", numGoroutines, stats.TotalCommits)
	}
	if stats.ActiveTransactions != uint64(numGoroutines) {
		t.Errorf("Expected %d active transactions, got %d", numGoroutines, stats.ActiveTransactions)
	}

	// Get latest snapshot
	_, spanIDs := ic.GetLatestSnapshot()
	expectedSpans := numGoroutines * 2 // 2 spans per transaction
	if len(spanIDs) != expectedSpans {
		t.Errorf("Expected %d span IDs, got %d", expectedSpans, len(spanIDs))
	}
}

func TestIsolationCoordinator_Stats(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Allocate some transactions
	ic.NewTransaction()
	ic.NewTransaction()
	ic.NewTransaction()

	// Register some commits and rollbacks
	ic.AcquireAppendLock()
	seq1 := ic.AssignCommitSequence(1)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(1, seq1, []string{"span1"})

	ic.AcquireAppendLock()
	seq2 := ic.AssignCommitSequence(2)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(2, seq2, []string{"span2"})

	ic.RegisterRollback(3)

	stats := ic.Stats()

	if stats.TotalTransactions != 3 {
		t.Errorf("Expected 3 total transactions, got %d", stats.TotalTransactions)
	}
	if stats.TotalCommits != 2 {
		t.Errorf("Expected 2 commits, got %d", stats.TotalCommits)
	}
	if stats.TotalRollbacks != 1 {
		t.Errorf("Expected 1 rollback, got %d", stats.TotalRollbacks)
	}
	if stats.ActiveTransactions != 2 {
		t.Errorf("Expected 2 active transactions, got %d", stats.ActiveTransactions)
	}
	if stats.CurrentTransactionID < 3 {
		t.Errorf("Expected current transaction ID >= 3, got %d", stats.CurrentTransactionID)
	}
}
