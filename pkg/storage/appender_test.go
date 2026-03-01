package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/saswatamcode/artemis/pkg/span"
)

// mockWAL implements the WAL interface for testing
type mockWAL struct {
	mu            sync.Mutex
	spans         []*span.Span
	links         []*span.SpanLink
	currentSeg    int
	failSpanWrite bool
	failLinkWrite bool
}

func newMockWAL() *mockWAL {
	return &mockWAL{
		spans:      make([]*span.Span, 0),
		links:      make([]*span.SpanLink, 0),
		currentSeg: 1,
	}
}

func (m *mockWAL) WriteSpan(s *span.Span) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failSpanWrite {
		return 0, fmt.Errorf("mock WAL span write failure")
	}

	m.spans = append(m.spans, s)
	return m.currentSeg, nil
}

func (m *mockWAL) WriteLink(link *span.SpanLink) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failLinkWrite {
		return 0, fmt.Errorf("mock WAL link write failure")
	}

	m.links = append(m.links, link)
	return m.currentSeg, nil
}

func (m *mockWAL) spanCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.spans)
}

func (m *mockWAL) linkCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.links)
}

func setupAppenderTest() (*IsolationCoordinator, *ArrowStorage, *ArrowLinkStorage, *mockWAL) {
	isolation := NewIsolationCoordinator()
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	wal := newMockWAL()

	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	return isolation, storage, linkStorage, wal
}

func TestArrowAppender_BasicCommit(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	// Create appender
	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add spans
	span1 := &span.Span{SpanID: "span1", Name: "test1"}
	span2 := &span.Span{SpanID: "span2", Name: "test2"}

	if err := appender.AddSpan(span1); err != nil {
		t.Fatalf("Failed to add span1: %v", err)
	}
	if err := appender.AddSpan(span2); err != nil {
		t.Fatalf("Failed to add span2: %v", err)
	}

	// Verify span count before commit
	if appender.SpanCount() != 2 {
		t.Errorf("Expected 2 spans in appender, got %d", appender.SpanCount())
	}

	// Commit
	if err := appender.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify WAL received spans
	if wal.spanCount() != 2 {
		t.Errorf("Expected 2 spans in WAL, got %d", wal.spanCount())
	}

	// Verify Arrow storage received spans
	if storage.RowCount() != 2 {
		t.Errorf("Expected 2 spans in storage, got %d", storage.RowCount())
	}

	// Verify MVCC registration
	stats := isolation.Stats()
	if stats.TotalCommits != 1 {
		t.Errorf("Expected 1 commit, got %d", stats.TotalCommits)
	}
	if stats.TotalRollbacks != 0 {
		t.Errorf("Expected 0 rollbacks, got %d", stats.TotalRollbacks)
	}
}

func TestArrowAppender_CommitWithLinks(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add span
	span1 := &span.Span{SpanID: "span1", Name: "test"}
	if err := appender.AddSpan(span1); err != nil {
		t.Fatalf("Failed to add span: %v", err)
	}

	// Add links
	link1 := &span.SpanLink{SpanID: "span1", LinkedSpanID: "linked1"}
	link2 := &span.SpanLink{SpanID: "span1", LinkedSpanID: "linked2"}
	if err := appender.AddLink(link1); err != nil {
		t.Fatalf("Failed to add link1: %v", err)
	}
	if err := appender.AddLink(link2); err != nil {
		t.Fatalf("Failed to add link2: %v", err)
	}

	// Commit
	if err := appender.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify WAL received links
	if wal.linkCount() != 2 {
		t.Errorf("Expected 2 links in WAL, got %d", wal.linkCount())
	}

	// Verify link storage received links
	if linkStorage.RowCount() != 2 {
		t.Errorf("Expected 2 links in link storage, got %d", linkStorage.RowCount())
	}
}

func TestArrowAppender_Rollback(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add spans
	span1 := &span.Span{SpanID: "span1", Name: "test"}
	if err := appender.AddSpan(span1); err != nil {
		t.Fatalf("Failed to add span: %v", err)
	}

	// Rollback
	if err := appender.Rollback(); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify WAL has no spans
	if wal.spanCount() != 0 {
		t.Errorf("Expected 0 spans in WAL after rollback, got %d", wal.spanCount())
	}

	// Verify storage has no spans
	if storage.RowCount() != 0 {
		t.Errorf("Expected 0 spans in storage after rollback, got %d", storage.RowCount())
	}

	// Verify MVCC registration
	stats := isolation.Stats()
	if stats.TotalCommits != 0 {
		t.Errorf("Expected 0 commits, got %d", stats.TotalCommits)
	}
	if stats.TotalRollbacks != 1 {
		t.Errorf("Expected 1 rollback, got %d", stats.TotalRollbacks)
	}
}

func TestArrowAppender_WALWriteFailure(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add span
	span1 := &span.Span{SpanID: "span1", Name: "test"}
	if err := appender.AddSpan(span1); err != nil {
		t.Fatalf("Failed to add span: %v", err)
	}

	// Make WAL fail
	wal.failSpanWrite = true

	// Commit should fail
	if err := appender.Commit(); err == nil {
		t.Fatal("Expected commit to fail due to WAL failure")
	}

	// Verify storage has no spans (transaction rolled back)
	if storage.RowCount() != 0 {
		t.Errorf("Expected 0 spans in storage after failed commit, got %d", storage.RowCount())
	}

	// Verify MVCC shows rollback
	stats := isolation.Stats()
	if stats.TotalCommits != 0 {
		t.Errorf("Expected 0 commits after WAL failure, got %d", stats.TotalCommits)
	}
	if stats.TotalRollbacks != 1 {
		t.Errorf("Expected 1 rollback after WAL failure, got %d", stats.TotalRollbacks)
	}
}

func TestArrowAppender_DoubleCommit(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add and commit
	span1 := &span.Span{SpanID: "span1", Name: "test"}
	appender.AddSpan(span1)
	if err := appender.Commit(); err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	// Try to commit again
	if err := appender.Commit(); err == nil {
		t.Fatal("Expected second commit to fail")
	}
}

func TestArrowAppender_DoubleRollback(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add and rollback
	span1 := &span.Span{SpanID: "span1", Name: "test"}
	appender.AddSpan(span1)
	if err := appender.Rollback(); err != nil {
		t.Fatalf("First rollback failed: %v", err)
	}

	// Try to rollback again
	if err := appender.Rollback(); err == nil {
		t.Fatal("Expected second rollback to fail")
	}
}

func TestArrowAppender_CommitAfterRollback(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add and rollback
	span1 := &span.Span{SpanID: "span1", Name: "test"}
	appender.AddSpan(span1)
	if err := appender.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Try to commit after rollback
	if err := appender.Commit(); err == nil {
		t.Fatal("Expected commit after rollback to fail")
	}
}

func TestArrowAppender_AddAfterCommit(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add and commit
	span1 := &span.Span{SpanID: "span1", Name: "test"}
	appender.AddSpan(span1)
	if err := appender.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Try to add after commit
	span2 := &span.Span{SpanID: "span2", Name: "test"}
	if err := appender.AddSpan(span2); err == nil {
		t.Fatal("Expected AddSpan after commit to fail")
	}
}

func TestArrowAppender_ConcurrentTransactions(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	const numTransactions = 100
	const spansPerTxn = 10

	var wg sync.WaitGroup

	// Run concurrent transactions
	for i := 0; i < numTransactions; i++ {
		wg.Add(1)
		go func(txnID int) {
			defer wg.Done()

			appender := NewArrowAppender(isolation, storage, linkStorage, wal)

			// Add spans
			for j := 0; j < spansPerTxn; j++ {
				s := &span.Span{
					SpanID: fmt.Sprintf("span-%d-%d", txnID, j),
					Name:   "test",
				}
				if err := appender.AddSpan(s); err != nil {
					t.Errorf("Failed to add span: %v", err)
					return
				}
			}

			// Commit
			if err := appender.Commit(); err != nil {
				t.Errorf("Failed to commit txn %d: %v", txnID, err)
				return
			}
		}(i)
	}

	wg.Wait()

	// Verify all spans were written
	expectedSpans := numTransactions * spansPerTxn
	if wal.spanCount() != expectedSpans {
		t.Errorf("Expected %d spans in WAL, got %d", expectedSpans, wal.spanCount())
	}
	if storage.RowCount() != int64(expectedSpans) {
		t.Errorf("Expected %d spans in storage, got %d", expectedSpans, storage.RowCount())
	}

	// Verify MVCC
	stats := isolation.Stats()
	if stats.TotalCommits != uint64(numTransactions) {
		t.Errorf("Expected %d commits, got %d", numTransactions, stats.TotalCommits)
	}
}

func TestArrowAppender_EmptyCommit(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Commit without adding anything
	if err := appender.Commit(); err != nil {
		t.Fatalf("Empty commit should succeed: %v", err)
	}

	// Verify nothing was written
	if wal.spanCount() != 0 {
		t.Errorf("Expected 0 spans in WAL, got %d", wal.spanCount())
	}
	if storage.RowCount() != 0 {
		t.Errorf("Expected 0 spans in storage, got %d", storage.RowCount())
	}

	// But commit should be registered
	stats := isolation.Stats()
	if stats.TotalCommits != 1 {
		t.Errorf("Expected 1 commit (even if empty), got %d", stats.TotalCommits)
	}
}

func TestArrowAppender_TxnID(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	appender1 := NewArrowAppender(isolation, storage, linkStorage, wal)
	appender2 := NewArrowAppender(isolation, storage, linkStorage, wal)
	appender3 := NewArrowAppender(isolation, storage, linkStorage, wal)

	txn1 := appender1.TxnID()
	txn2 := appender2.TxnID()
	txn3 := appender3.TxnID()

	// Transaction IDs should be unique and monotonically increasing
	if txn2 <= txn1 {
		t.Errorf("Expected txn2 (%d) > txn1 (%d)", txn2, txn1)
	}
	if txn3 <= txn2 {
		t.Errorf("Expected txn3 (%d) > txn2 (%d)", txn3, txn2)
	}

	// Clean up (rollback uncommitted transactions)
	appender1.Rollback()
	appender2.Rollback()
	appender3.Rollback()
}

func TestArrowAppender_BufferReuse(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	// Create and commit first transaction
	appender1 := NewArrowAppender(isolation, storage, linkStorage, wal)
	span1 := &span.Span{SpanID: "span1", Name: "test"}
	appender1.AddSpan(span1)
	if err := appender1.Commit(); err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	// Create second transaction (should reuse buffers from pool)
	appender2 := NewArrowAppender(isolation, storage, linkStorage, wal)
	span2 := &span.Span{SpanID: "span2", Name: "test"}
	appender2.AddSpan(span2)

	// Buffer should be empty initially (reused and cleared)
	if appender2.SpanCount() != 1 {
		t.Errorf("Expected 1 span in appender2 buffer, got %d", appender2.SpanCount())
	}

	if err := appender2.Commit(); err != nil {
		t.Fatalf("Second commit failed: %v", err)
	}
}

func TestArrowAppender_LargeTransaction(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add 10,000 spans
	const numSpans = 10000
	for i := 0; i < numSpans; i++ {
		s := &span.Span{
			SpanID: fmt.Sprintf("span-%d", i),
			Name:   "test",
		}
		if err := appender.AddSpan(s); err != nil {
			t.Fatalf("Failed to add span %d: %v", i, err)
		}
	}

	// Commit
	if err := appender.Commit(); err != nil {
		t.Fatalf("Failed to commit large transaction: %v", err)
	}

	// Verify all spans were written
	if wal.spanCount() != numSpans {
		t.Errorf("Expected %d spans in WAL, got %d", numSpans, wal.spanCount())
	}
	if storage.RowCount() != numSpans {
		t.Errorf("Expected %d spans in storage, got %d", numSpans, storage.RowCount())
	}
}

func TestArrowAppender_FactoryMethod(t *testing.T) {
	isolation, storage, linkStorage, wal := setupAppenderTest()

	// Set dependencies on storage
	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	// Create appender via factory method
	appender := storage.BeginTransaction()

	if appender == nil {
		t.Fatal("Expected non-nil appender from BeginTransaction()")
	}

	// Add and commit
	span1 := &span.Span{SpanID: "span1", Name: "test"}
	if err := appender.AddSpan(span1); err != nil {
		t.Fatalf("Failed to add span: %v", err)
	}

	if err := appender.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify
	if storage.RowCount() != 1 {
		t.Errorf("Expected 1 span in storage, got %d", storage.RowCount())
	}
}
