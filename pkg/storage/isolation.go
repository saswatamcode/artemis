package storage

import (
	"sync"
	"sync/atomic"

	"github.com/saswatamcode/artemis/pkg/span"
)

// IsolationCoordinator manages transaction isolation for concurrent ingestion.
// It provides MVCC-style snapshot isolation by tracking which spans belong to which transactions.
type IsolationCoordinator struct {
	// Monotonically increasing transaction ID
	nextTxnID atomic.Uint64

	// Commit sequence number (protected by appendMu)
	// Establishes actual commit order for MVCC
	nextCommitSeq uint64

	// Append mutex - ensures only one append operation at a time
	appendMu sync.Mutex

	// MVCC map: txnID -> span IDs in that transaction
	// Used for snapshot isolation during queries
	mvccMu        sync.RWMutex
	mvccMap       map[uint64][]string
	commitSeqMap  map[uint64]uint64 // txnID -> commit sequence number
	committedSeqs []uint64          // Ordered list of committed sequences
	spanIDToTxn   map[string]uint64 // Reverse index: spanID -> txnID (for lock-free visibility checks)

	// Buffer pools for efficient memory reuse
	spanBufferPool sync.Pool
	linkBufferPool sync.Pool

	// Statistics
	totalTransactions atomic.Uint64
	totalCommits      atomic.Uint64
	totalRollbacks    atomic.Uint64
}

// NewIsolationCoordinator creates a new isolation coordinator.
func NewIsolationCoordinator() *IsolationCoordinator {
	ic := &IsolationCoordinator{
		mvccMap:       make(map[uint64][]string),
		commitSeqMap:  make(map[uint64]uint64),
		committedSeqs: make([]uint64, 0, 1024),
		spanIDToTxn:   make(map[string]uint64),
	}

	// Initialize buffer pools with pre-allocated slices
	ic.spanBufferPool = sync.Pool{
		New: func() interface{} {
			// Pre-allocate buffer for typical OTLP batch size (100-1000 spans)
			spans := make([]*span.Span, 0, 1000)
			return &spans
		},
	}

	ic.linkBufferPool = sync.Pool{
		New: func() interface{} {
			links := make([]*span.SpanLink, 0, 100)
			return &links
		},
	}

	return ic
}

// NewTransaction allocates a new transaction ID.
// Transaction IDs are monotonically increasing and unique.
func (ic *IsolationCoordinator) NewTransaction() uint64 {
	txnID := ic.nextTxnID.Add(1)
	ic.totalTransactions.Add(1)
	return txnID
}

// GetSpanBuffer returns a span buffer from the pool.
func (ic *IsolationCoordinator) GetSpanBuffer() *[]*span.Span {
	return ic.spanBufferPool.Get().(*[]*span.Span)
}

// ReleaseSpanBuffer releases a span buffer back to the pool.
func (ic *IsolationCoordinator) ReleaseSpanBuffer(buf *[]*span.Span) {
	// Clear the buffer to avoid memory leaks
	*buf = (*buf)[:0]
	ic.spanBufferPool.Put(buf)
}

// GetLinkBuffer returns a link buffer from the pool.
func (ic *IsolationCoordinator) GetLinkBuffer() *[]*span.SpanLink {
	return ic.linkBufferPool.Get().(*[]*span.SpanLink)
}

// ReleaseLinkBuffer releases a link buffer back to the pool.
func (ic *IsolationCoordinator) ReleaseLinkBuffer(buf *[]*span.SpanLink) {
	*buf = (*buf)[:0]
	ic.linkBufferPool.Put(buf)
}

// AcquireAppendLock acquires the global append lock.
// This ensures serialized append operations to Arrow storage.
func (ic *IsolationCoordinator) AcquireAppendLock() {
	ic.appendMu.Lock()
}

// ReleaseAppendLock releases the global append lock.
func (ic *IsolationCoordinator) ReleaseAppendLock() {
	ic.appendMu.Unlock()
}

// AssignCommitSequence assigns a commit sequence number to a transaction.
// MUST be called while holding appendMu to establish commit order.
// Returns the assigned commit sequence number.
func (ic *IsolationCoordinator) AssignCommitSequence(txnID uint64) uint64 {
	// appendMu MUST be held by caller
	ic.nextCommitSeq++
	commitSeq := ic.nextCommitSeq

	// Store the mapping under mvccMu
	ic.mvccMu.Lock()
	ic.commitSeqMap[txnID] = commitSeq
	ic.mvccMu.Unlock()

	return commitSeq
}

// RegisterCommit registers a committed transaction with its span IDs.
// This is used for MVCC snapshot isolation during queries.
// The commitSeq parameter establishes the actual commit order.
func (ic *IsolationCoordinator) RegisterCommit(txnID uint64, commitSeq uint64, spanIDs []string) {
	ic.mvccMu.Lock()
	defer ic.mvccMu.Unlock()

	// Store span IDs for this transaction
	ic.mvccMap[txnID] = spanIDs

	// Build reverse index for lock-free visibility checks
	for _, spanID := range spanIDs {
		ic.spanIDToTxn[spanID] = txnID
	}

	// Insert commit sequence in order (binary search for insertion point)
	insertIdx := len(ic.committedSeqs)
	for i := len(ic.committedSeqs) - 1; i >= 0; i-- {
		if ic.committedSeqs[i] < commitSeq {
			insertIdx = i + 1
			break
		}
		if i == 0 {
			insertIdx = 0
		}
	}

	// Insert at the correct position
	ic.committedSeqs = append(ic.committedSeqs, 0)
	copy(ic.committedSeqs[insertIdx+1:], ic.committedSeqs[insertIdx:])
	ic.committedSeqs[insertIdx] = commitSeq

	ic.totalCommits.Add(1)

	// Cleanup old transactions if map grows too large
	// Keep last 10,000 transactions for snapshot isolation
	if len(ic.committedSeqs) > 10000 {
		oldSeq := ic.committedSeqs[0]
		// Find txnID with this sequence
		for txnID, seq := range ic.commitSeqMap {
			if seq == oldSeq {
				// Remove from reverse index
				if spanIDs, ok := ic.mvccMap[txnID]; ok {
					for _, spanID := range spanIDs {
						delete(ic.spanIDToTxn, spanID)
					}
				}
				delete(ic.mvccMap, txnID)
				delete(ic.commitSeqMap, txnID)
				break
			}
		}
		ic.committedSeqs = ic.committedSeqs[1:]
	}
}

// RegisterRollback registers a rolled back transaction.
func (ic *IsolationCoordinator) RegisterRollback(txnID uint64) {
	ic.totalRollbacks.Add(1)
	// No need to add to MVCC map since transaction was aborted
}

// BeginQuery returns a snapshot commit sequence for lock-free querying.
// This is the Prometheus-style approach: capture snapshot, release locks, read lock-free.
//
// Usage:
//
//	snapshotSeq := isolation.BeginQuery()
//	// Query executes without holding locks
//	// Check span visibility with IsVisible(spanID, snapshotSeq)
//
// This is a lightweight operation that just captures the current commit sequence.
// Queries then read data without holding locks by checking span visibility.
func (ic *IsolationCoordinator) BeginQuery() uint64 {
	ic.mvccMu.RLock()
	defer ic.mvccMu.RUnlock()

	if len(ic.committedSeqs) == 0 {
		return 0
	}
	return ic.committedSeqs[len(ic.committedSeqs)-1]
}

// IsVisible returns true if a span is visible at the given snapshot commit sequence.
// This is a lock-free read operation for MVCC snapshot isolation.
//
// Returns true if:
//   - Span is not in MVCC map (written directly via AddSpan, always visible)
//   - Span's transaction was committed at or before the snapshot
//
// Returns false if:
//   - Span's transaction was not committed
//   - Span's transaction was committed after the snapshot
func (ic *IsolationCoordinator) IsVisible(spanID string, snapshotSeq uint64) bool {
	ic.mvccMu.RLock()
	defer ic.mvccMu.RUnlock()

	// Look up transaction for this span
	txnID, ok := ic.spanIDToTxn[spanID]
	if !ok {
		// Span not in MVCC map - written directly via AddSpan (non-transactional)
		// Such spans are always visible for backwards compatibility
		return true
	}

	// Look up commit sequence for this transaction
	commitSeq, ok := ic.commitSeqMap[txnID]
	if !ok {
		return false // Transaction not committed
	}

	// Span is visible if it was committed at or before the snapshot
	return commitSeq <= snapshotSeq
}

// GetSnapshot returns all span IDs visible to a transaction at given commit sequence.
// This returns all transactions committed before or at the given sequence number.
// This is used by query layer for snapshot isolation.
func (ic *IsolationCoordinator) GetSnapshot(snapshotCommitSeq uint64) []string {
	ic.mvccMu.RLock()
	defer ic.mvccMu.RUnlock()

	var visibleSpanIDs []string

	// Collect all span IDs from transactions with commit sequence <= snapshot
	for _, seq := range ic.committedSeqs {
		if seq <= snapshotCommitSeq {
			// Find txnID for this sequence
			for txnID, commitSeq := range ic.commitSeqMap {
				if commitSeq == seq {
					visibleSpanIDs = append(visibleSpanIDs, ic.mvccMap[txnID]...)
					break
				}
			}
		}
	}

	return visibleSpanIDs
}

// GetLatestSnapshot returns all span IDs from all committed transactions.
func (ic *IsolationCoordinator) GetLatestSnapshot() (uint64, []string) {
	ic.mvccMu.RLock()
	defer ic.mvccMu.RUnlock()

	var visibleSpanIDs []string
	var latestCommitSeq uint64

	if len(ic.committedSeqs) > 0 {
		latestCommitSeq = ic.committedSeqs[len(ic.committedSeqs)-1]
	}

	// Collect all span IDs from all committed transactions
	for _, seq := range ic.committedSeqs {
		// Find txnID for this sequence
		for txnID, commitSeq := range ic.commitSeqMap {
			if commitSeq == seq {
				visibleSpanIDs = append(visibleSpanIDs, ic.mvccMap[txnID]...)
				break
			}
		}
	}

	return latestCommitSeq, visibleSpanIDs
}

// Stats returns statistics about the isolation coordinator.
func (ic *IsolationCoordinator) Stats() IsolationStats {
	ic.mvccMu.RLock()
	defer ic.mvccMu.RUnlock()

	return IsolationStats{
		TotalTransactions:    ic.totalTransactions.Load(),
		TotalCommits:         ic.totalCommits.Load(),
		TotalRollbacks:       ic.totalRollbacks.Load(),
		ActiveTransactions:   uint64(len(ic.mvccMap)),
		CurrentTransactionID: ic.nextTxnID.Load(),
	}
}

// IsolationStats contains statistics about the isolation coordinator.
type IsolationStats struct {
	TotalTransactions    uint64
	TotalCommits         uint64
	TotalRollbacks       uint64
	ActiveTransactions   uint64
	CurrentTransactionID uint64
}
