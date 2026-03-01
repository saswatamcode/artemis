package storage

import (
	"fmt"

	"github.com/saswatamcode/artemis/pkg/span"
)

// Appender provides a transactional interface for ingesting spans.
// Each Appender instance represents a single transaction (typically one OTLP batch).
//
// Usage:
//
//	appender := storage.NewAppender()
//	for _, span := range otlpBatch {
//	    appender.AddSpan(span)
//	}
//	if err := appender.Commit(); err != nil {
//	    appender.Rollback()
//	}
type Appender interface {
	// AddSpan adds a span to the transaction buffer.
	// This is a fast, in-memory operation.
	AddSpan(s *span.Span) error

	// AddLink adds a span link to the transaction buffer.
	AddLink(link *span.SpanLink) error

	// Commit commits the transaction:
	// 1. Writes all data to WAL (durability)
	// 2. Inserts data into Arrow storage (performance)
	// 3. Registers commit with isolation coordinator (MVCC)
	// 4. Releases buffers and locks
	//
	// Returns error if commit fails. On error, caller should call Rollback().
	Commit() error

	// Rollback aborts the transaction:
	// 1. Clears all buffers
	// 2. Releases buffers to pool
	// 3. Releases append lock
	// 4. Registers rollback with isolation coordinator
	Rollback() error

	// TxnID returns the transaction ID for this appender.
	TxnID() uint64

	// SpanCount returns the number of spans in this transaction.
	SpanCount() int
}

// ArrowAppender implements the Appender interface for Arrow in-memory storage.
type ArrowAppender struct {
	// Transaction metadata
	txnID      uint64
	committed  bool
	rolledBack bool

	// Dependencies
	isolation   *IsolationCoordinator
	storage     *ArrowStorage
	linkStorage *ArrowLinkStorage
	wal         WAL

	// Buffers from sync.Pool
	spanBuffer *[]*span.Span
	linkBuffer *[]*span.SpanLink

	// Track WAL segments written to
	walSegments map[int]bool
}

// NewArrowAppender creates a new transactional appender for Arrow storage.
func NewArrowAppender(
	isolation *IsolationCoordinator,
	storage *ArrowStorage,
	linkStorage *ArrowLinkStorage,
	wal WAL,
) *ArrowAppender {
	// Get a unique transaction ID
	txnID := isolation.NewTransaction()

	// Get buffers from pool
	spanBuffer := isolation.GetSpanBuffer()
	linkBuffer := isolation.GetLinkBuffer()

	// NOTE: We do NOT acquire appendMutex here!
	// Lock is acquired in Commit() to allow concurrent transaction preparation

	return &ArrowAppender{
		txnID:       txnID,
		isolation:   isolation,
		storage:     storage,
		linkStorage: linkStorage,
		wal:         wal,
		spanBuffer:  spanBuffer,
		linkBuffer:  linkBuffer,
		walSegments: make(map[int]bool),
	}
}

// AddSpan adds a span to the transaction buffer.
func (a *ArrowAppender) AddSpan(s *span.Span) error {
	if a.committed {
		return fmt.Errorf("cannot add span: transaction already committed")
	}
	if a.rolledBack {
		return fmt.Errorf("cannot add span: transaction already rolled back")
	}

	// Append to in-memory buffer (fast!)
	*a.spanBuffer = append(*a.spanBuffer, s)

	return nil
}

// AddLink adds a span link to the transaction buffer.
func (a *ArrowAppender) AddLink(link *span.SpanLink) error {
	if a.committed {
		return fmt.Errorf("cannot add link: transaction already committed")
	}
	if a.rolledBack {
		return fmt.Errorf("cannot add link: transaction already rolled back")
	}

	*a.linkBuffer = append(*a.linkBuffer, link)

	return nil
}

// Commit commits the transaction using two-phase commit protocol:
// Phase 1: Serialized WAL write (establishes commit order)
// Phase 2: Parallel Arrow update (protected by storage.mu only)
func (a *ArrowAppender) Commit() error {
	if a.committed {
		return fmt.Errorf("transaction already committed")
	}
	if a.rolledBack {
		return fmt.Errorf("transaction already rolled back")
	}

	spans := *a.spanBuffer
	links := *a.linkBuffer

	// PHASE 1 (Serialized): Write to WAL + establish commit order
	// Lock ONLY held for WAL write to serialize commits and establish durability
	a.isolation.AcquireAppendLock()

	walErr := a.writeToWAL(spans, links)

	// Assign commit sequence while holding lock to establish commit order
	commitSeq := a.isolation.AssignCommitSequence(a.txnID)

	// Release lock immediately after WAL write
	// This allows other transactions to commit to WAL while we update Arrow
	a.isolation.ReleaseAppendLock()

	if walErr != nil {
		// WAL write failed - rollback transaction
		a.isolation.RegisterRollback(a.txnID)
		a.releaseResources()
		return fmt.Errorf("WAL write failed: %w", walErr)
	}

	// PHASE 2 (Parallel): Update in-memory Arrow storage
	// At this point, data is durable in WAL, so even if this fails,
	// it will be replayed on restart
	// Multiple transactions can update Arrow concurrently (protected by storage.mu)
	if err := a.updateArrowStorage(spans, links); err != nil {
		// Arrow update failed, but data is safe in WAL
		// Register as committed since WAL has the data
		a.collectSpanIDs(spans, commitSeq)
		a.releaseResources()
		return fmt.Errorf("Arrow update failed (data safe in WAL): %w", err)
	}

	// PHASE 3: Register commit with isolation coordinator (MVCC)
	a.collectSpanIDs(spans, commitSeq)

	a.committed = true
	a.releaseResources()
	return nil
}

// writeToWAL writes all buffered data to WAL.
func (a *ArrowAppender) writeToWAL(
	spans []*span.Span,
	links []*span.SpanLink,
) error {
	// Write spans to WAL
	for _, s := range spans {
		segmentIndex, err := a.wal.WriteSpan(s)
		if err != nil {
			return fmt.Errorf("failed to write span to WAL: %w", err)
		}
		a.walSegments[segmentIndex] = true
	}

	// Write links to WAL
	for _, link := range links {
		segmentIndex, err := a.wal.WriteLink(link)
		if err != nil {
			return fmt.Errorf("failed to write link to WAL: %w", err)
		}
		a.walSegments[segmentIndex] = true
	}

	return nil
}

// updateArrowStorage updates the in-memory Arrow storage.
func (a *ArrowAppender) updateArrowStorage(
	spans []*span.Span,
	links []*span.SpanLink,
) error {
	// Acquire storage lock for span storage
	// Note: We hold appendMutex from isolation coordinator for transaction isolation,
	// but we still need s.mu to protect Arrow storage's internal data structures
	a.storage.mu.Lock()

	// Update WAL segment tracking in Arrow storage
	for seg := range a.walSegments {
		if a.storage.minWALSegment == -1 || seg < a.storage.minWALSegment {
			a.storage.minWALSegment = seg
		}
		if seg > a.storage.maxWALSegment {
			a.storage.maxWALSegment = seg
		}
	}

	// Add spans to Arrow storage (bulk operation, already locked)
	if err := a.storage.addSpansLocked(spans); err != nil {
		a.storage.mu.Unlock()
		return fmt.Errorf("failed to add spans to Arrow storage: %w", err)
	}

	a.storage.mu.Unlock()

	// Add links to link storage (bulk operation)
	if len(links) > 0 {
		if err := a.linkStorage.AddLinks(links); err != nil {
			return fmt.Errorf("failed to add links to Arrow link storage: %w", err)
		}
	}

	return nil
}

// collectSpanIDs collects span IDs from the transaction and registers with MVCC.
func (a *ArrowAppender) collectSpanIDs(spans []*span.Span, commitSeq uint64) {
	spanIDs := make([]string, 0, len(spans))
	for _, s := range spans {
		spanIDs = append(spanIDs, s.SpanID)
	}

	// Register commit with isolation coordinator for MVCC
	a.isolation.RegisterCommit(a.txnID, commitSeq, spanIDs)
}

// Rollback aborts the transaction and releases resources.
func (a *ArrowAppender) Rollback() error {
	if a.committed {
		return fmt.Errorf("cannot rollback: transaction already committed")
	}
	if a.rolledBack {
		return fmt.Errorf("transaction already rolled back")
	}

	defer func() {
		a.releaseResources()
	}()

	// Register rollback with isolation coordinator
	a.isolation.RegisterRollback(a.txnID)

	a.rolledBack = true
	return nil
}

// releaseResources releases buffers back to the isolation coordinator.
// Note: Does NOT release appendMutex - that's handled explicitly in Commit().
func (a *ArrowAppender) releaseResources() {
	// Release buffers to pool
	if a.spanBuffer != nil {
		a.isolation.ReleaseSpanBuffer(a.spanBuffer)
		a.spanBuffer = nil
	}
	if a.linkBuffer != nil {
		a.isolation.ReleaseLinkBuffer(a.linkBuffer)
		a.linkBuffer = nil
	}
}

// TxnID returns the transaction ID.
func (a *ArrowAppender) TxnID() uint64 {
	return a.txnID
}

// SpanCount returns the number of spans in this transaction.
func (a *ArrowAppender) SpanCount() int {
	if a.spanBuffer == nil {
		return 0
	}
	return len(*a.spanBuffer)
}

// Ensure ArrowAppender implements Appender interface
var _ Appender = (*ArrowAppender)(nil)
