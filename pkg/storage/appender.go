package storage

import (
	"context"
	"fmt"

	"github.com/saswatamcode/artemis/pkg/span"
)

// Appender provides a transactional interface for ingesting spans.
// Each Appender instance represents a single transaction (typically one OTLP batch).
//
// Usage (with context support):
//
//	appender := storage.NewAppender()
//	for _, span := range otlpBatch {
//	    appender.AddSpan(span)
//	}
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	if err := appender.CommitContext(ctx); err != nil {
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
	// For timeout/cancellation support, use CommitContext instead.
	Commit() error

	// CommitContext commits the transaction with context support for cancellation/timeout.
	// This is the recommended method for production use.
	//
	// Context is checked at key points:
	// - Before acquiring append lock (allows cancellation before serialization)
	// - During WAL writes (allows cancellation during large batches)
	// - During Arrow storage updates (allows cancellation during parallel updates)
	//
	// If context is cancelled, the transaction is rolled back automatically.
	CommitContext(ctx context.Context) error

	// Rollback aborts the transaction:
	// 1. Clears all buffers
	// 2. Releases buffers to pool
	// 3. Releases append lock
	// 4. Registers rollback with isolation coordinator
	Rollback() error

	// RollbackContext is like Rollback but with context support.
	// Context is checked before acquiring locks.
	RollbackContext(ctx context.Context) error

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

// Commit commits the transaction using two-phase commit protocol.
// For backward compatibility, this delegates to CommitContext with a background context.
// For production use with timeout/cancellation support, use CommitContext directly.
func (a *ArrowAppender) Commit() error {
	return a.CommitContext(context.Background())
}

// CommitContext commits the transaction with context support for cancellation/timeout.
// Uses two-phase commit protocol:
// Phase 1: Serialized WAL write (establishes commit order)
// Phase 2: Parallel Arrow update (protected by storage.mu only)
//
// Context is checked at critical points:
// - Before acquiring append lock (allows cancellation before serialization)
// - During WAL writes (checked periodically in large batches)
// - During Arrow storage updates
func (a *ArrowAppender) CommitContext(ctx context.Context) error {
	if a.committed {
		return fmt.Errorf("transaction already committed")
	}
	if a.rolledBack {
		return fmt.Errorf("transaction already rolled back")
	}

	// Check context before we start any work
	if err := ctx.Err(); err != nil {
		a.isolation.RegisterRollback(a.txnID)
		a.releaseResources()
		return fmt.Errorf("context cancelled before commit: %w", err)
	}

	spans := *a.spanBuffer
	links := *a.linkBuffer

	// PHASE 1 (Serialized): Write to WAL + establish commit order
	// Check context before acquiring lock to allow cancellation before serialization
	select {
	case <-ctx.Done():
		a.isolation.RegisterRollback(a.txnID)
		a.releaseResources()
		return fmt.Errorf("context cancelled before acquiring append lock: %w", ctx.Err())
	default:
	}

	// Lock ONLY held for WAL write to serialize commits and establish durability
	a.isolation.AcquireAppendLock()

	walErr := a.writeToWALContext(ctx, spans, links)

	// Assign commit sequence while holding lock to establish commit order
	commitSeq := a.isolation.AssignCommitSequence(a.txnID)

	// NEW: Mark transaction as in-flight immediately after getting commit sequence
	// This prevents queries from seeing spans before we fully commit
	a.isolation.MarkInFlight(a.txnID, commitSeq)

	// Release lock immediately after WAL write
	// This allows other transactions to commit to WAL while we update Arrow
	a.isolation.ReleaseAppendLock()

	if walErr != nil {
		// WAL write failed - rollback transaction
		a.isolation.RegisterRollback(a.txnID)
		a.releaseResources()
		return fmt.Errorf("WAL write failed: %w", walErr)
	}

	// PHASE 2 (NEW - moved before Arrow update): Register commit with MVCC
	// This must happen BEFORE Arrow update to prevent visibility races
	// Collect span IDs
	spanIDs := make([]string, 0, len(spans))
	for _, s := range spans {
		spanIDs = append(spanIDs, s.SpanID)
	}

	// Check context before registering
	if err := ctx.Err(); err != nil {
		// Data is in WAL but commit cancelled
		// Unregister from MVCC and rollback
		a.isolation.RegisterRollback(a.txnID)
		a.releaseResources()
		return fmt.Errorf("context cancelled before MVCC registration (rolled back): %w", err)
	}

	// Register commit with isolation coordinator (marks spans as visible)
	// This removes the in-flight marker and makes spans visible to queries
	a.isolation.RegisterCommit(a.txnID, commitSeq, spanIDs)

	// PHASE 3 (Parallel): Update in-memory Arrow storage
	// Spans are now visible in MVCC, so we can safely add them to Arrow
	// Check context before Arrow update
	if err := ctx.Err(); err != nil {
		// MVCC is registered, but Arrow update cancelled
		// This is OK - data is in WAL and MVCC is consistent
		// On restart, WAL replay will populate Arrow
		a.releaseResources()
		return fmt.Errorf("context cancelled before Arrow update (data safe in WAL, MVCC committed): %w", err)
	}

	// Multiple transactions can update Arrow concurrently (protected by storage.mu)
	if err := a.updateArrowStorageContext(ctx, spans, links); err != nil {
		// Arrow update failed, but MVCC is already committed
		// This is OK - data is in WAL and MVCC is consistent
		// On restart, WAL replay will populate Arrow
		// Queries will see the spans in MVCC but won't find them in Arrow until WAL replay
		a.releaseResources()
		return fmt.Errorf("Arrow update failed (data safe in WAL, MVCC committed): %w", err)
	}

	a.committed = true
	a.releaseResources()
	return nil
}

// writeToWAL writes all buffered data to WAL.
// For backward compatibility. Use writeToWALContext for context support.
func (a *ArrowAppender) writeToWAL(
	spans []*span.Span,
	links []*span.SpanLink,
) error {
	return a.writeToWALContext(context.Background(), spans, links)
}

// writeToWALContext writes all buffered data to WAL with context support.
// Context is checked periodically during large batches.
func (a *ArrowAppender) writeToWALContext(
	ctx context.Context,
	spans []*span.Span,
	links []*span.SpanLink,
) error {
	// Write spans to WAL
	for i, s := range spans {
		// Check context every 100 spans to allow cancellation during large batches
		if i%100 == 0 {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("context cancelled during WAL write (wrote %d/%d spans): %w", i, len(spans), err)
			}
		}

		segmentIndex, err := a.wal.WriteSpan(s)
		if err != nil {
			return fmt.Errorf("failed to write span to WAL: %w", err)
		}
		a.walSegments[segmentIndex] = true
	}

	// Write links to WAL
	for i, link := range links {
		// Check context every 100 links
		if i%100 == 0 {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("context cancelled during WAL write (wrote %d/%d links): %w", i, len(links), err)
			}
		}

		segmentIndex, err := a.wal.WriteLink(link)
		if err != nil {
			return fmt.Errorf("failed to write link to WAL: %w", err)
		}
		a.walSegments[segmentIndex] = true
	}

	return nil
}

// updateArrowStorage updates the in-memory Arrow storage.
// For backward compatibility. Use updateArrowStorageContext for context support.
func (a *ArrowAppender) updateArrowStorage(
	spans []*span.Span,
	links []*span.SpanLink,
) error {
	return a.updateArrowStorageContext(context.Background(), spans, links)
}

// updateArrowStorageContext updates the in-memory Arrow storage with context support.
func (a *ArrowAppender) updateArrowStorageContext(
	ctx context.Context,
	spans []*span.Span,
	links []*span.SpanLink,
) error {
	// Check context before acquiring lock
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled before Arrow storage update: %w", err)
	}

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

	// Check context before link storage update
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled before link storage update: %w", err)
	}

	// Add links to link storage (bulk operation)
	if len(links) > 0 {
		if err := a.linkStorage.AddLinks(links); err != nil {
			return fmt.Errorf("failed to add links to Arrow link storage: %w", err)
		}
	}

	return nil
}


// Rollback aborts the transaction and releases resources.
// For backward compatibility, this delegates to RollbackContext with a background context.
func (a *ArrowAppender) Rollback() error {
	return a.RollbackContext(context.Background())
}

// RollbackContext aborts the transaction with context support.
func (a *ArrowAppender) RollbackContext(ctx context.Context) error {
	if a.committed {
		return fmt.Errorf("cannot rollback: transaction already committed")
	}
	if a.rolledBack {
		return fmt.Errorf("transaction already rolled back")
	}

	// Check context before proceeding
	if err := ctx.Err(); err != nil {
		// Still need to release resources even if context is cancelled
		defer a.releaseResources()
		a.isolation.RegisterRollback(a.txnID)
		a.rolledBack = true
		return fmt.Errorf("context cancelled during rollback (rollback still completed): %w", err)
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
