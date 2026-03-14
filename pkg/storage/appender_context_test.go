package storage

import (
	"context"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// TestCommitContext_Cancellation tests that CommitContext respects context cancellation
func TestCommitContext_Cancellation(t *testing.T) {
	// Create storage with WAL
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	isolation := NewIsolationCoordinator()
	storage.SetTransactionDependencies(isolation, linkStorage, nil)

	// Use a no-op WAL for testing
	wal := &noopWAL{}

	// Set up dependencies
	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	// Create appender
	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add a span
	testSpan := &span.Span{
		TraceID:     "1234567890abcdef1234567890abcdef",
		SpanID:      "1234567890abcdef",
		Name:        "test-span",
		ServiceName: "test-service",
		Duration:    1000,
	}
	if err := appender.AddSpan(testSpan); err != nil {
		t.Fatalf("AddSpan failed: %v", err)
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Try to commit with cancelled context
	err := appender.CommitContext(ctx)
	if err == nil {
		t.Fatal("Expected CommitContext to fail with cancelled context")
	}

	// Check that error message indicates cancellation
	if !contains(err.Error(), "context cancel") {
		t.Errorf("Expected cancellation error, got: %v", err)
	}
}

// TestCommitContext_Timeout tests that CommitContext respects timeouts
func TestCommitContext_Timeout(t *testing.T) {
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	isolation := NewIsolationCoordinator()
	wal := &noopWAL{}

	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add a span
	testSpan := &span.Span{
		TraceID:     "1234567890abcdef1234567890abcdef",
		SpanID:      "1234567890abcdef",
		Name:        "test-span",
		ServiceName: "test-service",
		Duration:    1000,
	}
	if err := appender.AddSpan(testSpan); err != nil {
		t.Fatalf("AddSpan failed: %v", err)
	}

	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait for timeout
	time.Sleep(10 * time.Millisecond)

	// Try to commit with timed-out context
	err := appender.CommitContext(ctx)
	if err == nil {
		t.Fatal("Expected CommitContext to fail with timed-out context")
	}
}

// TestCommitContext_Success tests that CommitContext works with valid context
func TestCommitContext_Success(t *testing.T) {
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	isolation := NewIsolationCoordinator()
	wal := &noopWAL{}

	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add a span
	testSpan := &span.Span{
		TraceID:     "1234567890abcdef1234567890abcdef",
		SpanID:      "1234567890abcdef",
		Name:        "test-span",
		ServiceName: "test-service",
		Duration:    1000,
	}
	if err := appender.AddSpan(testSpan); err != nil {
		t.Fatalf("AddSpan failed: %v", err)
	}

	// Create context with reasonable timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Commit should succeed
	if err := appender.CommitContext(ctx); err != nil {
		t.Fatalf("CommitContext failed: %v", err)
	}

	// Verify span was committed
	if appender.TxnID() == 0 {
		t.Error("Expected valid transaction ID")
	}
}

// TestRollbackContext_Success tests that RollbackContext works
func TestRollbackContext_Success(t *testing.T) {
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	isolation := NewIsolationCoordinator()
	wal := &noopWAL{}

	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add a span
	testSpan := &span.Span{
		TraceID:     "1234567890abcdef1234567890abcdef",
		SpanID:      "1234567890abcdef",
		Name:        "test-span",
		ServiceName: "test-service",
		Duration:    1000,
	}
	if err := appender.AddSpan(testSpan); err != nil {
		t.Fatalf("AddSpan failed: %v", err)
	}

	ctx := context.Background()

	// Rollback should succeed
	if err := appender.RollbackContext(ctx); err != nil {
		t.Fatalf("RollbackContext failed: %v", err)
	}

	// Trying to commit after rollback should fail
	if err := appender.CommitContext(ctx); err == nil {
		t.Fatal("Expected commit to fail after rollback")
	}
}

// TestCommit_BackwardCompatibility tests that old Commit() still works
func TestCommit_BackwardCompatibility(t *testing.T) {
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	isolation := NewIsolationCoordinator()
	wal := &noopWAL{}

	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add a span
	testSpan := &span.Span{
		TraceID:     "1234567890abcdef1234567890abcdef",
		SpanID:      "1234567890abcdef",
		Name:        "test-span",
		ServiceName: "test-service",
		Duration:    1000,
	}
	if err := appender.AddSpan(testSpan); err != nil {
		t.Fatalf("AddSpan failed: %v", err)
	}

	// Old Commit() method should still work
	if err := appender.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

// TestRollback_BackwardCompatibility tests that old Rollback() still works
func TestRollback_BackwardCompatibility(t *testing.T) {
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	isolation := NewIsolationCoordinator()
	wal := &noopWAL{}

	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	appender := NewArrowAppender(isolation, storage, linkStorage, wal)

	// Add a span
	testSpan := &span.Span{
		TraceID:     "1234567890abcdef1234567890abcdef",
		SpanID:      "1234567890abcdef",
		Name:        "test-span",
		ServiceName: "test-service",
		Duration:    1000,
	}
	if err := appender.AddSpan(testSpan); err != nil {
		t.Fatalf("AddSpan failed: %v", err)
	}

	// Old Rollback() method should still work
	if err := appender.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
}

// noopWAL is a no-op WAL implementation for testing
type noopWAL struct{}

func (w *noopWAL) WriteSpan(s *span.Span) (int, error) {
	return 0, nil
}

func (w *noopWAL) WriteLink(link *span.SpanLink) (int, error) {
	return 0, nil
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || contains(s[1:], substr)))
}
