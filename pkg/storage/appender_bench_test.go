package storage

import (
	"context"
	"testing"

	"github.com/saswatamcode/artemis/pkg/span"
)

// BenchmarkCommit benchmarks the old Commit() method (uses context.Background internally)
func BenchmarkCommit(b *testing.B) {
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	isolation := NewIsolationCoordinator()
	wal := &noopWAL{}

	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	// Create test span
	testSpan := &span.Span{
		TraceID:     "1234567890abcdef1234567890abcdef",
		SpanID:      "1234567890abcdef",
		Name:        "test-span",
		ServiceName: "test-service",
		Duration:    1000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		appender := NewArrowAppender(isolation, storage, linkStorage, wal)
		_ = appender.AddSpan(testSpan)
		_ = appender.Commit()
	}
}

// BenchmarkCommitContext benchmarks the new CommitContext() method
func BenchmarkCommitContext(b *testing.B) {
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	isolation := NewIsolationCoordinator()
	wal := &noopWAL{}

	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	// Create test span
	testSpan := &span.Span{
		TraceID:     "1234567890abcdef1234567890abcdef",
		SpanID:      "1234567890abcdef",
		Name:        "test-span",
		ServiceName: "test-service",
		Duration:    1000,
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		appender := NewArrowAppender(isolation, storage, linkStorage, wal)
		_ = appender.AddSpan(testSpan)
		_ = appender.CommitContext(ctx)
	}
}

// BenchmarkCommitContext_LargeBatch benchmarks context overhead with large batches
func BenchmarkCommitContext_LargeBatch(b *testing.B) {
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	isolation := NewIsolationCoordinator()
	wal := &noopWAL{}

	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	// Create 1000 test spans
	spans := make([]*span.Span, 1000)
	for i := range spans {
		spans[i] = &span.Span{
			TraceID:     "1234567890abcdef1234567890abcdef",
			SpanID:      "1234567890abcdef",
			Name:        "test-span",
			ServiceName: "test-service",
			Duration:    1000,
		}
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		appender := NewArrowAppender(isolation, storage, linkStorage, wal)
		for _, s := range spans {
			_ = appender.AddSpan(s)
		}
		_ = appender.CommitContext(ctx)
	}
}

// BenchmarkRollback benchmarks the old Rollback() method
func BenchmarkRollback(b *testing.B) {
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	isolation := NewIsolationCoordinator()
	wal := &noopWAL{}

	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	testSpan := &span.Span{
		TraceID:     "1234567890abcdef1234567890abcdef",
		SpanID:      "1234567890abcdef",
		Name:        "test-span",
		ServiceName: "test-service",
		Duration:    1000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		appender := NewArrowAppender(isolation, storage, linkStorage, wal)
		_ = appender.AddSpan(testSpan)
		_ = appender.Rollback()
	}
}

// BenchmarkRollbackContext benchmarks the new RollbackContext() method
func BenchmarkRollbackContext(b *testing.B) {
	storage := NewArrowStorage()
	linkStorage := NewArrowLinkStorage()
	isolation := NewIsolationCoordinator()
	wal := &noopWAL{}

	storage.SetTransactionDependencies(isolation, linkStorage, wal)

	testSpan := &span.Span{
		TraceID:     "1234567890abcdef1234567890abcdef",
		SpanID:      "1234567890abcdef",
		Name:        "test-span",
		ServiceName: "test-service",
		Duration:    1000,
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		appender := NewArrowAppender(isolation, storage, linkStorage, wal)
		_ = appender.AddSpan(testSpan)
		_ = appender.RollbackContext(ctx)
	}
}
