package executor

import "github.com/saswatamcode/artemis/pkg/span"

// clearBatch clears a span batch buffer without deallocating.
// This is used before returning buffers to the pool to prevent memory leaks.
//
// Note: IsolationCoordinator.ReleaseSpanBuffer already does this,
// but this helper is useful for operators that manage buffers manually.
func clearBatch(buf *[]*span.Span) {
	if buf == nil {
		return
	}
	*buf = (*buf)[:0]
}

// appendBatch appends spans from src to dst, managing buffer capacity.
// If dst is nil, allocates a new buffer from the pool.
// If dst is full, this does NOT handle overflow (caller must manage batching).
//
// Returns the updated dst buffer.
func appendBatch(dst *[]*span.Span, src []*span.Span) *[]*span.Span {
	if dst == nil {
		// This shouldn't happen if caller uses GetSpanBatch properly,
		// but handle gracefully
		buf := make([]*span.Span, 0, 1000)
		dst = &buf
	}

	*dst = append(*dst, src...)
	return dst
}

// copyBatch creates a new batch buffer and copies spans into it.
// The returned buffer must be released back to the pool by the caller.
//
// This is used when an operator needs to preserve a batch while the input
// buffer is released back to the pool.
func copyBatch(ctx *ExecutionContext, src []*span.Span) *[]*span.Span {
	dst := ctx.GetSpanBatch()
	*dst = append(*dst, src...)
	return dst
}
