package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/saswatamcode/artemis/pkg/span"
)

const (
	// WAL file format: similar to Prometheus WAL
	// Each record: [CRC32 (4 bytes)][Length (4 bytes)][Type (1 byte)][Data (Length bytes)]
	recordHeaderSize   = 9                           // 4 + 4 + 1
	pageSize           = 256 * 1024                  // 256KB per page
	maxRecordDataSize  = pageSize - recordHeaderSize // Maximum data size per record
	defaultSegmentSize = 128 * 1024 * 1024           // 128MB per segment (default)

	// Magic number written to mark padding at the end of segments
	// This is written in the CRC field to indicate the rest is padding
	paddingMagic = 0xFFAA55FF
)

// RecordType indicates the type of WAL record
type RecordType byte

const (
	RecordTypeSpan  RecordType = 1
	RecordTypeEvent RecordType = 2 // Span events
	RecordTypeLink  RecordType = 3 // Span links
	RecordTypeFull  RecordType = 4 // For full records (future use)
)

// page is an in-memory buffer used to batch disk writes.
// Records are written to the page buffer and flushed when:
// - The page is full (can't fit another record header)
// - An explicit flush is requested (checkpoint, close, rotation)
type page struct {
	alloc   int            // Current allocation position in the page
	flushed int            // How much of the page has been flushed to disk
	buf     [pageSize]byte // Fixed-size buffer
}

func (p *page) remaining() int {
	return pageSize - p.alloc
}

func (p *page) full() bool {
	return pageSize-p.alloc < recordHeaderSize
}

func (p *page) reset() {
	// Only zero the used portion of the buffer for efficiency
	// This is much faster than zeroing the entire 32KB when only a small portion was used
	for i := 0; i < p.alloc; i++ {
		p.buf[i] = 0
	}
	p.alloc = 0
	p.flushed = 0
}

// WAL implements a Write-Ahead Log for spans
type WAL struct {
	dir          string
	currentFile  *os.File
	currentSize  int64
	segmentSize  int64 // Maximum segment size before rotation
	segmentIndex int
	mu           sync.Mutex
	page         *page // Active page buffer
	logger       *slog.Logger
}

// NewWAL creates a new WAL instance with default segment size
func NewWAL(dir string, logger *slog.Logger) (*WAL, error) {
	return NewWALWithSegmentSize(dir, defaultSegmentSize, logger)
}

// NewWALWithSegmentSize creates a new WAL instance with custom segment size
func NewWALWithSegmentSize(dir string, segmentSize int64, logger *slog.Logger) (*WAL, error) {
	if logger == nil {
		logger = slog.Default()
	}

	if segmentSize <= 0 {
		segmentSize = defaultSegmentSize
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Find the highest existing segment index to ensure monotonically increasing segment numbers
	maxSegmentIndex := -1
	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err == nil {
		for _, file := range files {
			var segmentIndex int
			if _, err := fmt.Sscanf(filepath.Base(file), "%06d.wal", &segmentIndex); err == nil {
				if segmentIndex > maxSegmentIndex {
					maxSegmentIndex = segmentIndex
				}
			}
		}
	}

	// Start from the next index after the highest existing segment
	// This ensures segment numbers always increase, even after restarts
	startIndex := 0
	if maxSegmentIndex >= 0 {
		startIndex = maxSegmentIndex + 1
	}

	w := &WAL{
		dir:          dir,
		segmentSize:  segmentSize,
		segmentIndex: startIndex,
		page:         &page{},
		logger:       logger,
	}

	if err := w.createNewSegment(); err != nil {
		return nil, err
	}

	return w, nil
}

// WriteSpan writes a span to the WAL and returns the segment index it was written to
func (w *WAL) WriteSpan(s *span.Span) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := s.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("failed to marshal span: %w", err)
	}

	// Check if we need to rotate to a new segment
	if w.currentSize+int64(len(data))+recordHeaderSize > w.segmentSize {
		if err := w.rotateSegment(); err != nil {
			return 0, err
		}
	}

	// CRITICAL: Capture segment index AFTER rotation check
	// This ensures we return the segment that actually contains the span
	segmentIndex := w.segmentIndex

	if err := w.writeRecord(RecordTypeSpan, data); err != nil {
		return 0, err
	}

	// Flush the page to ensure durability
	if err := w.flushPage(false); err != nil {
		return 0, err
	}

	return segmentIndex, nil
}

// WriteEvent writes a span event to the WAL and returns the segment index it was written to
func (w *WAL) WriteEvent(e *span.SpanEvent) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := e.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("failed to marshal event: %w", err)
	}

	// Check if we need to rotate to a new segment
	if w.currentSize+int64(len(data))+recordHeaderSize > w.segmentSize {
		if err := w.rotateSegment(); err != nil {
			return 0, err
		}
	}

	segmentIndex := w.segmentIndex

	if err := w.writeRecord(RecordTypeEvent, data); err != nil {
		return 0, err
	}

	// Flush the page to ensure durability
	if err := w.flushPage(false); err != nil {
		return 0, err
	}

	return segmentIndex, nil
}

// WriteEvents writes multiple span events to the WAL and returns the segment index
// More efficient than calling WriteEvent multiple times as it batches the writes
func (w *WAL) WriteEvents(events []*span.SpanEvent) (int, error) {
	if len(events) == 0 {
		return w.SegmentIndex(), nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var segmentIndex int

	for _, e := range events {
		data, err := e.MarshalBinary()
		if err != nil {
			return 0, fmt.Errorf("failed to marshal event: %w", err)
		}

		// Check if we need to rotate to a new segment
		if w.currentSize+int64(len(data))+recordHeaderSize > w.segmentSize {
			if err := w.rotateSegment(); err != nil {
				return 0, err
			}
		}

		// Capture the segment index (will be the latest after all rotations)
		segmentIndex = w.segmentIndex

		if err := w.writeRecord(RecordTypeEvent, data); err != nil {
			return 0, err
		}
	}

	// Flush the page to ensure durability
	if err := w.flushPage(false); err != nil {
		return 0, err
	}

	return segmentIndex, nil
}

// WriteLink writes a span link to the WAL and returns the segment index it was written to
func (w *WAL) WriteLink(l *span.SpanLink) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := l.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("failed to marshal link: %w", err)
	}

	// Check if we need to rotate to a new segment
	if w.currentSize+int64(len(data))+recordHeaderSize > w.segmentSize {
		if err := w.rotateSegment(); err != nil {
			return 0, err
		}
	}

	segmentIndex := w.segmentIndex

	if err := w.writeRecord(RecordTypeLink, data); err != nil {
		return 0, err
	}

	// Flush the page to ensure durability
	if err := w.flushPage(false); err != nil {
		return 0, err
	}

	return segmentIndex, nil
}

// WriteLinks writes multiple span links to the WAL and returns the segment index
// More efficient than calling WriteLink multiple times as it batches the writes
func (w *WAL) WriteLinks(links []*span.SpanLink) (int, error) {
	if len(links) == 0 {
		return w.SegmentIndex(), nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var segmentIndex int

	for _, l := range links {
		data, err := l.MarshalBinary()
		if err != nil {
			return 0, fmt.Errorf("failed to marshal link: %w", err)
		}

		// Check if we need to rotate to a new segment
		if w.currentSize+int64(len(data))+recordHeaderSize > w.segmentSize {
			if err := w.rotateSegment(); err != nil {
				return 0, err
			}
		}

		// Capture the segment index (will be the latest after all rotations)
		segmentIndex = w.segmentIndex

		if err := w.writeRecord(RecordTypeLink, data); err != nil {
			return 0, err
		}
	}

	// Flush the page to ensure durability
	if err := w.flushPage(false); err != nil {
		return 0, err
	}

	return segmentIndex, nil
}

// flushPage writes the current page buffer to disk and fsyncs.
// If forceClear is true, writes padding marker and zeros out remaining space.
func (w *WAL) flushPage(forceClear bool) error {
	p := w.page
	shouldClear := forceClear || p.full()

	// If forcing clear or page is full, write padding marker and fill to end
	if shouldClear && p.alloc < pageSize {
		// Write padding magic marker at current position
		if p.remaining() >= 4 {
			binary.BigEndian.PutUint32(p.buf[p.alloc:], paddingMagic)
			p.alloc += 4
		}
		// Extend to end of page (rest is already zeros from reset or initialization)
		p.alloc = pageSize
	}

	// Write unflushed portion of page to disk
	n, err := w.currentFile.Write(p.buf[p.flushed:p.alloc])
	if err != nil {
		p.flushed += n
		return err
	}
	p.flushed += n

	// Fsync to ensure data is on disk
	if err := w.currentFile.Sync(); err != nil {
		return err
	}

	// Reset page if we flushed everything
	if shouldClear {
		p.reset()
	}

	return nil
}

// writeRecord writes a single record to the page buffer.
// Flushes the page if it becomes full.
func (w *WAL) writeRecord(typ RecordType, data []byte) error {
	// Validate record size - records cannot exceed page size
	if len(data) > maxRecordDataSize {
		return fmt.Errorf("record data size %d exceeds maximum %d bytes", len(data), maxRecordDataSize)
	}

	recordSize := recordHeaderSize + len(data)

	// If page is full, flush it WITHOUT padding (we're in the middle of a segment)
	if w.page.full() {
		if err := w.flushPage(false); err != nil {
			return err
		}
		// After flushing without padding, manually reset the page for the next write
		w.page.reset()
	}

	// If record doesn't fit in page, flush current page and start fresh
	// Don't pad - we're in the middle of a segment
	if w.page.remaining() < recordSize {
		if err := w.flushPage(false); err != nil {
			return err
		}
		// After flushing without padding, manually reset the page for the next write
		w.page.reset()
	}

	// Write record to page buffer
	buf := w.page.buf[w.page.alloc:]
	crc := crc32.ChecksumIEEE(data)

	// Write CRC (4 bytes)
	binary.BigEndian.PutUint32(buf[0:4], crc)

	// Write length (4 bytes)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(data)))

	// Write type (1 byte)
	buf[8] = byte(typ)

	// Write data
	copy(buf[9:], data)

	w.page.alloc += recordSize
	w.currentSize += int64(recordSize)

	return nil
}

// createNewSegment creates a new WAL segment file
func (w *WAL) createNewSegment() error {
	filename := filepath.Join(w.dir, fmt.Sprintf("%06d.wal", w.segmentIndex))
	// Use O_EXCL to ensure we don't accidentally append to an existing file
	// This prevents data corruption if a segment file already exists
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("failed to create WAL segment: %w", err)
	}

	w.currentFile = f
	w.currentSize = 0

	return nil
}

// rotateSegment closes the current segment and creates a new one
func (w *WAL) rotateSegment() error {
	// Flush current page with zeros padding
	var flushErr error
	if w.page.alloc > 0 {
		flushErr = w.flushPage(true)
	}

	// Always close the old file, even if flush failed
	closeErr := w.currentFile.Close()

	// If flush or close failed, return error without rotating
	if flushErr != nil {
		return flushErr
	}
	if closeErr != nil {
		return closeErr
	}

	// Only increment segment index after successful close
	// This ensures if createNewSegment fails, we can retry with same index
	w.segmentIndex++
	if err := w.createNewSegment(); err != nil {
		// Failed to create new segment - decrement back to maintain consistency
		// This leaves WAL in a closed state, but at least segment numbering is correct
		w.segmentIndex--
		return err
	}

	return nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush current page with zeros padding
	var flushErr error
	if w.page.alloc > 0 {
		flushErr = w.flushPage(true)
	}

	// Always close the file, even if flush failed
	closeErr := w.currentFile.Close()

	// Return flush error if it occurred, otherwise return close error
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// Flush flushes the current page to disk without clearing it
// This is useful for checkpointing to ensure all data is durable
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.page.alloc > 0 {
		return w.flushPage(false)
	}
	return nil
}

// SegmentIndex returns the current WAL segment index
func (w *WAL) SegmentIndex() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segmentIndex
}

// Reader reads spans and events from WAL files
type Reader struct {
	dir    string
	logger *slog.Logger
}

// NewReader creates a new WAL reader
func NewReader(dir string, logger *slog.Logger) *Reader {
	if logger == nil {
		logger = slog.Default()
	}
	return &Reader{
		dir:    dir,
		logger: logger,
	}
}

// ReadAll reads all spans from all WAL segments
func (r *Reader) ReadAll(callback func(*span.Span) error) error {
	files, err := filepath.Glob(filepath.Join(r.dir, "*.wal"))
	if err != nil {
		return err
	}

	for _, file := range files {
		if err := r.readSegment(file, callback); err != nil {
			return fmt.Errorf("failed to read segment %s: %w", file, err)
		}
	}

	return nil
}

// ReadAllWithEvents reads all spans and events from all WAL segments
// Provides separate callbacks for spans and events
func (r *Reader) ReadAllWithEvents(
	spanCallback func(*span.Span) error,
	eventCallback func(*span.SpanEvent) error,
) error {
	files, err := filepath.Glob(filepath.Join(r.dir, "*.wal"))
	if err != nil {
		return err
	}

	for _, file := range files {
		if err := r.readSegmentWithEvents(file, spanCallback, eventCallback); err != nil {
			return fmt.Errorf("failed to read segment %s: %w", file, err)
		}
	}

	return nil
}

// ReadAllWithEventsAndLinks reads all spans, events, and links from all WAL segments
// Provides separate callbacks for spans, events, and links
func (r *Reader) ReadAllWithEventsAndLinks(
	spanCallback func(*span.Span) error,
	eventCallback func(*span.SpanEvent) error,
	linkCallback func(*span.SpanLink) error,
) error {
	files, err := filepath.Glob(filepath.Join(r.dir, "*.wal"))
	if err != nil {
		return err
	}

	for _, file := range files {
		if err := r.readSegmentWithAll(file, spanCallback, eventCallback, linkCallback); err != nil {
			return fmt.Errorf("failed to read segment %s: %w", file, err)
		}
	}

	return nil
}

// readSegment reads a single WAL segment (spans only, for backward compatibility)
func (r *Reader) readSegment(filename string, callback func(*span.Span) error) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	for {
		// Read CRC (4 bytes)
		var crc uint32
		if err := binary.Read(reader, binary.BigEndian, &crc); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// Read length (4 bytes)
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			return err
		}

		// Read type (1 byte)
		typ, err := reader.ReadByte()
		if err != nil {
			return err
		}

		// Detect padding - either explicit magic marker or all-zeros fallback
		// The all-zeros fallback handles edge case where <4 bytes remained for magic
		if crc == paddingMagic || (crc == 0 && length == 0 && typ == 0) {
			return nil
		}

		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}

		// Verify CRC
		if crc32.ChecksumIEEE(data) != crc {
			return fmt.Errorf("CRC mismatch")
		}

		// Process record based on type
		switch RecordType(typ) {
		case RecordTypeSpan:
			var s span.Span
			if err := s.UnmarshalBinary(data); err != nil {
				return fmt.Errorf("failed to unmarshal span: %w", err)
			}

			if err := callback(&s); err != nil {
				return err
			}
		case RecordTypeEvent:
			// Skip events in span-only mode
			continue
		default:
			// Return error instead of panic for unknown record types
			// This allows the replay logic to handle the error gracefully (skip or stop)
			// instead of crashing the entire database
			return fmt.Errorf("unknown record type: %d", typ)
		}
	}
}

// readSegmentWithEvents reads a single WAL segment with both spans and events
func (r *Reader) readSegmentWithEvents(
	filename string,
	spanCallback func(*span.Span) error,
	eventCallback func(*span.SpanEvent) error,
) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	for {
		// Read CRC (4 bytes)
		var crc uint32
		if err := binary.Read(reader, binary.BigEndian, &crc); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// Read length (4 bytes)
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			return err
		}

		// Read type (1 byte)
		typ, err := reader.ReadByte()
		if err != nil {
			return err
		}

		// Detect padding - either explicit magic marker or all-zeros fallback
		// The all-zeros fallback handles edge case where <4 bytes remained for magic
		if crc == paddingMagic || (crc == 0 && length == 0 && typ == 0) {
			return nil
		}

		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}

		// Verify CRC
		if crc32.ChecksumIEEE(data) != crc {
			return fmt.Errorf("CRC mismatch")
		}

		// Process record based on type
		switch RecordType(typ) {
		case RecordTypeSpan:
			if spanCallback != nil {
				var s span.Span
				if err := s.UnmarshalBinary(data); err != nil {
					return fmt.Errorf("failed to unmarshal span: %w", err)
				}

				if err := spanCallback(&s); err != nil {
					return err
				}
			}
		case RecordTypeEvent:
			if eventCallback != nil {
				var e span.SpanEvent
				if err := e.UnmarshalBinary(data); err != nil {
					return fmt.Errorf("failed to unmarshal event: %w", err)
				}

				if err := eventCallback(&e); err != nil {
					return err
				}
			}
		case RecordTypeLink:
			// Skip links in events-only mode (backward compatibility)
			continue
		default:
			return fmt.Errorf("unknown record type: %d", typ)
		}
	}
}

// readSegmentWithAll reads a single WAL segment with spans, events, and links
func (r *Reader) readSegmentWithAll(
	filename string,
	spanCallback func(*span.Span) error,
	eventCallback func(*span.SpanEvent) error,
	linkCallback func(*span.SpanLink) error,
) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	for {
		// Read CRC (4 bytes)
		var crc uint32
		if err := binary.Read(reader, binary.BigEndian, &crc); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// Read length (4 bytes)
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			return err
		}

		// Read type (1 byte)
		typ, err := reader.ReadByte()
		if err != nil {
			return err
		}

		// Detect padding - either explicit magic marker or all-zeros fallback
		// The all-zeros fallback handles edge case where <4 bytes remained for magic
		if crc == paddingMagic || (crc == 0 && length == 0 && typ == 0) {
			return nil
		}

		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}

		// Verify CRC
		if crc32.ChecksumIEEE(data) != crc {
			return fmt.Errorf("CRC mismatch")
		}

		// Process record based on type
		switch RecordType(typ) {
		case RecordTypeSpan:
			if spanCallback != nil {
				var s span.Span
				if err := s.UnmarshalBinary(data); err != nil {
					return fmt.Errorf("failed to unmarshal span: %w", err)
				}

				if err := spanCallback(&s); err != nil {
					return err
				}
			}
		case RecordTypeEvent:
			if eventCallback != nil {
				var e span.SpanEvent
				if err := e.UnmarshalBinary(data); err != nil {
					return fmt.Errorf("failed to unmarshal event: %w", err)
				}

				if err := eventCallback(&e); err != nil {
					return err
				}
			}
		case RecordTypeLink:
			if linkCallback != nil {
				var l span.SpanLink
				if err := l.UnmarshalBinary(data); err != nil {
					return fmt.Errorf("failed to unmarshal link: %w", err)
				}

				if err := linkCallback(&l); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unknown record type: %d", typ)
		}
	}
}
