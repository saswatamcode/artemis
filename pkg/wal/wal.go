package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
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
	recordHeaderSize   = 9                 // 4 + 4 + 1
	defaultSegmentSize = 128 * 1024 * 1024 // 128MB per segment (default)
)

// RecordType indicates the type of WAL record
type RecordType byte

const (
	RecordTypeSpan RecordType = 1
	RecordTypeFull RecordType = 2 // For full records (future use)
)

// WAL implements a Write-Ahead Log for spans
type WAL struct {
	dir          string
	currentFile  *os.File
	currentSize  int64
	segmentSize  int64 // Maximum segment size before rotation
	segmentIndex int
	mu           sync.Mutex
	writer       *bufio.Writer
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
		logger:       logger,
	}

	if err := w.createNewSegment(); err != nil {
		return nil, err
	}

	return w, nil
}

// WriteSpan writes a span to the WAL
func (w *WAL) WriteSpan(s *span.Span) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("failed to marshal span: %w", err)
	}

	// Check if we need to rotate to a new segment
	if w.currentSize+int64(len(data))+recordHeaderSize > w.segmentSize {
		if err := w.rotateSegment(); err != nil {
			return err
		}
	}

	if err := w.writeRecord(RecordTypeSpan, data); err != nil {
		return err
	}

	return w.writer.Flush()
}

// writeRecord writes a single record to the WAL
func (w *WAL) writeRecord(typ RecordType, data []byte) error {
	crc := crc32.ChecksumIEEE(data)

	// Write CRC (4 bytes)
	if err := binary.Write(w.writer, binary.BigEndian, crc); err != nil {
		return err
	}

	// Write length (4 bytes)
	length := uint32(len(data))
	if err := binary.Write(w.writer, binary.BigEndian, length); err != nil {
		return err
	}

	// Write type (1 byte)
	if err := w.writer.WriteByte(byte(typ)); err != nil {
		return err
	}

	// Write data
	n, err := w.writer.Write(data)
	if err != nil {
		return err
	}

	w.currentSize += int64(recordHeaderSize + n)
	return nil
}

// createNewSegment creates a new WAL segment file
func (w *WAL) createNewSegment() error {
	filename := filepath.Join(w.dir, fmt.Sprintf("%06d.wal", w.segmentIndex))
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create WAL segment: %w", err)
	}

	w.currentFile = f
	w.writer = bufio.NewWriter(f)
	w.currentSize = 0

	return nil
}

// rotateSegment closes the current segment and creates a new one
func (w *WAL) rotateSegment() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.currentFile.Sync(); err != nil {
		return err
	}
	if err := w.currentFile.Close(); err != nil {
		return err
	}

	w.segmentIndex++
	return w.createNewSegment()
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.currentFile.Sync(); err != nil {
		return err
	}
	return w.currentFile.Close()
}

// SegmentIndex returns the current WAL segment index
func (w *WAL) SegmentIndex() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segmentIndex
}

// Reader reads spans from WAL files
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

// readSegment reads a single WAL segment
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
			if err := json.Unmarshal(data, &s); err != nil {
				return fmt.Errorf("failed to unmarshal span: %w", err)
			}

			if err := callback(&s); err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("unknown record type: %d", typ))
		}
	}
}

// verifyCRC verifies the CRC checksum
func verifyCRC(crc uint32, data []byte) bool {
	return crc32.ChecksumIEEE(data) == crc
}

func unmarshalSpan(data []byte) (*span.Span, error) {
	var s span.Span
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("failed to unmarshal span: %w", err)
	}
	return &s, nil
}

// readRecordFull reads a record and returns additional metadata including offset
func readRecordFull(reader *bufio.Reader) (crc uint32, length uint32, recordType byte, data []byte, offset int64, err error) {
	// Note: We can't track exact offset without seeking, so we'll use 0
	// TODO: Track this more precisely
	offset = 0

	// Read CRC (4 bytes)
	if err = binary.Read(reader, binary.BigEndian, &crc); err != nil {
		if err == io.EOF {
			err = fmt.Errorf("EOF")
		}
		return
	}

	// Read length (4 bytes)
	if err = binary.Read(reader, binary.BigEndian, &length); err != nil {
		return
	}

	// Read type (1 byte)
	recordType, err = reader.ReadByte()
	if err != nil {
		return
	}

	// Read data
	data = make([]byte, length)
	_, err = io.ReadFull(reader, data)
	return
}
