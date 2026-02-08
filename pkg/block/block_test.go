package block

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/saswatamcode/artemis/pkg/span"
	"github.com/saswatamcode/artemis/pkg/storage"
)

func TestBlockMeta_Duration(t *testing.T) {
	meta := &BlockMeta{
		MinTime: time.Unix(0, 0).UnixNano(),
		MaxTime: time.Unix(0, 5*time.Second.Nanoseconds()).UnixNano(),
	}

	duration := meta.Duration()
	if duration != 5*time.Second {
		t.Errorf("Duration() = %v, want 5s", duration)
	}
}

func TestBlockMeta_Contains(t *testing.T) {
	meta := &BlockMeta{
		MinTime: 1000,
		MaxTime: 2000,
	}

	tests := []struct {
		name      string
		timestamp int64
		want      bool
	}{
		{"before range", 500, false},
		{"at min", 1000, true},
		{"in range", 1500, true},
		{"at max", 2000, true},
		{"after range", 2500, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := meta.Contains(tt.timestamp)
			if got != tt.want {
				t.Errorf("Contains(%d) = %v, want %v", tt.timestamp, got, tt.want)
			}
		})
	}
}

func TestBlockMeta_Overlaps(t *testing.T) {
	meta1 := &BlockMeta{
		MinTime: 1000,
		MaxTime: 2000,
	}

	tests := []struct {
		name  string
		other *BlockMeta
		want  bool
	}{
		{
			"completely before",
			&BlockMeta{MinTime: 500, MaxTime: 900},
			false,
		},
		{
			"completely after",
			&BlockMeta{MinTime: 2100, MaxTime: 3000},
			false,
		},
		{
			"overlaps start",
			&BlockMeta{MinTime: 900, MaxTime: 1500},
			true,
		},
		{
			"overlaps end",
			&BlockMeta{MinTime: 1500, MaxTime: 2500},
			true,
		},
		{
			"completely contained",
			&BlockMeta{MinTime: 1200, MaxTime: 1800},
			true,
		},
		{
			"completely contains",
			&BlockMeta{MinTime: 500, MaxTime: 3000},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := meta1.Overlaps(tt.other)
			if got != tt.want {
				t.Errorf("Overlaps() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBlockMeta_Level(t *testing.T) {
	// L0 block (no compaction metadata)
	meta0 := &BlockMeta{
		ULID:       ulid.Make(),
		Compaction: nil,
	}
	if meta0.Level() != 0 {
		t.Errorf("L0 block Level() = %d, want 0", meta0.Level())
	}

	// L1 block
	meta1 := &BlockMeta{
		ULID:       ulid.Make(),
		Compaction: &CompactionMeta{Level: 1},
	}
	if meta1.Level() != 1 {
		t.Errorf("L1 block Level() = %d, want 1", meta1.Level())
	}
}

func TestBlockMeta_String(t *testing.T) {
	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   time.Unix(0, 0).UnixNano(),
		MaxTime:   time.Unix(0, time.Second.Nanoseconds()).UnixNano(),
		SpanCount: 100,
	}

	str := meta.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}
}

func TestManager_Creation(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		Dir:              filepath.Join(tmpDir, "blocks"),
		MaxBlockDuration: 1 * time.Hour,
		MaxBlockSpans:    1000,
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	mgr, err := NewManager(cfg, arrowStorage)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer mgr.Close()

	stats := mgr.Stats()
	if stats.PersistedBlocks != 0 {
		t.Errorf("New manager should have 0 persisted blocks, got %d", stats.PersistedBlocks)
	}
}

func TestManager_ShouldFlush(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		Dir:              filepath.Join(tmpDir, "blocks"),
		MaxBlockDuration: 100 * time.Millisecond,
		MaxBlockSpans:    10,
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	mgr, err := NewManager(cfg, arrowStorage)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer mgr.Close()

	// Empty storage should not flush
	if mgr.ShouldFlush() {
		t.Error("Empty storage should not flush")
	}

	// Add spans below threshold
	for i := range 5 {
		sp := &span.Span{
			SpanID:      "span-" + string(rune(i)),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		arrowStorage.AddSpan(sp)
	}

	// Update time range
	mgr.UpdateHeadTimeRange(
		time.Now().UnixNano(),
		time.Now().Add(time.Millisecond).UnixNano(),
	)

	// Should not flush yet (not enough spans, duration too short)
	if mgr.ShouldFlush() {
		t.Error("Should not flush with only 5 spans and short duration")
	}

	// Add more spans to exceed threshold
	for i := 5; i < 15; i++ {
		sp := &span.Span{
			SpanID:      "span-" + string(rune(i)),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		arrowStorage.AddSpan(sp)
	}

	// Should flush now (exceeded span count)
	if !mgr.ShouldFlush() {
		t.Error("Should flush after exceeding span count")
	}
}

func TestManager_FlushHead(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		Dir:              filepath.Join(tmpDir, "blocks"),
		MaxBlockDuration: 1 * time.Hour,
		MaxBlockSpans:    1000,
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	mgr, err := NewManager(cfg, arrowStorage)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer mgr.Close()

	// Add some spans
	now := time.Now()
	for i := range 10 {
		sp := &span.Span{
			SpanID:      "span-" + string(rune(i)),
			StartTime:   now.Add(time.Duration(i) * time.Millisecond),
			EndTime:     now.Add(time.Duration(i+1) * time.Millisecond),
			ServiceName: "service",
		}
		arrowStorage.AddSpan(sp)
	}
	arrowStorage.Flush()

	// Update time range
	mgr.UpdateHeadTimeRange(now.UnixNano(), now.Add(11*time.Millisecond).UnixNano())

	// Flush head (WAL segments 0-0 for test)
	meta, err := mgr.FlushHead(0, 0)
	if err != nil {
		t.Fatalf("FlushHead() error = %v", err)
	}

	if meta.SpanCount != 10 {
		t.Errorf("Flushed block SpanCount = %d, want 10", meta.SpanCount)
	}

	if meta.MinWALSegment != 0 {
		t.Errorf("MinWALSegment = %d, want 0", meta.MinWALSegment)
	}

	if meta.MaxWALSegment != 0 {
		t.Errorf("MaxWALSegment = %d, want 0", meta.MaxWALSegment)
	}

	// Verify block was created on disk
	blockDir := filepath.Join(cfg.Dir, meta.ULID.String())
	if _, err := os.Stat(blockDir); os.IsNotExist(err) {
		t.Error("Block directory should exist after flush")
	}

	// Verify manager stats updated
	stats := mgr.Stats()
	if stats.PersistedBlocks != 1 {
		t.Errorf("PersistedBlocks = %d, want 1", stats.PersistedBlocks)
	}
}

func TestManager_GetBlocks(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		Dir:              filepath.Join(tmpDir, "blocks"),
		MaxBlockDuration: 1 * time.Hour,
		MaxBlockSpans:    1000,
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	mgr, err := NewManager(cfg, arrowStorage)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer mgr.Close()

	// Initially no blocks
	blocks := mgr.GetBlocks()
	if len(blocks) != 0 {
		t.Errorf("GetBlocks() returned %d blocks, want 0", len(blocks))
	}

	// Add and flush some spans
	for i := range 5 {
		sp := &span.Span{
			SpanID:      "span-" + string(rune(i)),
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			ServiceName: "service",
		}
		arrowStorage.AddSpan(sp)
	}
	arrowStorage.Flush()

	mgr.UpdateHeadTimeRange(time.Now().UnixNano(), time.Now().Add(time.Millisecond).UnixNano())
	mgr.FlushHead(0, 0)

	// Should have one block now
	blocks = mgr.GetBlocks()
	if len(blocks) != 1 {
		t.Errorf("GetBlocks() returned %d blocks, want 1", len(blocks))
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg == nil {
		t.Fatal("DefaultConfig() should not return nil")
	}

	if cfg.Dir == "" {
		t.Error("Default Dir should not be empty")
	}

	if cfg.MaxBlockDuration == 0 {
		t.Error("MaxBlockDuration should not be 0")
	}

	if cfg.MaxBlockSpans == 0 {
		t.Error("MaxBlockSpans should not be 0")
	}
}

func TestFlushBlock(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "test-block")

	// Create test data
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
		Tags: map[string]string{
			"key": "value",
		},
	}
	arrowStorage.AddSpan(sp)
	arrowStorage.Flush()

	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   sp.StartTime.UnixNano(),
		MaxTime:   sp.EndTime.UnixNano(),
		SpanCount: 1,
		Version:   1,
		CreatedAt: time.Now(),
	}

	// Flush block
	err := FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("FlushBlock() error = %v", err)
	}

	// Verify files were created
	metaPath := filepath.Join(blockDir, "meta.json")
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Error("meta.json should exist")
	}

	spansPath := filepath.Join(blockDir, "spans.arrow")
	if _, err := os.Stat(spansPath); os.IsNotExist(err) {
		t.Error("spans.arrow should exist")
	}

	indexPath := filepath.Join(blockDir, "index.json")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Error("index.json should exist")
	}
}

func TestLoadPersistedBlock(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "test-block")

	// Create a block
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "operation",
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Millisecond),
		ServiceName: "service",
	}
	arrowStorage.AddSpan(sp)
	arrowStorage.Flush()

	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   sp.StartTime.UnixNano(),
		MaxTime:   sp.EndTime.UnixNano(),
		SpanCount: 1,
		Version:   1,
		CreatedAt: time.Now(),
	}

	FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())

	// Load the block
	block, err := LoadBlock(blockDir)
	if err != nil {
		t.Fatalf("LoadBlock() error = %v", err)
	}
	defer block.Close()

	// Verify metadata
	loadedMeta := block.Meta()
	if loadedMeta.SpanCount != 1 {
		t.Errorf("Loaded SpanCount = %d, want 1", loadedMeta.SpanCount)
	}

	// Verify we can read spans using the unified interface
	allSpans, err := block.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(allSpans) != 1 {
		t.Errorf("ReadAll() returned %d spans, want 1", len(allSpans))
	}

	// Verify index
	idx := block.Index()
	if idx == nil {
		t.Error("Index() should not return nil")
	}

	_, found := idx.LookupSpanID("span-1")
	if !found {
		t.Error("Index should contain span-1")
	}
}

// TestWriteParquetBlock tests writing a Parquet block to disk
func TestWriteParquetBlock(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "parquet-block")

	// Create test spans
	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:      "trace-1",
			SpanID:       "span-1",
			ParentSpanID: "",
			Name:         "operation-1",
			StartTime:    now,
			EndTime:      now.Add(10 * time.Millisecond),
			Duration:     10_000_000, // 10ms in nanoseconds
			ServiceName:  "service-1",
			Tags: map[string]string{
				"http.method": "GET",
				"http.status": "200",
			},
		},
		{
			TraceID:      "trace-1",
			SpanID:       "span-2",
			ParentSpanID: "span-1",
			Name:         "operation-2",
			StartTime:    now.Add(2 * time.Millisecond),
			EndTime:      now.Add(8 * time.Millisecond),
			Duration:     6_000_000,
			ServiceName:  "service-2",
			Tags: map[string]string{
				"db.statement": "SELECT * FROM users",
			},
		},
		{
			TraceID:      "trace-2",
			SpanID:       "span-3",
			ParentSpanID: "",
			Name:         "operation-3",
			StartTime:    now.Add(15 * time.Millisecond),
			EndTime:      now.Add(25 * time.Millisecond),
			Duration:     10_000_000,
			ServiceName:  "service-1",
			Tags:         map[string]string{},
		},
	}

	// Create index
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   spans[0].StartTime.UnixNano(),
		MaxTime:   spans[2].EndTime.UnixNano(),
		SpanCount: int64(len(spans)),
		Version:   1,
		CreatedAt: now,
		Compaction: &CompactionMeta{
			Level:       1,
			Sources:     []ulid.ULID{ulid.Make()},
			CompactedAt: now,
		},
	}

	// Write Parquet block
	err := WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	// Verify files exist
	metaPath := filepath.Join(blockDir, "meta.json")
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Error("meta.json should exist")
	}

	parquetPath := filepath.Join(blockDir, "spans.parquet")
	if _, err := os.Stat(parquetPath); os.IsNotExist(err) {
		t.Error("spans.parquet should exist")
	}

	indexPath := filepath.Join(blockDir, "index.json")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Error("index.json should exist")
	}
}

// TestNewParquetBlock tests loading a Parquet block from disk
func TestNewParquetBlock(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "parquet-block")

	// Create and write a Parquet block
	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "test-op",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "test-service",
			Tags:        map[string]string{"key": "value"},
		},
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	arrowStorage.AddSpan(spans[0])

	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   now.UnixNano(),
		MaxTime:   now.Add(time.Millisecond).UnixNano(),
		SpanCount: 1,
		Version:   1,
		CreatedAt: now,
		Compaction: &CompactionMeta{
			Level: 1,
		},
	}

	err := WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	// Load the Parquet block
	pb, err := NewParquetBlock(blockDir)
	if err != nil {
		t.Fatalf("NewParquetBlock() error = %v", err)
	}
	defer pb.Close()

	// Verify metadata
	loadedMeta := pb.Meta()
	if loadedMeta.SpanCount != 1 {
		t.Errorf("Meta().SpanCount = %d, want 1", loadedMeta.SpanCount)
	}
	if loadedMeta.Level() != 1 {
		t.Errorf("Meta().Level() = %d, want 1", loadedMeta.Level())
	}

	// Verify index
	if !pb.HasIndex() {
		t.Error("HasIndex() should return true")
	}
	idx := pb.Index()
	if idx == nil {
		t.Error("Index() should not return nil")
	}
}

// TestParquetBlock_GetSpan tests retrieving a span by ID using the index
func TestParquetBlock_GetSpan(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "parquet-block")

	// Create test spans
	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "op-1",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service-1",
			Tags:        map[string]string{"tag1": "value1"},
		},
		{
			TraceID:     "trace-1",
			SpanID:      "span-2",
			Name:        "op-2",
			StartTime:   now.Add(time.Millisecond),
			EndTime:     now.Add(2 * time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service-2",
			Tags:        map[string]string{"tag2": "value2"},
		},
		{
			TraceID:     "trace-2",
			SpanID:      "span-3",
			Name:        "op-3",
			StartTime:   now.Add(2 * time.Millisecond),
			EndTime:     now.Add(3 * time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service-1",
			Tags:        map[string]string{"tag3": "value3"},
		},
	}

	// Create and write Parquet block
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(3 * time.Millisecond).UnixNano(),
		SpanCount:  int64(len(spans)),
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	err := WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	// Load and test GetSpan
	pb, err := NewParquetBlock(blockDir)
	if err != nil {
		t.Fatalf("NewParquetBlock() error = %v", err)
	}
	defer pb.Close()

	// Test retrieving each span
	for i, originalSpan := range spans {
		retrievedSpan, err := pb.GetSpanByID(originalSpan.SpanID)
		if err != nil {
			t.Errorf("GetSpanByID(%s) error = %v", originalSpan.SpanID, err)
			continue
		}

		// Verify span data
		if retrievedSpan.TraceID != originalSpan.TraceID {
			t.Errorf("Span %d: TraceID = %s, want %s", i, retrievedSpan.TraceID, originalSpan.TraceID)
		}
		if retrievedSpan.SpanID != originalSpan.SpanID {
			t.Errorf("Span %d: SpanID = %s, want %s", i, retrievedSpan.SpanID, originalSpan.SpanID)
		}
		if retrievedSpan.Name != originalSpan.Name {
			t.Errorf("Span %d: Name = %s, want %s", i, retrievedSpan.Name, originalSpan.Name)
		}
		if retrievedSpan.ServiceName != originalSpan.ServiceName {
			t.Errorf("Span %d: ServiceName = %s, want %s", i, retrievedSpan.ServiceName, originalSpan.ServiceName)
		}
		if retrievedSpan.Duration != originalSpan.Duration {
			t.Errorf("Span %d: Duration = %d, want %d", i, retrievedSpan.Duration, originalSpan.Duration)
		}

		// Verify timestamps (within 1ms tolerance due to precision)
		timeDiff := retrievedSpan.StartTime.Sub(originalSpan.StartTime).Abs()
		if timeDiff > time.Millisecond {
			t.Errorf("Span %d: StartTime diff = %v, want < 1ms", i, timeDiff)
		}

		// Verify tags
		if len(retrievedSpan.Tags) != len(originalSpan.Tags) {
			t.Errorf("Span %d: Tags length = %d, want %d", i, len(retrievedSpan.Tags), len(originalSpan.Tags))
		}
		for k, v := range originalSpan.Tags {
			if retrievedSpan.Tags[k] != v {
				t.Errorf("Span %d: Tag[%s] = %s, want %s", i, k, retrievedSpan.Tags[k], v)
			}
		}
	}

	// Test non-existent span
	_, err = pb.GetSpanByID("non-existent")
	if err == nil {
		t.Error("GetSpanByID(non-existent) should return error")
	}
}

// TestParquetBlock_ReadAll tests reading all spans from a Parquet block
func TestParquetBlock_ReadAll(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "parquet-block")

	// Create test spans
	now := time.Now()
	originalSpans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "op-1",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service-1",
			Tags:        map[string]string{"key": "value"},
		},
		{
			TraceID:     "trace-1",
			SpanID:      "span-2",
			Name:        "op-2",
			StartTime:   now.Add(time.Millisecond),
			EndTime:     now.Add(2 * time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service-2",
			Tags:        map[string]string{},
		},
	}

	// Write Parquet block
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range originalSpans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(2 * time.Millisecond).UnixNano(),
		SpanCount:  int64(len(originalSpans)),
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	err := WriteParquetBlock(blockDir, meta, originalSpans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	// Load and read all spans
	pb, err := NewParquetBlock(blockDir)
	if err != nil {
		t.Fatalf("NewParquetBlock() error = %v", err)
	}
	defer pb.Close()

	readSpans, err := pb.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	// Verify count
	if len(readSpans) != len(originalSpans) {
		t.Fatalf("ReadAll() returned %d spans, want %d", len(readSpans), len(originalSpans))
	}

	// Verify each span
	for i, readSpan := range readSpans {
		if readSpan.SpanID != originalSpans[i].SpanID {
			t.Errorf("Span %d: SpanID = %s, want %s", i, readSpan.SpanID, originalSpans[i].SpanID)
		}
		if readSpan.TraceID != originalSpans[i].TraceID {
			t.Errorf("Span %d: TraceID = %s, want %s", i, readSpan.TraceID, originalSpans[i].TraceID)
		}
		if readSpan.Name != originalSpans[i].Name {
			t.Errorf("Span %d: Name = %s, want %s", i, readSpan.Name, originalSpans[i].Name)
		}
	}
}

// TestPersistedBlock_LoadArrowBlock tests loading an Arrow IPC block (L0)
func TestPersistedBlock_LoadArrowBlock(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "arrow-block")

	// Create an Arrow block (L0)
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	now := time.Now()
	sp := &span.Span{
		TraceID:     "trace-1",
		SpanID:      "span-1",
		Name:        "test-op",
		StartTime:   now,
		EndTime:     now.Add(time.Millisecond),
		Duration:    1_000_000,
		ServiceName: "test-service",
		Tags:        map[string]string{"test": "value"},
	}
	arrowStorage.AddSpan(sp)
	arrowStorage.Flush()

	meta := &BlockMeta{
		ULID:      ulid.Make(),
		MinTime:   now.UnixNano(),
		MaxTime:   now.Add(time.Millisecond).UnixNano(),
		SpanCount: 1,
		Version:   1,
		CreatedAt: now,
		// No Compaction metadata = L0
	}

	err := FlushBlock(blockDir, meta, arrowStorage.GetRecords(), arrowStorage.Schema(), arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("FlushBlock() error = %v", err)
	}

	// Load as ArrowBlock
	block, err := LoadBlock(blockDir)
	if err != nil {
		t.Fatalf("LoadBlock() error = %v", err)
	}
	defer block.Close()

	// Verify it's an L0 block
	if block.Meta().Level() != 0 {
		t.Errorf("Level() = %d, want 0", block.Meta().Level())
	}

	// Verify we can read spans using the unified interface
	allSpans, err := block.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(allSpans) == 0 {
		t.Error("ReadAll() should return at least one span")
	}

	// Verify index
	idx := block.Index()
	if idx == nil {
		t.Fatal("Index() should not return nil")
	}

	_, found := idx.LookupSpanID("span-1")
	if !found {
		t.Error("Index should contain span-1")
	}
}

// TestPersistedBlock_LoadParquetBlock tests loading a Parquet block (L1+)
func TestPersistedBlock_LoadParquetBlock(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "parquet-block")

	// Create a Parquet block (L1)
	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "test-op",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "test-service",
			Tags:        map[string]string{"test": "value"},
		},
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	arrowStorage.AddSpan(spans[0])

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(time.Millisecond).UnixNano(),
		SpanCount:  1,
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	err := WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	// Load as ParquetBlock using LoadBlock
	block, err := LoadBlock(blockDir)
	if err != nil {
		t.Fatalf("LoadBlock() error = %v", err)
	}
	defer block.Close()

	// Verify it's an L1 block
	if block.Meta().Level() != 1 {
		t.Errorf("Level() = %d, want 1", block.Meta().Level())
	}

	// Verify it's a Parquet block by checking it's not an Arrow block
	if _, ok := block.(*ArrowBlock); ok {
		t.Error("L1 block should not be an Arrow block")
	}

	// Verify index
	idx := block.Index()
	if idx == nil {
		t.Fatal("Index() should not return nil")
	}

	_, found := idx.LookupSpanID("span-1")
	if !found {
		t.Error("Index should contain span-1")
	}
}

// TestParquetBlock_RoundTrip tests writing and reading Parquet with data integrity
func TestParquetBlock_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "roundtrip-block")

	// Create complex test data
	now := time.Now()
	originalSpans := []*span.Span{
		{
			TraceID:      "trace-abc123",
			SpanID:       "span-001",
			ParentSpanID: "",
			Name:         "http.request",
			StartTime:    now,
			EndTime:      now.Add(100 * time.Millisecond),
			Duration:     100_000_000,
			ServiceName:  "api-gateway",
			Tags: map[string]string{
				"http.method":      "POST",
				"http.url":         "/api/v1/users",
				"http.status_code": "201",
				"user.id":          "12345",
			},
		},
		{
			TraceID:      "trace-abc123",
			SpanID:       "span-002",
			ParentSpanID: "span-001",
			Name:         "db.query",
			StartTime:    now.Add(10 * time.Millisecond),
			EndTime:      now.Add(60 * time.Millisecond),
			Duration:     50_000_000,
			ServiceName:  "database-service",
			Tags: map[string]string{
				"db.type":      "postgres",
				"db.statement": "INSERT INTO users VALUES (...)",
				"db.instance":  "primary",
			},
		},
		{
			TraceID:      "trace-abc123",
			SpanID:       "span-003",
			ParentSpanID: "span-001",
			Name:         "cache.set",
			StartTime:    now.Add(65 * time.Millisecond),
			EndTime:      now.Add(70 * time.Millisecond),
			Duration:     5_000_000,
			ServiceName:  "cache-service",
			Tags: map[string]string{
				"cache.key": "user:12345",
			},
		},
	}

	// Write Parquet block
	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range originalSpans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(100 * time.Millisecond).UnixNano(),
		SpanCount:  int64(len(originalSpans)),
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	err := WriteParquetBlock(blockDir, meta, originalSpans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	// Read back and verify
	pb, err := NewParquetBlock(blockDir)
	if err != nil {
		t.Fatalf("NewParquetBlock() error = %v", err)
	}
	defer pb.Close()

	readSpans, err := pb.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if len(readSpans) != len(originalSpans) {
		t.Fatalf("ReadAll() returned %d spans, want %d", len(readSpans), len(originalSpans))
	}

	// Verify complete data integrity for each span
	for i, original := range originalSpans {
		read := readSpans[i]

		if read.TraceID != original.TraceID {
			t.Errorf("Span %d: TraceID = %s, want %s", i, read.TraceID, original.TraceID)
		}
		if read.SpanID != original.SpanID {
			t.Errorf("Span %d: SpanID = %s, want %s", i, read.SpanID, original.SpanID)
		}
		if read.ParentSpanID != original.ParentSpanID {
			t.Errorf("Span %d: ParentSpanID = %s, want %s", i, read.ParentSpanID, original.ParentSpanID)
		}
		if read.Name != original.Name {
			t.Errorf("Span %d: Name = %s, want %s", i, read.Name, original.Name)
		}
		if read.ServiceName != original.ServiceName {
			t.Errorf("Span %d: ServiceName = %s, want %s", i, read.ServiceName, original.ServiceName)
		}
		if read.Duration != original.Duration {
			t.Errorf("Span %d: Duration = %d, want %d", i, read.Duration, original.Duration)
		}

		// Verify timestamps
		if !read.StartTime.Equal(original.StartTime) {
			t.Errorf("Span %d: StartTime = %v, want %v", i, read.StartTime, original.StartTime)
		}
		if !read.EndTime.Equal(original.EndTime) {
			t.Errorf("Span %d: EndTime = %v, want %v", i, read.EndTime, original.EndTime)
		}

		// Verify all tags
		if len(read.Tags) != len(original.Tags) {
			t.Errorf("Span %d: Tags count = %d, want %d", i, len(read.Tags), len(original.Tags))
		}
		for key, expectedValue := range original.Tags {
			if actualValue, ok := read.Tags[key]; !ok {
				t.Errorf("Span %d: Missing tag %s", i, key)
			} else if actualValue != expectedValue {
				t.Errorf("Span %d: Tag[%s] = %s, want %s", i, key, actualValue, expectedValue)
			}
		}
	}

	// Also verify index-based retrieval
	for _, original := range originalSpans {
		retrieved, err := pb.GetSpanByID(original.SpanID)
		if err != nil {
			t.Errorf("GetSpanByID(%s) error = %v", original.SpanID, err)
			continue
		}
		if retrieved.SpanID != original.SpanID {
			t.Errorf("GetSpan(%s) returned wrong span: %s", original.SpanID, retrieved.SpanID)
		}
	}
}

// TestParquetBlock_EmptyTags tests handling of spans with no tags
func TestParquetBlock_EmptyTags(t *testing.T) {
	tmpDir := t.TempDir()
	blockDir := filepath.Join(tmpDir, "empty-tags-block")

	now := time.Now()
	spans := []*span.Span{
		{
			TraceID:     "trace-1",
			SpanID:      "span-1",
			Name:        "op-no-tags",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service",
			Tags:        map[string]string{}, // Empty tags
		},
		{
			TraceID:     "trace-1",
			SpanID:      "span-2",
			Name:        "op-nil-tags",
			StartTime:   now,
			EndTime:     now.Add(time.Millisecond),
			Duration:    1_000_000,
			ServiceName: "service",
			Tags:        nil, // Nil tags
		},
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(time.Millisecond).UnixNano(),
		SpanCount:  2,
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	err := WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())
	if err != nil {
		t.Fatalf("WriteParquetBlock() error = %v", err)
	}

	pb, err := NewParquetBlock(blockDir)
	if err != nil {
		t.Fatalf("NewParquetBlock() error = %v", err)
	}
	defer pb.Close()

	readSpans, err := pb.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if len(readSpans) != 2 {
		t.Fatalf("ReadAll() returned %d spans, want 2", len(readSpans))
	}

	// Both should have non-nil but possibly empty tags maps
	for i, sp := range readSpans {
		if sp.Tags == nil {
			// Nil is acceptable
			continue
		}
		if len(sp.Tags) != 0 {
			t.Errorf("Span %d: expected empty tags, got %d tags", i, len(sp.Tags))
		}
	}
}

func BenchmarkManager_FlushHead(b *testing.B) {
	tmpDir := b.TempDir()

	cfg := &Config{
		Dir:              filepath.Join(tmpDir, "blocks"),
		MaxBlockDuration: 1 * time.Hour,
		MaxBlockSpans:    10000,
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()

	mgr, err := NewManager(cfg, arrowStorage)
	if err != nil {
		b.Fatalf("NewManager() error = %v", err)
	}
	defer mgr.Close()

	// Add test data
	now := time.Now()
	for i := range 1000 {
		sp := &span.Span{
			SpanID:      "span-" + string(rune(i)),
			StartTime:   now.Add(time.Duration(i) * time.Millisecond),
			EndTime:     now.Add(time.Duration(i+1) * time.Millisecond),
			ServiceName: "service",
		}
		arrowStorage.AddSpan(sp)
	}
	arrowStorage.Flush()

	mgr.UpdateHeadTimeRange(now.UnixNano(), now.Add(1001*time.Millisecond).UnixNano())

	for i := 0; b.Loop(); i++ {
		mgr.FlushHead(i, i)
	}
}

func BenchmarkParquetBlock_GetSpan(b *testing.B) {
	tmpDir := b.TempDir()
	blockDir := filepath.Join(tmpDir, "bench-block")

	// Create block with 10,000 spans
	now := time.Now()
	spans := make([]*span.Span, 10000)
	for i := range 10000 {
		spans[i] = &span.Span{
			TraceID:     "trace-1",
			SpanID:      ulid.Make().String(),
			Name:        "operation",
			StartTime:   now.Add(time.Duration(i) * time.Microsecond),
			EndTime:     now.Add(time.Duration(i+1) * time.Microsecond),
			Duration:    1000,
			ServiceName: "service",
			Tags:        map[string]string{"key": "value"},
		}
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(10000 * time.Microsecond).UnixNano(),
		SpanCount:  10000,
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())

	pb, _ := NewParquetBlock(blockDir)
	defer pb.Close()

	// Pick a span ID in the middle
	targetSpanID := spans[5000].SpanID

	for b.Loop() {
		_, err := pb.GetSpanByID(targetSpanID)
		if err != nil {
			b.Fatalf("GetSpanByID() error = %v", err)
		}
	}
}

func BenchmarkParquetBlock_ReadAll(b *testing.B) {
	tmpDir := b.TempDir()
	blockDir := filepath.Join(tmpDir, "bench-block")

	// Create block with 1,000 spans
	now := time.Now()
	spans := make([]*span.Span, 1000)
	for i := range 1000 {
		spans[i] = &span.Span{
			TraceID:     "trace-1",
			SpanID:      ulid.Make().String(),
			Name:        "operation",
			StartTime:   now.Add(time.Duration(i) * time.Microsecond),
			EndTime:     now.Add(time.Duration(i+1) * time.Microsecond),
			Duration:    1000,
			ServiceName: "service",
			Tags:        map[string]string{"key": "value"},
		}
	}

	arrowStorage := storage.NewArrowStorage()
	defer arrowStorage.Release()
	for _, sp := range spans {
		arrowStorage.AddSpan(sp)
	}

	meta := &BlockMeta{
		ULID:       ulid.Make(),
		MinTime:    now.UnixNano(),
		MaxTime:    now.Add(1000 * time.Microsecond).UnixNano(),
		SpanCount:  1000,
		Version:    1,
		CreatedAt:  now,
		Compaction: &CompactionMeta{Level: 1},
	}

	WriteParquetBlock(blockDir, meta, spans, arrowStorage.GetIndex())

	pb, _ := NewParquetBlock(blockDir)
	defer pb.Close()

	for b.Loop() {
		_, err := pb.ReadAll()
		if err != nil {
			b.Fatalf("ReadAll() error = %v", err)
		}
	}
}
