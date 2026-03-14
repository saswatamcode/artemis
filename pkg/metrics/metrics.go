package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// DatabaseMetrics holds Prometheus metrics for database operations
type DatabaseMetrics struct {
	// Write metrics
	spansWritten      *prometheus.CounterVec
	spansWrittenBatch prometheus.Counter
	writeDuration     *prometheus.HistogramVec
	writeErrors       *prometheus.CounterVec

	// WAL metrics
	walWrites           prometheus.Counter
	walFlushes          prometheus.Counter
	walSegmentRotations prometheus.Counter
	walWriteDuration    prometheus.Histogram

	// Storage metrics
	headBlockSpans         prometheus.Gauge
	headBlockRecordBatches prometheus.Gauge
	persistedBlocks        *prometheus.GaugeVec

	// Compaction metrics
	compactionRuns     *prometheus.CounterVec
	compactionDuration *prometheus.HistogramVec
	blocksCompacted    *prometheus.CounterVec

	// Checkpoint metrics
	checkpointCreations   prometheus.Counter
	checkpointErrors      prometheus.Counter
	walSegmentsDeleted    prometheus.Counter

	// Query metrics
	queries          *prometheus.CounterVec
	queryDuration    *prometheus.HistogramVec
	querySpansReturned *prometheus.HistogramVec
}

// NewDatabaseMetrics creates and registers database metrics
func NewDatabaseMetrics(reg prometheus.Registerer) *DatabaseMetrics {
	m := &DatabaseMetrics{
		spansWritten: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "artemis_spans_written_total",
				Help: "Total number of spans written to the database",
			},
			[]string{"method"},
		),
		spansWrittenBatch: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "artemis_spans_written_batch_total",
				Help: "Total number of batch write operations",
			},
		),
		writeDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "artemis_write_duration_seconds",
				Help:    "Duration of write operations in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation"},
		),
		writeErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "artemis_write_errors_total",
				Help: "Total number of write errors",
			},
			[]string{"component"},
		),
		walWrites: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "artemis_wal_writes_total",
				Help: "Total number of WAL write operations",
			},
		),
		walFlushes: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "artemis_wal_flushes_total",
				Help: "Total number of WAL flush operations",
			},
		),
		walSegmentRotations: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "artemis_wal_segment_rotations_total",
				Help: "Total number of WAL segment rotations",
			},
		),
		walWriteDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "artemis_wal_write_duration_seconds",
				Help:    "Duration of WAL write operations in seconds",
				Buckets: prometheus.DefBuckets,
			},
		),
		headBlockSpans: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "artemis_head_block_spans",
				Help: "Current number of spans in the head block",
			},
		),
		headBlockRecordBatches: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "artemis_head_block_record_batches",
				Help: "Current number of record batches in the head block",
			},
		),
		persistedBlocks: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "artemis_persisted_blocks",
				Help: "Number of persisted blocks by level",
			},
			[]string{"level"},
		),
		compactionRuns: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "artemis_compaction_runs_total",
				Help: "Total number of compaction runs by type and status",
			},
			[]string{"type", "status"},
		),
		compactionDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "artemis_compaction_duration_seconds",
				Help:    "Duration of compaction operations in seconds",
				Buckets: []float64{.1, .5, 1, 5, 10, 30, 60, 120, 300},
			},
			[]string{"type"},
		),
		blocksCompacted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "artemis_blocks_compacted_total",
				Help: "Total number of blocks compacted",
			},
			[]string{"from_level", "to_level"},
		),
		checkpointCreations: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "artemis_checkpoint_creations_total",
				Help: "Total number of checkpoint creations",
			},
		),
		checkpointErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "artemis_checkpoint_errors_total",
				Help: "Total number of checkpoint errors",
			},
		),
		walSegmentsDeleted: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "artemis_wal_segments_deleted_total",
				Help: "Total number of WAL segments deleted during checkpoints",
			},
		),
		queries: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "artemis_queries_total",
				Help: "Total number of queries by API and status",
			},
			[]string{"api", "status"},
		),
		queryDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "artemis_query_duration_seconds",
				Help:    "Duration of query operations in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"api", "operation"},
		),
		querySpansReturned: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "artemis_query_spans_returned",
				Help:    "Number of spans returned by queries",
				Buckets: []float64{1, 10, 50, 100, 500, 1000, 5000, 10000, 50000},
			},
			[]string{"api"},
		),
	}

	// Register all metrics
	if reg != nil {
		reg.MustRegister(
			m.spansWritten,
			m.spansWrittenBatch,
			m.writeDuration,
			m.writeErrors,
			m.walWrites,
			m.walFlushes,
			m.walSegmentRotations,
			m.walWriteDuration,
			m.headBlockSpans,
			m.headBlockRecordBatches,
			m.persistedBlocks,
			m.compactionRuns,
			m.compactionDuration,
			m.blocksCompacted,
			m.checkpointCreations,
			m.checkpointErrors,
			m.walSegmentsDeleted,
			m.queries,
			m.queryDuration,
			m.querySpansReturned,
		)
	}

	return m
}

// RecordSpanWrite records a span write operation
func (m *DatabaseMetrics) RecordSpanWrite(method string, count int) {
	if m == nil {
		return
	}
	m.spansWritten.WithLabelValues(method).Add(float64(count))
}

// RecordBatchWrite records a batch write operation
func (m *DatabaseMetrics) RecordBatchWrite() {
	if m == nil {
		return
	}
	m.spansWrittenBatch.Inc()
}

// RecordWriteDuration records write operation duration
func (m *DatabaseMetrics) RecordWriteDuration(operation string, duration float64) {
	if m == nil {
		return
	}
	m.writeDuration.WithLabelValues(operation).Observe(duration)
}

// RecordWriteError records a write error
func (m *DatabaseMetrics) RecordWriteError(component string) {
	if m == nil {
		return
	}
	m.writeErrors.WithLabelValues(component).Inc()
}

// RecordWALWrite records a WAL write operation
func (m *DatabaseMetrics) RecordWALWrite() {
	if m == nil {
		return
	}
	m.walWrites.Inc()
}

// RecordWALFlush records a WAL flush operation
func (m *DatabaseMetrics) RecordWALFlush() {
	if m == nil {
		return
	}
	m.walFlushes.Inc()
}

// RecordWALSegmentRotation records a WAL segment rotation
func (m *DatabaseMetrics) RecordWALSegmentRotation() {
	if m == nil {
		return
	}
	m.walSegmentRotations.Inc()
}

// RecordWALWriteDuration records WAL write duration
func (m *DatabaseMetrics) RecordWALWriteDuration(duration float64) {
	if m == nil {
		return
	}
	m.walWriteDuration.Observe(duration)
}

// SetHeadBlockSpans sets the current number of spans in the head block
func (m *DatabaseMetrics) SetHeadBlockSpans(count int64) {
	if m == nil {
		return
	}
	m.headBlockSpans.Set(float64(count))
}

// SetHeadBlockRecordBatches sets the current number of record batches in the head block
func (m *DatabaseMetrics) SetHeadBlockRecordBatches(count int) {
	if m == nil {
		return
	}
	m.headBlockRecordBatches.Set(float64(count))
}

// SetPersistedBlocks sets the number of persisted blocks for a given level
func (m *DatabaseMetrics) SetPersistedBlocks(level int, count int) {
	if m == nil {
		return
	}
	m.persistedBlocks.WithLabelValues(strconv.Itoa(level)).Set(float64(count))
}

// RecordCompactionRun records a compaction run
func (m *DatabaseMetrics) RecordCompactionRun(compactionType, status string) {
	if m == nil {
		return
	}
	m.compactionRuns.WithLabelValues(compactionType, status).Inc()
}

// RecordCompactionDuration records compaction duration
func (m *DatabaseMetrics) RecordCompactionDuration(compactionType string, duration float64) {
	if m == nil {
		return
	}
	m.compactionDuration.WithLabelValues(compactionType).Observe(duration)
}

// RecordBlocksCompacted records blocks compacted
func (m *DatabaseMetrics) RecordBlocksCompacted(fromLevel, toLevel int, count int) {
	if m == nil {
		return
	}
	m.blocksCompacted.WithLabelValues(
		strconv.Itoa(fromLevel),
		strconv.Itoa(toLevel),
	).Add(float64(count))
}

// RecordCheckpointCreation records a checkpoint creation
func (m *DatabaseMetrics) RecordCheckpointCreation() {
	if m == nil {
		return
	}
	m.checkpointCreations.Inc()
}

// RecordCheckpointError records a checkpoint error
func (m *DatabaseMetrics) RecordCheckpointError() {
	if m == nil {
		return
	}
	m.checkpointErrors.Inc()
}

// RecordWALSegmentsDeleted records WAL segments deleted
func (m *DatabaseMetrics) RecordWALSegmentsDeleted(count int) {
	if m == nil {
		return
	}
	m.walSegmentsDeleted.Add(float64(count))
}

// RecordQuery records a query operation
func (m *DatabaseMetrics) RecordQuery(api, status string) {
	if m == nil {
		return
	}
	m.queries.WithLabelValues(api, status).Inc()
}

// RecordQueryDuration records query duration
func (m *DatabaseMetrics) RecordQueryDuration(api, operation string, duration float64) {
	if m == nil {
		return
	}
	m.queryDuration.WithLabelValues(api, operation).Observe(duration)
}

// RecordQuerySpansReturned records the number of spans returned by a query
func (m *DatabaseMetrics) RecordQuerySpansReturned(api string, count int) {
	if m == nil {
		return
	}
	m.querySpansReturned.WithLabelValues(api).Observe(float64(count))
}

// APIMetrics holds Prometheus metrics for API operations
type APIMetrics struct {
	httpRequests       *prometheus.CounterVec
	httpRequestDuration *prometheus.HistogramVec
	httpRequestSize    *prometheus.HistogramVec
	httpResponseSize   *prometheus.HistogramVec
}

// NewAPIMetrics creates and registers API metrics
func NewAPIMetrics(reg prometheus.Registerer) *APIMetrics {
	m := &APIMetrics{
		httpRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "artemis_http_requests_total",
				Help: "Total number of HTTP requests by API, method, endpoint, and status",
			},
			[]string{"api", "method", "endpoint", "status"},
		),
		httpRequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "artemis_http_request_duration_seconds",
				Help:    "Duration of HTTP requests in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"api", "method", "endpoint"},
		),
		httpRequestSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "artemis_http_request_size_bytes",
				Help:    "Size of HTTP request bodies in bytes",
				Buckets: prometheus.ExponentialBuckets(100, 10, 7),
			},
			[]string{"api"},
		),
		httpResponseSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "artemis_http_response_size_bytes",
				Help:    "Size of HTTP response bodies in bytes",
				Buckets: prometheus.ExponentialBuckets(100, 10, 7),
			},
			[]string{"api"},
		),
	}

	// Register all metrics
	if reg != nil {
		reg.MustRegister(
			m.httpRequests,
			m.httpRequestDuration,
			m.httpRequestSize,
			m.httpResponseSize,
		)
	}

	return m
}

// RecordHTTPRequest records an HTTP request
func (m *APIMetrics) RecordHTTPRequest(api, method, endpoint, status string, duration float64, requestSize, responseSize int64) {
	if m == nil {
		return
	}
	m.httpRequests.WithLabelValues(api, method, endpoint, status).Inc()
	m.httpRequestDuration.WithLabelValues(api, method, endpoint).Observe(duration)
	if requestSize > 0 {
		m.httpRequestSize.WithLabelValues(api).Observe(float64(requestSize))
	}
	if responseSize > 0 {
		m.httpResponseSize.WithLabelValues(api).Observe(float64(responseSize))
	}
}
