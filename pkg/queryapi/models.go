package queryapi

// Metadata responses
type MetadataAttributeKeysResponse struct {
	Status string                     `json:"status"`
	Data   MetadataAttributeKeysData  `json:"data"`
}

type MetadataAttributeKeysData struct {
	AttributeKeys []string `json:"attributeKeys"`
	Count         int      `json:"count"`
}

type MetadataAttributeValuesResponse struct {
	Status string                       `json:"status"`
	Data   MetadataAttributeValuesData  `json:"data"`
}

type MetadataAttributeValuesData struct {
	AttributeKey string   `json:"attributeKey"`
	Values       []string `json:"values"`
	Count        int      `json:"count"`
	Limited      bool     `json:"limited"`
}

// Query range response (Prometheus-compatible with exemplars)
type QueryRangeResponse struct {
	Status string         `json:"status"`
	Data   QueryRangeData `json:"data"`
}

type QueryRangeData struct {
	ResultType string      `json:"resultType"` // "vector", "matrix", "scalar"
	Result     interface{} `json:"result"`     // Prometheus format
	Stats      *QueryStats `json:"stats,omitempty"`
}

type QueryStats struct {
	SpansScanned  int64  `json:"spansScanned"`
	BlocksScanned int64  `json:"blocksScanned"`
	ExecutionTime string `json:"executionTime"`
}

// Exemplar attached to metric data points
type Exemplar struct {
	Timestamp int64             `json:"timestamp"` // Unix seconds
	TraceID   string            `json:"traceID"`
	SpanID    string            `json:"spanID"`
	Duration  int64             `json:"duration"`  // Nanoseconds
	Labels    map[string]string `json:"labels"`
}

// Trace detail response
type TraceResponse struct {
	Status string    `json:"status"`
	Data   TraceData `json:"data"`
}

type TraceData struct {
	TraceID   string       `json:"traceID"`
	Spans     []SpanDetail `json:"spans"`
	SpanCount int          `json:"spanCount"`
}

type SpanDetail struct {
	SpanID       string            `json:"spanID"`
	Name         string            `json:"name"`
	StartTime    int64             `json:"startTime"`    // Unix nanoseconds
	EndTime      int64             `json:"endTime"`      // Unix nanoseconds
	Duration     int64             `json:"duration"`     // Nanoseconds
	ServiceName  string            `json:"serviceName"`
	Attributes   map[string]string `json:"attributes"`
	ParentSpanID string            `json:"parentSpanID,omitempty"`
}
