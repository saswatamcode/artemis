package queryapi

import (
	"log/slog"
	"net/http"

	"github.com/saswatamcode/artemis/pkg/query"
)

// handleQueryTrace retrieves full span details for a specific trace ID
// GET /api/v1/query/trace?traceID=T&start=X&end=Y
//
// This endpoint returns all spans for a given trace in a structured format
// suitable for visualization and analysis.
//
// Parameters:
//   - traceID: The trace ID to retrieve (required)
//   - start: Optional start time hint for filtering blocks
//   - end: Optional end time hint for filtering blocks
//
// Returns: JSON with trace details including all spans
func (s *Server) handleQueryTrace(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	traceID := q.Get("traceID")
	if traceID == "" {
		respondError(w, http.StatusBadRequest, "query parameter 'traceID' is required")
		return
	}

	s.logger.Debug("Retrieving trace by ID",
		slog.String("trace_id", traceID))

	// Create matcher for trace_id
	matcher, err := query.NewMatcher(query.MatchEqual, "trace_id", traceID)
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid trace ID: "+err.Error())
		return
	}

	// Parse optional time range hint
	var timeRange *query.TimeRange
	if startStr := q.Get("start"); startStr != "" {
		start, end, err := parseTimeRange(startStr, q.Get("end"))
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		timeRange = query.NewTimeRange(start, end)
	}

	// Query spans from the database
	querier := s.db.GetQuerier()
	var result *query.SelectResult

	if timeRange != nil {
		result, err = querier.SelectWithTimeRange(timeRange, matcher)
	} else {
		result, err = querier.Select(matcher)
	}

	if err != nil {
		s.logger.Error("Failed to retrieve trace",
			slog.String("trace_id", traceID),
			slog.String("error", err.Error()))
		respondError(w, http.StatusInternalServerError, "failed to retrieve trace: "+err.Error())
		return
	}

	// Convert spans to response format
	spans := make([]SpanDetail, 0, len(result.Spans))
	for _, sp := range result.Spans {
		spans = append(spans, convertSpanToDetail(sp))
	}

	response := TraceResponse{
		Status: "success",
		Data: TraceData{
			TraceID:   traceID,
			Spans:     spans,
			SpanCount: len(spans),
		},
	}

	s.logger.Debug("Trace retrieved successfully",
		slog.String("trace_id", traceID),
		slog.Int("span_count", len(spans)))

	respondJSON(w, response)
}
