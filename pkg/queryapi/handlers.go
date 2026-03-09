package queryapi

import (
	"log/slog"
	"net/http"
	"sort"
	"strconv"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/query"
)

// handleMetadataAttributeKeys returns all available attribute keys across all blocks
// GET /api/v1/metadata/attribute_keys?start=X&end=Y
//
// This endpoint efficiently discovers all unique attribute keys by reading only
// the schema from each block's attributes.parquet file (no data I/O).
func (s *Server) handleMetadataAttributeKeys(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Parse optional time range to filter blocks
	var timeRange *query.TimeRange
	if startStr := q.Get("start"); startStr != "" {
		start, end, err := parseTimeRange(startStr, q.Get("end"))
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		timeRange = query.NewTimeRange(start, end)
	}

	s.logger.Debug("Processing metadata attribute_keys request",
		slog.Any("time_range", timeRange))

	// Get all blocks
	blocks := s.db.GetBlocks()
	keysSet := make(map[string]bool)

	// Read attribute keys from each block
	for _, blk := range blocks {
		meta := blk.Meta()

		// Skip blocks outside time range if specified
		if timeRange != nil && !timeRange.Overlaps(meta.MinTime, meta.MaxTime) {
			continue
		}

		// Read attribute keys from the block (schema-only read, very fast)
		keys, err := block.ReadAttributeKeysFromParquet(blk.Dir())
		if err != nil {
			s.logger.Warn("Failed to read attribute keys from block",
				slog.String("block", meta.ULID.String()),
				slog.String("error", err.Error()))
			continue
		}

		// Merge keys into set
		for _, key := range keys {
			keysSet[key] = true
		}
	}

	// Convert to sorted list
	keys := make([]string, 0, len(keysSet))
	for key := range keysSet {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	response := MetadataAttributeKeysResponse{
		Status: "success",
		Data: MetadataAttributeKeysData{
			AttributeKeys: keys,
			Count:         len(keys),
		},
	}

	respondJSON(w, response)
}

// handleMetadataAttributeValues returns all values for a specific attribute key
// GET /api/v1/metadata/attribute_values?key=K&start=X&end=Y&limit=N
//
// This endpoint efficiently discovers attribute values by using column projection
// to read only the requested attribute column from attributes.parquet files.
func (s *Server) handleMetadataAttributeValues(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Attribute key is required
	attrKey := q.Get("key")
	if attrKey == "" {
		respondError(w, http.StatusBadRequest, "query parameter 'key' is required")
		return
	}

	// Parse optional limit (default 1000)
	limit := 1000
	if limitStr := q.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	// Parse optional time range to filter blocks
	var timeRange *query.TimeRange
	if startStr := q.Get("start"); startStr != "" {
		start, end, err := parseTimeRange(startStr, q.Get("end"))
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		timeRange = query.NewTimeRange(start, end)
	}

	s.logger.Debug("Processing metadata attribute_values request",
		slog.String("key", attrKey),
		slog.Int("limit", limit),
		slog.Any("time_range", timeRange))

	// Get all blocks
	blocks := s.db.GetBlocks()
	valuesSet := make(map[string]bool)
	limited := false

	// Read attribute values from each block
	for _, blk := range blocks {
		meta := blk.Meta()

		// Skip blocks outside time range if specified
		if timeRange != nil && !timeRange.Overlaps(meta.MinTime, meta.MaxTime) {
			continue
		}

		// Early termination if we've reached the limit
		if len(valuesSet) >= limit {
			limited = true
			break
		}

		// Read attribute values from the block
		// This uses efficient column projection to read only the attribute column
		values, err := block.ReadAttributeValuesFromParquet(blk.Dir(), attrKey, limit-len(valuesSet))
		if err != nil {
			s.logger.Warn("Failed to read attribute values from block",
				slog.String("block", meta.ULID.String()),
				slog.String("key", attrKey),
				slog.String("error", err.Error()))
			continue
		}

		// Merge values into set
		for _, value := range values {
			if len(valuesSet) >= limit {
				limited = true
				break
			}
			valuesSet[value] = true
		}
	}

	// Convert to sorted list
	values := make([]string, 0, len(valuesSet))
	for value := range valuesSet {
		values = append(values, value)
	}
	sort.Strings(values)

	response := MetadataAttributeValuesResponse{
		Status: "success",
		Data: MetadataAttributeValuesData{
			AttributeKey: attrKey,
			Values:       values,
			Count:        len(values),
			Limited:      limited,
		},
	}

	respondJSON(w, response)
}
