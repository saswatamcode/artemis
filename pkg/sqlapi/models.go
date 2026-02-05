package sqlapi

// SQLQueryRequest represents a SQL query request
type SQLQueryRequest struct {
	Query string `json:"query"`
}

// SQLQueryResponse represents a SQL query response
type SQLQueryResponse struct {
	Success  bool                     `json:"success"`
	Columns  []string                 `json:"columns"`
	RowCount int                      `json:"row_count"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	Error    string                   `json:"error,omitempty"`
}
