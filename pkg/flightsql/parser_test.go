package flightsql

import (
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/query"
)

func TestParseSQLBasic(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantCols   []string
		wantLimit  int
		wantOffset int
		wantErr    bool
	}{
		{
			name:       "select all",
			sql:        "SELECT * FROM spans",
			wantCols:   []string{"*"},
			wantLimit:  -1,
			wantOffset: 0,
		},
		{
			name:       "select specific columns",
			sql:        "SELECT trace_id, service_name FROM spans",
			wantCols:   []string{"trace_id", "service_name"},
			wantLimit:  -1,
			wantOffset: 0,
		},
		{
			name:       "with limit",
			sql:        "SELECT * FROM spans LIMIT 10",
			wantCols:   []string{"*"},
			wantLimit:  10,
			wantOffset: 0,
		},
		{
			name:       "with limit and offset",
			sql:        "SELECT * FROM spans LIMIT 10 OFFSET 5",
			wantCols:   []string{"*"},
			wantLimit:  10,
			wantOffset: 5,
		},
		{
			name:    "invalid sql",
			sql:     "INVALID SQL",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseSQL(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if len(q.Columns) != len(tt.wantCols) {
				t.Errorf("ParseSQL() columns = %v, want %v", q.Columns, tt.wantCols)
			}

			if q.Limit != tt.wantLimit {
				t.Errorf("ParseSQL() limit = %v, want %v", q.Limit, tt.wantLimit)
			}

			if q.Offset != tt.wantOffset {
				t.Errorf("ParseSQL() offset = %v, want %v", q.Offset, tt.wantOffset)
			}
		})
	}
}

func TestParseSQLWithWhere(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		wantMatchers int
		wantTimeRange bool
	}{
		{
			name:         "simple equality",
			sql:          "SELECT * FROM spans WHERE service_name = 'test'",
			wantMatchers: 1,
		},
		{
			name:         "not equal",
			sql:          "SELECT * FROM spans WHERE service_name != 'test'",
			wantMatchers: 1,
		},
		{
			name:         "multiple conditions",
			sql:          "SELECT * FROM spans WHERE service_name = 'test' AND trace_id = '123'",
			wantMatchers: 2,
		},
		{
			name:          "time range",
			sql:           "SELECT * FROM spans WHERE start_time >= 1000000000 AND end_time <= 2000000000",
			wantMatchers:  0,
			wantTimeRange: true,
		},
		{
			name:         "like pattern",
			sql:          "SELECT * FROM spans WHERE name LIKE 'GET%'",
			wantMatchers: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseSQL(tt.sql)
			if err != nil {
				t.Fatalf("ParseSQL() error = %v", err)
			}

			if len(q.Matchers) != tt.wantMatchers {
				t.Errorf("ParseSQL() matchers count = %v, want %v", len(q.Matchers), tt.wantMatchers)
			}

			if (q.TimeRange != nil) != tt.wantTimeRange {
				t.Errorf("ParseSQL() has time range = %v, want %v", q.TimeRange != nil, tt.wantTimeRange)
			}
		})
	}
}

func TestParseSQLOrderBy(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantColumn string
		wantDesc   bool
	}{
		{
			name:       "order by asc",
			sql:        "SELECT * FROM spans ORDER BY duration ASC",
			wantColumn: "duration",
			wantDesc:   false,
		},
		{
			name:       "order by desc",
			sql:        "SELECT * FROM spans ORDER BY duration DESC",
			wantColumn: "duration",
			wantDesc:   true,
		},
		{
			name:       "order by default (asc)",
			sql:        "SELECT * FROM spans ORDER BY start_time",
			wantColumn: "start_time",
			wantDesc:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseSQL(tt.sql)
			if err != nil {
				t.Fatalf("ParseSQL() error = %v", err)
			}

			if len(q.OrderBy) == 0 {
				t.Fatal("ParseSQL() no order by clause found")
			}

			if q.OrderBy[0].Column != tt.wantColumn {
				t.Errorf("ParseSQL() order by column = %v, want %v", q.OrderBy[0].Column, tt.wantColumn)
			}

			if q.OrderBy[0].Descending != tt.wantDesc {
				t.Errorf("ParseSQL() order by descending = %v, want %v", q.OrderBy[0].Descending, tt.wantDesc)
			}
		})
	}
}

func TestParseWhereClause(t *testing.T) {
	tests := []struct {
		name      string
		where     string
		wantCount int
		wantType  query.MatchType
	}{
		{
			name:      "equal",
			where:     "service_name = 'test'",
			wantCount: 1,
			wantType:  query.MatchEqual,
		},
		{
			name:      "not equal",
			where:     "service_name != 'test'",
			wantCount: 1,
			wantType:  query.MatchNotEqual,
		},
		{
			name:      "like",
			where:     "name LIKE 'GET%'",
			wantCount: 1,
			wantType:  query.MatchRegexp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matchers, _, err := parseWhereClause(tt.where)
			if err != nil {
				t.Fatalf("parseWhereClause() error = %v", err)
			}

			if len(matchers) != tt.wantCount {
				t.Errorf("parseWhereClause() matcher count = %v, want %v", len(matchers), tt.wantCount)
			}

			if len(matchers) > 0 && matchers[0].Type != tt.wantType {
				t.Errorf("parseWhereClause() matcher type = %v, want %v", matchers[0].Type, tt.wantType)
			}
		})
	}
}

func TestParseWhereClauseTimeRange(t *testing.T) {
	where := "start_time >= 1000000000000000000 AND end_time <= 2000000000000000000"
	matchers, tr, err := parseWhereClause(where)
	if err != nil {
		t.Fatalf("parseWhereClause() error = %v", err)
	}

	if len(matchers) != 0 {
		t.Errorf("parseWhereClause() expected no matchers, got %d", len(matchers))
	}

	if tr == nil {
		t.Fatal("parseWhereClause() expected time range, got nil")
	}

	expectedStart := time.Unix(0, 1000000000000000000)
	expectedEnd := time.Unix(0, 2000000000000000000)

	if !tr.Start.Equal(expectedStart) {
		t.Errorf("parseWhereClause() start time = %v, want %v", tr.Start, expectedStart)
	}

	if !tr.End.Equal(expectedEnd) {
		t.Errorf("parseWhereClause() end time = %v, want %v", tr.End, expectedEnd)
	}
}
