package flightsql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/saswatamcode/artemis/pkg/query"
)

// SQLQuery represents a parsed SQL query
type SQLQuery struct {
	Columns      []string
	Matchers     []*query.Matcher
	TimeRange    *query.TimeRange
	WhereFilters []WhereFilter // Additional filters for Arrow compute
	OrderBy      []OrderByClause
	Limit        int
	Offset       int
}

// OrderByClause represents an ORDER BY clause
type OrderByClause struct {
	Column     string
	Descending bool
}

// WhereFilter represents a WHERE condition for Arrow compute
type WhereFilter struct {
	Column   string
	Operator string // ">", "<", ">=", "<=", "=", "!="
	Value    string
}

// ParseSQL parses a simple SQL SELECT statement
// Supports: SELECT columns FROM table WHERE conditions ORDER BY column LIMIT n
func ParseSQL(sql string) (*SQLQuery, error) {
	sql = strings.TrimSpace(sql)

	// Simple regex-based parser for Phase 1
	// Extract SELECT columns
	selectRe := regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM`)
	selectMatch := selectRe.FindStringSubmatch(sql)
	if selectMatch == nil {
		return nil, fmt.Errorf("invalid SELECT statement")
	}

	columnsStr := strings.TrimSpace(selectMatch[1])
	var columns []string
	if columnsStr == "*" {
		columns = []string{"*"}
	} else {
		for _, col := range strings.Split(columnsStr, ",") {
			columns = append(columns, strings.TrimSpace(col))
		}
	}

	result := &SQLQuery{
		Columns: columns,
		Limit:   -1, // No limit by default
		Offset:  0,  // No offset by default
	}

	// Extract WHERE clause
	whereRe := regexp.MustCompile(`(?i)WHERE\s+(.+?)(?:\s+ORDER BY|\s+LIMIT|$)`)
	whereMatch := whereRe.FindStringSubmatch(sql)
	if whereMatch != nil {
		whereClause := strings.TrimSpace(whereMatch[1])
		matchers, timeRange, err := parseWhereClause(whereClause)
		if err != nil {
			return nil, fmt.Errorf("failed to parse WHERE clause: %w", err)
		}
		result.Matchers = matchers
		result.TimeRange = timeRange
	}

	// Extract ORDER BY
	orderByRe := regexp.MustCompile(`(?i)ORDER BY\s+(\w+)(?:\s+(ASC|DESC))?`)
	orderByMatch := orderByRe.FindStringSubmatch(sql)
	if orderByMatch != nil {
		column := orderByMatch[1]
		descending := strings.ToUpper(orderByMatch[2]) == "DESC"
		result.OrderBy = []OrderByClause{{Column: column, Descending: descending}}
	}

	// Extract LIMIT and OFFSET
	limitRe := regexp.MustCompile(`(?i)LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?`)
	limitMatch := limitRe.FindStringSubmatch(sql)
	if limitMatch != nil {
		limit, err := strconv.Atoi(limitMatch[1])
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT value: %w", err)
		}
		result.Limit = limit

		// Parse OFFSET if present
		if len(limitMatch) > 2 && limitMatch[2] != "" {
			offset, err := strconv.Atoi(limitMatch[2])
			if err != nil {
				return nil, fmt.Errorf("invalid OFFSET value: %w", err)
			}
			result.Offset = offset
		}
	}

	return result, nil
}

// parseWhereClause parses WHERE conditions into matchers and time range
func parseWhereClause(where string) ([]*query.Matcher, *query.TimeRange, error) {
	var matchers []*query.Matcher
	var startTime, endTime *int64

	// Split by AND (simple parser - doesn't handle OR yet)
	conditions := strings.Split(where, " AND ")

	for _, cond := range conditions {
		cond = strings.TrimSpace(cond)

		// Parse different operators in order of precedence (longest first to avoid partial matches)
		if strings.Contains(cond, "!=") {
			parts := strings.SplitN(cond, "!=", 2)
			name := strings.TrimSpace(parts[0])
			value := strings.Trim(strings.TrimSpace(parts[1]), "'\"")

			m, err := query.NewMatcher(query.MatchNotEqual, name, value)
			if err != nil {
				return nil, nil, err
			}
			matchers = append(matchers, m)
		} else if strings.Contains(cond, " LIKE ") {
			parts := strings.SplitN(cond, " LIKE ", 2)
			name := strings.TrimSpace(parts[0])
			pattern := strings.Trim(strings.TrimSpace(parts[1]), "'\"")

			// Convert SQL LIKE to regexp
			// % -> .*, _ -> .
			regexPattern := strings.ReplaceAll(pattern, "%", ".*")
			regexPattern = strings.ReplaceAll(regexPattern, "_", ".")

			m, err := query.NewMatcher(query.MatchRegexp, name, regexPattern)
			if err != nil {
				return nil, nil, err
			}
			matchers = append(matchers, m)
		} else if strings.Contains(cond, ">=") {
			parts := strings.SplitN(cond, ">=", 2)
			name := strings.TrimSpace(parts[0])
			valueStr := strings.Trim(strings.TrimSpace(parts[1]), "'\"")

			// Handle time range filtering separately for optimization
			if name == "start_time" || name == "end_time" {
				value, err := strconv.ParseInt(valueStr, 10, 64)
				if err != nil {
					return nil, nil, fmt.Errorf("invalid time value: %w", err)
				}
				if name == "start_time" {
					startTime = &value
				}
			}
			// Note: >= for other fields would need Arrow compute filtering (not implemented in MVP)
		} else if strings.Contains(cond, "<=") {
			parts := strings.SplitN(cond, "<=", 2)
			name := strings.TrimSpace(parts[0])
			valueStr := strings.Trim(strings.TrimSpace(parts[1]), "'\"")

			// Handle time range filtering separately for optimization
			if name == "start_time" || name == "end_time" {
				value, err := strconv.ParseInt(valueStr, 10, 64)
				if err != nil {
					return nil, nil, fmt.Errorf("invalid time value: %w", err)
				}
				if name == "end_time" {
					endTime = &value
				}
			}
			// Note: <= for other fields would need Arrow compute filtering (not implemented in MVP)
		} else if strings.Contains(cond, ">") && !strings.Contains(cond, ">=") {
			// Note: > operator for non-time fields would need Arrow compute filtering (not implemented in MVP)
			// Skip for now
		} else if strings.Contains(cond, "<") && !strings.Contains(cond, "<=") {
			// Note: < operator for non-time fields would need Arrow compute filtering (not implemented in MVP)
			// Skip for now
		} else if strings.Contains(cond, "=") && !strings.Contains(cond, "!=") && !strings.Contains(cond, ">=") && !strings.Contains(cond, "<=") {
			parts := strings.SplitN(cond, "=", 2)
			name := strings.TrimSpace(parts[0])
			value := strings.Trim(strings.TrimSpace(parts[1]), "'\"")

			m, err := query.NewMatcher(query.MatchEqual, name, value)
			if err != nil {
				return nil, nil, err
			}
			matchers = append(matchers, m)
		}
	}

	// Build time range if we have time filters
	var timeRange *query.TimeRange
	if startTime != nil || endTime != nil {
		start := time.Unix(0, 0)
		if startTime != nil {
			start = time.Unix(0, *startTime)
		}

		end := time.Now().Add(100 * 365 * 24 * time.Hour) // Far future
		if endTime != nil {
			end = time.Unix(0, *endTime)
		}

		timeRange = query.NewTimeRange(start, end)
	}

	return matchers, timeRange, nil
}
