package query

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/saswatamcode/artemis/pkg/span"
)

// MatchType defines the type of matching operation
type MatchType int

const (
	// MatchEqual matches exact string equality
	MatchEqual MatchType = iota
	// MatchNotEqual matches anything except exact string
	MatchNotEqual
	// MatchRegexp matches using regular expression
	MatchRegexp
	// MatchNotRegexp matches anything not matching the regexp
	MatchNotRegexp
)

// Matcher matches span attributes/tags
type Matcher struct {
	Type  MatchType
	Name  string
	Value string
	re    *regexp.Regexp
}

// NewMatcher creates a new matcher
func NewMatcher(typ MatchType, name, value string) (*Matcher, error) {
	m := &Matcher{
		Type:  typ,
		Name:  name,
		Value: value,
	}

	// Compile regexp if needed
	if typ == MatchRegexp || typ == MatchNotRegexp {
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return nil, fmt.Errorf("invalid regexp %q: %w", value, err)
		}
		m.re = re
	}

	return m, nil
}

// MatchesValue checks if a value matches this matcher
// Used for filtering at metadata level before fetching full spans
func (m *Matcher) MatchesValue(val string, exists bool) bool {
	switch m.Type {
	case MatchEqual:
		return exists && val == m.Value
	case MatchNotEqual:
		return !exists || val != m.Value
	case MatchRegexp:
		return exists && m.re.MatchString(val)
	case MatchNotRegexp:
		return !exists || !m.re.MatchString(val)
	}
	return false
}

// Matches returns true if the matcher matches the given span
func (m *Matcher) Matches(s *span.Span) bool {
	// Check top-level fields first (these are NOT in Tags map)
	var val string
	var ok bool

	switch m.Name {
	case "trace_id":
		val = s.TraceID
		ok = val != ""
	case "span_id":
		val = s.SpanID
		ok = val != ""
	case "parent_span_id":
		val = s.ParentSpanID
		ok = val != ""
	case "name":
		val = s.Name
		ok = val != ""
	case "service.name", "service_name":
		val = s.ServiceName
		ok = val != ""
	default:
		// Fall back to tag lookup
		val, ok = s.Tags[m.Name]
	}

	switch m.Type {
	case MatchEqual:
		return ok && val == m.Value
	case MatchNotEqual:
		return !ok || val != m.Value
	case MatchRegexp:
		return ok && m.re.MatchString(val)
	case MatchNotRegexp:
		return !ok || !m.re.MatchString(val)
	}

	return false
}

// Matchers is a list of matchers
type Matchers []*Matcher

// Matches returns true if all matchers match the span
func (ms Matchers) Matches(s *span.Span) bool {
	for _, m := range ms {
		if !m.Matches(s) {
			return false
		}
	}
	return true
}
