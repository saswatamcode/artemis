package planner

import (
	"fmt"

	"github.com/saswatamcode/artemis/pkg/query"
	"github.com/saswatamcode/artemis/pkg/reduced_promql/parser"
)

// convertMatcher converts an AST LabelMatcher to a query.Matcher.
//
// AST matchers use string operators: "=", "!=", "=~", "!~"
// query.Matcher uses typed MatchType enum: MatchEqual, MatchNotEqual, etc.
//
// This is the bridge between the parser layer and the execution layer.
func convertMatcher(lm parser.LabelMatcher) (*query.Matcher, error) {
	var matchType query.MatchType

	switch lm.Op {
	case "=":
		matchType = query.MatchEqual
	case "!=":
		matchType = query.MatchNotEqual
	case "=~":
		matchType = query.MatchRegexp
	case "!~":
		matchType = query.MatchNotRegexp
	default:
		return nil, fmt.Errorf("unknown matcher operator: %s", lm.Op)
	}

	// NewMatcher validates and compiles regexps if needed
	return query.NewMatcher(matchType, lm.Name, lm.Value)
}

// convertMatchers converts a slice of AST LabelMatchers to query.Matchers.
func convertMatchers(astMatchers []parser.LabelMatcher) (query.Matchers, error) {
	matchers := make([]*query.Matcher, 0, len(astMatchers))

	for _, am := range astMatchers {
		m, err := convertMatcher(am)
		if err != nil {
			return nil, fmt.Errorf("failed to convert matcher %s: %w", am.String(), err)
		}
		matchers = append(matchers, m)
	}

	return matchers, nil
}
