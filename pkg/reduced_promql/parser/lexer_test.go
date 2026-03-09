package parser

import (
	"strings"
	"testing"
)

// tokenDesc describes a single token for testing (type + value).
type tokenDesc struct {
	tok int
	str string
	f64 float64
}

// tokenize runs the lexer to completion and returns token descriptions.
// EOF is not included. Returns nil on lex error.
func tokenize(input string) ([]tokenDesc, error) {
	lex := NewLexer(input)
	var out []tokenDesc
	for {
		var lval yySymType
		tok := lex.Lex(&lval)
		if tok == 0 {
			if err := lex.Err(); err != nil {
				return nil, err
			}
			break
		}
		out = append(out, tokenDesc{tok: tok, str: lval.str, f64: lval.f64})
	}
	return out, nil
}

func TestLexer_Punctuation(t *testing.T) {
	tests := []struct {
		input string
		want  []int
	}{
		{"(", []int{int('(')}},
		{")", []int{int(')')}},
		{"{", []int{int('{')}},
		{"}", []int{int('}')}},
		{"[", []int{int('[')}},
		{"]", []int{int(']')}},
		{",", []int{int(',')}},
		{"(){}[],", []int{int('('), int(')'), int('{'), int('}'), int('['), int(']'), int(',')}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := tokenize(tt.input)
			if err != nil {
				t.Fatalf("tokenize: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d tokens, want %d: %v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i].tok != tt.want[i] {
					t.Errorf("token %d: got %d, want %d", i, got[i].tok, tt.want[i])
				}
			}
		})
	}
}

func TestLexer_Operators(t *testing.T) {
	tests := []struct {
		input string
		want  []int
	}{
		{"=", []int{EQ}},
		{"=~", []int{RE}},
		{"!=", []int{NEQ}},
		{"!~", []int{NRE}},
		{"= =~ != !~", []int{EQ, RE, NEQ, NRE}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := tokenize(tt.input)
			if err != nil {
				t.Fatalf("tokenize: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d tokens, want %d: %v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i].tok != tt.want[i] {
					t.Errorf("token %d: got %d, want %d", i, got[i].tok, tt.want[i])
				}
			}
		})
	}
}

func TestLexer_Strings(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`""`, ""},
		{`"foo"`, "foo"},
		{`"500"`, "500"},
		{`"req.status"`, "req.status"},
		{`"\"quoted\""`, `"quoted"`},
		{`"\\"`, `\`},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := tokenize(tt.input)
			if err != nil {
				t.Fatalf("tokenize: %v", err)
			}
			if len(got) != 1 || got[0].tok != STRING {
				t.Fatalf("got %v, want single STRING token", got)
			}
			if got[0].str != tt.want {
				t.Errorf("got str %q, want %q", got[0].str, tt.want)
			}
		})
	}
}

func TestLexer_Numbers(t *testing.T) {
	tests := []struct {
		input string
		want  float64
	}{
		{"0", 0},
		{"1", 1},
		{"42", 42},
		{"0.95", 0.95},
		{"1.5", 1.5},
		{"1e0", 1},
		{"1e-1", 0.1},
		{"2.5e2", 250},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := tokenize(tt.input)
			if err != nil {
				t.Fatalf("tokenize: %v", err)
			}
			if len(got) != 1 || got[0].tok != NUMBER {
				t.Fatalf("got %v, want single NUMBER token", got)
			}
			if got[0].f64 != tt.want {
				t.Errorf("got f64 %g, want %g", got[0].f64, tt.want)
			}
		})
	}
}

func TestLexer_Durations(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"5s", "5s"},
		{"1m", "1m"},
		{"1h", "1h"},
		{"2d", "2d"},
		{"1w", "1w"},
		{"1y", "1y"},
		{"30s", "30s"},
		{"5m30s", "5m30s"},
		{"1h30m", "1h30m"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := tokenize(tt.input)
			if err != nil {
				t.Fatalf("tokenize: %v", err)
			}
			if len(got) != 1 || got[0].tok != DURATION {
				t.Fatalf("got %v, want single DURATION token", got)
			}
			if got[0].str != tt.want {
				t.Errorf("got str %q, want %q", got[0].str, tt.want)
			}
		})
	}
}

func TestLexer_IdentifiersAndKeywords(t *testing.T) {
	tests := []struct {
		input string
		want  []tokenDesc
	}{
		{"rate", []tokenDesc{{tok: RATE}}},
		{"count", []tokenDesc{{tok: COUNT}}},
		{"sum", []tokenDesc{{tok: SUM}}},
		{"avg", []tokenDesc{{tok: AVG}}},
		{"min", []tokenDesc{{tok: MIN}}},
		{"max", []tokenDesc{{tok: MAX}}},
		{"by", []tokenDesc{{tok: BY}}},
		{"without", []tokenDesc{{tok: WITHOUT}}},
		{"group", []tokenDesc{{tok: GROUP}}},
		{"histogram_quantile", []tokenDesc{{tok: HISTOGRAM_QUANTILE}}},
		{"heatmap", []tokenDesc{{tok: HEATMAP}}},
		{"prometheus", []tokenDesc{{tok: IDENT, str: "prometheus"}}},
		{"req.status", []tokenDesc{{tok: IDENT, str: "req.status"}}},
		{"_metric", []tokenDesc{{tok: IDENT, str: "_metric"}}},
		{"metric_name", []tokenDesc{{tok: IDENT, str: "metric_name"}}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := tokenize(tt.input)
			if err != nil {
				t.Fatalf("tokenize: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d tokens, want %d: %v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i].tok != tt.want[i].tok || got[i].str != tt.want[i].str {
					t.Errorf("token %d: got %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestLexer_WhitespaceAndComments(t *testing.T) {
	// Comments and whitespace are skipped; we only see the following token.
	got, err := tokenize("  \t\n  # comment\n  rate  ")
	if err != nil {
		t.Fatalf("tokenize: %v", err)
	}
	if len(got) != 1 || got[0].tok != RATE {
		t.Fatalf("got %v, want single RATE after whitespace/comment", got)
	}
}

func TestLexer_Sequence(t *testing.T) {
	input := `rate({req.status="500"}[5m])`
	got, err := tokenize(input)
	if err != nil {
		t.Fatalf("tokenize: %v", err)
	}
	wantTypes := []int{RATE, int('('), int('{'), IDENT, EQ, STRING, int('}'), int('['), DURATION, int(']'), int(')')}
	if len(got) != len(wantTypes) {
		t.Fatalf("got %d tokens, want %d: %v", len(got), len(wantTypes), got)
	}
	for i := range wantTypes {
		if got[i].tok != wantTypes[i] {
			t.Errorf("token %d: got %d, want %d", i, got[i].tok, wantTypes[i])
		}
	}
	if got[3].str != "req.status" {
		t.Errorf("label name: got %q", got[3].str)
	}
	if got[5].str != "500" {
		t.Errorf("label value: got %q", got[5].str)
	}
	if got[8].str != "5m" {
		t.Errorf("duration: got %q", got[8].str)
	}
}

func TestLexer_Errors(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantErrCont string
	}{
		{"unexpected !", "!", "unexpected '!'"},
		{"unterminated string", `"foo`, "unterminated string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tokenize(tt.input)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if tt.wantErrCont != "" {
				if !strings.Contains(err.Error(), tt.wantErrCont) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.wantErrCont)
				}
			}
		})
	}
}
