package parser

import (
	"strings"
	"testing"
)

func TestParse_VectorSelector(t *testing.T) {
	tests := []struct {
		input string
		check func(t *testing.T, e Expr)
	}{
		{
			"prometheus",
			func(t *testing.T, e Expr) {
				v, ok := e.(*VectorSelector)
				if !ok {
					t.Fatalf("expected *VectorSelector, got %T", e)
				}
				if v.Metric == nil || *v.Metric != "prometheus" {
					t.Errorf("metric: got %v", v.Metric)
				}
				if len(v.Matchers) != 0 {
					t.Errorf("matchers: got %d", len(v.Matchers))
				}
			},
		},
		{
			"prometheus{}",
			func(t *testing.T, e Expr) {
				v, ok := e.(*VectorSelector)
				if !ok {
					t.Fatalf("expected *VectorSelector, got %T", e)
				}
				if v.Metric == nil || *v.Metric != "prometheus" {
					t.Errorf("metric: got %v", v.Metric)
				}
			},
		},
		{
			`{job="api"}`,
			func(t *testing.T, e Expr) {
				v, ok := e.(*VectorSelector)
				if !ok {
					t.Fatalf("expected *VectorSelector, got %T", e)
				}
				if v.Metric != nil {
					t.Errorf("expected metric-less selector, got metric %q", *v.Metric)
				}
				if len(v.Matchers) != 1 || v.Matchers[0].Name != "job" || v.Matchers[0].Op != "=" || v.Matchers[0].Value != "api" {
					t.Errorf("matchers: got %v", v.Matchers)
				}
			},
		},
		{
			`prometheus{req.status="500"}`,
			func(t *testing.T, e Expr) {
				v, ok := e.(*VectorSelector)
				if !ok {
					t.Fatalf("expected *VectorSelector, got %T", e)
				}
				if len(v.Matchers) != 1 || v.Matchers[0].Name != "req.status" || v.Matchers[0].Value != "500" {
					t.Errorf("matchers: got %v", v.Matchers)
				}
			},
		},
		{
			"{}",
			func(t *testing.T, e Expr) {
				v, ok := e.(*VectorSelector)
				if !ok {
					t.Fatalf("expected *VectorSelector, got %T", e)
				}
				if v.Metric != nil || len(v.Matchers) != 0 {
					t.Errorf("expected empty selector: metric=%v matchers=%d", v.Metric, len(v.Matchers))
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			e, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			tt.check(t, e)
		})
	}
}

func TestParse_MatrixSelector(t *testing.T) {
	e, err := Parse("prometheus{}[5m]")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	mat, ok := e.(*MatrixSelector)
	if !ok {
		t.Fatalf("expected *MatrixSelector, got %T", e)
	}
	if mat.RangeStr != "5m" {
		t.Errorf("RangeStr: got %q", mat.RangeStr)
	}
	if mat.Vector == nil || mat.Vector.String() != "prometheus{}" {
		t.Errorf("Vector: got %v", mat.Vector)
	}
}

func TestParse_Number(t *testing.T) {
	e, err := Parse("0.95")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	s, ok := e.(Scalar)
	if !ok {
		t.Fatalf("expected Scalar, got %T", e)
	}
	if s.Val != 0.95 {
		t.Errorf("Val: got %g", s.Val)
	}
}

func TestParse_Parens(t *testing.T) {
	e, err := Parse("(prometheus{})")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	v, ok := e.(*VectorSelector)
	if !ok {
		t.Fatalf("expected *VectorSelector, got %T", e)
	}
	if v.String() != "prometheus{}" {
		t.Errorf("got %q", v.String())
	}
}

func TestParse_Rate(t *testing.T) {
	e, err := Parse("rate(prometheus{job=\"api\"}[1m])")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	call, ok := e.(*Call)
	if !ok {
		t.Fatalf("expected *Call, got %T", e)
	}
	if call.Func != "rate" {
		t.Errorf("Func: got %q", call.Func)
	}
	if len(call.Args) != 1 {
		t.Fatalf("Args: got %d", len(call.Args))
	}
	if _, ok := call.Args[0].(*MatrixSelector); !ok {
		t.Errorf("rate arg: got %T", call.Args[0])
	}
}

func TestParse_Heatmap(t *testing.T) {
	e, err := Parse("heatmap(prometheus{})")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	call, ok := e.(*Call)
	if !ok {
		t.Fatalf("expected *Call, got %T", e)
	}
	if call.Func != "heatmap" {
		t.Errorf("Func: got %q", call.Func)
	}
	if _, ok := call.Args[0].(*VectorSelector); !ok {
		t.Errorf("heatmap arg: got %T", call.Args[0])
	}
}

func TestParse_HistogramQuantile(t *testing.T) {
	e, err := Parse("histogram_quantile(0.95, prometheus{})")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	call, ok := e.(*Call)
	if !ok {
		t.Fatalf("expected *Call, got %T", e)
	}
	if call.Func != "histogram_quantile" {
		t.Errorf("Func: got %q", call.Func)
	}
	if len(call.Args) != 2 {
		t.Fatalf("Args: got %d", len(call.Args))
	}
	if _, ok := call.Args[0].(Scalar); !ok {
		t.Errorf("arg0: got %T", call.Args[0])
	}
	if _, ok := call.Args[1].(*VectorSelector); !ok {
		t.Errorf("arg1: got %T", call.Args[1])
	}
}

func TestParse_Aggregation(t *testing.T) {
	tests := []struct {
		input string
		op    string
		by    bool
		keys  []string
	}{
		{"sum (prometheus{})", "sum", false, nil},
		{"sum by (job) (prometheus{})", "sum", true, []string{"job"}},
		{"sum by (job, pod) (prometheus{})", "sum", true, []string{"job", "pod"}},
		{"avg without (pod) (prometheus{})", "avg", false, []string{"pod"}},
		{"count group (a) (prometheus{})", "count", true, []string{"a"}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			e, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			agg, ok := e.(*Aggregation)
			if !ok {
				t.Fatalf("expected *Aggregation, got %T", e)
			}
			if agg.Op != tt.op {
				t.Errorf("Op: got %q, want %q", agg.Op, tt.op)
			}
			if tt.by || len(tt.keys) > 0 {
				if agg.Grouping == nil {
					t.Fatal("expected Grouping")
				}
				kind := "without"
				if tt.by {
					kind = "by"
				}
				if agg.Grouping.Kind != kind {
					t.Errorf("Grouping.Kind: got %q, want %q", agg.Grouping.Kind, kind)
				}
				if len(agg.Grouping.Keys) != len(tt.keys) {
					t.Errorf("Grouping.Keys: got %v", agg.Grouping.Keys)
				}
			}
		})
	}
}

func TestParse_Complex(t *testing.T) {
	input := `sum by (req.status) (rate({req.status=~"5.."}[1m]))`
	e, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	agg, ok := e.(*Aggregation)
	if !ok {
		t.Fatalf("expected *Aggregation, got %T", e)
	}
	if agg.Op != "sum" || agg.Grouping == nil || len(agg.Grouping.Keys) != 1 || agg.Grouping.Keys[0] != "req.status" {
		t.Errorf("aggregation: %+v", agg)
	}
	call, ok := agg.Expr.(*Call)
	if !ok || call.Func != "rate" {
		t.Errorf("inner: %T %+v", agg.Expr, agg.Expr)
	}
	mat, ok := call.Args[0].(*MatrixSelector)
	if !ok || mat.RangeStr != "1m" {
		t.Errorf("matrix: %T %+v", call.Args[0], call.Args[0])
	}
	vec := mat.Vector
	if vec.Metric != nil || len(vec.Matchers) != 1 || vec.Matchers[0].Name != "req.status" || vec.Matchers[0].Op != "=~" {
		t.Errorf("vector: %+v", vec)
	}
}

func TestParse_Invalid(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantErrCont string
	}{
		{"empty", "", "error"},
		{"invalid token", "!", "unexpected"},
		{"unterminated string", `prometheus{"x`, "unterminated"},
		// heatmap() only accepts vector_selector in grammar, so matrix is syntax error
		{"heatmap matrix", "heatmap(prometheus{}[5m])", "error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if tt.wantErrCont != "" && !strings.Contains(err.Error(), tt.wantErrCont) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantErrCont)
			}
		})
	}
}

// TestParse_ValidateRateVector ensures rate(vector) parses but fails Validate (rate needs matrix).
func TestParse_ValidateRateVector(t *testing.T) {
	e, err := Parse("rate(prometheus{})")
	if err != nil {
		t.Fatalf("Parse unexpectedly failed: %v", err)
	}
	verr := Validate(e)
	if verr == nil {
		t.Fatal("Validate expected to fail for rate(vector), got nil")
	}
	if !strings.Contains(verr.Error(), "range vector") {
		t.Errorf("expected 'range vector' in error, got %q", verr.Error())
	}
}

func TestParse_RoundTrip(t *testing.T) {
	inputs := []string{
		"prometheus",
		"prometheus{}",
		"{}",
		`{job="api"}`,
		`prometheus{req.status="500"}`,
		"prometheus{}[5m]",
		"0.95",
		"rate(prometheus{}[1m])",
		"heatmap(prometheus{})",
		"histogram_quantile(0.9, prometheus{})",
		"sum (prometheus{})",
		"sum by (job) (rate(prometheus{}[1m]))",
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			e, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got := e.String()
			// Round-trip: parse again and compare strings (normalize to handle matcher order etc.)
			e2, err := Parse(got)
			if err != nil {
				t.Fatalf("Parse(round-trip): %v", err)
			}
			norm := Normalize(e).String()
			norm2 := Normalize(e2).String()
			if norm != norm2 {
				t.Errorf("round-trip mismatch: %q -> %q -> %q (norm: %q vs %q)", input, got, e2.String(), norm, norm2)
			}
		})
	}
}
