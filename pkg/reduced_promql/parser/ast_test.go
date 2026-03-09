package parser

import (
	"strings"
	"testing"
)

func posRange(start, end int) PosRange {
	return PosRange{Start: Pos(start), End: Pos(end)}
}

func TestPosRange_String(t *testing.T) {
	pr := posRange(0, 5)
	if got := pr.String(); got != "[0..5)" {
		t.Errorf("PosRange.String() = %q, want [0..5)", got)
	}
}

func TestVectorSelector_String(t *testing.T) {
	metric := "prometheus"
	tests := []struct {
		name string
		v    *VectorSelector
		want string
	}{
		{
			"metric only",
			&VectorSelector{Metric: &metric, Matchers: nil},
			"prometheus{}",
		},
		{
			"metric with one matcher",
			&VectorSelector{
				Metric:   &metric,
				Matchers: []LabelMatcher{{Name: "job", Op: "=", Value: "api"}},
			},
			"prometheus{job=\"api\"}",
		},
		{
			"metric with two matchers",
			&VectorSelector{
				Metric: &metric,
				Matchers: []LabelMatcher{
					{Name: "job", Op: "=", Value: "api"},
					{Name: "req.status", Op: "=~", Value: "5.."},
				},
			},
			"prometheus{job=\"api\",req.status=~\"5..\"}",
		},
		{
			"metric-less with matchers",
			&VectorSelector{
				Metric:   nil,
				Matchers: []LabelMatcher{{Name: "job", Op: "!=", Value: ""}},
			},
			"{job!=\"\"}",
		},
		{
			"empty matchers",
			&VectorSelector{Metric: nil, Matchers: []LabelMatcher{}},
			"{}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.v.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLabelMatcher_String(t *testing.T) {
	tests := []struct {
		m    LabelMatcher
		want string
	}{
		{LabelMatcher{Name: "a", Op: "=", Value: "b"}, `a="b"`},
		{LabelMatcher{Name: "a", Op: "!=", Value: "b"}, `a!="b"`},
		{LabelMatcher{Name: "a", Op: "=~", Value: "x.*"}, `a=~"x.*"`},
		{LabelMatcher{Name: "a", Op: "!~", Value: "x"}, `a!~"x"`},
	}
	for i, tt := range tests {
		if got := tt.m.String(); got != tt.want {
			t.Errorf("LabelMatcher[%d] String() = %q, want %q", i, got, tt.want)
		}
	}
}

func TestMatrixSelector_String(t *testing.T) {
	metric := "prometheus"
	vec := &VectorSelector{Metric: &metric, Matchers: nil}
	mat := &MatrixSelector{Vector: vec, RangeStr: "5m"}
	if got := mat.String(); got != "prometheus{}[5m]" {
		t.Errorf("MatrixSelector.String() = %q, want prometheus{}[5m]", got)
	}
}

func TestCall_String(t *testing.T) {
	metric := "prometheus"
	vec := &VectorSelector{Metric: &metric, Matchers: nil}
	call := &Call{Func: "rate", Args: []Expr{&MatrixSelector{Vector: vec, RangeStr: "1m"}}}
	if got := call.String(); got != "rate(prometheus{}[1m])" {
		t.Errorf("Call.String() = %q, want rate(prometheus{}[1m])", got)
	}
}

func TestAggregation_String(t *testing.T) {
	metric := "prometheus"
	vec := &VectorSelector{Metric: &metric, Matchers: nil}
	tests := []struct {
		name string
		a    *Aggregation
		want string
	}{
		{
			"sum no grouping",
			&Aggregation{Op: "sum", Expr: vec},
			"sum (prometheus{})",
		},
		{
			"sum by (a,b)",
			&Aggregation{
				Op:       "sum",
				Grouping: &Grouping{Kind: "by", Keys: []string{"a", "b"}},
				Expr:     vec,
			},
			"sum by (a,b) (prometheus{})",
		},
		{
			"count without (pod)",
			&Aggregation{
				Op:       "count",
				Grouping: &Grouping{Kind: "without", Keys: []string{"pod"}},
				Expr:     vec,
			},
			"count without (pod) (prometheus{})",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGrouping_String(t *testing.T) {
	g := &Grouping{Kind: "by", Keys: []string{"a", "b"}}
	if got := g.String(); got != "by (a,b)" {
		t.Errorf("Grouping.String() = %q, want by (a,b)", got)
	}
}

func TestScalar_String(t *testing.T) {
	s := Scalar{Val: 0.95}
	if got := s.String(); got != "0.95" {
		t.Errorf("Scalar.String() = %q, want 0.95", got)
	}
}

func TestVectorSelector_Pretty(t *testing.T) {
	metric := "prometheus"
	v := &VectorSelector{Metric: &metric, Matchers: nil}
	got := v.Pretty(0)
	want := "prometheus{}"
	if got != want {
		t.Errorf("Pretty(0) = %q, want %q", got, want)
	}
	got1 := v.Pretty(2)
	want1 := "    prometheus{}"
	if got1 != want1 {
		t.Errorf("Pretty(2) = %q, want %q", got1, want1)
	}
}

func TestPositionRange(t *testing.T) {
	metric := "prometheus"
	pr := posRange(10, 20)
	v := &VectorSelector{Metric: &metric, Matchers: nil, PosRange: pr}
	if got := v.PositionRange(); got != pr {
		t.Errorf("PositionRange() = %v, want %v", got, pr)
	}
}

func TestWalk(t *testing.T) {
	// rate(prometheus{}[5m]) -> 4 nodes: Call, MatrixSelector, VectorSelector, and we don't count Grouping. So Call, MatrixSelector, VectorSelector = 3. Plus the sub-nodes. Walk visits: Call, MatrixSelector, VectorSelector. So 3.
	metric := "prometheus"
	vec := &VectorSelector{Metric: &metric, Matchers: nil}
	mat := &MatrixSelector{Vector: vec, RangeStr: "5m"}
	call := &Call{Func: "rate", Args: []Expr{mat}}
	var count int
	Walk(call, func(Node) { count++ })
	if count != 3 {
		t.Errorf("Walk visited %d nodes, want 3 (Call, MatrixSelector, VectorSelector)", count)
	}

	// sum by (a) (rate(x{}[1m])) -> Aggregation, Grouping, Call, MatrixSelector, VectorSelector = 5
	vec2 := &VectorSelector{Metric: &metric, Matchers: nil}
	mat2 := &MatrixSelector{Vector: vec2, RangeStr: "1m"}
	rateCall := &Call{Func: "rate", Args: []Expr{mat2}}
	agg := &Aggregation{Op: "sum", Grouping: &Grouping{Kind: "by", Keys: []string{"a"}}, Expr: rateCall}
	count = 0
	Walk(agg, func(Node) { count++ })
	if count != 5 {
		t.Errorf("Walk visited %d nodes, want 5 (Aggregation, Grouping, Call, MatrixSelector, VectorSelector)", count)
	}
}

func TestWalk_Count(t *testing.T) {
	metric := "prometheus"
	vec := &VectorSelector{Metric: &metric, Matchers: nil}
	mat := &MatrixSelector{Vector: vec, RangeStr: "5m"}
	call := &Call{Func: "rate", Args: []Expr{mat}}
	var n int
	Walk(call, func(Node) { n++ })
	if n != 3 {
		t.Errorf("Walk: got %d nodes, want 3", n)
	}
}

func TestRewrite_Identity(t *testing.T) {
	metric := "prometheus"
	vec := &VectorSelector{Metric: &metric, Matchers: nil}
	mat := &MatrixSelector{Vector: vec, RangeStr: "5m"}
	call := &Call{Func: "rate", Args: []Expr{mat}}
	out := Rewrite(call, func(e Expr) (Expr, bool) { return e, false })
	// Rewrite may copy nodes; check that the result is structurally equivalent.
	if out.String() != call.String() {
		t.Errorf("identity rewrite: got %q, want %q", out.String(), call.String())
	}
}

func TestRewrite_Replace(t *testing.T) {
	metric := "prometheus"
	vec := &VectorSelector{Metric: &metric, Matchers: nil}
	replacement := Scalar{Val: 42}
	out := Rewrite(vec, func(e Expr) (Expr, bool) {
		if _, ok := e.(*VectorSelector); ok {
			return replacement, true
		}
		return e, false
	})
	if out != replacement {
		t.Error("Rewrite should replace when fn returns (new, true)")
	}
}

func TestNormalize_MatcherOrder(t *testing.T) {
	metric := "prometheus"
	v := &VectorSelector{
		Metric: &metric,
		Matchers: []LabelMatcher{
			{Name: "z", Op: "=", Value: "1"},
			{Name: "a", Op: "=", Value: "2"},
		},
	}
	out := Normalize(v).(*VectorSelector)
	if len(out.Matchers) != 2 {
		t.Fatal("expected 2 matchers")
	}
	if out.Matchers[0].Name != "a" || out.Matchers[1].Name != "z" {
		t.Errorf("matchers should be sorted by name: got %v", out.Matchers)
	}
}

func TestNormalize_GroupingKeysOrder(t *testing.T) {
	vec := &VectorSelector{Metric: nil, Matchers: nil}
	agg := &Aggregation{
		Op:       "sum",
		Grouping: &Grouping{Kind: "by", Keys: []string{"z", "a"}},
		Expr:     vec,
	}
	out := Normalize(agg).(*Aggregation)
	if out.Grouping == nil || len(out.Grouping.Keys) != 2 {
		t.Fatal("expected grouping with 2 keys")
	}
	if out.Grouping.Keys[0] != "a" || out.Grouping.Keys[1] != "z" {
		t.Errorf("keys should be sorted: got %v", out.Grouping.Keys)
	}
}

func TestValidate_VectorSelector(t *testing.T) {
	tests := []struct {
		name    string
		e       Expr
		wantErr bool
		msg     string
	}{
		{"valid matchers", &VectorSelector{Matchers: []LabelMatcher{{Name: "a", Op: "=", Value: "b"}}}, false, ""},
		{"invalid matcher op", &VectorSelector{Matchers: []LabelMatcher{{Name: "a", Op: "??", Value: "b"}}}, true, "invalid matcher op"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.e)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.msg != "" && !strings.Contains(err.Error(), tt.msg) {
				t.Errorf("Validate() error %q does not contain %q", err.Error(), tt.msg)
			}
		})
	}
}

func TestValidate_MatrixSelector(t *testing.T) {
	err := Validate(&MatrixSelector{Vector: nil, RangeStr: "5m"})
	if err == nil || !strings.Contains(err.Error(), "nil vector") {
		t.Errorf("expected nil vector error, got %v", err)
	}
	err = Validate(&MatrixSelector{Vector: &VectorSelector{}, RangeStr: ""})
	if err == nil || !strings.Contains(err.Error(), "missing range") {
		t.Errorf("expected missing range error, got %v", err)
	}
}

func TestValidate_Call(t *testing.T) {
	vec := &VectorSelector{Metric: nil, Matchers: nil}
	mat := &MatrixSelector{Vector: vec, RangeStr: "5m"}
	tests := []struct {
		name    string
		e       Expr
		wantErr bool
		msg     string
	}{
		{"rate with matrix", &Call{Func: "rate", Args: []Expr{mat}}, false, ""},
		{"rate with vector", &Call{Func: "rate", Args: []Expr{vec}}, true, "range vector"},
		{"rate wrong arity", &Call{Func: "rate", Args: nil}, true, "1 arg"},
		{"heatmap with vector", &Call{Func: "heatmap", Args: []Expr{vec}}, false, ""},
		{"heatmap with matrix", &Call{Func: "heatmap", Args: []Expr{mat}}, true, "vector selector"},
		{"histogram_quantile scalar and vector", &Call{Func: "histogram_quantile", Args: []Expr{Scalar{Val: 0.95}, vec}}, false, ""},
		{"histogram_quantile wrong arg0", &Call{Func: "histogram_quantile", Args: []Expr{vec, vec}}, true, "arg0 must be scalar"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.e)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.msg != "" && !strings.Contains(err.Error(), tt.msg) {
				t.Errorf("Validate() error %q does not contain %q", err.Error(), tt.msg)
			}
		})
	}
}

func TestValidate_Aggregation(t *testing.T) {
	vec := &VectorSelector{}
	err := Validate(&Aggregation{Op: "sum", Expr: vec})
	if err != nil {
		t.Errorf("valid sum: %v", err)
	}
	err = Validate(&Aggregation{Op: "invalid", Expr: vec})
	if err == nil || !strings.Contains(err.Error(), "unknown aggregation") {
		t.Errorf("expected unknown aggregation error, got %v", err)
	}
	err = Validate(&Aggregation{Op: "sum", Grouping: &Grouping{Kind: "invalid", Keys: nil}, Expr: vec})
	if err == nil || !strings.Contains(err.Error(), "invalid grouping") {
		t.Errorf("expected invalid grouping error, got %v", err)
	}
}

func TestValidate_Scalar(t *testing.T) {
	if err := Validate(Scalar{Val: 1}); err != nil {
		t.Errorf("Validate(Scalar) = %v", err)
	}
}
