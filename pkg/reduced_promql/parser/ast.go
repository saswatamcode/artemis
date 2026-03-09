// ast.go
package parser

import (
	"fmt"
	"sort"
	"strings"
)

// --------------------
// Positions
// --------------------

// Pos is a byte offset in the original query string.
type Pos int

type PosRange struct {
	Start Pos
	End   Pos
}

func (pr PosRange) String() string {
	return fmt.Sprintf("[%d..%d)", pr.Start, pr.End)
}

// --------------------
// AST interfaces (Prometheus-ish)
// --------------------

type Node interface {
	// String should re-parse to the same node (modulo normalization).
	String() string
	// Pretty prints with indentation.
	Pretty(level int) string
	// PositionRange in the source query.
	PositionRange() PosRange
}

// Expr is a generic interface for all expression nodes.
type Expr interface {
	Node
	reducedPromQLExpr()
}

// --------------------
// AST nodes
// --------------------

type VectorSelector struct {
	Metric   *string
	Matchers []LabelMatcher
	PosRange PosRange
}

func (*VectorSelector) reducedPromQLExpr()        {}
func (v *VectorSelector) PositionRange() PosRange { return v.PosRange }

type MatrixSelector struct {
	Vector   *VectorSelector
	RangeStr string
	PosRange PosRange
}

func (*MatrixSelector) reducedPromQLExpr()        {}
func (m *MatrixSelector) PositionRange() PosRange { return m.PosRange }

type LabelMatcher struct {
	Name     string
	Op       string // "=", "!=", "=~", "!~"
	Value    string
	PosRange PosRange
}

type Call struct {
	Func     string
	Args     []Expr
	PosRange PosRange
}

func (*Call) reducedPromQLExpr()        {}
func (c *Call) PositionRange() PosRange { return c.PosRange }

type Aggregation struct {
	Op       string // sum, avg, min, max, count
	Grouping *Grouping
	Expr     Expr
	PosRange PosRange
}

func (*Aggregation) reducedPromQLExpr()        {}
func (a *Aggregation) PositionRange() PosRange { return a.PosRange }

type Grouping struct {
	Kind     string // "by" or "without"
	Keys     []string
	PosRange PosRange
}

func (g *Grouping) PositionRange() PosRange { return g.PosRange }

type Scalar struct {
	Val      float64
	PosRange PosRange
}

func (Scalar) reducedPromQLExpr()        {}
func (s Scalar) PositionRange() PosRange { return s.PosRange }

// --------------------
// Parse entrypoint
// --------------------

// Result is set by the parser top-level rule.
var Result Expr

func Parse(input string) (Expr, error) {
	lex := NewLexer(input)
	if yyParse(lex) != 0 {
		if err := lex.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("parse failed")
	}
	if err := lex.Err(); err != nil {
		return nil, err
	}
	return Result, nil
}

// --------------------
// String / Pretty
// --------------------

func indent(level int) string { return strings.Repeat("  ", level) }

func (v *VectorSelector) String() string {
	var b strings.Builder
	if v.Metric != nil {
		b.WriteString(*v.Metric)
	}
	b.WriteString("{")
	for i, m := range v.Matchers {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(m.String())
	}
	b.WriteString("}")
	return b.String()
}

func (v *VectorSelector) Pretty(level int) string {
	return indent(level) + v.String()
}

func (m *MatrixSelector) String() string {
	return fmt.Sprintf("%s[%s]", m.Vector.String(), m.RangeStr)
}

func (m *MatrixSelector) Pretty(level int) string {
	return indent(level) + m.String()
}

func (m LabelMatcher) String() string {
	val := strings.ReplaceAll(m.Value, `\`, `\\`)
	val = strings.ReplaceAll(val, `"`, `\"`)
	return fmt.Sprintf(`%s%s"%s"`, m.Name, m.Op, val)
}

func (c *Call) String() string {
	var b strings.Builder
	b.WriteString(c.Func)
	b.WriteString("(")
	for i, a := range c.Args {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(a.String())
	}
	b.WriteString(")")
	return b.String()
}

func (c *Call) Pretty(level int) string {
	var b strings.Builder
	b.WriteString(indent(level))
	b.WriteString(c.Func)
	b.WriteString("(\n")
	for i, a := range c.Args {
		b.WriteString(a.Pretty(level + 1))
		if i < len(c.Args)-1 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	b.WriteString(indent(level))
	b.WriteString(")")
	return b.String()
}

func (a *Aggregation) String() string {
	var b strings.Builder
	b.WriteString(a.Op)
	if a.Grouping != nil {
		b.WriteString(" ")
		b.WriteString(a.Grouping.String())
	}
	b.WriteString(" (")
	b.WriteString(a.Expr.String())
	b.WriteString(")")
	return b.String()
}

func (a *Aggregation) Pretty(level int) string {
	var b strings.Builder
	b.WriteString(indent(level))
	b.WriteString(a.Op)
	if a.Grouping != nil {
		b.WriteString(" ")
		b.WriteString(a.Grouping.String())
	}
	b.WriteString(" (\n")
	b.WriteString(a.Expr.Pretty(level + 1))
	b.WriteString("\n")
	b.WriteString(indent(level))
	b.WriteString(")")
	return b.String()
}

func (g *Grouping) String() string {
	var b strings.Builder
	b.WriteString(g.Kind)
	b.WriteString(" (")
	for i, k := range g.Keys {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(k)
	}
	b.WriteString(")")
	return b.String()
}

func (g *Grouping) Pretty(level int) string {
	return indent(level) + g.String()
}

func (s Scalar) String() string          { return fmt.Sprintf("%g", s.Val) }
func (s Scalar) Pretty(level int) string { return indent(level) + s.String() }

// --------------------
// Walk / Rewrite
// --------------------

func Walk(e Expr, fn func(Node)) {
	if e == nil {
		return
	}
	fn(e)

	switch n := e.(type) {
	case *Aggregation:
		if n.Grouping != nil {
			fn(n.Grouping)
		}
		Walk(n.Expr, fn)

	case *Call:
		for _, a := range n.Args {
			Walk(a, fn)
		}

	case *MatrixSelector:
		Walk(n.Vector, fn)

	case *VectorSelector, Scalar:
		// leaf
	}
}

// Rewrite applies fn top-down. If fn returns (newExpr, true), subtree is replaced.
func Rewrite(e Expr, fn func(Expr) (Expr, bool)) Expr {
	if e == nil {
		return nil
	}
	if ne, ok := fn(e); ok {
		return ne
	}

	switch n := e.(type) {
	case *Aggregation:
		cp := *n
		cp.Expr = Rewrite(cp.Expr, fn)
		return &cp

	case *Call:
		cp := *n
		cp.Args = make([]Expr, len(n.Args))
		for i := range n.Args {
			cp.Args[i] = Rewrite(n.Args[i], fn)
		}
		return &cp

	case *MatrixSelector:
		cp := *n
		cp.Vector = Rewrite(cp.Vector, fn).(*VectorSelector)
		return &cp

	case *VectorSelector, Scalar:
		return e

	default:
		return e
	}
}

// --------------------
// Normalize / Validate
// --------------------

func Normalize(e Expr) Expr {
	switch n := e.(type) {
	case *VectorSelector:
		cp := *n
		if len(cp.Matchers) > 1 {
			ms := append([]LabelMatcher(nil), cp.Matchers...)
			sort.Slice(ms, func(i, j int) bool {
				if ms[i].Name != ms[j].Name {
					return ms[i].Name < ms[j].Name
				}
				if ms[i].Op != ms[j].Op {
					return ms[i].Op < ms[j].Op
				}
				return ms[i].Value < ms[j].Value
			})
			cp.Matchers = ms
		}
		return &cp

	case *MatrixSelector:
		cp := *n
		cp.Vector = Normalize(cp.Vector).(*VectorSelector)
		return &cp

	case *Call:
		cp := *n
		cp.Args = make([]Expr, len(n.Args))
		for i := range n.Args {
			cp.Args[i] = Normalize(n.Args[i])
		}
		return &cp

	case *Aggregation:
		cp := *n
		if cp.Grouping != nil && len(cp.Grouping.Keys) > 1 {
			gcp := *cp.Grouping
			keys := append([]string(nil), gcp.Keys...)
			sort.Strings(keys)
			gcp.Keys = keys
			cp.Grouping = &gcp
		}
		cp.Expr = Normalize(cp.Expr)
		return &cp

	case Scalar:
		return n

	default:
		return e
	}
}

func Validate(e Expr) error {
	switch n := e.(type) {
	case *VectorSelector:
		for _, m := range n.Matchers {
			switch m.Op {
			case "=", "!=", "=~", "!~":
			default:
				return fmt.Errorf("invalid matcher op %q", m.Op)
			}
		}
		return nil

	case *MatrixSelector:
		if n.Vector == nil {
			return fmt.Errorf("matrix selector has nil vector")
		}
		if n.RangeStr == "" {
			return fmt.Errorf("matrix selector missing range")
		}
		return Validate(n.Vector)

	case *Call:
		switch n.Func {
		case "rate":
			if len(n.Args) != 1 {
				return fmt.Errorf("rate() expects 1 arg")
			}
			if _, ok := n.Args[0].(*MatrixSelector); !ok {
				return fmt.Errorf("rate() expects a range vector (selector[dur])")
			}
			return Validate(n.Args[0])

		case "histogram_quantile":
			if len(n.Args) != 2 {
				return fmt.Errorf("histogram_quantile() expects 2 args")
			}
			if _, ok := n.Args[0].(Scalar); !ok {
				return fmt.Errorf("histogram_quantile() arg0 must be scalar")
			}
			if err := Validate(n.Args[0]); err != nil {
				return err
			}
			return Validate(n.Args[1])

		case "heatmap":
			if len(n.Args) != 1 {
				return fmt.Errorf("heatmap() expects 1 arg")
			}
			if _, ok := n.Args[0].(*VectorSelector); !ok {
				return fmt.Errorf("heatmap() expects a vector selector")
			}
			return Validate(n.Args[0])

		default:
			return fmt.Errorf("unknown function %q", n.Func)
		}

	case *Aggregation:
		switch n.Op {
		case "sum", "avg", "min", "max", "count":
		default:
			return fmt.Errorf("unknown aggregation %q", n.Op)
		}
		if n.Grouping != nil {
			if n.Grouping.Kind != "by" && n.Grouping.Kind != "without" {
				return fmt.Errorf("invalid grouping kind %q", n.Grouping.Kind)
			}
		}
		return Validate(n.Expr)

	case Scalar:
		return nil

	default:
		return fmt.Errorf("unknown expr type %T", e)
	}
}
