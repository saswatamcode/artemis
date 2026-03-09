// generated_parser.y
//
// goyacc -l -o reduced_promql/parser/generated_parser.y.go reduced_promql/parser/generated_parser.y
//
// NOTE: You MUST implement the lexer so that:
//
// 1) IDENT includes UTF-8 letters/digits/_ and ALSO '.' for label keys,
//    e.g. "req.status" => IDENT("req.status").
// 2) Match operators return tokens: EQ, NEQ, RE, NRE for =, !=, =~, !~
// 3) STRING is a quoted string literal: "500"
// 4) NUMBER is a float/int literal: 0.95, 1, 42
// 5) DURATION is a PromQL-like duration literal: 5m, 1h, 30s, 2w, etc.
// 6) Keywords return their own tokens (RATE, SUM, BY, ...), not IDENT.
//
// PUNCTUATION: return int('{'), int('}'), int('('), int(')'), int('['), int(']'), int(',')

%{
package parser
%}

// --- union ---
%union {
	str   string
	f64   float64
	expr  Expr
	vec   *VectorSelector
	mat   *MatrixSelector
	lms   []LabelMatcher
	lm    LabelMatcher
	strs  []string
	call  *Call
	agg   *Aggregation
	grp   *Grouping
    pos  PosRange
}

// --- tokens ---
%token <str> IDENT
%token <str> STRING
%token <f64> NUMBER
%token <str> DURATION

%token EQ NEQ RE NRE

%token RATE
%token COUNT SUM AVG MIN MAX
%token HISTOGRAM_QUANTILE
%token HEATMAP

%token BY WITHOUT GROUP

%start input

%type <expr> input expr primary
%type <vec>  vector_selector
%type <mat>  matrix_selector
%type <lms>  matcher_list_opt matcher_list
%type <lm>   matcher
%type <str>  matcher_op
%type <strs> ident_list_opt ident_list
%type <call> func_call
%type <agg>  aggregation
%type <grp>  grouping_opt grouping

%%

// -------- top level --------

input
	: expr
	  {
	    Result = $1
	    $$ = $1
	  }
	;

// -------- expressions --------

expr
	: aggregation                 { $$ = $1 }
	| func_call                   { $$ = $1 }
	| primary                     { $$ = $1 }
	;

primary
	: vector_selector             { $$ = $1 }
	| matrix_selector             { $$ = $1 }
	| NUMBER                      { $$ = Scalar{Val: $1} }
	| '(' expr ')'                { $$ = $2 }
	;

// -------- vector / matrix selectors --------

// Supports both:
//   prometheus{req.status="500"}
//   prometheus{}
//   {req.status="500"}   // metric-less
//   {}                   // metric-less empty matchers (allowed)
//   prometheus           // metric-only
vector_selector
	: IDENT '{' matcher_list_opt '}'
	  {
	    m := $1
	    $$ = &VectorSelector{Metric: &m, Matchers: $3}
	  }
	| '{' matcher_list_opt '}'
	  {
	    $$ = &VectorSelector{Metric: nil, Matchers: $2}
	  }
	| IDENT
	  {
	    m := $1
	    $$ = &VectorSelector{Metric: &m, Matchers: nil}
	  }
	;

// Range vector (matrix) selector:
//   prometheus{}[5m]
//   {req.status="500"}[1h]
matrix_selector
	: vector_selector '[' DURATION ']'
	  {
	    $$ = &MatrixSelector{Vector: $1, RangeStr: $3}
	  }
	;

// -------- label matchers --------

matcher_list_opt
	: /* empty */                 { $$ = nil }
	| matcher_list                { $$ = $1 }
	;

matcher_list
	: matcher                     { $$ = []LabelMatcher{$1} }
	| matcher_list ',' matcher    { $$ = append($1, $3) }
	;

matcher
	: IDENT matcher_op STRING
	  {
	    $$ = LabelMatcher{Name: $1, Op: $2, Value: $3}
	  }
	;

matcher_op
	: EQ                          { $$ = "=" }
	| NEQ                         { $$ = "!=" }
	| RE                          { $$ = "=~" }
	| NRE                         { $$ = "!~" }
	;

// -------- function calls --------
//
// Only these functions are supported:
// - rate(x)                  (typically x is a matrix_selector)
// - histogram_quantile(q, x)
// - heatmap(v)               (STRICT: v must be a vector_selector)
//
// NOTE: This enforces that heatmap ONLY accepts a vector selector like prometheus{} or {a="b"}.
func_call
	: RATE '(' expr ')'
	  { $$ = &Call{Func: "rate", Args: []Expr{$3}} }
	| HISTOGRAM_QUANTILE '(' expr ',' expr ')'
	  { $$ = &Call{Func: "histogram_quantile", Args: []Expr{$3, $5}} }
	| HEATMAP '(' vector_selector ')'
	  { $$ = &Call{Func: "heatmap", Args: []Expr{$3}} }
	;

// -------- aggregations --------
//
// PromQL-ish form:
//   sum by (a,b) (expr)
//   avg without (pod) (expr)
//
// Also supports "group" as an alias for "by":
//   sum group (a,b) (expr)
aggregation
	: SUM   grouping_opt '(' expr ')'  { $$ = &Aggregation{Op:"sum",   Grouping:$2, Expr:$4} }
	| AVG   grouping_opt '(' expr ')'  { $$ = &Aggregation{Op:"avg",   Grouping:$2, Expr:$4} }
	| MIN   grouping_opt '(' expr ')'  { $$ = &Aggregation{Op:"min",   Grouping:$2, Expr:$4} }
	| MAX   grouping_opt '(' expr ')'  { $$ = &Aggregation{Op:"max",   Grouping:$2, Expr:$4} }
	| COUNT grouping_opt '(' expr ')'  { $$ = &Aggregation{Op:"count", Grouping:$2, Expr:$4} }
	;

// Optional grouping clause
grouping_opt
	: /* empty */                 { $$ = nil }
	| grouping                    { $$ = $1 }
	;

// by (...) | without (...) | group (...)
grouping
	: BY '(' ident_list_opt ')'
	  { $$ = &Grouping{Kind:"by", Keys:$3} }
	| GROUP '(' ident_list_opt ')'
	  { $$ = &Grouping{Kind:"by", Keys:$3} }
	| WITHOUT '(' ident_list_opt ')'
	  { $$ = &Grouping{Kind:"without", Keys:$3} }
	;

// (a,b,c) or empty ()
ident_list_opt
	: /* empty */                 { $$ = nil }
	| ident_list                  { $$ = $1 }
	;

ident_list
	: IDENT                       { $$ = []string{$1} }
	| ident_list ',' IDENT        { $$ = append($1, $3) }
	;

%%

// You must provide a lexer that satisfies goyacc's expectations:
//
// type Lexer interface {
//   Lex(lval *promql_likeSymType) int
//   Error(e string)
// }
//
// Key lexer requirement for your dotted label keys:
// - scan runes (UTF-8)
// - accept '.' as part of IDENT so `req.status` becomes IDENT("req.status")
//
// Examples accepted:
//   {req.status = "500"}
//   prometheus{}
//   heatmap(prometheus{})
//   sum by (req.status) (rate({req.status="500"}[1m]))
//
// Examples rejected because of the heatmap restriction:
//   heatmap(rate(prometheus{}[5m]))
//   heatmap(prometheus{}[5m])