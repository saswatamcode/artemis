// lexer.go
package parser

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Lexer struct {
	input string
	pos   int // byte offset
	err   error
}

func NewLexer(s string) *Lexer { return &Lexer{input: s} }

func (l *Lexer) Error(s string) {
	if l.err == nil {
		l.err = fmt.Errorf("%s", s)
	}
}

func (l *Lexer) Err() error { return l.err }

// setTokRange stamps the current token's [start,end) into lval.
func setTokRange(lval *yySymType, start, end int) {
	lval.pos = PosRange{Start: Pos(start), End: Pos(end)}
}

func (l *Lexer) Lex(lval *yySymType) int {
	// Skip whitespace
	for {
		r, w := l.peek()
		if r == utf8.RuneError && w == 0 {
			return 0 // EOF
		}
		if r == '#' {
			l.skipLineComment()
			continue
		}
		if unicode.IsSpace(r) {
			l.pos += w
			continue
		}
		break
	}

	r, w := l.peek()
	if r == utf8.RuneError && w == 0 {
		return 0
	}

	tokStart := l.pos

	// Punctuation tokens: return the rune value as int.
	switch r {
	case '(', ')', '{', '}', '[', ']', ',':
		l.pos += w
		setTokRange(lval, tokStart, l.pos)
		return int(r)
	}

	// Operators
	if r == '=' {
		l.pos += w
		r2, w2 := l.peek()
		if r2 == '~' {
			l.pos += w2
			setTokRange(lval, tokStart, l.pos)
			return RE // =~
		}
		setTokRange(lval, tokStart, l.pos)
		return EQ // =
	}
	if r == '!' {
		l.pos += w
		r2, w2 := l.peek()
		if r2 == '=' {
			l.pos += w2
			setTokRange(lval, tokStart, l.pos)
			return NEQ // !=
		}
		if r2 == '~' {
			l.pos += w2
			setTokRange(lval, tokStart, l.pos)
			return NRE // !~
		}
		l.failf("unexpected '!' (expected != or !~)")
		setTokRange(lval, tokStart, l.pos)
		return 0
	}

	// String literal
	if r == '"' {
		s, ok := l.scanString()
		if !ok {
			return 0
		}
		lval.str = s
		setTokRange(lval, tokStart, l.pos)
		return STRING
	}

	// Number or duration: starts with digit
	if unicode.IsDigit(r) {
		start := l.pos
		l.scanNumberLike()
		lex := l.input[start:l.pos]

		if l.isDurationUnitAhead() {
			l.pos = start
			d := l.scanDuration()
			if d == "" {
				return 0
			}
			lval.str = d
			setTokRange(lval, tokStart, l.pos)
			return DURATION
		}

		f, err := strconv.ParseFloat(lex, 64)
		if err != nil {
			l.failf("invalid number %q: %v", lex, err)
			return 0
		}
		lval.f64 = f
		setTokRange(lval, tokStart, l.pos)
		return NUMBER
	}

	// Identifier / keyword (UTF-8), also allows '.' inside.
	if isIdentStart(r) {
		ident := l.scanIdent()
		setTokRange(lval, tokStart, l.pos)

		switch ident {
		case "rate":
			return RATE
		case "count":
			return COUNT
		case "sum":
			return SUM
		case "avg":
			return AVG
		case "min":
			return MIN
		case "max":
			return MAX
		case "histogram_quantile":
			return HISTOGRAM_QUANTILE
		case "heatmap":
			return HEATMAP
		case "by":
			return BY
		case "without":
			return WITHOUT
		case "group":
			return GROUP
		default:
			lval.str = ident
			return IDENT
		}
	}

	l.failf("unexpected character %q", r)
	setTokRange(lval, tokStart, l.pos+w) // best effort; might be invalid if w==0
	return 0
}

// --- scanning helpers ---

func (l *Lexer) peek() (rune, int) {
	if l.pos >= len(l.input) {
		return utf8.RuneError, 0
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	return r, w
}

func (l *Lexer) next() (rune, int) {
	r, w := l.peek()
	l.pos += w
	return r, w
}

func (l *Lexer) failf(format string, args ...any) {
	if l.err != nil {
		return
	}
	const ctx = 20
	start := l.pos - ctx
	if start < 0 {
		start = 0
	}
	end := l.pos + ctx
	if end > len(l.input) {
		end = len(l.input)
	}
	context := strings.ReplaceAll(l.input[start:end], "\n", "\\n")
	l.err = fmt.Errorf(format+": at byte %d near %q", append(args, l.pos, context)...)
}

func (l *Lexer) skipLineComment() {
	for {
		r, w := l.next()
		if w == 0 || r == '\n' {
			return
		}
	}
}

func isIdentStart(r rune) bool { return r == '_' || unicode.IsLetter(r) }
func isIdentPart(r rune) bool {
	return r == '_' || r == '.' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func (l *Lexer) scanIdent() string {
	start := l.pos
	l.next()
	for {
		r, w := l.peek()
		if w == 0 || !isIdentPart(r) {
			break
		}
		l.pos += w
	}
	return l.input[start:l.pos]
}

func (l *Lexer) scanString() (string, bool) {
	start := l.pos
	l.next()

	for {
		r, w := l.peek()
		if w == 0 {
			l.failf("unterminated string literal")
			return "", false
		}
		if r == '\\' {
			l.pos += w
			_, w2 := l.peek()
			if w2 == 0 {
				l.failf("unterminated escape sequence")
				return "", false
			}
			l.pos += w2
			continue
		}
		if r == '"' {
			l.pos += w
			raw := l.input[start:l.pos]
			s, err := strconv.Unquote(raw)
			if err != nil {
				l.failf("invalid string literal %q: %v", raw, err)
				return "", false
			}
			return s, true
		}
		l.pos += w
	}
}

func (l *Lexer) scanNumberLike() {
	for {
		r, w := l.peek()
		if w == 0 || !unicode.IsDigit(r) {
			break
		}
		l.pos += w
	}
	r, w := l.peek()
	if r == '.' {
		l.pos += w
		for {
			r2, w2 := l.peek()
			if w2 == 0 || !unicode.IsDigit(r2) {
				break
			}
			l.pos += w2
		}
	}
	r, w = l.peek()
	if r == 'e' || r == 'E' {
		l.pos += w
		r2, w2 := l.peek()
		if r2 == '+' || r2 == '-' {
			l.pos += w2
		}
		for {
			r3, w3 := l.peek()
			if w3 == 0 || !unicode.IsDigit(r3) {
				break
			}
			l.pos += w3
		}
	}
}

func (l *Lexer) isDurationUnitAhead() bool {
	r, w := l.peek()
	if w == 0 {
		return false
	}
	switch r {
	case 's', 'm', 'h', 'd', 'w', 'y':
		return true
	default:
		return false
	}
}

func (l *Lexer) scanDuration() string {
	start := l.pos
	segments := 0

	for {
		dStart := l.pos
		for {
			r, w := l.peek()
			if w == 0 || !unicode.IsDigit(r) {
				break
			}
			l.pos += w
		}
		if l.pos == dStart {
			break
		}

		r, w := l.peek()
		if w == 0 {
			break
		}
		if !(r == 's' || r == 'm' || r == 'h' || r == 'd' || r == 'w' || r == 'y') {
			break
		}
		l.pos += w
		segments++

		r2, w2 := l.peek()
		if w2 == 0 || !unicode.IsDigit(r2) {
			break
		}
	}

	if segments == 0 {
		l.failf("invalid duration literal")
		return ""
	}
	return l.input[start:l.pos]
}
