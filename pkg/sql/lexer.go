package sql

import (
	"strconv"
	"strings"
)

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokNumber
	tokString
	tokStar       // *
	tokLParen     // (
	tokRParen     // )
	tokComma      // ,
	tokSemicolon  // ;
	tokEq         // =
	tokNe         // !=
	tokLt         // <
	tokGt         // >
	tokLe         // <=
	tokGe         // >=
	tokDot        // .
	tokKeyword    // 关键字（大写原文）
)

type token struct {
	kind tokKind
	text string
	pos  int
}

// keywords 关键字表（大小写不敏感）。
var keywords = map[string]bool{
	"CREATE": true, "TABLE": true, "DROP": true, "INSERT": true,
	"INTO": true, "VALUES": true, "SELECT": true, "FROM": true,
	"WHERE": true, "ORDER": true, "BY": true, "ASC": true, "DESC": true,
	"LIMIT": true, "PRIMARY": true, "KEY": true, "UPDATE": true,
	"SET": true, "DELETE": true, "AND": true, "INDEX": true, "ON": true,
	"JOIN": true, "INNER": true, "LEFT": true, "OUTER": true,
	"GROUP": true, "BETWEEN": true, "IN": true, "AS": true,
	"BEGIN": true, "COMMIT": true, "ROLLBACK": true,
	"EXPORT": true, "IMPORT": true, "TO": true,
	// P2：运维语句 + 表达式增强
	"SHOW": true, "EXPLAIN": true, "TABLES": true, "SLOWQUERIES": true,
	"LIKE": true, "CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
}

type lexer struct {
	src  string
	pos  int
	toks []token
}

func lex(sql string) ([]token, error) {
	l := &lexer{src: sql}
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n':
			l.pos++
		case c == '(':
			l.emit(tokLParen, "(", 1)
		case c == ')':
			l.emit(tokRParen, ")", 1)
		case c == ',':
			l.emit(tokComma, ",", 1)
		case c == '.':
			l.emit(tokDot, ".", 1)
		case c == ';':
			l.emit(tokSemicolon, ";", 1)
		case c == '*':
			l.emit(tokStar, "*", 1)
		case c == '=':
			l.emit(tokEq, "=", 1)
		case c == '!':
			if l.peek(1) == '=' {
				l.emit(tokNe, "!=", 2)
			} else {
				return nil, errf("unexpected character %q at %d", c, l.pos)
			}
		case c == '<':
			if l.peek(1) == '=' {
				l.emit(tokLe, "<=", 2)
			} else {
				l.emit(tokLt, "<", 1)
			}
		case c == '>':
			if l.peek(1) == '=' {
				l.emit(tokGe, ">=", 2)
			} else {
				l.emit(tokGt, ">", 1)
			}
		case c == '\'' || c == '"':
			if err := l.lexString(c); err != nil {
				return nil, err
			}
		case isDigit(c) || (c == '-' && isDigit(l.peek(1))):
			if err := l.lexNumber(); err != nil {
				return nil, err
			}
		case isIdentStart(c):
			l.lexIdent()
		default:
			return nil, errf("unexpected character %q at %d", c, l.pos)
		}
	}
	l.toks = append(l.toks, token{kind: tokEOF, pos: l.pos})
	return l.toks, nil
}

func (l *lexer) peek(n int) byte {
	if l.pos+n >= len(l.src) {
		return 0
	}
	return l.src[l.pos+n]
}

func (l *lexer) emit(k tokKind, text string, width int) {
	l.toks = append(l.toks, token{kind: k, text: text, pos: l.pos})
	l.pos += width
}

func (l *lexer) lexString(q byte) error {
	start := l.pos
	l.pos++
	var sb strings.Builder
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == q {
			l.pos++
			l.toks = append(l.toks, token{kind: tokString, text: sb.String(), pos: start})
			return nil
		}
		sb.WriteByte(c)
		l.pos++
	}
	return errf("unterminated string at %d", start)
}

func (l *lexer) lexNumber() error {
	start := l.pos
	if l.src[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
		l.pos++
	}
	// P1：支持小数（DECIMAL 字面量，如 123.45 / -0.5）
	if l.pos < len(l.src) && l.src[l.pos] == '.' {
		l.pos++
		if l.pos >= len(l.src) || !isDigit(l.src[l.pos]) {
			return errf("invalid number %q at %d", l.src[start:l.pos], start)
		}
		for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
			l.pos++
		}
	}
	text := l.src[start:l.pos]
	if !strings.Contains(text, ".") {
		if _, err := strconv.ParseInt(text, 10, 64); err != nil {
			return errf("invalid number %q at %d", text, start)
		}
	}
	l.toks = append(l.toks, token{kind: tokNumber, text: text, pos: start})
	return nil
}

func (l *lexer) lexIdent() {
	start := l.pos
	for l.pos < len(l.src) && isIdentPart(l.src[l.pos]) {
		l.pos++
	}
	text := l.src[start:l.pos]
	if keywords[strings.ToUpper(text)] {
		l.toks = append(l.toks, token{kind: tokKeyword, text: strings.ToUpper(text), pos: start})
		return
	}
	l.toks = append(l.toks, token{kind: tokIdent, text: text, pos: start})
}

func isDigit(c byte) bool     { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
func isIdentPart(c byte) bool { return isIdentStart(c) || isDigit(c) }
