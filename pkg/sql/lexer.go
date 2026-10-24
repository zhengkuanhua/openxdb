package sql

import (
	"strings"
)

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokNumber
	tokString
	tokStar      // *
	tokPlus      // +
	tokMinus     // -
	tokSlash     // /
	tokMod       // %
	tokLParen    // (
	tokRParen    // )
	tokComma     // ,
	tokSemicolon // ;
	tokEq        // =
	tokAssign    // :=（P0-2 存储过程局部变量赋值风格）
	tokNe        // !=
	tokLt        // <
	tokGt        // >
	tokLe        // <=
	tokGe        // >=
	tokDot       // .
	tokColon     // :
	tokKeyword   // 关键字（大写原文）
	tokVar       // @ident（存储过程变量/参数引用，M15-3）
	tokQuestion  // ?（M18 prepared statement 参数占位符）
)

type token struct {
	kind tokKind
	text string
	pos  int
	raw  string // 关键字原文（未大写化），用于表名等需保留原始大小写场景（M18-C）
}

// keywords 关键字表（大小写不敏感）。
var keywords = map[string]bool{
	"CREATE": true, "TABLE": true, "DROP": true, "IF": true, "EXISTS": true, "INSERT": true,
	"INTO": true, "VALUES": true, "SELECT": true, "FROM": true, "DISTINCT": true,
	"WHERE": true, "ORDER": true, "BY": true, "ASC": true, "DESC": true,
	"LIMIT": true, "OFFSET": true, "PRIMARY": true, "KEY": true, "UPDATE": true,
	"SET": true, "DELETE": true, "AND": true, "INDEX": true, "ON": true,
	"JOIN": true, "INNER": true, "LEFT": true, "OUTER": true,
	"GROUP": true, "BETWEEN": true, "IN": true, "AS": true,
	// P0-4：HAVING 子句（GROUP BY 后过滤）
	"HAVING": true,
	"BEGIN": true, "COMMIT": true, "ROLLBACK": true,
	"EXPORT": true, "IMPORT": true, "TO": true,
	// M18：prepared statement + 生态导出 FORMAT
	"PREPARE": true, "EXECUTE": true, "DEALLOCATE": true, "USING": true, "FORMAT": true,
	// P2：运维语句 + 表达式增强
	"SHOW": true, "EXPLAIN": true, "TABLES": true, "SLOWQUERIES": true, "STATS": true,
	"LIKE": true, "CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
	// M4：多节点集群管理语句
	"ADD": true, "NODE": true, "NODES": true, "REGION": true, "ROUTES": true, "ASSIGN": true,
	// M7：分裂与负载均衡管理语句
	"SPLIT": true, "BALANCE": true,
	// M8(BR)：备份/恢复语句（BACKUP/RESTORE+PITR）
	"BACKUP": true, "RESTORE": true, "LSN": true,
	// M9：在线 DDL（ALTER TABLE）
	"ALTER": true, "COLUMN": true, "RENAME": true,
	// M9：外键约束（FOREIGN KEY / REFERENCES / ON DELETE|ON UPDATE CASCADE|RESTRICT|SET NULL）
	"FOREIGN": true, "REFERENCES": true, "CASCADE": true, "RESTRICT": true, "NULL": true,
	// M9：视图（CREATE VIEW / DROP VIEW / SHOW VIEWS）
	"VIEW": true, "VIEWS": true,
	// M15：物化视图（CREATE/REFRESH/DROP MATERIALIZED VIEW [CONCURRENTLY]）
	"MATERIALIZED": true, "REFRESH": true, "CONCURRENTLY": true,
	// M15-2：触发器（CREATE/DROP TRIGGER、SHOW TRIGGERS、BEFORE/AFTER、FOR EACH ROW/STATEMENT）
	"TRIGGER": true, "TRIGGERS": true, "BEFORE": true, "AFTER": true, "EACH": true, "ROW": true,
	"FOR": true, "STATEMENT": true,
	// M15-3：存储过程（CREATE/DROP PROCEDURE、SHOW PROCEDURES、CALL）
	"PROCEDURE": true, "PROCEDURES": true, "CALL": true,
	// M10：SQL 兼容性增强簇（CTE / 窗口函数 / UPSERT）
	"WITH": true, "OVER": true, "PARTITION": true,
	"REPLACE": true, "DUPLICATE": true, "UNIQUE": true,
	// P0-5：显式窗口帧（ROWS/RANGE、UNBOUNDED PRECEDING/FOLLOWING、CURRENT ROW、n PRECEDING/FOLLOWING）
	"ROWS": true, "RANGE": true, "UNBOUNDED": true, "PRECEDING": true, "FOLLOWING": true, "CURRENT": true,
	// M11：用户管理（CREATE/DROP/ALTER USER ... IDENTIFIED BY ...）
	"USER": true, "IDENTIFIED": true,
	// M11：权限模型（GRANT/REVOKE ... ON ... TO/FROM ...）
	"GRANT": true, "REVOKE": true, "ALL": true,
	// M17：细粒度权限与角色体系（库级/列级授权、CREATE ROLE、GRANT ROLE、
	// SHOW ROLES / SHOW GRANTS）
	"ROLE": true, "ROLES": true, "DATABASE": true, "GRANTS": true,
	// M17：审计日志（SHOW AUDITLOG；SET audit_log 配置名走 ident 不注册关键字，
	// 避免保留 audit 作为表名破坏既有测试/用户库表）
	"AUDITLOG": true,
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
		case c == '@':
			// M15-3：存储过程局部变量/参数引用 @ident
			l.lexVar()
		case c == '?':
			// M18：prepared statement 参数占位符
			l.emit(tokQuestion, "?", 1)
		case c == ';':
			l.emit(tokSemicolon, ";", 1)
		case c == '*':
			l.emit(tokStar, "*", 1)
		case c == '+':
			l.emit(tokPlus, "+", 1)
		case c == '-' && l.peek(1) == '-' && l.dashCommentAhead():
			// MySQL 风格 -- 行注释（-- 后随空白或行尾）
			l.skipToLineEnd()
		case c == '-':
			// T40：负号消歧。前一 token 以值结束时为二元减号；否则若后随数字/小数点，
			// 并入数字字面量（-5 / -1.0，保持 VALUES 等旧路径 tokNumber 语义）。
			if l.prevEndsValue() {
				l.emit(tokMinus, "-", 1)
			} else if l.pos+1 < len(l.src) && (isDigit(l.src[l.pos+1]) || l.src[l.pos+1] == '.') {
				if err := l.lexNumber(); err != nil {
					return nil, err
				}
			} else {
				l.emit(tokMinus, "-", 1)
			}
		case c == '/' && l.peek(1) == '*':
			// MySQL 风格 /* ... */ 块注释（含 /*! 版本注释一并跳过）
			l.skipBlockComment()
		case c == '/':
			l.emit(tokSlash, "/", 1)
		case c == '#':
			// MySQL 风格 # 行注释
			l.skipToLineEnd()
		case c == '%':
			l.emit(tokMod, "%", 1)
		case c == ':' && l.peek(1) == '=':
			l.emit(tokAssign, ":=", 2)
		case c == ':':
			l.emit(tokColon, ":", 1)
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
		case isDigit(c):
			if err := l.lexNumber(); err != nil {
				return nil, err
			}
		case isIdentStart(c):
			// v1.5.0 P2-12：E'...' / e'...' 转义字符串（驱动参数化含换行/反斜杠文本
			// 时生成；普通字符串字面量保持原语义，兼容既有 SQL 与 Windows 路径）。
			if (c == 'E' || c == 'e') && l.pos+1 < len(l.src) &&
				(l.src[l.pos+1] == '\'' || l.src[l.pos+1] == '"') {
				if err := l.lexEString(l.src[l.pos+1]); err != nil {
					return nil, err
				}
				continue
			}
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

// dashCommentAhead 判断 "--" 是否构成 MySQL 行注释：
// 后随空白或到达行尾/输入末尾时为注释，否则视为减法/负号序列。
func (l *lexer) dashCommentAhead() bool {
	n := l.pos + 2
	if n >= len(l.src) {
		return true
	}
	c := l.src[n]
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

// skipToLineEnd 跳过直到行尾（不含换行符本身）。
func (l *lexer) skipToLineEnd() {
	for l.pos < len(l.src) && l.src[l.pos] != '\n' {
		l.pos++
	}
}

// skipBlockComment 跳过 /* ... */ 块注释（从当前 '/' 起，含 /*! 版本注释）。
// 未闭合时跳到输入末尾（宽容处理，与 MySQL 报错的差异可接受）。
func (l *lexer) skipBlockComment() {
	i := strings.Index(l.src[l.pos+2:], "*/")
	if i < 0 {
		l.pos = len(l.src)
		return
	}
	l.pos += 2 + i + 2
}

// prevEndsValue 判断上一 token 是否为可结束操作数的值类 token
// （用于 "-" 的负数/减法消歧，T40）。行首/运算符/关键字后返回 false。
func (l *lexer) prevEndsValue() bool {
	if len(l.toks) == 0 {
		return false
	}
	switch l.toks[len(l.toks)-1].kind {
	case tokIdent, tokNumber, tokString, tokRParen, tokStar, tokVar, tokQuestion:
		return true
	}
	return false
}

func (l *lexer) emit(k tokKind, text string, width int) {
	l.toks = append(l.toks, token{kind: k, text: text, pos: l.pos})
	l.pos += width
}

// lexVar 词法识别存储过程变量/参数引用 @ident（M15-3）。
// token 文本为完整 "@name"（保留前缀，变量替换阶段按名匹配）。
func (l *lexer) lexVar() {
	start := l.pos
	l.pos++ // 跳过 @
	for l.pos < len(l.src) && isIdentPart(l.src[l.pos]) {
		l.pos++
	}
	l.toks = append(l.toks, token{kind: tokVar, text: l.src[start:l.pos], pos: start})
}

func (l *lexer) lexString(q byte) error {
	start := l.pos
	l.pos++
	var sb strings.Builder
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == q {
			// SQL 标准转义：两个连续引号表示一个字面引号（如 'O''Brien'）。
			if l.pos+1 < len(l.src) && l.src[l.pos+1] == q {
				sb.WriteByte(q)
				l.pos += 2
				continue
			}
			l.pos++
			l.toks = append(l.toks, token{kind: tokString, text: sb.String(), pos: start})
			return nil
		}
		sb.WriteByte(c)
		l.pos++
	}
	return errf("unterminated string at %d", start)
}

func (l *lexer) lexEString(q byte) error {
	start := l.pos
	l.pos += 2 // skip E prefix + opening quote
	var sb strings.Builder
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == '\\' && l.pos+1 < len(l.src) {
			// 转义序列：\\ → 反斜杠，\n → 换行，\r → 回车；其余 \x 保持原样。
			next := l.src[l.pos+1]
			switch next {
			case '\\':
				sb.WriteByte('\\')
				l.pos += 2
				continue
			case 'n':
				sb.WriteByte('\n')
				l.pos += 2
				continue
			case 'r':
				sb.WriteByte('\r')
				l.pos += 2
				continue
			}
		}
		if c == q {
			if l.pos+1 < len(l.src) && l.src[l.pos+1] == q {
				sb.WriteByte(q)
				l.pos += 2
				continue
			}
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
	// 科学计数法：1e5 / 6.0E-4 / 1E+06（MySQL 兼容，Connector/J setDouble 内联输出）。
	// e/E 后必须跟数字或 +/-数字 才构成指数；否则回退保持"数字 + 后续 ident"的旧行为（如 1E 表名）。
	if l.pos < len(l.src) && (l.src[l.pos] == 'e' || l.src[l.pos] == 'E') {
		save := l.pos
		exp := l.pos + 1
		if exp < len(l.src) && (l.src[exp] == '+' || l.src[exp] == '-') {
			exp++
		}
		if exp < len(l.src) && isDigit(l.src[exp]) {
			l.pos = exp
			for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
				l.pos++
			}
		} else {
			l.pos = save
		}
	}
	text := l.src[start:l.pos]
	// 数字 token 不在此处做 int64 范围校验：隐式 region ID（tableID|1<<63）等
	// uint64 值也应可词法识别；有效性由下游解析（parseInt10 / ParseUint）按语义校验。
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
		l.toks = append(l.toks, token{kind: tokKeyword, text: strings.ToUpper(text), raw: text, pos: start})
		return
	}
	l.toks = append(l.toks, token{kind: tokIdent, text: text, pos: start})
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
func isIdentPart(c byte) bool { return isIdentStart(c) || isDigit(c) }
