package sql

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// proceduresKey 存储过程目录键（独立于 m:tables / m:views / m:matviews / m:triggers）。
// ProcedureMeta 定义于 ast.go（M15-3）。
var proceduresKey = []byte("m:procedures")

// maxProcDepth 存储过程递归/嵌套调用最大深度（P0-2）。
// 放开 M15-3 的"禁止递归"限制：改为显式深度上限，配合每层独立 env 与
// defer 递减保证栈安全；超限时报错（与事务/错误语义一致，调用方语句失败）。
const maxProcDepth = 64

// loadProcedures 读取存储过程目录。
func loadProcedures(get func(key []byte) ([]byte, error)) ([]*ProcedureMeta, error) {
	raw, err := get(proceduresKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	if raw == nil || len(raw) == 0 {
		return nil, nil
	}
	var procs []*ProcedureMeta
	if err := json.Unmarshal(raw, &procs); err != nil {
		return nil, &SQLError{Msg: "corrupted procedure catalog"}
	}
	return procs, nil
}

// saveProcedures 序列化存储过程目录。
func saveProcedures(procs []*ProcedureMeta) ([]byte, error) {
	return json.Marshal(procs)
}

// findProcedure 按名查找存储过程。
func findProcedure(procs []*ProcedureMeta, name string) *ProcedureMeta {
	for _, p := range procs {
		if p.Name == name {
			return p
		}
	}
	return nil
}

// procedureBodySupported 存储过程体支持的 SQL 语句（创建时白名单校验）。
// 允许嵌套 CALL（跨过程调用 / 递归），执行时以 maxProcDepth 深度上限防护，
// 避免无限递归撑爆调用栈。
func procedureBodySupported(stmt Stmt) bool {
	switch stmt.(type) {
	case *InsertStmt, *UpdateStmt, *DeleteStmt, *SelectStmt, *CallStmt:
		return true
	}
	return false
}

// valueToToken 把调用实参 / 局部变量值还原为词法 token（M15-3）。
// INT / DECIMAL 还原为 tokNumber 原文，TEXT 还原为 tokString 带引号原文，
// 代入过程体后重新 Parse 走标准执行路径。
func valueToToken(v Value) token {
	switch v.Kind {
	case "INT":
		return token{kind: tokNumber, text: strconv.FormatInt(v.I, 10)}
	case "DECIMAL":
		return token{kind: tokNumber, text: v.S}
	default:
		return token{kind: tokString, text: v.S}
	}
}

// setValueToken 用变量值替换 @var token 的文本（保留原 token 位置）。
func setValueToken(t token, v Value) token {
	nt := valueToToken(v)
	nt.pos = t.pos
	return nt
}

// tokensToString 将 token 列表重建为 SQL 文本（用于变量代入后的语句重解析）。
// 字符串 token 的 text 不含引号（lexString 剥离），重建时统一补回单引号；
// 仅在相邻 token 都以标识符成分开头/结尾时插入空格，避免 "SELECT*" 等粘连。
func tokensToString(toks []token) string {
	var sb strings.Builder
	for i, t := range toks {
		if i > 0 && needSpace(toks[i-1].text, t.text) {
			sb.WriteByte(' ')
		}
		if t.kind == tokString {
			sb.WriteByte('\'')
			sb.WriteString(t.text)
			sb.WriteByte('\'')
		} else {
			sb.WriteString(t.text)
		}
	}
	return sb.String()
}

func needSpace(prev, cur string) bool {
	if prev == "" || cur == "" {
		return false
	}
	prevEnd := prev[len(prev)-1]
	curStart := cur[0]
	identByte := func(c byte) bool { return isIdentPart(c) || c == '\'' || c == '"' }
	return identByte(prevEnd) && identByte(curStart)
}

// replaceVars 扫描语句文本，把 @name 变量引用替换为当前环境中的值。
// env 的 key 统一小写（参数名 / 局部变量名大小写不敏感）。
// missingErr=true 时未定义变量直接报错（执行期）；false 用于创建期预校验
// （保留原 token，仅验证语句可解析）。
func replaceVars(src string, env map[string]Value, missingErr bool) (string, error) {
	toks, err := lex(src)
	if err != nil {
		return "", err
	}
	changed := false
	for i := range toks {
		if toks[i].kind != tokVar {
			continue
		}
		key := strings.ToLower(toks[i].text[1:])
		if v, ok := env[key]; ok {
			toks[i] = setValueToken(toks[i], v)
			changed = true
		} else if missingErr {
			return "", &SQLError{Msg: "unknown procedure variable: " + toks[i].text}
		}
	}
	if !changed {
		return src, nil
	}
	return tokensToString(toks), nil
}

// tokenToValue 把字面量 token 还原为 Value。
func tokenToValue(t token) (Value, error) {
	switch t.kind {
	case tokNumber:
		if strings.Contains(t.text, ".") {
			scaled, canon, err := parseDecimal(t.text)
			if err != nil {
				return Value{}, err
			}
			return Value{Kind: "DECIMAL", I: scaled, S: canon}, nil
		}
		n, err := parseInt10(t.text)
		if err != nil {
			return Value{}, err
		}
		return IntVal(n), nil
	case tokString:
		return StrVal(t.text), nil
	}
	return Value{}, &SQLError{Msg: "expected literal value in procedure body"}
}

// ===== P0-2：存储过程控制流解释器 =====

// procStmt 过程体语句节点。
type procStmt interface{ procStmt() }

// procSQL 普通 SQL 语句（INSERT/UPDATE/DELETE/SELECT），执行期变量代入后走标准路径。
type procSQL struct{ raw string }

func (procSQL) procStmt() {}

// procCall 过程体内 CALL <proc>(<args...>)：实参为表达式 token，
// 执行期用调用方环境求值后再进入被调过程（支持 @var 与裸参数名）。
type procCall struct {
	name string
	args [][]token
}

func (procCall) procStmt() {}

// procSet 局部变量赋值 SET @x = <expr> 或 SET @x := <expr>。
type procSet struct {
	name string
	expr []token
}

func (procSet) procStmt() {}

// procDeclare 局部变量声明 DECLARE @x [TYPE] [DEFAULT <expr>]。
type procDeclare struct {
	name    string
	typ     string
	init    []token
	hasInit bool
}

func (procDeclare) procStmt() {}

// procIf IF ... THEN ... [ELSIF ... THEN ...]* [ELSE ...] END IF。
type procIf struct{ branches []procIfBranch }

func (procIf) procStmt() {}

type procIfBranch struct {
	cond   []token
	isElse bool
	body   []procStmt
}

// procLoop LOOP ... END LOOP（无限循环，配合 EXIT/LEAVE / ITERATE）。
// label 可选：[label:] LOOP ... END LOOP [label]（LEAVE <label> 跳转目标）。
type procLoop struct {
	label string
	body  []procStmt
}

func (procLoop) procStmt() {}

// procWhile WHILE <cond> LOOP ... END LOOP / WHILE <cond> DO ... END WHILE。
// label 可选：[label:] WHILE ... END WHILE [label]。
type procWhile struct {
	label string
	cond  []token
	body  []procStmt
}

func (procWhile) procStmt() {}

// procFor FOR @i IN <from> .. <to> | <from> TO <to> LOOP ... END LOOP。
// label 可选：[label:] FOR ... END FOR [label]。
type procFor struct {
	label string
	name  string
	from  []token
	to    []token
	body  []procStmt
}

func (procFor) procStmt() {}

// procRepeat REPEAT <body> UNTIL <cond> END REPEAT（先执行后判断）。
// label 可选：[label:] REPEAT ... END REPEAT [label]。
type procRepeat struct {
	label string
	cond  []token
	body  []procStmt
}

func (procRepeat) procStmt() {}

// procCase CASE 语句：简单 CASE（base 非空）或搜索 CASE（base 为空）。
type procCase struct {
	base     []token
	whens    []procCaseWhen
	elseBody []procStmt
	hasElse  bool
}

func (procCase) procStmt() {}

type procCaseWhen struct {
	cond []token
	body []procStmt
}

// procExit EXIT [WHEN <cond>] / LEAVE [label] [WHEN <cond>]：
// 跳出当前循环；LEAVE <label> 跳出指定标签的循环。
type procExit struct {
	label   string // LEAVE <label> 的目标（空表示最近循环）
	cond    []token
	hasCond bool
}

func (procExit) procStmt() {}

// procIterate ITERATE [label] [WHEN <cond>] / CONTINUE [WHEN <cond>]：
// 跳到当前循环的下一次迭代（重新判断循环条件）。
type procIterate struct {
	label   string // ITERATE <label> 的目标（空表示最近循环）
	cond    []token
	hasCond bool
}

func (procIterate) procStmt() {}

// maxLoopIterations 单层循环最大迭代次数（防死循环）。
// 任意循环（LOOP/WHILE/FOR/REPEAT）单次迭代计数超过该值即报错终止。
const maxLoopIterations = 10000

// procParser 过程体 token 流解析器。
type procParser struct {
	toks      []token
	pos       int
	loopDepth int // 当前解析嵌套的循环层数（校验 EXIT/LEAVE 位置）
}

func (p *procParser) eof() bool  { return p.pos >= len(p.toks) }
func (p *procParser) cur() token {
	if p.pos >= len(p.toks) {
		return token{}
	}
	return p.toks[p.pos]
}
func (p *procParser) kw() string    { return strings.ToUpper(p.cur().text) }
func (p *procParser) next()         { if p.pos < len(p.toks) { p.pos++ } }
func (p *procParser) expect(s string) error {
	if p.kw() != s {
		return &SQLError{Msg: "expected " + s + " in procedure body, got " + p.cur().text}
	}
	p.next()
	return nil
}

// procExprStopKW 表达式收集的固有边界关键字（不会出现在合法表达式中）。
var procExprStopKW = map[string]bool{
	"END": true, "ELSE": true, "ELSIF": true, "ELSEIF": true,
	"WHEN": true, "THEN": true, "LOOP": true, "DO": true, "TO": true,
}

// collectUntil 收集表达式 token 直到分号（stopSemicolon=true）、固有边界关键字、
// 附加 stop 关键字或连续 .. 处停止（不消费停止 token）。括号深度与 CASE ... END
// 内的关键字不触发停止。
func (p *procParser) collectUntil(stopSemicolon bool, extra ...string) ([]token, error) {
	start := p.pos
	parenDepth := 0
	caseDepth := 0
	for !p.eof() {
		t := p.cur()
		switch {
		case t.kind == tokLParen:
			parenDepth++
		case t.kind == tokRParen:
			parenDepth--
		case t.kind == tokKeyword && t.text == "CASE":
			caseDepth++
		case t.kind == tokKeyword && t.text == "END":
			if caseDepth > 0 {
				caseDepth--
			} else if parenDepth == 0 {
				// 过程块结束 END：停止收集（不消费）
				return p.toks[start:p.pos], nil
			}
		case t.kind == tokSemicolon:
			if parenDepth == 0 && caseDepth == 0 {
				return p.toks[start:p.pos], nil
			}
		default:
			if parenDepth == 0 && caseDepth == 0 {
				k := strings.ToUpper(t.text)
				if procExprStopKW[k] {
					return p.toks[start:p.pos], nil
				}
				for _, s := range extra {
					if k == s {
						return p.toks[start:p.pos], nil
					}
				}
				if t.kind == tokDot && p.pos+1 < len(p.toks) && p.toks[p.pos+1].kind == tokDot {
					return p.toks[start:p.pos], nil
				}
			}
		}
		p.next()
	}
	return p.toks[start:p.pos], nil
}

// parseProcedureBody 把过程体（BEGIN ... END 之间文本）解析为语句树。
func parseProcedureBody(body string) ([]procStmt, error) {
	toks, err := lex(body)
	if err != nil {
		return nil, err
	}
	p := &procParser{toks: toks}
	stmts, err := p.parseSeq(nil)
	if err != nil {
		return nil, err
	}
	if !p.eof() {
		return nil, &SQLError{Msg: "unexpected token in procedure body: " + p.cur().text}
	}
	return stmts, nil
}

// parseSeq 解析语句序列直到 EOF 或遇到 stops 中的关键字（不消费）。
func (p *procParser) parseSeq(stops map[string]bool) ([]procStmt, error) {
	var out []procStmt
	for {
		if p.eof() {
			break
		}
		// 标签循环前缀：[label:] LOOP/WHILE/FOR/REPEAT。
		if p.cur().kind == tokIdent && p.pos+1 < len(p.toks) && p.toks[p.pos+1].kind == tokColon &&
			p.pos+2 < len(p.toks) {
			switch strings.ToUpper(p.toks[p.pos+2].text) {
			case "LOOP", "WHILE", "FOR", "REPEAT":
				p.next() // label
				p.next() // :
			}
		}
		k := p.kw()
		if stops != nil && stops[k] {
			break
		}
		var s procStmt
		var err error
		switch k {
		case "IF":
			s, err = p.parseIf()
		case "LOOP":
			s, err = p.parseLoop()
		case "WHILE":
			s, err = p.parseWhile()
		case "FOR":
			s, err = p.parseFor()
		case "REPEAT":
			s, err = p.parseRepeat()
		case "CASE":
			s, err = p.parseCase()
		case "DECLARE":
			s, err = p.parseDeclare()
		case "SET":
			s, err = p.parseSet()
		case "EXIT", "LEAVE":
			s, err = p.parseExit()
		case "ITERATE", "CONTINUE":
			s, err = p.parseIterate()
		default:
			s, err = p.parseSQL(stops)
		}
		if err != nil {
			return nil, err
		}
		if s != nil {
			out = append(out, s)
		}
	}
	return out, nil
}

// parseSQL 收集一条普通 SQL 语句（直到顶层分号 / 块结束关键字 / EOF）。
// 括号深度与 CASE ... END 内的分号不计入切分；块结束关键字仅在深度 0 生效。
// CALL 语句单独走 parseProcCall（实参支持表达式 token）。
func (p *procParser) parseSQL(stops map[string]bool) (procStmt, error) {
	if p.kw() == "CALL" {
		return p.parseProcCall(stops)
	}
	start := p.pos
	parenDepth := 0
	caseDepth := 0
	for !p.eof() {
		t := p.cur()
		switch {
		case t.kind == tokLParen:
			parenDepth++
		case t.kind == tokRParen:
			parenDepth--
		case t.kind == tokKeyword && t.text == "CASE":
			caseDepth++
		case t.kind == tokKeyword && t.text == "END":
			if caseDepth > 0 {
				caseDepth--
			} else if parenDepth == 0 && stops != nil && stops["END"] {
				return p.finishSQL(start, parenDepth, caseDepth)
			}
		case t.kind == tokSemicolon:
			if parenDepth == 0 && caseDepth == 0 {
				return p.finishSQL(start, parenDepth, caseDepth)
			}
		default:
			if parenDepth == 0 && caseDepth == 0 && stops != nil && stops[strings.ToUpper(t.text)] {
				return p.finishSQL(start, parenDepth, caseDepth)
			}
		}
		p.next()
	}
	return p.finishSQL(start, parenDepth, caseDepth)
}

// finishSQL 收尾一条普通 SQL 语句：重建文本并消费结束分号。
func (p *procParser) finishSQL(start, parenDepth, caseDepth int) (procStmt, error) {
	if parenDepth != 0 || caseDepth != 0 {
		return nil, &SQLError{Msg: "unbalanced parentheses or CASE in procedure body"}
	}
	if p.pos == start {
		return nil, nil
	}
	raw := strings.TrimSpace(tokensToString(p.toks[start:p.pos]))
	if p.kw() == ";" {
		p.next()
	}
	if raw == "" {
		return nil, nil
	}
	return procSQL{raw: raw}, nil
}

// parseProcCall 解析 CALL <name> ( <arg>, ... ) ;：参数收集到顶层逗号/右括号。
func (p *procParser) parseProcCall(stops map[string]bool) (procStmt, error) {
	if err := p.expect("CALL"); err != nil {
		return nil, err
	}
	nameTok := p.cur()
	if nameTok.kind != tokIdent {
		return nil, &SQLError{Msg: "expected procedure name after CALL in procedure body"}
	}
	p.next()
	pc := &procCall{name: nameTok.text}
	if p.kw() == "(" {
		p.next()
		if p.kw() != ")" {
			for {
				arg, err := p.collectProcCallArg()
				if err != nil {
					return nil, err
				}
				pc.args = append(pc.args, arg)
				if p.kw() == "," {
					p.next()
					continue
				}
				break
			}
		}
		if err := p.expect(")"); err != nil {
			return nil, err
		}
	}
	if p.kw() == ";" {
		p.next()
	}
	return pc, nil
}

// collectProcCallArg 收集一个 CALL 实参表达式 token（直到顶层逗号/右括号）。
func (p *procParser) collectProcCallArg() ([]token, error) {
	start := p.pos
	parenDepth := 0
	for !p.eof() {
		t := p.cur()
		switch t.kind {
		case tokLParen:
			parenDepth++
		case tokRParen:
			if parenDepth == 0 {
				return p.toks[start:p.pos], nil
			}
			parenDepth--
		case tokComma:
			if parenDepth == 0 {
				return p.toks[start:p.pos], nil
			}
		}
		p.next()
	}
	return nil, &SQLError{Msg: "unterminated CALL argument in procedure body"}
}

// parseIf 解析 IF/ELSIF/ELSE/END IF（也接受 ENDIF）。
func (p *procParser) parseIf() (procStmt, error) {
	p.next() // IF
	var branches []procIfBranch
	for {
		cond, err := p.collectUntil(false, "THEN")
		if err != nil {
			return nil, err
		}
		if err := p.expect("THEN"); err != nil {
			return nil, err
		}
		body, err := p.parseSeq(map[string]bool{"ELSIF": true, "ELSEIF": true, "ELSE": true, "END": true})
		if err != nil {
			return nil, err
		}
		branches = append(branches, procIfBranch{cond: cond, body: body})
		k := p.kw()
		if k == "ELSIF" || k == "ELSEIF" {
			p.next()
			continue
		}
		if k == "ELSE" {
			p.next()
			elseBody, err := p.parseSeq(map[string]bool{"END": true})
			if err != nil {
				return nil, err
			}
			branches = append(branches, procIfBranch{isElse: true, body: elseBody})
			break
		}
		break
	}
	if err := p.expect("END"); err != nil {
		return nil, err
	}
	if p.kw() == "IF" {
		p.next()
	} else if p.kw() == "ENDIF" {
		p.next()
	} else {
		return nil, &SQLError{Msg: "expected END IF in procedure body"}
	}
	if p.kw() == ";" {
		p.next()
	}
	return procIf{branches: branches}, nil
}

// parseLabel 解析可选的 [label:] 前缀（当前为 tokIdent + ':' 时消费并返回 label）。
func (p *procParser) parseLabel() string {
	if p.cur().kind == tokIdent && p.pos+1 < len(p.toks) && p.toks[p.pos+1].kind == tokColon {
		lbl := p.cur().text
		p.next()
		p.next()
		return lbl
	}
	return ""
}

// consumeEndLabel 消费可选的 END ... [label] 结束标签并返回。
func (p *procParser) consumeEndLabel() string {
	if p.cur().kind == tokIdent {
		lbl := p.cur().text
		p.next()
		return lbl
	}
	return ""
}

// parseLoop 解析 [label:] LOOP ... END LOOP [label]。
func (p *procParser) parseLoop() (procStmt, error) {
	label := p.parseLabel()
	p.next() // LOOP
	p.loopDepth++
	body, err := p.parseSeq(map[string]bool{"END": true})
	p.loopDepth--
	if err != nil {
		return nil, err
	}
	if err := p.expect("END"); err != nil {
		return nil, err
	}
	if err := p.expect("LOOP"); err != nil {
		return nil, err
	}
	endLabel := p.consumeEndLabel()
	if label != "" && endLabel != "" && !strings.EqualFold(label, endLabel) {
		return nil, &SQLError{Msg: "loop end label mismatch: " + label + " vs " + endLabel}
	}
	if p.kw() == ";" {
		p.next()
	}
	return procLoop{label: label, body: body}, nil
}

// parseWhile 解析 [label:] WHILE <cond> LOOP ... END LOOP 或
// WHILE <cond> DO ... END WHILE [label]。
func (p *procParser) parseWhile() (procStmt, error) {
	label := p.parseLabel()
	p.next() // WHILE
	cond, err := p.collectUntil(false, "LOOP", "DO")
	if err != nil {
		return nil, err
	}
	endKW := ""
	k := p.kw()
	if k == "LOOP" {
		p.next()
		endKW = "LOOP"
	} else if k == "DO" {
		p.next()
		endKW = "WHILE"
	} else {
		return nil, &SQLError{Msg: "expected LOOP or DO after WHILE condition in procedure body"}
	}
	p.loopDepth++
	body, err := p.parseSeq(map[string]bool{"END": true})
	p.loopDepth--
	if err != nil {
		return nil, err
	}
	if err := p.expect("END"); err != nil {
		return nil, err
	}
	if err := p.expect(endKW); err != nil {
		return nil, err
	}
	endLabel := p.consumeEndLabel()
	if label != "" && endLabel != "" && !strings.EqualFold(label, endLabel) {
		return nil, &SQLError{Msg: "loop end label mismatch: " + label + " vs " + endLabel}
	}
	if p.kw() == ";" {
		p.next()
	}
	return procWhile{label: label, cond: cond, body: body}, nil
}

// parseFor 解析 [label:] FOR @i IN <from> .. <to> LOOP ... END LOOP [label]
//（也接受 TO 与 END FOR）。
// 注：lexer 数字解析会把 `1..10` 视为非法小数，范围分隔符必须写作 `1 .. 10`（带空格）。
func (p *procParser) parseFor() (procStmt, error) {
	label := p.parseLabel()
	p.next() // FOR
	if p.cur().kind != tokVar {
		return nil, &SQLError{Msg: "expected @loop variable after FOR in procedure body"}
	}
	name := p.cur().text[1:]
	p.next()
	if err := p.expect("IN"); err != nil {
		return nil, err
	}
	from, err := p.collectUntil(false, "TO")
	if err != nil {
		return nil, err
	}
	if p.kw() == "TO" {
		p.next()
	} else if p.cur().kind == tokDot {
		p.next()
		if p.cur().kind != tokDot {
			return nil, &SQLError{Msg: "expected .. or TO in FOR range in procedure body"}
		}
		p.next()
	} else {
		return nil, &SQLError{Msg: "expected .. or TO in FOR range in procedure body"}
	}
	to, err := p.collectUntil(false, "LOOP")
	if err != nil {
		return nil, err
	}
	if err := p.expect("LOOP"); err != nil {
		return nil, err
	}
	p.loopDepth++
	body, err := p.parseSeq(map[string]bool{"END": true})
	p.loopDepth--
	if err != nil {
		return nil, err
	}
	if err := p.expect("END"); err != nil {
		return nil, err
	}
	if p.kw() == "LOOP" {
		p.next()
	} else if p.kw() == "FOR" {
		p.next()
	} else {
		return nil, &SQLError{Msg: "expected END LOOP after FOR body in procedure body"}
	}
	if p.kw() == ";" {
		p.next()
	}
	return procFor{label: label, name: name, from: from, to: to, body: body}, nil
}

// parseRepeat 解析 [label:] REPEAT <body> UNTIL <cond> END REPEAT [label]
//（先执行后判断：UNTIL 条件为真时结束循环，与 WHILE 前置判断相反）。
func (p *procParser) parseRepeat() (procStmt, error) {
	label := p.parseLabel()
	p.next() // REPEAT
	p.loopDepth++
	body, err := p.parseSeq(map[string]bool{"UNTIL": true, "END": true})
	p.loopDepth--
	if err != nil {
		return nil, err
	}
	if p.kw() != "UNTIL" {
		return nil, &SQLError{Msg: "REPEAT UNTIL condition required in procedure body"}
	}
	p.next() // UNTIL
	cond, err := p.collectUntil(false, "END")
	if err != nil {
		return nil, err
	}
	if len(cond) == 0 {
		return nil, &SQLError{Msg: "REPEAT UNTIL condition required in procedure body"}
	}
	if err := p.expect("END"); err != nil {
		return nil, err
	}
	if err := p.expect("REPEAT"); err != nil {
		return nil, err
	}
	endLabel := p.consumeEndLabel()
	if label != "" && endLabel != "" && !strings.EqualFold(label, endLabel) {
		return nil, &SQLError{Msg: "loop end label mismatch: " + label + " vs " + endLabel}
	}
	if p.kw() == ";" {
		p.next()
	}
	return procRepeat{label: label, cond: cond, body: body}, nil
}

// parseCase 解析 CASE 语句：CASE <base> WHEN ... END CASE 或搜索 CASE WHEN ... END CASE。
func (p *procParser) parseCase() (procStmt, error) {
	p.next() // CASE
	var base []token
	var err error
	if p.kw() != "WHEN" {
		base, err = p.collectUntil(false, "WHEN")
		if err != nil {
			return nil, err
		}
	}
	var whens []procCaseWhen
	for {
		if err := p.expect("WHEN"); err != nil {
			return nil, err
		}
		cond, err := p.collectUntil(false, "THEN")
		if err != nil {
			return nil, err
		}
		if err := p.expect("THEN"); err != nil {
			return nil, err
		}
		body, err := p.parseSeq(map[string]bool{"WHEN": true, "ELSE": true, "END": true})
		if err != nil {
			return nil, err
		}
		whens = append(whens, procCaseWhen{cond: cond, body: body})
		if p.kw() == "WHEN" {
			continue
		}
		break
	}
	var elseBody []procStmt
	hasElse := false
	if p.kw() == "ELSE" {
		p.next()
		elseBody, err = p.parseSeq(map[string]bool{"END": true})
		if err != nil {
			return nil, err
		}
		hasElse = true
	}
	if err := p.expect("END"); err != nil {
		return nil, err
	}
	if err := p.expect("CASE"); err != nil {
		return nil, err
	}
	if p.kw() == ";" {
		p.next()
	}
	return procCase{base: base, whens: whens, elseBody: elseBody, hasElse: hasElse}, nil
}

// parseDeclare 解析 DECLARE @x [TYPE] [DEFAULT <expr>]。
func (p *procParser) parseDeclare() (procStmt, error) {
	p.next() // DECLARE
	if p.cur().kind != tokVar {
		return nil, &SQLError{Msg: "expected @variable after DECLARE in procedure body"}
	}
	name := p.cur().text[1:]
	p.next()
	st := procDeclare{name: name}
	if p.kw() != "" && p.kw() != "DEFAULT" && p.kw() != "=" && p.kw() != ":=" && p.kw() != ";" {
		st.typ = p.cur().text
		p.next()
	}
	if p.kw() == "DEFAULT" {
		p.next()
		init, err := p.collectUntil(true)
		if err != nil {
			return nil, err
		}
		st.init = init
		st.hasInit = true
	} else if p.kw() == "=" || p.kw() == ":=" {
		p.next()
		init, err := p.collectUntil(true)
		if err != nil {
			return nil, err
		}
		st.init = init
		st.hasInit = true
	}
	if p.kw() == ";" {
		p.next()
	}
	return st, nil
}

// parseSet 解析 SET @x = <expr> 或 SET @x := <expr>（过程局部变量赋值）。
func (p *procParser) parseSet() (procStmt, error) {
	p.next() // SET
	if p.cur().kind != tokVar {
		return nil, &SQLError{Msg: "expected @variable after SET in procedure body"}
	}
	name := p.cur().text[1:]
	p.next()
	if p.kw() != "=" && p.kw() != ":=" {
		return nil, &SQLError{Msg: "expected = or := after SET @variable in procedure body"}
	}
	p.next()
	expr, err := p.collectUntil(true)
	if err != nil {
		return nil, err
	}
	if len(expr) == 0 {
		return nil, &SQLError{Msg: "SET requires an expression in procedure body"}
	}
	if p.kw() == ";" {
		p.next()
	}
	return procSet{name: name, expr: expr}, nil
}

// parseExit 解析 EXIT [WHEN <cond>] / LEAVE [WHEN <cond>]。
func (p *procParser) parseExit() (procStmt, error) {
	p.next() // EXIT / LEAVE
	if p.loopDepth == 0 {
		return nil, &SQLError{Msg: "EXIT/LEAVE outside loop in procedure body"}
	}
	st := procExit{}
	if p.kw() == "WHEN" {
		p.next()
		cond, err := p.collectUntil(true)
		if err != nil {
			return nil, err
		}
		st.cond = cond
		st.hasCond = true
	}
	if p.kw() == ";" {
		p.next()
	}
	return st, nil
}

// parseIterate 解析 ITERATE [label] [WHEN <cond>] / CONTINUE [label] [WHEN <cond>]：
// 跳到当前循环的下一次迭代（重新判断循环条件）。
// label 仅当后续紧跟分号或 WHEN 时才按标签消费，避免误吞 WHEN 关键字。
func (p *procParser) parseIterate() (procStmt, error) {
	p.next() // ITERATE / CONTINUE
	if p.loopDepth == 0 {
		return nil, &SQLError{Msg: "ITERATE/CONTINUE outside loop in procedure body"}
	}
	st := procIterate{}
	if p.cur().kind == tokIdent && p.pos+1 < len(p.toks) {
		nk := strings.ToUpper(p.toks[p.pos+1].text)
		if nk == ";" || nk == "WHEN" {
			st.label = p.cur().text
			p.next()
		}
	}
	if p.kw() == "WHEN" {
		p.next()
		cond, err := p.collectUntil(true)
		if err != nil {
			return nil, err
		}
		st.cond = cond
		st.hasCond = true
	}
	if p.kw() == ";" {
		p.next()
	}
	return st, nil
}

// validProcVarType 校验 DECLARE 变量类型白名单（宽松：INT/DECIMAL/TEXT/BOOLEAN 系列）。
func validProcVarType(typ string) bool {
	switch strings.ToUpper(typ) {
	case "INT", "INTEGER", "TEXT", "DECIMAL", "BOOLEAN", "VARCHAR", "CHAR":
		return true
	}
	return false
}

// validateProcedureBody 创建期预校验过程体：可解析为受支持的控制流 / SQL 语句树，
// 每条普通 SQL 语句可解析且为受支持类型（INSERT / UPDATE / DELETE / SELECT /
// CALL），DECLARE 类型合法，IF/WHILE/CASE 条件与 FOR 范围非空，EXIT 位于循环内。
// @var 引用在执行期由实参/局部变量代入，此处以占位字面量替换后仅做语法/类型级校验。
func validateProcedureBody(body string) error {
	stmts, err := parseProcedureBody(body)
	if err != nil {
		return err
	}
	if len(stmts) == 0 {
		return &SQLError{Msg: "procedure body required"}
	}
	return validateProcStmts(stmts)
}

// validateProcStmts 递归校验语句树。
func validateProcStmts(stmts []procStmt) error {
	for _, s := range stmts {
		switch st := s.(type) {
		case procSQL:
			toks, err := lex(st.raw)
			if err != nil {
				return err
			}
			env := make(map[string]Value)
			for _, t := range toks {
				if t.kind == tokVar {
					env[strings.ToLower(t.text[1:])] = IntVal(1)
				}
			}
			sub, err := replaceVars(st.raw, env, true)
			if err != nil {
				return err
			}
			stmt, err := Parse(sub)
			if err != nil {
				return err
			}
			if !procedureBodySupported(stmt) {
				return &SQLError{Msg: "unsupported statement in procedure body"}
			}
		case *procCall:
			if st.name == "" {
				return &SQLError{Msg: "CALL procedure name required in procedure body"}
			}
		case procDeclare:
			if st.typ != "" && !validProcVarType(st.typ) {
				return &SQLError{Msg: "unsupported variable type: " + st.typ}
			}
		case procIf:
			for _, br := range st.branches {
				if !br.isElse && len(br.cond) == 0 {
					return &SQLError{Msg: "IF condition required in procedure body"}
				}
				if err := validateProcStmts(br.body); err != nil {
					return err
				}
			}
		case procLoop:
			if err := validateProcStmts(st.body); err != nil {
				return err
			}
		case procWhile:
			if len(st.cond) == 0 {
				return &SQLError{Msg: "WHILE condition required in procedure body"}
			}
			if err := validateProcStmts(st.body); err != nil {
				return err
			}
		case procFor:
			if len(st.from) == 0 || len(st.to) == 0 {
				return &SQLError{Msg: "FOR range required in procedure body"}
			}
			if err := validateProcStmts(st.body); err != nil {
				return err
			}
		case procCase:
			for _, w := range st.whens {
				if len(w.cond) == 0 {
					return &SQLError{Msg: "CASE WHEN condition required in procedure body"}
				}
				if err := validateProcStmts(w.body); err != nil {
					return err
				}
			}
			if st.hasElse {
				if err := validateProcStmts(st.elseBody); err != nil {
					return err
				}
			}
		case procExit:
			// EXIT/LEAVE 必须位于循环内，已在解析期校验。
		case procRepeat:
			if len(st.cond) == 0 {
				return &SQLError{Msg: "REPEAT UNTIL condition required in procedure body"}
			}
			if err := validateProcStmts(st.body); err != nil {
				return err
			}
		case procIterate:
			// ITERATE/CONTINUE 必须位于循环内，已在解析期校验。
		}
	}
	return nil
}

// execCreateProcedure CREATE PROCEDURE <name> [( [IN] <param> <TYPE> [, ...] )]
// BEGIN <stmt>; [<stmt>; ...] END（M15-3）。
// 创建时校验：名称唯一、参数类型合法（parser 已校验）、体可解析且为受支持
// 语句；随后注册到 m:procedures 持久化。与触发器 / 物化视图 / 虚拟视图 /
// 表命名空间互相独立，不触发任何 DML。
func (e *Executor) execCreateProcedure(sess *Session, s *CreateProcedureStmt) (*Result, error) {
	if s.Name == "" {
		return nil, &SQLError{Msg: "procedure name required"}
	}
	if s.Body == "" {
		return nil, &SQLError{Msg: "procedure body required"}
	}
	if err := validateProcedureBody(s.Body); err != nil {
		return nil, &SQLError{Msg: "invalid procedure body: " + err.Error()}
	}
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	procs, err := loadProcedures(tx.Get)
	if err != nil {
		return nil, err
	}
	if findProcedure(procs, s.Name) != nil {
		// T41：CREATE PROCEDURE IF NOT EXISTS——存储过程已存在时静默跳过
		if s.IfNotExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "procedure already exists: " + s.Name}
	}
	procs = append(procs, &ProcedureMeta{
		Name:   s.Name,
		Params: s.Params,
		Body:   s.Body,
	})
	raw, err := saveProcedures(procs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(proceduresKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 1}, nil
}

// execDropProcedure DROP PROCEDURE [IF EXISTS] <name>（M15-3）。
// 仅移除元数据注册，不动任何表数据。
func (e *Executor) execDropProcedure(sess *Session, s *DropProcedureStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	procs, err := loadProcedures(tx.Get)
	if err != nil {
		return nil, err
	}
	if findProcedure(procs, s.Name) == nil {
		if s.IfExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "procedure not exists: " + s.Name}
	}
	var out []*ProcedureMeta
	for _, p := range procs {
		if p.Name == s.Name {
			continue
		}
		out = append(out, p)
	}
	raw, err := saveProcedures(out)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(proceduresKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 1}, nil
}

// execShowProcedures SHOW PROCEDURES（M15-3）。
func (e *Executor) execShowProcedures(sess *Session, s *ShowProceduresStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	procs, err := loadProcedures(tx.Get)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: []string{"procedure", "params", "statement"}}
	sort.Slice(procs, func(i, j int) bool { return procs[i].Name < procs[j].Name })
	for _, p := range procs {
		var params []string
		for _, pm := range p.Params {
			params = append(params, pm.Name+" "+pm.Type)
		}
		res.Rows = append(res.Rows, []Value{
			StrVal(p.Name),
			StrVal(strings.Join(params, ", ")),
			StrVal(p.Body),
		})
	}
	return res, nil
}

// execCall CALL <name> [( <expr> [, <expr> ...] )]（M15-3 / P0-2）。
// 实参为表达式列表，逐项求值后按位置一一对应代入 IN 参数（个数严格匹配；
// 类型不强制，按字面量代入后由体语句自身类型规则校验）。体语句经控制流
// 解释器逐条执行：局部变量赋值（SET/:=）与 DECLARE 更新本次调用局部作用域；
// 其余 SQL 语句把 @var 引用替换为当前值后走标准 execStmt 路径（INSERT/
// UPDATE/DELETE 各自独立隐式事务提交，SELECT 执行但结果集不返回——CALL
// 返回固定 AffectedRows=1）。procDepth 递归/嵌套防护：允许递归调用，但
// 深度超过 maxProcDepth 时报错，避免无限递归撑爆调用栈。
func (e *Executor) execCall(sess *Session, s *CallStmt) (*Result, error) {
	if s.Name == "" {
		return nil, &SQLError{Msg: "procedure name required"}
	}
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	procs, err := loadProcedures(tx.Get)
	if err != nil {
		return nil, err
	}
	proc := findProcedure(procs, s.Name)
	if proc == nil {
		return nil, &SQLError{Msg: "procedure not exists: " + s.Name}
	}
	if len(s.Args) != len(proc.Params) {
		return nil, &SQLError{Msg: "procedure " + s.Name + " expects " + strconv.Itoa(len(proc.Params)) + " argument(s), got " + strconv.Itoa(len(s.Args))}
	}
	if sess.procDepth >= maxProcDepth {
		return nil, &SQLError{Msg: "procedure recursion depth exceeded (max " + strconv.Itoa(maxProcDepth) + "): " + s.Name}
	}
	env := make(map[string]Value, len(proc.Params))
	for i, pm := range proc.Params {
		v, err := evalExprForRow(nil, nil, s.Args[i])
		if err != nil {
			return nil, &SQLError{Msg: "procedure " + s.Name + " argument " + strconv.Itoa(i+1) + ": " + err.Error()}
		}
		env[strings.ToLower(pm.Name)] = v
	}
	sess.procDepth++
	defer func() { sess.procDepth-- }()
	if err := e.execProcedureBody(sess, proc.Body, env); err != nil {
		return nil, &SQLError{Msg: "procedure " + s.Name + " failed: " + err.Error()}
	}
	return &Result{AffectedRows: 1}, nil
}

// execProcedureBody 按控制流解释器执行过程体语句（M15-3 / P0-2 内部实现）。
func (e *Executor) execProcedureBody(sess *Session, body string, env map[string]Value) error {
	stmts, err := parseProcedureBody(body)
	if err != nil {
		return err
	}
	r := &procRunner{e: e, sess: sess, env: env}
	if _, err := r.run(stmts); err != nil {
		return err
	}
	return nil
}

// procRunner 过程体执行上下文。
type procRunner struct {
	e       *Executor
	sess    *Session
	env     map[string]Value
	iterate bool // ITERATE/CONTINUE 触发（由循环层消费，表示继续下一次迭代而非退出）
}

// run 顺序执行语句列表；返回 exited=true 表示 EXIT/LEAVE 触发（由循环层消费）。
func (r *procRunner) run(stmts []procStmt) (bool, error) {
	for _, s := range stmts {
		exited, err := r.runStmt(s)
		if err != nil {
			return false, err
		}
		if exited {
			return true, nil
		}
	}
	return false, nil
}

// runStmt 执行单条过程语句；返回 exited=true 表示 EXIT/LEAVE 触发。
func (r *procRunner) runStmt(s procStmt) (bool, error) {
	switch st := s.(type) {
	case procSQL:
		sub, err := replaceVars(st.raw, r.env, true)
		if err != nil {
			return false, err
		}
		stmt, err := Parse(sub)
		if err != nil {
			return false, err
		}
		if !procedureBodySupported(stmt) {
			return false, &SQLError{Msg: "unsupported statement in procedure body"}
		}
		if _, err := r.e.execStmt(r.sess, stmt); err != nil {
			return false, err
		}
		return false, nil
	case *procCall:
		call := &CallStmt{Name: st.name}
		for _, argToks := range st.args {
			v, err := evalProcExpr(argToks, r.env)
			if err != nil {
				return false, err
			}
			call.Args = append(call.Args, &Expr{Val: &v})
		}
		if _, err := r.e.execCall(r.sess, call); err != nil {
			return false, err
		}
		return false, nil
	case procSet:
		v, err := evalProcExpr(st.expr, r.env)
		if err != nil {
			return false, err
		}
		r.env[strings.ToLower(st.name)] = v
		return false, nil
	case procDeclare:
		if st.hasInit {
			v, err := evalProcExpr(st.init, r.env)
			if err != nil {
				return false, err
			}
			r.env[strings.ToLower(st.name)] = v
		} else {
			r.env[strings.ToLower(st.name)] = procDefaultValue(st.typ)
		}
		return false, nil
	case procIf:
		for _, br := range st.branches {
			if br.isElse {
				continue
			}
			ok, err := evalProcCond(br.cond, r.env)
			if err != nil {
				return false, err
			}
			if ok {
				return r.run(br.body)
			}
		}
		for _, br := range st.branches {
			if br.isElse {
				return r.run(br.body)
			}
		}
		return false, nil
	case procLoop:
		for {
			exited, err := r.run(st.body)
			if err != nil {
				return false, err
			}
			if exited {
				if r.iterate {
					r.iterate = false
					continue
				}
				return false, nil
			}
		}
	case procWhile:
		for {
			ok, err := evalProcCond(st.cond, r.env)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			exited, err := r.run(st.body)
			if err != nil {
				return false, err
			}
			if exited {
				if r.iterate {
					r.iterate = false
					continue
				}
				return false, nil
			}
		}
	case procFor:
		fromV, err := evalProcExpr(st.from, r.env)
		if err != nil {
			return false, err
		}
		toV, err := evalProcExpr(st.to, r.env)
		if err != nil {
			return false, err
		}
		from, ok1 := procToInt(fromV)
		to, ok2 := procToInt(toV)
		if !ok1 || !ok2 {
			return false, &SQLError{Msg: "FOR range must be integers in procedure body"}
		}
		key := strings.ToLower(st.name)
		for i := from; i <= to; i++ {
			r.env[key] = IntVal(i)
			exited, err := r.run(st.body)
			if err != nil {
				return false, err
			}
			if exited {
				if r.iterate {
					r.iterate = false
					continue
				}
				return false, nil
			}
		}
		return false, nil
	case procCase:
		var baseV Value
		var err error
		if len(st.base) > 0 {
			baseV, err = evalProcExpr(st.base, r.env)
			if err != nil {
				return false, err
			}
		}
		for _, w := range st.whens {
			if len(st.base) > 0 {
				wv, err := evalProcExpr(w.cond, r.env)
				if err != nil {
					return false, err
				}
				eq, err := procEqual(baseV, wv)
				if err != nil {
					return false, err
				}
				if eq {
					return r.run(w.body)
				}
			} else {
				ok, err := evalProcCond(w.cond, r.env)
				if err != nil {
					return false, err
				}
				if ok {
					return r.run(w.body)
				}
			}
		}
		if st.hasElse {
			return r.run(st.elseBody)
		}
		return false, nil
	case procExit:
		if !st.hasCond {
			return true, nil
		}
		ok, err := evalProcCond(st.cond, r.env)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		return false, nil
	case procRepeat:
		// REPEAT ... UNTIL：先执行 body，再判断 UNTIL 条件，条件为真即结束。
		for {
			exited, err := r.run(st.body)
			if err != nil {
				return false, err
			}
			if exited {
				if r.iterate {
					r.iterate = false
					continue
				}
				return false, nil
			}
			ok, err := evalProcCond(st.cond, r.env)
			if err != nil {
				return false, err
			}
			if ok {
				return false, nil
			}
		}
	case procIterate:
		// ITERATE/CONTINUE：跳过本次迭代剩余语句，回到循环层继续下一次迭代。
		if !st.hasCond {
			r.iterate = true
			return true, nil
		}
		ok, err := evalProcCond(st.cond, r.env)
		if err != nil {
			return false, err
		}
		if ok {
			r.iterate = true
			return true, nil
		}
		return false, nil
	}
	return false, &SQLError{Msg: "unsupported procedure statement"}
}

// ===== P0-2：过程表达式求值 =====

// procExprParser 过程表达式递归下降解析器（支持字面量、@var、算术、
// 比较、逻辑与括号，比较/逻辑返回 INT 0/1）。
type procExprParser struct {
	toks []token
	pos  int
	env  map[string]Value
}

func (p *procExprParser) eof() bool { return p.pos >= len(p.toks) }
func (p *procExprParser) cur() token { return p.toks[p.pos] }
func (p *procExprParser) kw() string { return strings.ToUpper(p.cur().text) }
func (p *procExprParser) next() { if p.pos < len(p.toks) { p.pos++ } }

// evalProcExpr 求值过程表达式。
func evalProcExpr(toks []token, env map[string]Value) (Value, error) {
	ep := &procExprParser{toks: toks, env: env}
	v, err := ep.parseOr()
	if err != nil {
		return Value{}, err
	}
	if !ep.eof() {
		return Value{}, &SQLError{Msg: "unexpected token in procedure expression: " + ep.cur().text}
	}
	return v, nil
}

// evalProcCond 求值过程条件（真值判定）。
func evalProcCond(toks []token, env map[string]Value) (bool, error) {
	if len(toks) == 0 {
		return false, &SQLError{Msg: "empty condition in procedure body"}
	}
	v, err := evalProcExpr(toks, env)
	if err != nil {
		return false, err
	}
	return procTruthy(v), nil
}

func (p *procExprParser) parseOr() (Value, error) {
	l, err := p.parseAnd()
	if err != nil {
		return Value{}, err
	}
	for !p.eof() && p.kw() == "OR" {
		p.next()
		r, err := p.parseAnd()
		if err != nil {
			return Value{}, err
		}
		if procTruthy(l) || procTruthy(r) {
			l = IntVal(1)
		} else {
			l = IntVal(0)
		}
	}
	return l, nil
}

func (p *procExprParser) parseAnd() (Value, error) {
	l, err := p.parseNot()
	if err != nil {
		return Value{}, err
	}
	for !p.eof() && p.kw() == "AND" {
		p.next()
		r, err := p.parseNot()
		if err != nil {
			return Value{}, err
		}
		if procTruthy(l) && procTruthy(r) {
			l = IntVal(1)
		} else {
			l = IntVal(0)
		}
	}
	return l, nil
}

func (p *procExprParser) parseNot() (Value, error) {
	if !p.eof() && p.kw() == "NOT" {
		p.next()
		v, err := p.parseNot()
		if err != nil {
			return Value{}, err
		}
		if procTruthy(v) {
			return IntVal(0), nil
		}
		return IntVal(1), nil
	}
	return p.parseCmp()
}

func (p *procExprParser) parseCmp() (Value, error) {
	l, err := p.parseAdd()
	if err != nil {
		return Value{}, err
	}
	if p.eof() {
		return l, nil
	}
	k := p.kw()
	if k == "=" || k == "!=" || k == "<" || k == ">" || k == "<=" || k == ">=" {
		p.next()
		r, err := p.parseAdd()
		if err != nil {
			return Value{}, err
		}
		return procCompareValue(l, r, k)
	}
	return l, nil
}

func (p *procExprParser) parseAdd() (Value, error) {
	l, err := p.parseMul()
	if err != nil {
		return Value{}, err
	}
	for !p.eof() && (p.cur().kind == tokPlus || p.cur().kind == tokMinus) {
		op := p.cur().text
		p.next()
		r, err := p.parseMul()
		if err != nil {
			return Value{}, err
		}
		l, err = evalArith(l, r, op)
		if err != nil {
			return Value{}, err
		}
	}
	return l, nil
}

func (p *procExprParser) parseMul() (Value, error) {
	l, err := p.parseUnary()
	if err != nil {
		return Value{}, err
	}
	for !p.eof() && (p.cur().kind == tokStar || p.cur().kind == tokSlash || p.cur().kind == tokMod) {
		op := p.cur().text
		p.next()
		r, err := p.parseUnary()
		if err != nil {
			return Value{}, err
		}
		l, err = evalArith(l, r, op)
		if err != nil {
			return Value{}, err
		}
	}
	return l, nil
}

func (p *procExprParser) parseUnary() (Value, error) {
	if !p.eof() && p.cur().kind == tokMinus {
		p.next()
		v, err := p.parseUnary()
		if err != nil {
			return Value{}, err
		}
		switch v.Kind {
		case "INT":
			return IntVal(-v.I), nil
		case "DECIMAL":
			return decVal(-v.I), nil
		}
		return Value{}, &SQLError{Msg: "cannot apply unary minus to " + v.Kind + " value"}
	}
	return p.parsePrimary()
}

func (p *procExprParser) parsePrimary() (Value, error) {
	if p.eof() {
		return Value{}, &SQLError{Msg: "empty expression in procedure body"}
	}
	t := p.cur()
	switch t.kind {
	case tokNumber:
		p.next()
		return tokenToValue(t)
	case tokString:
		p.next()
		return StrVal(t.text), nil
	case tokVar:
		p.next()
		key := strings.ToLower(t.text[1:])
		v, ok := p.env[key]
		if !ok {
			return Value{}, &SQLError{Msg: "unknown procedure variable: " + t.text}
		}
		return v, nil
	case tokLParen:
		p.next()
		v, err := p.parseOr()
		if err != nil {
			return Value{}, err
		}
		if p.eof() || p.cur().kind != tokRParen {
			return Value{}, &SQLError{Msg: "expected ) in procedure expression"}
		}
		p.next()
		return v, nil
	default:
		k := strings.ToUpper(t.text)
		if k == "TRUE" {
			p.next()
			return IntVal(1), nil
		}
		if k == "FALSE" {
			p.next()
			return IntVal(0), nil
		}
		// P0-2：裸参数名/局部变量名（过程体习惯风格）按小写查环境；
		// 命中则作为其当前值参与运算（优先级低于关键字/运算符）。
		if t.kind == tokIdent || t.kind == tokKeyword {
			if v, ok := p.env[strings.ToLower(t.text)]; ok {
				p.next()
				return v, nil
			}
		}
		return Value{}, &SQLError{Msg: "unexpected token in procedure expression: " + t.text}
	}
}

// procTruthy 真值判定：INT/DECIMAL 按非零，其余按非空文本。
func procTruthy(v Value) bool {
	switch v.Kind {
	case "INT":
		return v.I != 0
	case "DECIMAL":
		return v.I != 0
	default:
		return v.S != ""
	}
}

// procToInt 尝试把值转为整数（FOR 循环范围用）。
func procToInt(v Value) (int64, bool) {
	switch v.Kind {
	case "INT":
		return v.I, true
	case "DECIMAL":
		return v.I / decUnit, true
	case "TEXT":
		n, err := strconv.ParseInt(strings.TrimSpace(v.S), 10, 64)
		return n, err == nil
	}
	return 0, false
}

// procDefaultValue DECLARE 未指定 DEFAULT 时的类型默认值。
func procDefaultValue(typ string) Value {
	switch strings.ToUpper(typ) {
	case "TEXT", "VARCHAR", "CHAR", "DATE", "BLOB":
		return StrVal("")
	case "DECIMAL":
		return Value{Kind: "DECIMAL", I: 0, S: "0"}
	default: // INT / BOOLEAN / 未指定类型
		return IntVal(0)
	}
}

// procCompareValue 数值优先比较，返回 INT 0/1。
func procCompareValue(a, b Value, op string) (Value, error) {
	if a.Kind == "NULL" || b.Kind == "NULL" {
		return IntVal(0), nil
	}
	var cmp int
	if isNumericKind(a.Kind) && isNumericKind(b.Kind) {
		switch {
		case a.Kind == "DECIMAL" && b.Kind == "DECIMAL":
			if a.I < b.I {
				cmp = -1
			} else if a.I > b.I {
				cmp = 1
			}
		case a.Kind == "DECIMAL":
			cmp = compareDecInt(a.I, b.I)
		case b.Kind == "DECIMAL":
			cmp = -compareDecInt(b.I, a.I)
		default:
			if a.I < b.I {
				cmp = -1
			} else if a.I > b.I {
				cmp = 1
			}
		}
	} else {
		cmp = strings.Compare(a.S, b.S)
	}
	var ok bool
	switch op {
	case "=":
		ok = cmp == 0
	case "!=":
		ok = cmp != 0
	case "<":
		ok = cmp < 0
	case ">":
		ok = cmp > 0
	case "<=":
		ok = cmp <= 0
	case ">=":
		ok = cmp >= 0
	default:
		return Value{}, &SQLError{Msg: "unsupported comparison operator: " + op}
	}
	if ok {
		return IntVal(1), nil
	}
	return IntVal(0), nil
}

// procEqual CASE 简单匹配：相等判断。
func procEqual(a, b Value) (bool, error) {
	v, err := procCompareValue(a, b, "=")
	if err != nil {
		return false, err
	}
	return procTruthy(v), nil
}
