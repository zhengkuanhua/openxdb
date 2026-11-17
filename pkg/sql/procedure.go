package sql

import (
	"encoding/json"
	"errors"
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
			// v1.19.0：变量值带入语句时按 SQL 标准转义单引号（'' 表示一个字面引号），
			// 否则含引号的字符串值（如 MESSAGE_TEXT 的诊断文本）会破坏重建后的语句。
			sb.WriteString(strings.ReplaceAll(t.text, "'", "''"))
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
			// v1.12.0 高精度：纯小数按字面量实际位数保留；科学计数走 scale=4 兼容路径。
			if !strings.ContainsAny(t.text, "eE") {
				ds := decimalLiteralScale(t.text)
				scaled, canon, err := parseDecimalScaled(t.text, ds)
				if err != nil {
					return Value{}, err
				}
				return Value{Kind: "DECIMAL", I: scaled, S: canon, DS: ds}, nil
			}
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

// ===== P1-3：游标（DECLARE CURSOR / OPEN / FETCH / CLOSE）=====

// procCursorDecl DECLARE <name> CURSOR FOR <SELECT ...>;（游标声明，P1-3）。
// 游标名为裸标识符；query 为 SELECT 原文 token（不含收尾分号）。
type procCursorDecl struct {
	name  string
	query []token
}

func (procCursorDecl) procStmt() {}

// procCursorOpen OPEN <name>;（打开游标：执行 SELECT 并收集行集快照）。
type procCursorOpen struct{ name string }

func (procCursorOpen) procStmt() {}

// procCursorFetch FETCH <name> INTO @a, @b, ...;（取下一条记录到局部变量）。
// vars 为变量名（不含 @，统一小写）。
type procCursorFetch struct {
	name string
	vars []string
}

func (procCursorFetch) procStmt() {}

// procCursorClose CLOSE <name>;（关闭游标并释放行集快照）。
type procCursorClose struct{ name string }

func (procCursorClose) procStmt() {}

// procCursorState 游标运行期状态：DECLARE 时保存 SELECT 原文，
// OPEN 时执行 SELECT 收集行集快照，FETCH 顺序消费
// （受限子集不做 NOT FOUND 处理，越界直接报错）。
type procCursorState struct {
	query []token
	rows  [][]Value
	pos   int
	open  bool
}

// maxLoopIterations 单层循环最大迭代次数（防死循环）。
// 任意循环（LOOP/WHILE/FOR/REPEAT）单次迭代计数超过该值即报错终止。
const maxLoopIterations = 10000

// procParser 过程体 token 流解析器。
type procParser struct {
	toks      []token
	pos       int
	loopDepth int // 当前解析嵌套的循环层数（校验 EXIT/LEAVE 位置）
	// v1.19.0：当前嵌套的 BEGIN ... END 块层数（块内允许 EXIT/LEAVE，仅结束本块）。
	blockDepth int
}

func (p *procParser) eof() bool { return p.pos >= len(p.toks) }
func (p *procParser) cur() token {
	if p.pos >= len(p.toks) {
		return token{}
	}
	return p.toks[p.pos]
}
func (p *procParser) kw() string { return strings.ToUpper(p.cur().text) }
func (p *procParser) next() {
	if p.pos < len(p.toks) {
		p.pos++
	}
}
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
		s, err := p.parseOneStmt(stops)
		if err != nil {
			return nil, err
		}
		if s != nil {
			out = append(out, s)
		}
	}
	return out, nil
}

// parseOneStmt 解析单条过程体语句并分发到对应节点解析器。
// 由 parseSeq（语句序列）与 HANDLER 体（单条语句）共用，保证两条路径语法一致。
func (p *procParser) parseOneStmt(stops map[string]bool) (procStmt, error) {
	switch p.kw() {
	case "IF":
		return p.parseIf()
	case "LOOP":
		return p.parseLoop()
	case "WHILE":
		return p.parseWhile()
	case "FOR":
		return p.parseFor()
	case "REPEAT":
		return p.parseRepeat()
	case "CASE":
		return p.parseCase()
	case "DECLARE":
		return p.parseDeclare()
	case "SET":
		return p.parseSet()
	case "EXIT", "LEAVE":
		return p.parseExit()
	case "ITERATE", "CONTINUE":
		return p.parseIterate()
	case "OPEN":
		return p.parseCursorOpen()
	case "FETCH":
		return p.parseCursorFetch()
	case "CLOSE":
		return p.parseCursorClose()
	// v1.19.0：异常处理与动态 SQL。
	case "SIGNAL":
		return p.parseSignal()
	case "RESIGNAL":
		return p.parseResignal()
	case "PREPARE":
		return p.parseDynPrepare()
	case "EXECUTE":
		return p.parseDynExecute()
	case "DEALLOCATE":
		return p.parseDynDeallocate()
	// v1.19.0：嵌套 BEGIN ... END 块与 GET DIAGNOSTICS。
	case "BEGIN":
		// 紧跟分号的 BEGIN（事务语句）保持原路径，过程体不支持事务语句时照旧报错，
		// 避免把既有语法误判为嵌套块。
		if p.pos+1 < len(p.toks) && p.toks[p.pos+1].kind == tokSemicolon {
			return p.parseSQL(stops)
		}
		return p.parseBlock()
	case "GET":
		if p.pos+1 < len(p.toks) {
			switch strings.ToUpper(p.toks[p.pos+1].text) {
			case "DIAGNOSTICS", "CURRENT":
				return p.parseGetDiagnostics()
			}
		}
		return p.parseSQL(stops)
	default:
		return p.parseSQL(stops)
	}
}

// parseBlock 解析嵌套块 BEGIN <stmts> END [<label>] ;（v1.19.0）。
// 块内可含 DECLARE 局部变量 / 游标 / 命名条件 / HANDLER，作用域限于块内。
func (p *procParser) parseBlock() (procStmt, error) {
	if err := p.expect("BEGIN"); err != nil {
		return nil, err
	}
	// v1.19.0：块内 EXIT/LEAVE 合法（只结束本块）。
	p.blockDepth++
	body, err := p.parseSeq(map[string]bool{"END": true})
	p.blockDepth--
	if err != nil {
		return nil, err
	}
	if err := p.expect("END"); err != nil {
		return nil, err
	}
	st := procBlock{body: body}
	// END <label> 标签形式：块尾标签仅作语法校验，不参与跳转。
	// 仅在 ident 后紧跟分号时按标签消费，避免把后续语句首 token 误当标签。
	if p.cur().kind == tokIdent && p.pos+1 < len(p.toks) && p.toks[p.pos+1].kind == tokSemicolon {
		st.label = p.cur().text
		p.next()
	}
	if p.kw() == ";" {
		p.next()
	}
	return st, nil
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
// （也接受 TO 与 END FOR）。
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
// （先执行后判断：UNTIL 条件为真时结束循环，与 WHILE 前置判断相反）。
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
	// v1.19.0：DECLARE {CONTINUE|EXIT} [<name>] HANDLER FOR <cond>[, ...] <stmt>
	// 异常处理器声明（先于 CURSOR / 变量判定，避免与后续写法歧义）。
	if p.isHandlerDecl() {
		return p.parseHandlerDecl()
	}
	// P1-3：DECLARE <name> CURSOR FOR <SELECT ...> 游标声明（游标名为裸 ident）。
	if p.cur().kind == tokIdent && p.pos+1 < len(p.toks) &&
		strings.ToUpper(p.toks[p.pos+1].text) == "CURSOR" {
		return p.parseCursorDecl()
	}
	// v1.19.0：DECLARE <name> CONDITION FOR {SQLSTATE '<code>' | <errno>} 命名条件。
	if p.cur().kind == tokIdent && p.pos+1 < len(p.toks) &&
		strings.ToUpper(p.toks[p.pos+1].text) == "CONDITION" {
		return p.parseConditionDecl()
	}
	if p.cur().kind != tokVar {
		return nil, &SQLError{Msg: "expected @variable or CURSOR after DECLARE in procedure body"}
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

// parseCursorDecl 解析 DECLARE <name> CURSOR FOR <SELECT ...>;（P1-3）。
// 调用时游标名 token 尚未消费；SELECT 原文收集到顶层分号（括号深度保护）。
func (p *procParser) parseCursorDecl() (procStmt, error) {
	nameTok := p.cur()
	if nameTok.kind != tokIdent {
		return nil, &SQLError{Msg: "expected cursor name after DECLARE in procedure body"}
	}
	p.next() // name
	if err := p.expect("CURSOR"); err != nil {
		return nil, err
	}
	if err := p.expect("FOR"); err != nil {
		return nil, err
	}
	start := p.pos
	parenDepth := 0
	for !p.eof() {
		t := p.cur()
		if t.kind == tokLParen {
			parenDepth++
		} else if t.kind == tokRParen {
			parenDepth--
		} else if t.kind == tokSemicolon && parenDepth == 0 {
			break
		}
		p.next()
	}
	if p.pos == start {
		return nil, &SQLError{Msg: "CURSOR FOR requires a SELECT query in procedure body"}
	}
	query := append([]token(nil), p.toks[start:p.pos]...)
	if p.kw() == ";" {
		p.next()
	}
	return procCursorDecl{name: nameTok.text, query: query}, nil
}

// parseCursorOpen 解析 OPEN <name>;。
func (p *procParser) parseCursorOpen() (procStmt, error) {
	p.next() // OPEN
	nameTok := p.cur()
	if nameTok.kind != tokIdent {
		return nil, &SQLError{Msg: "expected cursor name after OPEN in procedure body"}
	}
	p.next()
	if p.kw() == ";" {
		p.next()
	}
	return procCursorOpen{name: nameTok.text}, nil
}

// parseCursorFetch 解析 FETCH <name> INTO @a, @b, ...;。
func (p *procParser) parseCursorFetch() (procStmt, error) {
	p.next() // FETCH
	nameTok := p.cur()
	if nameTok.kind != tokIdent {
		return nil, &SQLError{Msg: "expected cursor name after FETCH in procedure body"}
	}
	p.next()
	if err := p.expect("INTO"); err != nil {
		return nil, err
	}
	var vars []string
	for {
		if p.cur().kind != tokVar {
			return nil, &SQLError{Msg: "expected @variable in FETCH INTO in procedure body"}
		}
		vars = append(vars, strings.ToLower(p.cur().text[1:]))
		p.next()
		if p.kw() == "," {
			p.next()
			continue
		}
		break
	}
	if p.kw() == ";" {
		p.next()
	}
	return procCursorFetch{name: nameTok.text, vars: vars}, nil
}

// parseCursorClose 解析 CLOSE <name>;。
func (p *procParser) parseCursorClose() (procStmt, error) {
	p.next() // CLOSE
	nameTok := p.cur()
	if nameTok.kind != tokIdent {
		return nil, &SQLError{Msg: "expected cursor name after CLOSE in procedure body"}
	}
	p.next()
	if p.kw() == ";" {
		p.next()
	}
	return procCursorClose{name: nameTok.text}, nil
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
	// v1.19.0：BEGIN ... END 块内的 EXIT 只结束本块，因此块内亦允许。
	if p.loopDepth == 0 && p.blockDepth == 0 {
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
	if err := validateProcStmts(stmts); err != nil {
		return err
	}
	// v1.19.0：命名条件（DECLARE ... CONDITION FOR）引用校验。
	if err := validateNamedCondRefs(stmts); err != nil {
		return err
	}
	// P1-3：游标声明唯一性 + OPEN/FETCH/CLOSE 引用校验。
	return validateCursorRefs(stmts)
}

// collectConditionDecls 递归收集命名条件声明（v1.19.0）。
func collectConditionDecls(stmts []procStmt, out map[string]bool) {
	for _, s := range stmts {
		switch st := s.(type) {
		case procConditionDecl:
			out[strings.ToLower(st.name)] = true
		case procBlock:
			collectConditionDecls(st.body, out)
		case procHandlerDecl:
			collectConditionDecls(st.body, out)
		case procIf:
			for _, br := range st.branches {
				collectConditionDecls(br.body, out)
			}
		case procLoop:
			collectConditionDecls(st.body, out)
		case procWhile:
			collectConditionDecls(st.body, out)
		case procFor:
			collectConditionDecls(st.body, out)
		case procRepeat:
			collectConditionDecls(st.body, out)
		case procCase:
			for _, w := range st.whens {
				collectConditionDecls(w.body, out)
			}
			if st.hasElse {
				collectConditionDecls(st.elseBody, out)
			}
		}
	}
}

// validateNamedCondRefs 校验 HANDLER FOR / SIGNAL / RESIGNAL 引用的命名条件均已声明（v1.19.0）。
func validateNamedCondRefs(stmts []procStmt) error {
	decls := make(map[string]bool)
	collectConditionDecls(stmts, decls)
	var walk func([]procStmt) error
	walk = func(ss []procStmt) error {
		for _, s := range ss {
			switch st := s.(type) {
			case procHandlerDecl:
				for _, c := range st.conds {
					if c.kind == procErrNamed && !decls[strings.ToLower(c.name)] {
						return &SQLError{Msg: "unknown condition in HANDLER FOR: " + c.name}
					}
				}
				if err := walk(st.body); err != nil {
					return err
				}
			case procSignal:
				if st.hasCond && st.cond.kind == procErrNamed && !decls[strings.ToLower(st.cond.name)] {
					return &SQLError{Msg: "unknown condition in SIGNAL: " + st.cond.name}
				}
			case procResignal:
				if st.hasCond && st.cond.kind == procErrNamed && !decls[strings.ToLower(st.cond.name)] {
					return &SQLError{Msg: "unknown condition in RESIGNAL: " + st.cond.name}
				}
			case procBlock:
				if err := walk(st.body); err != nil {
					return err
				}
			case procIf:
				for _, br := range st.branches {
					if err := walk(br.body); err != nil {
						return err
					}
				}
			case procLoop:
				if err := walk(st.body); err != nil {
					return err
				}
			case procWhile:
				if err := walk(st.body); err != nil {
					return err
				}
			case procFor:
				if err := walk(st.body); err != nil {
					return err
				}
			case procRepeat:
				if err := walk(st.body); err != nil {
					return err
				}
			case procCase:
				for _, w := range st.whens {
					if err := walk(w.body); err != nil {
						return err
					}
				}
				if st.hasElse {
					if err := walk(st.elseBody); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	return walk(stmts)
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
		case procCursorDecl:
			if st.name == "" {
				return &SQLError{Msg: "cursor name required in procedure body"}
			}
			if len(st.query) == 0 {
				return &SQLError{Msg: "CURSOR FOR requires a SELECT query in procedure body"}
			}
			// 创建期静态校验：游标查询在 @var 占位替换后可解析且必须为 SELECT。
			env := make(map[string]Value)
			for _, t := range st.query {
				if t.kind == tokVar {
					env[strings.ToLower(t.text[1:])] = IntVal(1)
				}
			}
			sub, err := replaceVars(tokensToString(st.query), env, true)
			if err != nil {
				return err
			}
			qstmt, err := Parse(sub)
			if err != nil {
				return err
			}
			if _, ok := qstmt.(*SelectStmt); !ok {
				return &SQLError{Msg: "CURSOR FOR only supports SELECT queries in procedure body"}
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
		case procHandlerDecl:
			// v1.19.0：HANDLER 动作、条件列表与处理器体校验。
			if st.action != "CONTINUE" && st.action != "EXIT" {
				return &SQLError{Msg: "HANDLER action must be CONTINUE or EXIT in procedure body"}
			}
			if len(st.conds) == 0 {
				return &SQLError{Msg: "HANDLER condition required in procedure body"}
			}
			if len(st.body) == 0 {
				return &SQLError{Msg: "HANDLER requires a statement in procedure body"}
			}
			if err := validateProcStmts(st.body); err != nil {
				return err
			}
		case procSignal:
			if st.hasCond && !st.cond.valid() {
				return &SQLError{Msg: "SIGNAL requires SQLSTATE or error code in procedure body"}
			}
			for _, a := range st.set {
				if len(a.expr) == 0 {
					return &SQLError{Msg: "SIGNAL SET requires a value in procedure body"}
				}
			}
		case procResignal:
			for _, a := range st.set {
				if len(a.expr) == 0 {
					return &SQLError{Msg: "RESIGNAL SET requires a value in procedure body"}
				}
			}
		case procPrepare:
			if st.name == "" || len(st.src) == 0 {
				return &SQLError{Msg: "PREPARE requires statement name and SQL text in procedure body"}
			}
		case procExecute:
			if st.name == "" {
				return &SQLError{Msg: "EXECUTE requires prepared statement name in procedure body"}
			}
			for _, a := range st.using {
				if len(a) == 0 {
					return &SQLError{Msg: "EXECUTE USING requires a value in procedure body"}
				}
			}
		case procDeallocate:
			if st.name == "" {
				return &SQLError{Msg: "DEALLOCATE PREPARE requires statement name in procedure body"}
			}
		// v1.19.0：嵌套块、命名条件与 GET DIAGNOSTICS。
		case procBlock:
			if err := validateProcStmts(st.body); err != nil {
				return err
			}
		case procConditionDecl:
			if st.name == "" {
				return &SQLError{Msg: "CONDITION requires a name in procedure body"}
			}
			if st.cond.kind != procErrSQLState && st.cond.kind != procErrErrno {
				return &SQLError{Msg: "CONDITION FOR requires SQLSTATE value or error number in procedure body"}
			}
			if !st.cond.valid() {
				return &SQLError{Msg: "CONDITION FOR requires SQLSTATE value or error number in procedure body"}
			}
		case procGetDiagnostics:
			if len(st.items) == 0 {
				return &SQLError{Msg: "GET DIAGNOSTICS requires at least one target in procedure body"}
			}
			for _, it := range st.items {
				switch it.field {
				case "ROW_COUNT":
					if st.hasCond {
						return &SQLError{Msg: "ROW_COUNT is not valid in GET DIAGNOSTICS CONDITION in procedure body"}
					}
				case "RETURNED_SQLSTATE", "MYSQL_ERRNO", "MESSAGE_TEXT":
					if !st.hasCond {
						return &SQLError{Msg: "GET DIAGNOSTICS " + it.field + " requires CONDITION in procedure body"}
					}
				default:
					return &SQLError{Msg: "unsupported GET DIAGNOSTICS item in procedure body: " + it.field}
				}
			}
		}
	}
	return nil
}

// collectCursorDecls 递归收集游标声明名（重复声明报错）。
func collectCursorDecls(stmts []procStmt, out map[string]bool) error {
	for _, s := range stmts {
		switch st := s.(type) {
		case procCursorDecl:
			key := strings.ToLower(st.name)
			if out[key] {
				return &SQLError{Msg: "duplicate cursor declaration: " + st.name}
			}
			out[key] = true
		case procIf:
			for _, br := range st.branches {
				if err := collectCursorDecls(br.body, out); err != nil {
					return err
				}
			}
		case procLoop:
			if err := collectCursorDecls(st.body, out); err != nil {
				return err
			}
		case procWhile:
			if err := collectCursorDecls(st.body, out); err != nil {
				return err
			}
		case procFor:
			if err := collectCursorDecls(st.body, out); err != nil {
				return err
			}
		case procRepeat:
			if err := collectCursorDecls(st.body, out); err != nil {
				return err
			}
		case procHandlerDecl:
			if err := collectCursorDecls(st.body, out); err != nil {
				return err
			}
		case procBlock:
			// v1.19.0：嵌套块内的游标声明同样参与唯一性校验。
			if err := collectCursorDecls(st.body, out); err != nil {
				return err
			}
		case procCase:
			for _, w := range st.whens {
				if err := collectCursorDecls(w.body, out); err != nil {
					return err
				}
			}
			if st.hasElse {
				if err := collectCursorDecls(st.elseBody, out); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// validateCursorRefs 校验 OPEN/FETCH/CLOSE 引用的游标已声明、FETCH 变量非空。
func validateCursorRefs(stmts []procStmt) error {
	decls := make(map[string]bool)
	if err := collectCursorDecls(stmts, decls); err != nil {
		return err
	}
	var walk func([]procStmt) error
	walk = func(ss []procStmt) error {
		for _, s := range ss {
			switch st := s.(type) {
			case procCursorOpen:
				if !decls[strings.ToLower(st.name)] {
					return &SQLError{Msg: "unknown cursor in OPEN: " + st.name}
				}
			case procCursorFetch:
				if !decls[strings.ToLower(st.name)] {
					return &SQLError{Msg: "unknown cursor in FETCH: " + st.name}
				}
				if len(st.vars) == 0 {
					return &SQLError{Msg: "FETCH INTO requires at least one @variable in procedure body"}
				}
			case procCursorClose:
				if !decls[strings.ToLower(st.name)] {
					return &SQLError{Msg: "unknown cursor in CLOSE: " + st.name}
				}
			case procIf:
				for _, br := range st.branches {
					if err := walk(br.body); err != nil {
						return err
					}
				}
			case procLoop:
				if err := walk(st.body); err != nil {
					return err
				}
			case procWhile:
				if err := walk(st.body); err != nil {
					return err
				}
			case procFor:
				if err := walk(st.body); err != nil {
					return err
				}
			case procRepeat:
				if err := walk(st.body); err != nil {
					return err
				}
			case procHandlerDecl:
				if err := walk(st.body); err != nil {
					return err
				}
			case procBlock:
				// v1.19.0：嵌套块内的 OPEN/FETCH/CLOSE 同样校验。
				if err := walk(st.body); err != nil {
					return err
				}
			case procCase:
				for _, w := range st.whens {
					if err := walk(w.body); err != nil {
						return err
					}
				}
				if st.hasElse {
					if err := walk(st.elseBody); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	return walk(stmts)
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
	return e.execCallOut(sess, s, nil, nil)
}

// execCallOut 是 execCall 的内部实现；outSink 非空时 OUT/INOUT 参数值
// 经回调写入（P1-3 过程体 CALL 回写外层 env），否则写入会话用户变量。
// lookupVar 提供"实参 @var 当前值"查询，缺省时查 sess.userVars。
func (e *Executor) execCallOut(sess *Session, s *CallStmt, outSink map[string]Value, lookupVar func(string) (Value, bool)) (*Result, error) {
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
		argVar, argIsVar := callArgVar(s.Args[i])
		switch pm.Mode {
		case "OUT":
			// 输出参数：实参必须为 @var，初始化为类型默认值，结束后回写。
			if !argIsVar {
				return nil, &SQLError{Msg: "procedure " + s.Name + " OUT parameter " + strconv.Itoa(i+1) + " requires a @variable argument"}
			}
			env[strings.ToLower(pm.Name)] = procDefaultValue(pm.Type)
		case "INOUT":
			// 双向参数：实参必须为 @var，初始化为调用方当前值，结束后回写。
			if !argIsVar {
				return nil, &SQLError{Msg: "procedure " + s.Name + " INOUT parameter " + strconv.Itoa(i+1) + " requires a @variable argument"}
			}
			if lv, ok := e.lookupCallVarValue(sess, argVar, lookupVar); ok {
				env[strings.ToLower(pm.Name)] = lv
			} else {
				env[strings.ToLower(pm.Name)] = procDefaultValue(pm.Type)
			}
		default: // IN
			if argIsVar {
				// 单个 @var 实参取调用方当前值（修复原先错误代入 VAR 字面量）。
				lv, ok := e.lookupCallVarValue(sess, argVar, lookupVar)
				if !ok {
					return nil, &SQLError{Msg: "user variable not set: @" + argVar}
				}
				env[strings.ToLower(pm.Name)] = lv
			} else {
				v, err := evalExprForRow(nil, nil, s.Args[i])
				if err != nil {
					return nil, &SQLError{Msg: "procedure " + s.Name + " argument " + strconv.Itoa(i+1) + ": " + err.Error()}
				}
				env[strings.ToLower(pm.Name)] = v
			}
		}
	}
	sess.procDepth++
	defer func() { sess.procDepth-- }()
	if err := e.execProcedureBody(sess, proc.Body, env); err != nil {
		return nil, &SQLError{Msg: "procedure " + s.Name + " failed: " + err.Error()}
	}
	// P1-3：OUT/INOUT 参数值回写调用方变量。
	for i, pm := range proc.Params {
		if pm.Mode != "OUT" && pm.Mode != "INOUT" {
			continue
		}
		v := env[strings.ToLower(pm.Name)]
		if outSink != nil {
			outSink[strings.ToLower(pm.Name)] = v
			continue
		}
		if argVar, ok := callArgVar(s.Args[i]); ok {
			sess.userVars["@"+argVar] = v
		}
	}
	return &Result{AffectedRows: 1}, nil
}

// callArgVar 判断 CALL 实参是否为单个 @var（Expr{Val:&Value{Kind:"VAR"}}），
// 返回变量名（小写，不含 @）与是否变量。非变量实参返回 ("", false)。
func callArgVar(arg *Expr) (string, bool) {
	if arg == nil || arg.Val == nil {
		return "", false
	}
	if arg.Val.Kind == "VAR" {
		name := arg.Val.S
		if len(name) > 1 && name[0] == '@' {
			return strings.ToLower(name[1:]), true
		}
		return strings.ToLower(name), true
	}
	return "", false
}

// lookupCallVarValue 查询 CALL 实参 @var 的当前值：优先 lookupVar 回调
// （过程体内外层 env），否则查会话用户变量。
func (e *Executor) lookupCallVarValue(sess *Session, name string, lookupVar func(string) (Value, bool)) (Value, bool) {
	if lookupVar != nil {
		return lookupVar(name)
	}
	if sess.userVars == nil {
		return Value{}, false
	}
	v, ok := sess.userVars["@"+name]
	return v, ok
}

// execProcedureBody 按控制流解释器执行过程体语句（M15-3 / P0-2 内部实现）。
func (e *Executor) execProcedureBody(sess *Session, body string, env map[string]Value) error {
	stmts, err := parseProcedureBody(body)
	if err != nil {
		return err
	}
	r := &procRunner{
		e:        e,
		sess:     sess,
		env:      env,
		cursors:  make(map[string]*procCursorState),
		prepared: make(map[string]string),
	}
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
	cursors map[string]*procCursorState // 游标名（小写）→ 运行期状态（P1-3）
	iterate bool                        // ITERATE/CONTINUE 触发（由循环层消费，表示继续下一次迭代而非退出）

	// v1.19.0：异常处理与动态 SQL 运行期状态。
	// handlers 为当前有效处理器栈（按块入栈，块退出时截断回原深度），
	// prepared 为本次过程调用内的动态语句表（名称小写 → SQL 文本），
	// curErr 为正在被处理器处理的错误（供 RESIGNAL 无参重抛使用）。
	handlers []*procHandlerState
	prepared map[string]string
	curErr   error

	// v1.19.0：命名条件、诊断区与行数状态。
	// conditions 为当前有效命名条件表（块内声明随块退出出栈），
	// lastRowCount 记录最近一条普通 SQL 的影响行数（GET DIAGNOSTICS ROW_COUNT），
	// diag 记录最近一次语句错误的诊断区（GET DIAGNOSTICS CONDITION）。
	conditions   map[string]procCond
	lastRowCount int
	diag         procDiagArea
}

// procHandlerState 处理器运行期状态（decl 为声明，active 标记正在执行避免自捕获）。
type procHandlerState struct {
	decl   procHandlerDecl
	active bool
}

// run 顺序执行语句列表；返回 exited=true 表示 EXIT/LEAVE 触发（由循环层消费）。
// v1.19.0：块作用域——块内声明的 HANDLER 在本块内生效，随块退出自动出栈；
// 无 HANDLER 声明的块完全走原执行路径（零行为变化）。
func (r *procRunner) run(stmts []procStmt) (bool, error) {
	depth := len(r.handlers)
	registered := false
	defer func() {
		if registered {
			r.handlers = r.handlers[:depth]
		}
	}()
	for _, s := range stmts {
		if h, ok := s.(procHandlerDecl); ok {
			// 声明本身不执行：注册为当前块的处理器，仅对其后语句生效。
			r.handlers = append(r.handlers, &procHandlerState{decl: h})
			registered = true
			continue
		}
		exited, err := r.runStmtGuarded(s)
		if err != nil {
			return false, err
		}
		if exited {
			return true, nil
		}
	}
	return false, nil
}

// runStmtGuarded 执行单条语句并在出错时按处理器栈匹配（内层优先）。
// CONTINUE 处理器执行后继续下一条语句；EXIT 处理器执行后结束当前块。
// 处理器体自身抛出的错误不再被同一处理器捕获（active 标记），直接上抛。
func (r *procRunner) runStmtGuarded(s procStmt) (bool, error) {
	exited, err := r.runStmt(s)
	if err != nil {
		// v1.19.0：诊断区在处理器执行前登记，供 GET DIAGNOSTICS CONDITION 读取。
		r.setDiag(err)
	}
	if err == nil || len(r.handlers) == 0 {
		return exited, err
	}
	h := r.matchHandler(err)
	if h == nil {
		return exited, err
	}
	prevErr := r.curErr
	r.curErr = err
	h.active = true
	bodyExited, herr := r.run(h.decl.body)
	h.active = false
	r.curErr = prevErr
	if herr != nil {
		return false, herr
	}
	if h.decl.action == "EXIT" || bodyExited {
		return true, nil
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
		res, err := r.e.execStmt(r.sess, stmt)
		if err != nil {
			return false, err
		}
		// v1.19.0：记录影响行数，供 GET DIAGNOSTICS ROW_COUNT 读取。
		if res != nil {
			r.lastRowCount = res.AffectedRows
		}
		return false, nil
	case *procCall:
		call := &CallStmt{Name: st.name}
		outSink := make(map[string]Value)
		for _, argToks := range st.args {
			// 单个 @var 实参保持引用身份：IN 取外层当前值，OUT/INOUT 回写外层 env。
			if len(argToks) == 1 && argToks[0].kind == tokVar {
				call.Args = append(call.Args, &Expr{Val: &Value{Kind: "VAR", S: argToks[0].text}})
				continue
			}
			v, err := evalProcExpr(argToks, r.env)
			if err != nil {
				return false, err
			}
			call.Args = append(call.Args, &Expr{Val: &v})
		}
		if _, err := r.e.execCallOut(r.sess, call, outSink, func(name string) (Value, bool) {
			v, ok := r.env[name]
			return v, ok
		}); err != nil {
			return false, err
		}
		for name, v := range outSink {
			r.env[name] = v
		}
		return false, nil
	case procCursorDecl:
		key := strings.ToLower(st.name)
		if _, ok := r.cursors[key]; ok {
			return false, &SQLError{Msg: "duplicate cursor declaration: " + st.name}
		}
		r.cursors[key] = &procCursorState{query: st.query}
		return false, nil
	case procCursorOpen:
		key := strings.ToLower(st.name)
		cs, ok := r.cursors[key]
		if !ok {
			return false, &SQLError{Msg: "unknown cursor: " + st.name}
		}
		// 查询内 @var 代入当前环境后走标准 SELECT 执行，收集行集快照。
		sub, err := replaceVars(tokensToString(cs.query), r.env, true)
		if err != nil {
			return false, err
		}
		stmt, err := Parse(sub)
		if err != nil {
			return false, err
		}
		sel, ok := stmt.(*SelectStmt)
		if !ok {
			return false, &SQLError{Msg: "CURSOR FOR only supports SELECT queries in procedure body"}
		}
		res, err := r.e.execSelect(r.sess, sel)
		if err != nil {
			return false, err
		}
		cs.rows = res.Rows
		cs.pos = 0
		cs.open = true
		return false, nil
	case procCursorFetch:
		key := strings.ToLower(st.name)
		cs, ok := r.cursors[key]
		if !ok {
			return false, &SQLError{Msg: "unknown cursor: " + st.name}
		}
		if !cs.open {
			return false, &SQLError{Msg: "cursor not open: " + st.name}
		}
		if cs.pos >= len(cs.rows) {
			// v1.19.0：游标取尽按 SQLSTATE 02000（NOT FOUND）抛出，错误文本保持原样，
			// 使 DECLARE CONTINUE HANDLER FOR NOT FOUND 可捕获。
			return false, &procStateError{
				state: procNotFoundState,
				cause: &SQLError{Msg: "cursor fetch out of rows: " + st.name},
			}
		}
		row := cs.rows[cs.pos]
		cs.pos++
		if len(st.vars) != len(row) {
			return false, &SQLError{Msg: "FETCH INTO variable count mismatch for cursor " + st.name}
		}
		for i, name := range st.vars {
			r.env[name] = row[i]
		}
		return false, nil
	case procCursorClose:
		key := strings.ToLower(st.name)
		if _, ok := r.cursors[key]; !ok {
			return false, &SQLError{Msg: "unknown cursor: " + st.name}
		}
		delete(r.cursors, key)
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
	// v1.19.0：异常处理与动态 SQL。
	case procHandlerDecl:
		// 处理器声明在 run 入栈阶段注册，不产生运行时动作。
		return false, nil
	case procSignal:
		return false, r.execSignal(st)
	case procResignal:
		return false, r.execResignal(st)
	case procPrepare:
		return false, r.execDynPrepare(st)
	case procExecute:
		return false, r.execDynExecute(st)
	case procDeallocate:
		return false, r.execDynDeallocate(st)
	// v1.19.0：嵌套块、命名条件与 GET DIAGNOSTICS。
	case procBlock:
		exited, err := r.runBlock(st)
		if err != nil {
			return false, err
		}
		// 块内 ITERATE/CONTINUE 需上抛给外层循环消费；块内 EXIT（含 EXIT 处理器）
		// 只结束本块，不终止外层块或过程体。
		if exited && r.iterate {
			return true, nil
		}
		return false, nil
	case procConditionDecl:
		if r.conditions == nil {
			r.conditions = make(map[string]procCond)
		}
		r.conditions[strings.ToLower(st.name)] = st.cond
		return false, nil
	case procGetDiagnostics:
		return false, r.execGetDiagnostics(st)
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

func (p *procExprParser) eof() bool  { return p.pos >= len(p.toks) }
func (p *procExprParser) cur() token { return p.toks[p.pos] }
func (p *procExprParser) kw() string { return strings.ToUpper(p.cur().text) }
func (p *procExprParser) next() {
	if p.pos < len(p.toks) {
		p.pos++
	}
}

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
			return decValScaled(-v.I, decDS(v)), nil
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
		return v.I / pow10Int64(decDS(v)), true
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
			// v1.12.0 高精度：跨 scale 对齐比较。
			dsA, dsB := decDS(a), decDS(b)
			ds := dsA
			if dsB > ds {
				ds = dsB
			}
			av := a.I * pow10Int64(ds-dsA)
			bv := b.I * pow10Int64(ds-dsB)
			if av < bv {
				cmp = -1
			} else if av > bv {
				cmp = 1
			}
		case a.Kind == "DECIMAL":
			cmp = compareDecScaledInt(a.I, decDS(a), b.I)
		case b.Kind == "DECIMAL":
			cmp = -compareDecScaledInt(b.I, decDS(b), a.I)
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

// ===== v1.19.0：存储过程异常处理（HANDLER / SIGNAL / RESIGNAL）与动态 SQL =====
//
// 设计要点（保持纯新增、默认不改变既有路径）：
//   - HANDLER 仅在被声明的块内生效，块退出（run 返回）时自动出栈；
//     未声明 HANDLER 的块完全走原 runStmt 路径，行为不变。
//   - 异常以 procStateError 承载 SQLSTATE / 错误码；无 SQLSTATE 的既有错误
//     统一归为 HY000（general error），错误文本保持不变。
//   - 动态 SQL 采用 PREPARE 登记文本 + EXECUTE 绑定 ? 与 @var 后重解析，
//     复用标准 Parse + execStmt，语句白名单与过程体一致。

// procNotFoundState NOT FOUND 条件的标准 SQLSTATE（游标取尽等无行场景）。
const procNotFoundState = "02000"

// procDefaultSQLState SIGNAL / RESIGNAL 未显式指定 SQLSTATE 时的默认值。
const procDefaultSQLState = "45000"

// procErrKind 处理器条件的匹配维度。
type procErrKind int

const (
	procErrSQLEXCEPTION procErrKind = iota // SQLEXCEPTION：非 01 / 02 类异常
	procErrSQLWARNING                      // SQLWARNING：01 类告警
	procErrNotFound                        // NOT FOUND：02 类无行
	procErrSQLState                        // 指定 SQLSTATE 值
	procErrErrno                           // 指定错误码
	procErrNamed                           // 命名条件（DECLARE <name> CONDITION FOR ...）
)

// procCond 单个异常条件。
type procCond struct {
	kind  procErrKind
	state string // SQLSTATE 5 位码
	code  int    // 错误码
	name  string // 命名条件名（小写，kind==procErrNamed 时有效）
}

// valid 条件是否携带可匹配的取值（SIGNAL 目标必须有效）。
func (c procCond) valid() bool {
	switch c.kind {
	case procErrSQLState:
		return c.state != ""
	case procErrErrno:
		return c.code != 0
	case procErrNamed:
		return c.name != ""
	}
	return true
}

// procHandlerDecl DECLARE {CONTINUE|EXIT} [<name>] HANDLER FOR <cond>[, ...] <stmt>。
type procHandlerDecl struct {
	action string // CONTINUE / EXIT
	name   string // 可选处理器名
	conds  []procCond
	body   []procStmt // 处理器体（单条语句）
}

func (procHandlerDecl) procStmt() {}

// procAssign SIGNAL / RESIGNAL 的 SET <条件信息项> = <表达式>。
type procAssign struct {
	name string
	expr []token
}

// procSignal SIGNAL <cond> [SET ...]：抛出用户自定义异常。
type procSignal struct {
	cond    procCond
	hasCond bool
	set     []procAssign
}

func (procSignal) procStmt() {}

// procResignal [<cond>] [SET ...]：处理器内重抛当前（或指定）异常。
type procResignal struct {
	cond    procCond
	hasCond bool
	set     []procAssign
}

func (procResignal) procStmt() {}

// procPrepare PREPARE <name> FROM <@var|字面量>：登记动态语句文本。
type procPrepare struct {
	name string
	src  []token
}

func (procPrepare) procStmt() {}

// procExecute EXECUTE <name> [USING <expr>[, ...]]：执行已登记的动态语句。
type procExecute struct {
	name  string
	using [][]token
}

func (procExecute) procStmt() {}

// procDeallocate DEALLOCATE PREPARE <name>：释放动态语句。
type procDeallocate struct {
	name string
}

func (procDeallocate) procStmt() {}

// procBlock BEGIN ... END 嵌套块（v1.19.0）：块内 DECLARE 的局部变量、游标、
// 命名条件与 HANDLER 均为块作用域，块退出时局部声明出栈；块内对块外既有变量的
// 赋值保留，仅在块内新建的变量随块退出移除。label 为可选块标签（END <label>）。
type procBlock struct {
	label string
	body  []procStmt
}

func (procBlock) procStmt() {}

// procConditionDecl DECLARE <name> CONDITION FOR {SQLSTATE '<code>' | <errno>}
// （v1.19.0）：登记命名条件，可在 HANDLER FOR / SIGNAL / RESIGNAL 中按名称引用。
type procConditionDecl struct {
	name string
	cond procCond
}

func (procConditionDecl) procStmt() {}

// procDiagItem GET DIAGNOSTICS 的单个赋值项 <@var> = <诊断字段>。
type procDiagItem struct {
	name  string // 目标变量名（不含 @，统一小写）
	field string // ROW_COUNT / RETURNED_SQLSTATE / MYSQL_ERRNO / MESSAGE_TEXT
}

// procGetDiagnostics GET [CURRENT] DIAGNOSTICS [CONDITION <n>] <@var> = <字段>[, ...]
// （v1.19.0）：ROW_COUNT 目标读取最近一条 DML 影响行数；CONDITION 目标读取诊断区
// 的 RETURNED_SQLSTATE / MYSQL_ERRNO / MESSAGE_TEXT（诊断区仅保留最近一条，序号须为 1）。
type procGetDiagnostics struct {
	condNum int    // CONDITION 序号（仅支持 1）
	condVar string // 序号以 @var 给出时的变量名（不含 @，统一小写）
	hasCond bool
	items   []procDiagItem
}

func (procGetDiagnostics) procStmt() {}

// procDiagArea 过程诊断区（最近一次语句错误的 SQLSTATE / 错误码 / 文本）。
type procDiagArea struct {
	state string
	code  int
	msg   string
	set   bool
}

// procStateError 携带 SQLSTATE / 错误码的过程异常：
// cause 非空表示包装既有错误（错误文本保持原样），否则使用 message。
type procStateError struct {
	state   string
	code    int
	message string
	cause   error
}

func (e *procStateError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.message
}

func (e *procStateError) Unwrap() error { return e.cause }

// newProcSignalError 构造 SIGNAL / RESIGNAL 抛出的异常（文本携带 SQLSTATE / 错误码）。
func newProcSignalError(state string, code int, msg string) error {
	if state == "" {
		state = procDefaultSQLState
	}
	text := "SQLSTATE '" + state + "'"
	if code != 0 {
		text += " (errno " + strconv.Itoa(code) + ")"
	}
	if msg == "" {
		text += ": unhandled user-defined exception"
	} else {
		text += ": " + msg
	}
	return &procStateError{state: state, code: code, message: text}
}

// procErrInfo 提取错误的 SQLSTATE 与错误码（无 SQLSTATE 的既有错误归为 HY000）。
func procErrInfo(err error) (string, int) {
	var pe *procStateError
	if errors.As(err, &pe) {
		return pe.state, pe.code
	}
	return "HY000", 0
}

// procCondMatch 条件是否匹配给定 SQLSTATE / 错误码。
func procCondMatch(c procCond, state string, code int) bool {
	switch c.kind {
	case procErrSQLState:
		return state != "" && strings.EqualFold(c.state, state)
	case procErrErrno:
		return c.code != 0 && c.code == code
	case procErrNotFound:
		return len(state) >= 2 && state[:2] == "02"
	case procErrSQLWARNING:
		return len(state) >= 2 && state[:2] == "01"
	case procErrSQLEXCEPTION:
		if len(state) < 2 {
			return state != ""
		}
		cls := state[:2]
		return cls != "01" && cls != "02"
	}
	return false
}

// matchHandler 自内向外匹配处理器（后声明者优先），跳过正在执行的处理器，
// 避免处理器体内的错误被其自身再次捕获（防死循环）。
func (r *procRunner) matchHandler(err error) *procHandlerState {
	state, code := procErrInfo(err)
	for i := len(r.handlers) - 1; i >= 0; i-- {
		h := r.handlers[i]
		if h.active {
			continue
		}
		for _, c := range h.decl.conds {
			// v1.19.0：命名条件（CONDITION）先解析为具体 SQLSTATE / 错误码再匹配。
			if rc, ok := r.resolveCond(c); ok && procCondMatch(rc, state, code) {
				return h
			}
		}
	}
	return nil
}

// resolveCond 把命名条件解析为具体条件；未声明或解析失败时返回 ok=false
// （限制解析深度，避免自引用条件导致死循环）。
func (r *procRunner) resolveCond(c procCond) (procCond, bool) {
	for depth := 0; depth < 16 && c.kind == procErrNamed; depth++ {
		d, ok := r.conditions[strings.ToLower(c.name)]
		if !ok {
			return procCond{}, false
		}
		c = d
	}
	if c.kind == procErrNamed {
		return procCond{}, false
	}
	return c, true
}

// runBlock 执行 BEGIN ... END 嵌套块：块内声明的局部变量、游标与命名条件具有块作用域，
// 块退出时出栈（块内对块外既有变量的赋值保留）；HANDLER 栈由 run 内部按深度截断。
func (r *procRunner) runBlock(st procBlock) (bool, error) {
	// 仅本块顶层声明的名字参与出栈，嵌套子块由各自的 runBlock 处理；
	// 若与块外同名，退出时恢复块外原值（内层声明遮蔽外层）。
	type varSnap struct {
		val Value
		ok  bool
	}
	vars := make(map[string]varSnap)
	curSnaps := make(map[string]*procCursorState)
	curOK := make(map[string]bool)
	conds := make(map[string]procCond)
	condOK := make(map[string]bool)
	for _, s := range st.body {
		switch d := s.(type) {
		case procDeclare:
			key := strings.ToLower(d.name)
			if _, seen := vars[key]; seen {
				continue
			}
			v, ok := r.env[key]
			vars[key] = varSnap{val: v, ok: ok}
		case procCursorDecl:
			key := strings.ToLower(d.name)
			if _, seen := curOK[key]; seen {
				continue
			}
			curSnaps[key] = r.cursors[key]
			curOK[key] = curSnaps[key] != nil
		case procConditionDecl:
			key := strings.ToLower(d.name)
			if _, seen := condOK[key]; seen {
				continue
			}
			c, ok := r.conditions[key]
			conds[key] = c
			condOK[key] = ok
		}
	}
	outerVars := make(map[string]bool, len(r.env))
	for k := range r.env {
		outerVars[k] = true
	}
	outerCursors := make(map[string]bool, len(r.cursors))
	for k := range r.cursors {
		outerCursors[k] = true
	}
	outerConds := make(map[string]bool, len(r.conditions))
	for k := range r.conditions {
		outerConds[k] = true
	}
	defer func() {
		for k := range r.env {
			if !outerVars[k] {
				delete(r.env, k)
			}
		}
		for k, snap := range vars {
			if snap.ok {
				r.env[k] = snap.val
			} else {
				delete(r.env, k)
			}
		}
		for k := range r.cursors {
			if !outerCursors[k] {
				delete(r.cursors, k)
			}
		}
		for k := range curOK {
			if curOK[k] {
				r.cursors[k] = curSnaps[k]
			} else {
				if s, ok := r.cursors[k]; ok && s != nil {
					delete(r.cursors, k)
				}
			}
		}
		for k := range r.conditions {
			if !outerConds[k] {
				delete(r.conditions, k)
			}
		}
		for k := range condOK {
			if condOK[k] {
				r.conditions[k] = conds[k]
			} else {
				delete(r.conditions, k)
			}
		}
	}()
	return r.run(st.body)
}

// setDiag 登记诊断区（最近一次语句错误的 SQLSTATE / 错误码 / 文本）。
func (r *procRunner) setDiag(err error) {
	state, code := procErrInfo(err)
	r.diag = procDiagArea{state: state, code: code, msg: err.Error(), set: true}
}

// execGetDiagnostics 执行 GET DIAGNOSTICS：ROW_COUNT 取最近一条 DML 影响行数，
// CONDITION <n> 取诊断区条件信息项（诊断区仅保留最近一条，序号须为 1）。
func (r *procRunner) execGetDiagnostics(st procGetDiagnostics) error {
	condNum := st.condNum
	if st.hasCond && st.condVar != "" {
		v, ok := r.env[st.condVar]
		if !ok {
			return &SQLError{Msg: "unknown procedure variable: @" + st.condVar}
		}
		n, ok := procToInt(v)
		if !ok {
			return &SQLError{Msg: "GET DIAGNOSTICS condition number must be an integer in procedure body"}
		}
		condNum = int(n)
	}
	if st.hasCond && condNum != 1 {
		return &SQLError{Msg: "GET DIAGNOSTICS CONDITION number out of range in procedure body"}
	}
	for _, it := range st.items {
		var v Value
		switch it.field {
		case "ROW_COUNT":
			if st.hasCond {
				return &SQLError{Msg: "ROW_COUNT is not valid in GET DIAGNOSTICS CONDITION in procedure body"}
			}
			v = IntVal(int64(r.lastRowCount))
		case "RETURNED_SQLSTATE":
			if !r.diag.set {
				return &SQLError{Msg: "GET DIAGNOSTICS CONDITION requires a prior statement condition in procedure body"}
			}
			v = StrVal(r.diag.state)
		case "MYSQL_ERRNO":
			if !r.diag.set {
				return &SQLError{Msg: "GET DIAGNOSTICS CONDITION requires a prior statement condition in procedure body"}
			}
			v = IntVal(int64(r.diag.code))
		case "MESSAGE_TEXT":
			if !r.diag.set {
				return &SQLError{Msg: "GET DIAGNOSTICS CONDITION requires a prior statement condition in procedure body"}
			}
			v = StrVal(r.diag.msg)
		default:
			return &SQLError{Msg: "unsupported GET DIAGNOSTICS item in procedure body: " + it.field}
		}
		r.env[it.name] = v
	}
	return nil
}

// execSignal 执行 SIGNAL：按 SQLSTATE / 错误码抛出异常，SET 子句可覆盖消息与错误码。
func (r *procRunner) execSignal(st procSignal) error {
	state := procDefaultSQLState
	code := 0
	if st.hasCond {
		cond := st.cond
		if cond.kind == procErrNamed {
			// v1.19.0：SIGNAL <命名条件> 按声明解析为具体 SQLSTATE / 错误码。
			rc, ok := r.resolveCond(cond)
			if !ok {
				return &SQLError{Msg: "unknown condition in SIGNAL: " + cond.name}
			}
			cond = rc
		}
		switch cond.kind {
		case procErrSQLState:
			if cond.state != "" {
				state = cond.state
			}
		case procErrErrno:
			code = cond.code
		}
	}
	msg := ""
	if len(st.set) > 0 {
		var err error
		state, code, msg, err = r.applySignalSet(st.set, state, code, msg)
		if err != nil {
			return err
		}
	}
	return newProcSignalError(state, code, msg)
}

// execResignal 执行 RESIGNAL：无参时原样重抛处理器正在处理的错误。
func (r *procRunner) execResignal(st procResignal) error {
	if !st.hasCond && len(st.set) == 0 {
		if r.curErr == nil {
			return &SQLError{Msg: "RESIGNAL without active handler error in procedure body"}
		}
		state, code := procErrInfo(r.curErr)
		return &procStateError{state: state, code: code, cause: r.curErr}
	}
	state := procDefaultSQLState
	code := 0
	if r.curErr != nil {
		state, code = procErrInfo(r.curErr)
	}
	if st.hasCond {
		cond := st.cond
		if cond.kind == procErrNamed {
			// v1.19.0：RESIGNAL <命名条件> 按声明解析为具体 SQLSTATE / 错误码。
			rc, ok := r.resolveCond(cond)
			if !ok {
				return &SQLError{Msg: "unknown condition in RESIGNAL: " + cond.name}
			}
			cond = rc
		}
		switch cond.kind {
		case procErrSQLState:
			if cond.state != "" {
				state = cond.state
			}
		case procErrErrno:
			code = cond.code
		}
	}
	msg := ""
	if r.curErr != nil && !st.hasCond {
		msg = r.curErr.Error()
	}
	if len(st.set) > 0 {
		var err error
		state, code, msg, err = r.applySignalSet(st.set, state, code, msg)
		if err != nil {
			return err
		}
	}
	return newProcSignalError(state, code, msg)
}

// applySignalSet 计算 SIGNAL / RESIGNAL 的 SET 子句：支持 MESSAGE_TEXT / MYSQL_ERRNO /
// SQLSTATE，其余条件信息项（CLASS_ORIGIN / SUBCLASS_ORIGIN 等）按标准接受但不参与匹配。
func (r *procRunner) applySignalSet(items []procAssign, state string, code int, msg string) (string, int, string, error) {
	for _, a := range items {
		v, err := evalProcExpr(a.expr, r.env)
		if err != nil {
			return state, code, msg, err
		}
		switch a.name {
		case "MESSAGE_TEXT":
			msg = procValueText(v)
		case "MYSQL_ERRNO":
			if n, ok := procToInt(v); ok {
				code = int(n)
			}
		case "SQLSTATE":
			if s := strings.TrimSpace(procValueText(v)); len(s) == 5 {
				state = s
			}
		}
	}
	return state, code, msg, nil
}

// execDynPrepare 执行 PREPARE：把 @var / 字面量 SQL 文本登记到本次调用的语句表。
func (r *procRunner) execDynPrepare(st procPrepare) error {
	text, err := r.evalDynamicSQLText(st.src)
	if err != nil {
		return err
	}
	if strings.TrimSpace(text) == "" {
		return &SQLError{Msg: "PREPARE requires non-empty SQL text in procedure body"}
	}
	if r.prepared == nil {
		r.prepared = make(map[string]string)
	}
	r.prepared[st.name] = text
	return nil
}

// evalDynamicSQLText 求值 PREPARE 的 SQL 文本（单 token 字面量 / 变量直取，
// 其余走过程表达式求值），结果统一转文本。
func (r *procRunner) evalDynamicSQLText(src []token) (string, error) {
	if len(src) == 1 {
		switch src[0].kind {
		case tokString:
			return src[0].text, nil
		case tokVar:
			key := strings.ToLower(src[0].text[1:])
			if v, ok := r.env[key]; ok {
				return procValueText(v), nil
			}
			return "", &SQLError{Msg: "unknown procedure variable: " + src[0].text}
		}
	}
	v, err := evalProcExpr(src, r.env)
	if err != nil {
		return "", err
	}
	return procValueText(v), nil
}

// execDynExecute 执行 EXECUTE：绑定 ? 占位符与 @var 后重解析，走标准执行路径。
func (r *procRunner) execDynExecute(st procExecute) error {
	text, ok := r.prepared[st.name]
	if !ok {
		return &SQLError{Msg: "prepared statement not exists: " + st.name}
	}
	args := make([]Value, 0, len(st.using))
	for _, argToks := range st.using {
		v, err := evalProcExpr(argToks, r.env)
		if err != nil {
			return err
		}
		args = append(args, v)
	}
	bound, err := bindDynamicSQL(text, args, r.env)
	if err != nil {
		return err
	}
	stmt, err := Parse(bound)
	if err != nil {
		return err
	}
	if !procedureBodySupported(stmt) {
		return &SQLError{Msg: "unsupported statement in dynamic SQL"}
	}
	if _, err := r.e.execStmt(r.sess, stmt); err != nil {
		return err
	}
	return nil
}

// execDynDeallocate 执行 DEALLOCATE PREPARE：释放动态语句。
func (r *procRunner) execDynDeallocate(st procDeallocate) error {
	if _, ok := r.prepared[st.name]; !ok {
		return &SQLError{Msg: "prepared statement not exists: " + st.name}
	}
	delete(r.prepared, st.name)
	return nil
}

// bindDynamicSQL 把动态 SQL 文本中的 ? 按顺序替换为实参字面量，并代入环境中的 @var
// （未定义变量保留原文，交由标准 Parse 报错）。占位符与实参个数不一致时报错。
func bindDynamicSQL(text string, args []Value, env map[string]Value) (string, error) {
	toks, err := lex(text)
	if err != nil {
		return "", err
	}
	out := make([]token, 0, len(toks))
	next := 0
	for _, t := range toks {
		switch t.kind {
		case tokQuestion:
			if next >= len(args) {
				return "", &SQLError{Msg: "EXECUTE USING argument count mismatch: placeholders exceed arguments"}
			}
			out = append(out, setValueToken(t, args[next]))
			next++
		case tokVar:
			key := strings.ToLower(t.text[1:])
			if v, ok := env[key]; ok {
				out = append(out, setValueToken(t, v))
				continue
			}
			out = append(out, t)
		default:
			out = append(out, t)
		}
	}
	if next != len(args) {
		return "", &SQLError{Msg: "EXECUTE USING argument count mismatch: " +
			strconv.Itoa(len(args)) + " argument(s) for " + strconv.Itoa(next) + " placeholder(s)"}
	}
	return tokensToString(out), nil
}

// procValueText 把过程值转为文本（MESSAGE_TEXT / 动态 SQL 文本）。
func procValueText(v Value) string {
	if v.Kind == "TEXT" {
		return v.S
	}
	return valueToToken(v).text
}

// isHandlerDecl 判断 DECLARE 之后是否为 {CONTINUE|EXIT} [<name>] HANDLER 形式。
func (p *procParser) isHandlerDecl() bool {
	k := p.kw()
	if k != "CONTINUE" && k != "EXIT" {
		return false
	}
	if p.pos+1 < len(p.toks) && strings.ToUpper(p.toks[p.pos+1].text) == "HANDLER" {
		return true
	}
	if p.pos+2 < len(p.toks) && p.toks[p.pos+1].kind == tokIdent &&
		strings.ToUpper(p.toks[p.pos+2].text) == "HANDLER" {
		return true
	}
	return false
}

// parseHandlerDecl 解析 DECLARE {CONTINUE|EXIT} [<name>] HANDLER FOR <cond>[, ...] <stmt>。
func (p *procParser) parseHandlerDecl() (procStmt, error) {
	st := procHandlerDecl{action: p.kw()}
	p.next() // CONTINUE / EXIT
	if p.kw() != "HANDLER" {
		st.name = p.cur().text
		p.next()
	}
	if err := p.expect("HANDLER"); err != nil {
		return nil, err
	}
	if err := p.expect("FOR"); err != nil {
		return nil, err
	}
	for {
		c, err := p.parseHandlerCond()
		if err != nil {
			return nil, err
		}
		st.conds = append(st.conds, c)
		if p.kw() == "," {
			p.next()
			continue
		}
		break
	}
	if p.eof() {
		return nil, &SQLError{Msg: "HANDLER requires a statement in procedure body"}
	}
	// v1.19.0：处理器体支持复合语句块 BEGIN ... END（块内可 DECLARE 局部变量等）。
	if p.kw() == "BEGIN" && !(p.pos+1 < len(p.toks) && p.toks[p.pos+1].kind == tokSemicolon) {
		block, err := p.parseBlock()
		if err != nil {
			return nil, err
		}
		st.body = []procStmt{block}
		return st, nil
	}
	body, err := p.parseOneStmt(nil)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, &SQLError{Msg: "HANDLER requires a statement in procedure body"}
	}
	st.body = []procStmt{body}
	return st, nil
}

// atCondStart 判断当前位置是否为一个异常条件的起始（v1.19.0）。
// 支持 SQLSTATE / CONDITION <name> / 错误码 / 裸命名条件；SET 与分号不是条件起始。
func (p *procParser) atCondStart() bool {
	if p.eof() {
		return false
	}
	switch p.kw() {
	case "SQLSTATE", "CONDITION":
		return true
	case "SET", ";":
		return false
	}
	return p.cur().kind == tokNumber || p.cur().kind == tokIdent
}

// parseConditionDecl 解析 DECLARE <name> CONDITION FOR {SQLSTATE '<code>' | <errno>} ;（v1.19.0）。
// 调用时条件名 token 尚未消费。
func (p *procParser) parseConditionDecl() (procStmt, error) {
	nameTok := p.cur()
	if nameTok.kind != tokIdent {
		return nil, &SQLError{Msg: "expected condition name after DECLARE in procedure body"}
	}
	p.next() // name
	if err := p.expect("CONDITION"); err != nil {
		return nil, err
	}
	if err := p.expect("FOR"); err != nil {
		return nil, err
	}
	cond, err := p.parseHandlerCond()
	if err != nil {
		return nil, err
	}
	// 命名条件只能指向具体 SQLSTATE 值或错误码，不能指向分类或另一命名条件。
	if cond.kind != procErrSQLState && cond.kind != procErrErrno {
		return nil, &SQLError{Msg: "CONDITION FOR requires SQLSTATE value or error number in procedure body"}
	}
	if p.kw() == ";" {
		p.next()
	}
	return procConditionDecl{name: nameTok.text, cond: cond}, nil
}

// parseGetDiagnostics 解析
// GET [CURRENT] DIAGNOSTICS [CONDITION <n> | CONDITION <@var>] <@var> = <字段>[, ...] ;（v1.19.0）。
// 支持的诊断字段：ROW_COUNT / RETURNED_SQLSTATE / MYSQL_ERRNO / MESSAGE_TEXT。
func (p *procParser) parseGetDiagnostics() (procStmt, error) {
	if err := p.expect("GET"); err != nil {
		return nil, err
	}
	if p.kw() == "CURRENT" {
		p.next()
	}
	if err := p.expect("DIAGNOSTICS"); err != nil {
		return nil, err
	}
	st := procGetDiagnostics{}
	if p.kw() == "CONDITION" {
		p.next()
		st.hasCond = true
		switch {
		case p.cur().kind == tokNumber:
			n, err := strconv.Atoi(p.cur().text)
			if err != nil {
				return nil, &SQLError{Msg: "invalid GET DIAGNOSTICS condition number in procedure body: " + p.cur().text}
			}
			st.condNum = n
			p.next()
		case p.cur().kind == tokVar:
			st.condVar = strings.ToLower(p.cur().text[1:])
			p.next()
		default:
			return nil, &SQLError{Msg: "expected condition number after GET DIAGNOSTICS CONDITION in procedure body"}
		}
	}
	for {
		if p.cur().kind != tokVar {
			return nil, &SQLError{Msg: "expected @variable in GET DIAGNOSTICS target list"}
		}
		it := procDiagItem{name: strings.ToLower(p.cur().text[1:])}
		p.next()
		if p.kw() != "=" && p.kw() != ":=" {
			return nil, &SQLError{Msg: "expected '=' in GET DIAGNOSTICS assignment"}
		}
		p.next()
		if p.cur().kind != tokIdent && p.cur().kind != tokKeyword {
			return nil, &SQLError{Msg: "expected diagnostics item in GET DIAGNOSTICS"}
		}
		field := strings.ToUpper(p.cur().text)
		switch field {
		case "ROW_COUNT", "RETURNED_SQLSTATE", "MYSQL_ERRNO", "MESSAGE_TEXT":
			it.field = field
			p.next()
		default:
			return nil, &SQLError{Msg: "unsupported GET DIAGNOSTICS item in procedure body: " + p.cur().text}
		}
		st.items = append(st.items, it)
		if p.kw() != "," {
			break
		}
		p.next()
	}
	if len(st.items) == 0 {
		return nil, &SQLError{Msg: "GET DIAGNOSTICS requires at least one target in procedure body"}
	}
	if p.kw() == ";" {
		p.next()
	}
	return st, nil
}

// parseHandlerCond 解析处理器条件：SQLSTATE '<code>' / <错误码> / SQLEXCEPTION /
// SQLWARNING / NOT FOUND / CONDITION <name> / <命名条件>。
func (p *procParser) parseHandlerCond() (procCond, error) {
	switch p.kw() {
	case "SQLSTATE":
		p.next()
		if p.kw() == "VALUE" {
			p.next()
		}
		return p.parseSQLStateValue()
	case "SQLEXCEPTION":
		p.next()
		return procCond{kind: procErrSQLEXCEPTION}, nil
	case "SQLWARNING":
		p.next()
		return procCond{kind: procErrSQLWARNING}, nil
	case "NOT":
		if p.pos+1 < len(p.toks) && strings.ToUpper(p.toks[p.pos+1].text) == "FOUND" {
			p.next()
			p.next()
			return procCond{kind: procErrNotFound}, nil
		}
	case "CONDITION":
		// v1.19.0：HANDLER FOR CONDITION <name> 按命名条件匹配（声明期登记、运行期解析）。
		p.next()
		return p.parseNamedCondRef()
	}
	if p.cur().kind == tokNumber {
		n, err := strconv.Atoi(p.cur().text)
		if err != nil {
			return procCond{}, &SQLError{Msg: "invalid error code in HANDLER condition: " + p.cur().text}
		}
		p.next()
		return procCond{kind: procErrErrno, code: n}, nil
	}
	// v1.19.0：裸条件名（HANDLER FOR <name>）按命名条件处理。
	if p.cur().kind == tokIdent {
		return p.parseNamedCondRef()
	}
	return procCond{}, &SQLError{Msg: "unsupported HANDLER condition in procedure body: " + p.cur().text}
}

// parseNamedCondRef 解析一个命名条件引用（裸名或 CONDITION 关键字之后的名字）。
func (p *procParser) parseNamedCondRef() (procCond, error) {
	t := p.cur()
	if t.kind != tokIdent {
		return procCond{}, &SQLError{Msg: "expected condition name in procedure body"}
	}
	p.next()
	return procCond{kind: procErrNamed, name: strings.ToLower(t.text)}, nil
}

// parseSQLStateValue 解析 SQLSTATE 值（5 位码，字符串或数字字面量写法）。
func (p *procParser) parseSQLStateValue() (procCond, error) {
	t := p.cur()
	if t.kind != tokString && t.kind != tokNumber {
		return procCond{}, &SQLError{Msg: "expected SQLSTATE value in procedure body"}
	}
	s := strings.TrimSpace(t.text)
	if len(s) != 5 {
		return procCond{}, &SQLError{Msg: "invalid SQLSTATE value in procedure body: " + s}
	}
	p.next()
	return procCond{kind: procErrSQLState, state: s}, nil
}

// parseSignal 解析 SIGNAL [SQLSTATE '<code>' | <错误码>] [SET ...]。
func (p *procParser) parseSignal() (procStmt, error) {
	p.next() // SIGNAL
	st := procSignal{}
	if p.atCondStart() {
		c, err := p.parseHandlerCond()
		if err != nil {
			return nil, err
		}
		st.cond = c
		st.hasCond = true
	}
	if !st.hasCond {
		return nil, &SQLError{Msg: "SIGNAL requires SQLSTATE or error code in procedure body"}
	}
	set, err := p.parseSignalSet()
	if err != nil {
		return nil, err
	}
	st.set = set
	return st, nil
}

// parseResignal 解析 RESIGNAL [<cond>] [SET ...]。
func (p *procParser) parseResignal() (procStmt, error) {
	p.next() // RESIGNAL
	st := procResignal{}
	if p.atCondStart() {
		c, err := p.parseHandlerCond()
		if err != nil {
			return nil, err
		}
		st.cond = c
		st.hasCond = true
	}
	set, err := p.parseSignalSet()
	if err != nil {
		return nil, err
	}
	st.set = set
	return st, nil
}

// parseSignalSet 解析 [SET] <条件信息项> = <表达式>[, ...] 并消费结束分号。
func (p *procParser) parseSignalSet() ([]procAssign, error) {
	var items []procAssign
	if p.kw() == "SET" {
		p.next()
		for {
			t := p.cur()
			if t.kind != tokIdent && t.kind != tokKeyword {
				return nil, &SQLError{Msg: "expected condition information item after SET in procedure body"}
			}
			name := strings.ToUpper(t.text)
			p.next()
			if p.kw() != "=" && p.kw() != ":=" {
				return nil, &SQLError{Msg: "expected = after " + name + " in procedure body"}
			}
			p.next()
			expr, err := p.collectUntilDelim(true)
			if err != nil {
				return nil, err
			}
			if len(expr) == 0 {
				return nil, &SQLError{Msg: "SET " + name + " requires a value in procedure body"}
			}
			items = append(items, procAssign{name: name, expr: append([]token(nil), expr...)})
			if p.kw() == "," {
				p.next()
				continue
			}
			break
		}
	}
	if p.kw() == ";" {
		p.next()
	}
	return items, nil
}

// parseDynPrepare 解析 PREPARE <name> FROM <@var|字面量>。
func (p *procParser) parseDynPrepare() (procStmt, error) {
	p.next() // PREPARE
	t := p.cur()
	if t.kind != tokIdent {
		return nil, &SQLError{Msg: "expected prepared statement name after PREPARE in procedure body"}
	}
	p.next()
	if err := p.expect("FROM"); err != nil {
		return nil, err
	}
	src, err := p.collectUntil(true)
	if err != nil {
		return nil, err
	}
	if len(src) == 0 {
		return nil, &SQLError{Msg: "PREPARE FROM requires a SQL text expression in procedure body"}
	}
	if p.kw() == ";" {
		p.next()
	}
	return procPrepare{name: strings.ToLower(t.text), src: append([]token(nil), src...)}, nil
}

// parseDynExecute 解析 EXECUTE <name> [USING <expr>[, ...]]。
func (p *procParser) parseDynExecute() (procStmt, error) {
	p.next() // EXECUTE
	t := p.cur()
	if t.kind != tokIdent {
		return nil, &SQLError{Msg: "expected prepared statement name after EXECUTE in procedure body"}
	}
	p.next()
	st := procExecute{name: strings.ToLower(t.text)}
	if p.kw() == "USING" {
		p.next()
		for {
			arg, err := p.collectUntilDelim(true)
			if err != nil {
				return nil, err
			}
			if len(arg) == 0 {
				return nil, &SQLError{Msg: "empty USING argument in procedure body"}
			}
			st.using = append(st.using, append([]token(nil), arg...))
			if p.kw() == "," {
				p.next()
				continue
			}
			break
		}
	}
	if p.kw() == ";" {
		p.next()
	}
	return st, nil
}

// parseDynDeallocate 解析 DEALLOCATE PREPARE <name>（PREPARE 可省略）。
func (p *procParser) parseDynDeallocate() (procStmt, error) {
	p.next() // DEALLOCATE
	if p.kw() == "PREPARE" {
		p.next()
	}
	t := p.cur()
	if t.kind != tokIdent {
		return nil, &SQLError{Msg: "expected prepared statement name after DEALLOCATE in procedure body"}
	}
	p.next()
	if p.kw() == ";" {
		p.next()
	}
	return procDeallocate{name: strings.ToLower(t.text)}, nil
}

// collectUntilDelim 收集 token 直至顶层逗号 / 分号 / 块结束（不消费停止 token）。
// 与 collectUntil 的差异：支持按逗号切分（SIGNAL SET 列表、EXECUTE USING 实参），
// 且不把其余关键字（如 FOR / WHEN）视为停止符。
func (p *procParser) collectUntilDelim(stopComma bool) ([]token, error) {
	start := p.pos
	parenDepth := 0
	caseDepth := 0
	for !p.eof() {
		t := p.cur()
		switch {
		case t.kind == tokLParen:
			parenDepth++
		case t.kind == tokRParen:
			if parenDepth > 0 {
				parenDepth--
			}
		case t.kind == tokKeyword && t.text == "CASE":
			caseDepth++
		case t.kind == tokKeyword && t.text == "END":
			if caseDepth > 0 {
				caseDepth--
			} else if parenDepth == 0 {
				return p.toks[start:p.pos], nil
			}
		case t.kind == tokSemicolon:
			if parenDepth == 0 && caseDepth == 0 {
				return p.toks[start:p.pos], nil
			}
		case stopComma && t.kind == tokComma && parenDepth == 0 && caseDepth == 0:
			return p.toks[start:p.pos], nil
		}
		p.next()
	}
	return p.toks[start:p.pos], nil
}
