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

// procedureBodySupported 存储过程体支持的语句集合（创建时白名单校验）。
// 允许嵌套 CALL（跨过程调用），但执行时递归深度防护（procDepth>0）拒绝展开，
// 避免无限递归。
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

// splitBodyStatements 把过程体（BEGIN ... END 之间的一段文本）按顶层分号
// 切分为语句列表。识别 BEGIN 起始与收尾 END（最后一个非 EOF token），
// 括号深度与 CASE ... END 内的分号不计入切分。
func splitBodyStatements(body string) ([]string, error) {
	src := "BEGIN " + body + " END"
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	// toks[0] 必为 BEGIN；最后一个非 EOF token 为收尾 END。
	if len(toks) < 3 {
		return nil, &SQLError{Msg: "invalid procedure body"}
	}
	var out []string
	start := 1
	parenDepth := 0
	caseDepth := 0
	for i := 1; i < len(toks)-1; i++ {
		t := toks[i]
		switch t.kind {
		case tokLParen:
			parenDepth++
		case tokRParen:
			parenDepth--
		case tokKeyword:
			if t.text == "CASE" {
				caseDepth++
			} else if t.text == "END" {
				if caseDepth > 0 {
					caseDepth--
				} else if parenDepth == 0 {
					// 收尾 END
					if seg := strings.TrimSpace(tokensToString(toks[start:i])); seg != "" {
						out = append(out, seg)
					}
					return out, nil
				}
			}
		case tokSemicolon:
			if parenDepth == 0 {
				if seg := strings.TrimSpace(tokensToString(toks[start:i])); seg != "" {
					out = append(out, seg)
				}
				start = i + 1
			}
		}
	}
	if seg := strings.TrimSpace(tokensToString(toks[start : len(toks)-1])); seg != "" {
		out = append(out, seg)
	}
	return out, nil
}

// parseLocalSetTokens 解析局部变量赋值 SET @name = <literal>（M15-3）。
// 仅支持字面量赋值（INT / DECIMAL / 字符串），不参与 execStmt 分发
// （execSet 是会话变量语义，此处按过程局部作用域处理）。
func parseLocalSetTokens(toks []token) (string, Value, error) {
	// lex 结果末尾总带 tokEOF，先剥离再校验（SET @x = <lit> 共 4 个 token）。
	if n := len(toks); n > 0 && toks[n-1].kind == tokEOF {
		toks = toks[:n-1]
	}
	if len(toks) != 4 {
		return "", Value{}, &SQLError{Msg: "invalid SET statement in procedure body"}
	}
	if toks[0].kind != tokKeyword || toks[0].text != "SET" {
		return "", Value{}, &SQLError{Msg: "expected SET at start of statement"}
	}
	if toks[1].kind != tokVar {
		return "", Value{}, &SQLError{Msg: "expected @variable after SET"}
	}
	if toks[2].kind != tokEq {
		return "", Value{}, &SQLError{Msg: "expected = after @variable"}
	}
	name := toks[1].text[1:]
	v, err := tokenToValue(toks[3])
	if err != nil {
		return "", Value{}, err
	}
	return name, v, nil
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

// validateProcedureBody 创建期预校验过程体：可切分为一条或多条语句，
// 每条语句可解析且为受支持类型（INSERT / UPDATE / DELETE / SELECT /
// 局部变量赋值 SET @x = <literal>）。@var 引用在执行期由实参/局部变量
// 代入，此处以占位字面量替换后仅做语法/类型级校验。
func validateProcedureBody(body string) error {
	stmts, err := splitBodyStatements(body)
	if err != nil {
		return err
	}
	if len(stmts) == 0 {
		return &SQLError{Msg: "procedure body required"}
	}
	for _, raw := range stmts {
		toks, err := lex(raw)
		if err != nil {
			return err
		}
		if len(toks) >= 2 && toks[0].kind == tokKeyword && toks[0].text == "SET" && toks[1].kind == tokVar {
			if _, _, err := parseLocalSetTokens(toks); err != nil {
				return err
			}
			continue
		}
		// 收集本句引用的 @var，统一以占位字面量 1 代入后解析校验。
		env := make(map[string]Value)
		for _, t := range toks {
			if t.kind == tokVar {
				env[strings.ToLower(t.text[1:])] = IntVal(1)
			}
		}
		sub, err := replaceVars(raw, env, true)
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

// execCall CALL <name> [( <value> [, <value> ...] )]（M15-3）。
// 实参按位置一一对应代入 IN 参数（个数严格匹配；类型不强制，按字面量代入
// 后由体语句自身类型规则校验）。体语句逐条执行：局部变量赋值 SET @x = <lit>
// 更新本次调用的局部作用域；其余语句把 @var 引用替换为当前值后走标准
// execStmt 路径（INSERT/UPDATE/DELETE 各自独立隐式事务提交，SELECT 执行
// 但结果集不返回——CALL 返回固定 AffectedRows=1）。procDepth 递归防护：
// 过程体再次 CALL 时不再展开，避免无限递归（文档化限制）。
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
	if sess.procDepth > 0 {
		return nil, &SQLError{Msg: "procedure recursion not supported: " + s.Name}
	}
	env := make(map[string]Value, len(proc.Params))
	for i, pm := range proc.Params {
		env[strings.ToLower(pm.Name)] = s.Args[i]
	}
	sess.procDepth++
	defer func() { sess.procDepth-- }()
	if err := e.execProcedureBody(sess, proc.Body, env); err != nil {
		return nil, &SQLError{Msg: "procedure " + s.Name + " failed: " + err.Error()}
	}
	return &Result{AffectedRows: 1}, nil
}

// execProcedureBody 逐条执行过程体语句（M15-3 内部实现）。
func (e *Executor) execProcedureBody(sess *Session, body string, env map[string]Value) error {
	stmts, err := splitBodyStatements(body)
	if err != nil {
		return err
	}
	for _, raw := range stmts {
		toks, err := lex(raw)
		if err != nil {
			return err
		}
		// 局部变量赋值：SET @x = <literal>，更新本次调用局部作用域。
		if len(toks) >= 2 && toks[0].kind == tokKeyword && toks[0].text == "SET" && toks[1].kind == tokVar {
			name, val, err := parseLocalSetTokens(toks)
			if err != nil {
				return err
			}
			env[strings.ToLower(name)] = val
			continue
		}
		sub, err := replaceVars(raw, env, true)
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
		if _, err := e.execStmt(sess, stmt); err != nil {
			return err
		}
	}
	return nil
}
