package sql

import (
	"sync"
)

// v1.16.0 C3：ro_parallel 下沉 executor —— 引擎层只读 SELECT 批执行接口。
//
// 背景：v1.14.0 P0 的 ro_parallel 并行只读 SELECT 批位于 server 层 REPL
// 循环（pkg/server/server.go REPLSession），仅覆盖命令行/网络 REPL 连接；
// E5 直调入口（tests/bench/sql_bench main.go runOltpWorkers）绕过 REPL 批
// 直接调用 e.ExecuteAsSession 逐条执行，OLTP 显式事务内 SELECT 又被既有
// InTx 判定排除，因此 E5/直调场景完全无法享受只读并行（见
// docs/experiments/openxdb_concurrency_audit.md §6/§8.4）。
//
// 本文件把并行只读批下沉到执行器层：新增 ExecuteBatchAsSession 批接口，
// E5/bench 直调场景（tests/bench/sql_bench runOltpWorkers）可把显式事务内
// 无依赖的连续只读 SELECT 攒批并行执行；autocommit 场景每条语句独立临时
// 事务 + 独立快照并行执行。批内语句级并发，按原始行序重组写回；任一条
// 不满足保守判定条件即整批回退串行（与逐条 ExecuteAsSession 完全等价）。
// server 层 REPL 批路径（server.go:678-772）原样保留，二者并存不冲突。

const (
	// roBatchMaxConcurrency 批内并发上限（与 server 层 REPL 批 v1.14.0
	// roMaxConcurrency=8 对齐，控制单会话批并发的最大扇出）。
	roBatchMaxConcurrency = 8
	// roBatchMaxDefensive 防御性批大小上限：超出即整批回退串行，防止
	// 调用方误用超大批放大并发扇出（bench 事务内攒批远小于此值）。
	roBatchMaxDefensive = 256
)

// ExecuteBatchAsSession 引擎层只读 SELECT 批执行（v1.16.0 C3）。
//
// 语义契约（与逐条 ExecuteAsSession 串行执行等价）：
//   - 输入 stmts 按原始顺序返回 results / errs，批内并发执行不改变
//     语句次序；任一条失败时其余语句仍按批继续执行（与调用方逐条
//     串行循环的行为一致），错误按原位置返回。
//   - autocommit 场景：批内每条 SELECT 独立临时事务 + 独立快照并行
//     （MVCC 快照隔离只读安全，RocksDB Snapshot 读线程安全）。
//   - 显式事务内（InTx）：批内每条 SELECT 共享事务快照（curTx）并行
//     执行；批内无写语句，txnImpl.Get/Scan 只读路径天然线程安全，
//     read-your-writes 语义保持（批外写语句先冲刷批再串行执行）。
//   - Serializable 隔离 / forcedTx 挂载 / 读取用户变量（@var）的语句
//     一律不进入并行批，整批回退串行，保持保守判定与错误传播等价。
//   - 批大小为 0 或超过 roBatchMaxDefensive 时同样回退串行。
//
// 调用方约定：批量场景调用方应只收集"连续无依赖只读 SELECT"（如
// bench 显式事务内连续读操作）；混入写语句（INSERT/UPDATE/SET 等）时
// 本接口整批回退串行执行，正确性由回退路径保证。
func (e *Executor) ExecuteBatchAsSession(stmts []string, user string, sess *Session) ([]*Result, []error) {
	results := make([]*Result, len(stmts))
	errs := make([]error, len(stmts))
	if len(stmts) == 0 || len(stmts) > roBatchMaxDefensive {
		// 空批 / 超大批：串行逐条执行（保持调用方语义）。
		for i, s := range stmts {
			ast, err := Parse(s)
			if err != nil {
				errs[i] = err
				continue
			}
			results[i], errs[i] = e.ExecuteTextAsSession(ast, s, user, sess)
		}
		return results, errs
	}

	parsed := make([]Stmt, len(stmts))
	parseErrs := make([]error, len(stmts))
	allEligible := true
	for i, s := range stmts {
		ast, err := Parse(s)
		if err != nil {
			parseErrs[i] = err
			allEligible = false
			continue
		}
		parsed[i] = ast
		if !e.roBatchEligible(ast, sess) {
			allEligible = false
		}
	}

	if !allEligible {
		// 保守回退：整批按原始顺序串行逐条执行，与逐条 ExecuteAsSession
		// 完全等价（解析失败的原样返回解析错误，与串行入口一致）。
		for i, s := range stmts {
			if parseErrs[i] != nil {
				errs[i] = parseErrs[i]
				continue
			}
			results[i], errs[i] = e.ExecuteTextAsSession(parsed[i], s, user, sess)
		}
		return results, errs
	}

	// 并行执行：语句级并发 + 按原始行序重组。
	sem := make(chan struct{}, roBatchMaxConcurrency)
	var wg sync.WaitGroup
	for i, s := range stmts {
		wg.Add(1)
		go func(i int, s string, st Stmt) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			res, err := e.ExecuteTextAsSession(st, s, user, sess)
			results[i], errs[i] = res, err
		}(i, s, parsed[i])
	}
	wg.Wait()
	return results, errs
}

// roBatchEligible 判定单条语句是否可安全进入只读并行批（v1.16.0 C3）。
// 判定条件保守对齐 server 层 REPL 批（roSelectEligible）并补充引擎层
// 专属约束：
//  1. ro_parallel 开关开启（关闭回退 v1.13.0 串行行为）；
//  2. 语句解析为 *SelectStmt（OpenXDB SELECT 无 INTO/行锁副作用，
//     FOR UPDATE 仅语法兼容无锁语义，与 IsROSelect 判定一致）；
//  3. 语句不读取用户变量（@var）：批内无 SET 写者，读 @var 理论上
//     并行安全，但为保持"用户变量依赖保守判定"的语义等价，含 @var
//     的 SELECT 一律回退串行；
//  4. 会话未挂载 forcedTx（物化视图刷新透传事务窗口，保持既有串行
//     语义，避免内部写挂载期间并发读产生竞态）；
//  5. 会话隔离级别非 Serializable：txnImpl 在 Serializable 下并发写
//     readSet/writeSet（rw-antidependency 检测），批内并行读会并发
//     写共享集合，非线程安全，必须回退串行。
//  6. 显式事务内与 autocommit 均可进入（区别于 server REPL 批的
//     InTx 排除——C3 目标正是让显式事务内连续只读 SELECT 享并行）。
func (e *Executor) roBatchEligible(st Stmt, sess *Session) bool {
	if !e.ROParallel() {
		return false
	}
	sel, ok := st.(*SelectStmt)
	if !ok {
		return false
	}
	if selectHasUserVar(sel) {
		return false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.forcedTx != nil {
		return false
	}
	if sess.txIso == IsoSerializable {
		return false
	}
	return true
}

// selectHasUserVar 保守扫描 SELECT 是否引用用户变量（@name）：
// 列投影（Var / CASE / 算术表达式）、WHERE/HAVING 条件（含 BETWEEN、
// IN 列表、行值元组、子查询、CASE、算术表达式、聚合左操作数）、
// JOIN ON、CTE、FROM 子查询中任意位置出现 Kind=="VAR" 的值即返回 true。
// 用于只读并行批判定：批内无 SET 写者，但保持保守判定（用户变量读取
// 依赖语义等价），含 @var 的 SELECT 一律回退串行。
func selectHasUserVar(s *SelectStmt) bool {
	if s == nil {
		return false
	}
	for i := range s.Columns {
		sc := &s.Columns[i]
		if sc.Var != "" {
			return true
		}
		if caseHasUserVar(sc.Case) {
			return true
		}
		if exprHasUserVar(sc.Expr) {
			return true
		}
	}
	if s.Subquery != nil && selectHasUserVar(s.Subquery) {
		return true
	}
	for i := range s.CTEs {
		if selectHasUserVar(s.CTEs[i].Select) {
			return true
		}
	}
	for i := range s.Joins {
		j := &s.Joins[i]
		if selectHasUserVar(j.Subquery) {
			return true
		}
		for k := range j.On {
			if condHasUserVar(&j.On[k]) {
				return true
			}
		}
	}
	return whereHasUserVar(s.Where) || whereHasUserVar(s.Having)
}

func whereHasUserVar(w *Where) bool {
	if w == nil {
		return false
	}
	for i := range w.Conds {
		if condHasUserVar(&w.Conds[i]) {
			return true
		}
	}
	return false
}

func condHasUserVar(c *Cond) bool {
	if c.Val.Kind == "VAR" || c.Val2.Kind == "VAR" {
		return true
	}
	for i := range c.InList {
		if c.InList[i].Kind == "VAR" {
			return true
		}
	}
	for i := range c.TupleVals {
		for j := range c.TupleVals[i] {
			if c.TupleVals[i][j].Kind == "VAR" {
				return true
			}
		}
	}
	if selectHasUserVar(c.Sub) {
		return true
	}
	if caseHasUserVar(c.LeftCase) {
		return true
	}
	if exprHasUserVar(c.LeftExpr) || exprHasUserVar(c.RightExpr) {
		return true
	}
	if c.LeftAgg != nil && (c.LeftAgg.Var != "" || caseHasUserVar(c.LeftAgg.Case) || exprHasUserVar(c.LeftAgg.Expr)) {
		return true
	}
	return false
}

func caseHasUserVar(c *CaseWhenClause) bool {
	if c == nil {
		return false
	}
	if c.Else.Kind == "VAR" {
		return true
	}
	for i := range c.Branches {
		b := &c.Branches[i]
		if b.Then.Kind == "VAR" {
			return true
		}
		if whereHasUserVar(&b.Conds) {
			return true
		}
	}
	return false
}

func exprHasUserVar(x *Expr) bool {
	if x == nil {
		return false
	}
	if x.Val != nil && x.Val.Kind == "VAR" {
		return true
	}
	return exprHasUserVar(x.Left) || exprHasUserVar(x.Right)
}
