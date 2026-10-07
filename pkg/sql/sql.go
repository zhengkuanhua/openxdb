package sql

import (
	"strings"

	"github.com/openxdb/openxdb/pkg/txn"
)

// Engine SQL 引擎入口：Parse + Execute 一条语句。
// 协议层/CLI 通过 Engine.Execute 处理 `SQL ...` 命令或 SQL 关键字语句。
type Engine struct {
	ex *Executor
}

// New 创建 SQL 引擎。
func New(tm txn.TxnManager) *Engine {
	return &Engine{ex: NewExecutor(tm)}
}

// Execute 解析并执行一条 SQL 语句。
func (e *Engine) Execute(stmt string) (*Result, error) {
	ast, err := Parse(stmt)
	if err != nil {
		return nil, err
	}
	return e.ex.Execute(ast)
}

// IsSQL 判断一行命令是否以 SQL 关键字开头（供协议层路由）。
func IsSQL(line string) bool {
	trimmed := strings.TrimLeft(line, " \t\r\n")
	for _, kw := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP"} {
		if strings.HasPrefix(strings.ToUpper(trimmed), kw+" ") ||
			strings.EqualFold(trimmed, kw) {
			return true
		}
	}
	return false
}
