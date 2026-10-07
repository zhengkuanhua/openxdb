package sql

import "fmt"

// M1 SQL 子集 AST（开发手册 §4：Parser 子集 + Executor 基础算子）。
// 支持：CREATE TABLE / DROP TABLE / INSERT / SELECT(点查/范围/过滤/聚合/ORDER BY/LIMIT)
//       UPDATE / DELETE；子集外语法返回 ErrUnsupported。

// Stmt 语句节点。
type Stmt interface{ stmt() }

// Value 字面量：INT 或 TEXT。
type Value struct {
	Kind string // "INT" | "TEXT"
	I    int64
	S    string
}

func IntVal(i int64) Value  { return Value{Kind: "INT", I: i} }
func StrVal(s string) Value { return Value{Kind: "TEXT", S: s} }

// ColumnDef 列定义。
type ColumnDef struct {
	Name string
	Type string // INT | TEXT
}

// Cond 过滤条件：col op value（AND 组合，M1 不支持 OR）。
type Cond struct {
	Col string
	Op  string // = != < > <= >=
	Val Value
}

// Where 条件组（全部 AND 语义）。
type Where struct {
	Conds []Cond
}

func (Where) stmt() {}

// CreateTableStmt CREATE TABLE。
type CreateTableStmt struct {
	Name    string
	Columns []ColumnDef
	PK      string // 主键列名；未声明 PRIMARY KEY 时默认第一列
}

func (CreateTableStmt) stmt() {}

// DropTableStmt DROP TABLE。
type DropTableStmt struct{ Name string }

func (DropTableStmt) stmt() {}

// CreateIndexStmt CREATE INDEX（T6 二级索引）。
type CreateIndexStmt struct {
	Name  string // 索引名（表内唯一）
	Table string
	Col   string // 单列索引
}

func (CreateIndexStmt) stmt() {}

// DropIndexStmt DROP INDEX。
type DropIndexStmt struct {
	Name  string
	Table string
}

func (DropIndexStmt) stmt() {}

// InsertStmt INSERT。
type InsertStmt struct {
	Table   string
	Columns []string // 空 = 全部列按定义序
	Rows    [][]Value
}

func (InsertStmt) stmt() {}

// SelectColumn SELECT 目标列：普通列 / 聚合函数。
type SelectColumn struct {
	Raw  string // 原始文本（列名、*、COUNT(*)、SUM(x)、AVG(x)）
	Agg  string // "" | "COUNT" | "SUM" | "AVG"
	Col  string // 聚合目标列（COUNT 可为空）
	Name string // 输出列名
}

// SelectStmt SELECT。
type SelectStmt struct {
	Columns []SelectColumn
	From    string
	Where   *Where
	OrderBy *OrderBy
	Limit   int // <=0 不限
}

func (SelectStmt) stmt() {}

// OrderBy 排序。
type OrderBy struct {
	Col  string
	Desc bool
}

// SetItem UPDATE 赋值项。
type SetItem struct {
	Col string
	Val Value
}

// UpdateStmt UPDATE。
type UpdateStmt struct {
	Table string
	Sets  []SetItem
	Where *Where
}

func (UpdateStmt) stmt() {}

// DeleteStmt DELETE。
type DeleteStmt struct {
	Table string
	Where *Where
}

func (DeleteStmt) stmt() {}

// Result 执行结果（面向协议层/CLI 输出）。
type Result struct {
	Columns      []string
	Rows         [][]Value
	AffectedRows int
}

// ErrUnsupported 子集外语法。
var ErrUnsupported = &SQLError{"unsupported statement"}

// SQLError SQL 层错误。
type SQLError struct{ Msg string }

func (e *SQLError) Error() string { return "SQL error: " + e.Msg }

func errf(format string, a ...interface{}) error {
	return &SQLError{Msg: fmt.Sprintf(format, a...)}
}

func sprintf(format string, a ...interface{}) string { return fmt.Sprintf(format, a...) }

// parseInt10 解析十进制整数。
func parseInt10(s string) (int64, error) {
	var n int64
	neg := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if i == 0 && c == '-' {
			neg = true
			continue
		}
		if c < '0' || c > '9' {
			return 0, &SQLError{Msg: "invalid number: " + s}
		}
		n = n*10 + int64(c-'0')
	}
	if neg {
		n = -n
	}
	return n, nil
}
