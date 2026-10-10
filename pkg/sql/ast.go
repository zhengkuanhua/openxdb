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
// Op 支持 = != < > <= >= BETWEEN IN LIKE；Col 可为裸列名或 t.col 限定名（Ref 拆分存放）。
// LeftCase 非空时表示左操作数为 CASE 表达式（忽略 Col），仍按 Op 与右值比较。
type Cond struct {
	Ref      string // 限定表名/别名（空 = 未限定）
	Col      string // 裸列名
	Op       string // = != < > <= >= BETWEEN IN LIKE
	Val      Value  // 右值（BETWEEN 下界）
	Val2     Value  // BETWEEN 上界
	RightRef string // 右值为列引用（t.col，JOIN ON 场景；空 = 字面量 Val）
	InList   []Value // IN 列表
	Sub      *SelectStmt // IN 子查询（右值）
	LeftCase *CaseWhenClause // 左操作数为 CASE 表达式（P2）
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
type DropTableStmt struct {
	Name     string
	IfExists bool // DROP TABLE IF EXISTS：表不存在时静默成功
}

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

// AlterTableStmt ALTER TABLE（M9 在线 DDL）。
// Action 取值：
//   "ADD_COLUMN"    新增列，列定义在 Column（已有行回填默认值）
//   "DROP_COLUMN"   删除列（连同引用该列的索引），列名在 Col
//   "RENAME_COLUMN" 重命名列，旧列在 Col、新列在 NewName
//   "RENAME_TABLE"  重命名表，新表名在 NewName
//   "ADD_INDEX"     新增索引，索引名在 Name、索引列在 Col
//   "DROP_INDEX"    删除索引，索引名在 Name
type AlterTableStmt struct {
	Table   string
	Action  string
	Col     string
	NewName string
	Name    string
	Column  ColumnDef
}

func (AlterTableStmt) stmt() {}

// InsertStmt INSERT。
type InsertStmt struct {
	Table   string
	Columns []string // 空 = 全部列按定义序
	Rows    [][]Value
}

func (InsertStmt) stmt() {}

// SelectColumn SELECT 目标列：普通列 / 聚合函数 / CASE 表达式（P2）。
type SelectColumn struct {
	Raw   string // 原始文本（列名、*、COUNT(*)、SUM(x)、AVG(x)、CASE 表达式）
	Agg   string // "" | "COUNT" | "SUM" | "AVG"
	Ref   string // 限定表名/别名（t.col 时非空；聚合参数亦可限定）
	Col   string // 聚合目标列 / 裸列名（COUNT 可为空）
	Name  string // 输出列名
	Alias string // AS 别名（空 = 默认输出名）
	Case  *CaseWhenClause // 非空 = 输出 CASE 表达式求值结果（忽略 Col）
}

// JoinClause JOIN 子句（INNER / LEFT，支持子查询右表）。
type JoinClause struct {
	Type     string // "INNER" | "LEFT"
	Table    string // 右表名（与 Subquery 二选一）
	Alias    string // 右表别名（空 = 表名）
	Subquery *SelectStmt
	On       []Cond // ON 条件（AND 组合）
}

// SelectStmt SELECT。
type SelectStmt struct {
	Columns   []SelectColumn
	From      string        // 表名（无 JOIN/子查询时）
	FromAlias string        // FROM 表别名（空 = 表名）
	Subquery  *SelectStmt   // FROM 子查询
	SubAlias  string        // FROM 子查询别名（必填）
	Joins     []JoinClause
	Where     *Where
	GroupBy   []string // GROUP BY 列（t.col / col）
	OrderBy   *OrderBy
	Limit     int // <=0 不限
}

func (SelectStmt) stmt() {}

// OrderBy 排序。
type OrderBy struct {
	Ref  string // 限定表名/别名（t.col 时非空）
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

// BeginStmt 开启会话级显式事务（BEGIN）。
type BeginStmt struct{}

func (BeginStmt) stmt() {}

// CommitStmt 提交会话级显式事务（COMMIT）。
type CommitStmt struct{}

func (CommitStmt) stmt() {}

// RollbackStmt 回滚会话级显式事务（ROLLBACK）。
type RollbackStmt struct{}

func (RollbackStmt) stmt() {}

// ExportStmt 导出表数据到 CSV（EXPORT TABLE t [(cols)] TO 'path'）。
type ExportStmt struct {
	Table   string
	Columns []string // 空 = 全部列按定义序
	Path    string
}

func (ExportStmt) stmt() {}

// ImportStmt 从 CSV 导入建行（IMPORT INTO t FROM 'path' [BATCH n] [IGNORE ERRORS]）。
// Batch>0 时逐批提交（每 n 行一批）；IgnoreErrors 时坏行计数跳过不整体回滚。
type ImportStmt struct {
	Table        string
	Path         string
	Batch        int
	IgnoreErrors bool
}

func (ImportStmt) stmt() {}

// ---- P2：运维语句 ----

// ShowTablesStmt SHOW TABLES：列出全部表（含列定义/主键/索引概要）。
type ShowTablesStmt struct{}

func (ShowTablesStmt) stmt() {}

// ShowIndexStmt SHOW INDEX FROM t：列出指定表的全部二级索引。
type ShowIndexStmt struct {
	Table string
}

func (ShowIndexStmt) stmt() {}

// ShowSlowQueriesStmt SHOW SLOWQUERIES：查看慢查询记录列表。
type ShowSlowQueriesStmt struct{}

func (ShowSlowQueriesStmt) stmt() {}

// ExplainStmt EXPLAIN <SELECT>：输出执行计划文本（不实际执行查询）。
type ExplainStmt struct {
	Select *SelectStmt
}

func (ExplainStmt) stmt() {}

// SlowQueryRecord 慢查询记录（内存保留最近 slowQueryMax 条）。
type SlowQueryRecord struct {
	SQL      string // SQL 原文
	Duration int64  // 耗时（毫秒）
	At       string // 触发时间 YYYY-MM-DD HH:MM:SS
}

// ---- P2：CASE WHEN 表达式 ----

// CaseBranch CASE 的一个 WHEN 分支：条件（AND 组合）+ THEN 结果。
type CaseBranch struct {
	Conds Where // WHEN 条件组（AND 组合；非空）
	Then  Value // THEN 值
}

// CaseWhenClause CASE WHEN ... THEN ... [ELSE ...] END。
// 无匹配分支且无 ELSE 时返回空串（TEXT）。
type CaseWhenClause struct {
	Branches []CaseBranch
	Else     Value
	HasElse  bool
}

// ---- M4：多节点集群管理语句 ----

// AddNodeStmt ADD NODE '<addr>'：与远端节点握手并注册到节点注册表。
type AddNodeStmt struct {
	Addr string // 节点链路地址 host:port
}

func (AddNodeStmt) stmt() {}

// ShowNodesStmt SHOW NODES：列出节点注册表（ID/Addr/Role/State/LastSeen）。
type ShowNodesStmt struct{}

func (ShowNodesStmt) stmt() {}

// ShowRegionRoutesStmt SHOW REGION ROUTES：列出集群 region 路由表（region → 归属节点）。
type ShowRegionRoutesStmt struct{}

func (ShowRegionRoutesStmt) stmt() {}

// ShowStatsStmt SHOW STATS：输出服务器运行指标（T20 运维监控，配合 openxdb stats）。
type ShowStatsStmt struct{}

// SetStmt 会话变量设置：SET <name> = <value>（M9 查询缓存开关等）。
type SetStmt struct {
	Name  string
	Value string
}

func (SetStmt) stmt() {}

func (ShowStatsStmt) stmt() {}


// AssignRegionStmt ASSIGN REGION <regionID> TO NODE '<nodeID>'：指派 region 归属节点。
type AssignRegionStmt struct {
	RegionID uint64 // 显式 region ID
	NodeID   string // 已注册节点 ID（本节点 ID 表示迁回本地）
}

func (AssignRegionStmt) stmt() {}

// ---- M7：region 分裂 / 负载均衡 ----

// SplitRegionStmt SPLIT REGION <regionID>：手动将 region 沿数据中点一分为二。
// 与 M3 手动 SplitTable（按指定边界）兼容：本语句按当前数据分布自动选边界，
// 用于等量分裂；显式边界仍走 Engine.SplitTable。
type SplitRegionStmt struct {
	RegionID uint64 // 显式 region ID（隐式单 region 也可）
}

func (SplitRegionStmt) stmt() {}

// BalanceStmt BALANCE：手动触发一次负载均衡（迁移过载本地 region 到最闲存活节点）。
type BalanceStmt struct{}

// ---- M8(BR)：备份/恢复（BACKUP/RESTORE+PITR）----
// BackupStmt BACKUP TO '<path>'：基于一致快照生成完整逻辑备份文件。
type BackupStmt struct{ Path string }

func (BackupStmt) stmt() {}

// RestoreStmt RESTORE FROM '<path>' [TO LSN <n>]：
// 校验并恢复备份快照；可选 TO LSN 通过 binlog 回放到指定 LSN（PITR 基础）。
type RestoreStmt struct {
	Path     string
	ToLSN    uint64
	HasToLSN bool
}

func (RestoreStmt) stmt() {}

func (BalanceStmt) stmt() {}

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
