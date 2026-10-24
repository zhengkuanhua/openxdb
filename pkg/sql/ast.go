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
	Name          string
	Type          string // INT | TEXT | DATE | DECIMAL | BLOB
	Params        []int  // 类型精度参数：varchar(10)→[10]、decimal(12,2)→[12,2]（仅解析接受，不强制长度）
	AutoIncrement bool   // AUTO_INCREMENT 列属性（MySQL DDL 兼容，仅登记）
}

// Cond 过滤条件：col op value（AND 组合，M1 不支持 OR）。
// Op 支持 = != < > <= >= BETWEEN IN LIKE；Col 可为裸列名或 t.col 限定名（Ref 拆分存放）。
// LeftCase 非空时表示左操作数为 CASE 表达式（忽略 Col），仍按 Op 与右值比较。
type Cond struct {
	Ref      string          // 限定表名/别名（空 = 未限定）
	Col      string          // 裸列名
	Op       string          // = != < > <= >= BETWEEN IN LIKE TUPLE_IN
	Val      Value           // 右值（BETWEEN 下界）
	Val2     Value           // BETWEEN 上界
	RightRef string          // 右值为列引用（t.col，JOIN ON 场景；空 = 字面量 Val）
	InList   []Value         // IN 列表
	Sub      *SelectStmt     // IN 子查询（右值）
	LeftCase *CaseWhenClause // 左操作数为 CASE 表达式（P2）
	LeftExpr *Expr           // 左操作数为算术表达式（T40；与 Ref/Col/LeftCase 互斥）
	LeftAgg  *SelectColumn   // 左操作数为聚合调用（P0-4 HAVING：COUNT/SUM/AVG/MAX/MIN）
	// TPC-C 兼容：行值 IN——(col1, col2) IN ((v1,v2),(v3,v4))。
	// Op == "TUPLE_IN" 时使用 TupleLeft + TupleVals，忽略 Col/Val/InList。
	TupleLeft []string  // 左操作数列名列表
	TupleVals [][]Value // 右值元组列表
	RightExpr *Expr     // 右操作数为算术表达式（T40；与 RightRef/Val/InList/Sub 互斥）
}

// Where 条件组（全部 AND 语义）。
type Where struct {
	Conds []Cond
}

func (Where) stmt() {}

// CreateTableStmt CREATE TABLE。
type CreateTableStmt struct {
	Name        string
	Columns     []ColumnDef
	PK          string       // 主键列名（复合主键时为首列）；未声明 PRIMARY KEY 时默认第一列
	PKCols      []string     // 复合主键全列（空 = 单列主键 PK）
	Indexes     []IndexMeta  // M18：表级 UNIQUE (col) 约束（建表时登记二级索引元数据）
	ForeignKeys []ForeignKey // M9 外键约束（列级/表级，解析时收集）
	IfNotExists bool         // T41：CREATE TABLE IF NOT EXISTS（表已存在时静默跳过）
	Select      *SelectStmt  // T41：CTAS——CREATE TABLE <name> AS SELECT ...（非空时走 CTAS 执行路径）
}

func (CreateTableStmt) stmt() {}

// ForeignKey 外键约束定义（M9，P0-3 扩展 ON UPDATE）。
// Columns 为子表（本表）外键列，RefTable/RefColumns 为引用父表与父列；
// OnDelete / OnUpdate 取值 "" / "CASCADE" / "RESTRICT" / "SET NULL"
// （缺省即 RESTRICT，即 NO ACTION 语义）。
type ForeignKey struct {
	Columns    []string `json:"columns"`
	RefTable   string   `json:"ref_table"`
	RefColumns []string `json:"ref_columns"`
	OnDelete   string   `json:"on_delete,omitempty"`
	OnUpdate   string   `json:"on_update,omitempty"`
}

// DropTableStmt DROP TABLE。
type DropTableStmt struct {
	Name     string
	IfExists bool // DROP TABLE IF EXISTS：表不存在时静默成功
}

func (DropTableStmt) stmt() {}

// ---- M11：用户管理 ----

// CreateUserStmt CREATE USER '<name>' IDENTIFIED BY '<pwd>'。
// 密码仅以哈希形式持久化（SHA-256+salt，见 user.go）。
type CreateUserStmt struct {
	Name        string
	Password    string
	IfNotExists bool // T41：CREATE USER IF NOT EXISTS（用户已存在时静默跳过）
}

func (CreateUserStmt) stmt() {}

// DropUserStmt DROP USER [IF EXISTS] '<name>'。
type DropUserStmt struct {
	Name     string
	IfExists bool // DROP USER IF EXISTS：用户不存在时静默成功
}

func (DropUserStmt) stmt() {}

// AlterUserStmt ALTER USER '<name>' IDENTIFIED BY '<pwd>'（重置密码）。
type AlterUserStmt struct {
	Name     string
	Password string
}

func (AlterUserStmt) stmt() {}

// ShowUsersStmt SHOW USERS：列出全部用户。
type ShowUsersStmt struct{}

func (ShowUsersStmt) stmt() {}

// GrantStmt GRANT <priv[,priv...]> ON <table> TO '<user>'（M17 扩展：
// Scope 为 TABLE / DATABASE / COLUMN / ROLE，分别配合 Table+Database /
// Database / Table+Columns / Role 使用）。
// Privileges 为大写权限名：SELECT / INSERT / UPDATE / DELETE / ALL。
type GrantStmt struct {
	Privileges []string
	Table      string
	User       string
	Scope      string   // M17：授权范围（TABLE/DATABASE/COLUMN/ROLE），空=TABLE
	Database   string   // M17：库级/列级授权目标库
	Columns    []string // M17：列级授权目标列
	Role       string   // M17：角色授权（GRANT <role> TO '<user>'）
	ToRole     bool     // M17：目标为角色（GRANT <privs> ON <obj> TO ROLE '<role>'）
}

func (GrantStmt) stmt() {}

// RevokeStmt REVOKE <priv[,priv...]> ON <table> FROM '<user>'（M17 扩展同
// GrantStmt.Scope 语义）。
// Privileges 为大写权限名：SELECT / INSERT / UPDATE / DELETE / ALL。
type RevokeStmt struct {
	Privileges []string
	Table      string
	User       string
	Scope      string   // M17：授权范围（TABLE/DATABASE/COLUMN/ROLE），空=TABLE
	Database   string   // M17：库级/列级授权目标库
	Columns    []string // M17：列级授权目标列
	Role       string   // M17：角色授权（REVOKE <role> FROM '<user>'）
	ToRole     bool     // M17：目标为角色（REVOKE <privs> ON <obj> FROM ROLE '<role>'）
}

func (RevokeStmt) stmt() {}

// CreateRoleStmt CREATE ROLE '<role>'（M17 RBAC）：创建角色（空权限集合）。
type CreateRoleStmt struct {
	Name        string
	IfNotExists bool // T41：CREATE ROLE IF NOT EXISTS（角色已存在时静默跳过）
}

func (CreateRoleStmt) stmt() {}

// DropRoleStmt DROP ROLE [IF EXISTS] '<role>'（M17 RBAC）：删除角色并联动
// 清理用户-角色映射与角色内授权。
type DropRoleStmt struct {
	Name     string
	IfExists bool
}

func (DropRoleStmt) stmt() {}

// ShowRolesStmt SHOW ROLES（M17 RBAC）：列出全部角色。
type ShowRolesStmt struct{}

func (ShowRolesStmt) stmt() {}

// ShowGrantsStmt SHOW GRANTS FOR '<user>'（M17 RBAC）：列出用户全部权限来源。
type ShowGrantsStmt struct {
	User string
}

func (ShowGrantsStmt) stmt() {}

// ShowAuditLogStmt SHOW AUDITLOG [LIMIT n]（M17-1 审计）：查看内存审计记录。
type ShowAuditLogStmt struct {
	Limit int
}

func (ShowAuditLogStmt) stmt() {}

// CreateIndexStmt CREATE INDEX（T6 二级索引；M10 支持 UNIQUE）。
type CreateIndexStmt struct {
	Name        string // 索引名（表内唯一）
	Table       string
	Col         string // 单列索引
	Unique      bool   // M10：UNIQUE 索引（唯一约束）
	IfNotExists bool   // T41：CREATE INDEX IF NOT EXISTS（索引已存在时静默跳过）
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
//
//	"ADD_COLUMN"    新增列，列定义在 Column（已有行回填默认值）
//	"DROP_COLUMN"   删除列（连同引用该列的索引），列名在 Col
//	"RENAME_COLUMN" 重命名列，旧列在 Col、新列在 NewName
//	"RENAME_TABLE"  重命名表，新表名在 NewName
//	"MODIFY_COLUMN" 列级类型变更（T39），列名在 Col、目标类型在 NewType
//	"ADD_INDEX"     新增索引，索引名在 Name、索引列在 Col
//	"DROP_INDEX"    删除索引，索引名在 Name
type AlterTableStmt struct {
	Table   string
	Action  string
	Col     string
	NewName string
	Name    string
	NewType string
	Column  ColumnDef
}

func (AlterTableStmt) stmt() {}

// InsertStmt INSERT / REPLACE（M10 UPSERT）。
// OnDup 非空表示 INSERT ... ON DUPLICATE KEY UPDATE（冲突时更新）；
// Replace=true 表示 REPLACE INTO（冲突时删除旧行再插入，事务内原子）；
// Select 非空表示 INSERT INTO ... SELECT（T42：忽略 Rows，按 SELECT 结果逐行灌入）。
type InsertStmt struct {
	Table   string
	Columns []string // 空 = 全部列按定义序
	Rows    [][]Value
	OnDup   *OnDuplicateUpdate // M10：ON DUPLICATE KEY UPDATE 赋值项
	Replace bool               // M10：REPLACE INTO 语义
	Select  *SelectStmt        // T42：INSERT INTO ... SELECT 源（非空时忽略 Rows）
}

func (InsertStmt) stmt() {}

// OnDuplicateUpdate INSERT ... ON DUPLICATE KEY UPDATE 更新项（M10）。
type OnDuplicateUpdate struct {
	Sets []SetItem
}

// SelectColumn SELECT 目标列：普通列 / 聚合函数 / 窗口函数 / CASE 表达式 / 算术表达式（P2 / M10 / T40）。
// Window 非空表示窗口函数列：Agg 为函数名（"ROW_NUMBER"|"RANK"|"DENSE_RANK"|"COUNT"|"SUM"|"AVG"|"MAX"|"MIN"），
// Window 描述 PARTITION BY / ORDER BY；仅允许出现在 SELECT 列表，其它位置解析报错。
// Expr 非空表示投影列或聚合参数为算术表达式（T40，忽略 Col/Ref 作普通列取值）。
type SelectColumn struct {
	Raw      string          // 原始文本（列名、*、COUNT(*)、SUM(x)、AVG(x)、CASE 表达式、算术表达式）
	Agg      string          // "" | "COUNT" | "SUM" | "AVG" | "MAX" | "MIN" | "ROW_NUMBER" | "RANK" | "DENSE_RANK"
	Distinct bool            // P0-4：聚合参数 DISTINCT（COUNT(DISTINCT expr/col) 等）
	Ref      string          // 限定表名/别名（t.col 时非空；聚合参数亦可限定）
	Col      string          // 聚合目标列 / 裸列名（COUNT 可为空）
	Name     string          // 输出列名
	Alias    string          // AS 别名（空 = 默认输出名）
	Case     *CaseWhenClause // 非空 = 输出 CASE 表达式求值结果（忽略 Col）
	Expr     *Expr           // 非空 = 投影/聚合参数为算术表达式（T40）
	Window   *WindowSpec     // 非空 = 窗口函数列（OVER 子句）
	Var      string          // M18-C：用户变量引用（SELECT @var，含 @）
}

// Expr 算术表达式节点（T40）：二元算术（+ - * / %）与一元负号（NEG）。
// Op 为空表示叶子节点：Val 非 nil 为字面量，否则 Ref/Col 为列引用（限定/裸）。
// Raw 为表达式原始文本（用于输出列名与报错信息）。
type Expr struct {
	Op    string // "" | "+" | "-" | "*" | "/" | "%" | "NEG"
	Left  *Expr  // 二元左操作数（NEG 时为空）
	Right *Expr  // 二元右操作数 / NEG 子表达式
	Ref   string // 叶子：限定表名/别名（空 = 未限定）
	Col   string // 叶子：裸列名（Val 为 nil 时有效）
	Val   *Value // 叶子：字面量（nil = 列引用）
	Raw   string // 原始文本
}

// isPlainColumn 判断 Expr 是否为纯列引用叶子（Op 为空且非字面量）。
func (e *Expr) isPlainColumn() bool { return e != nil && e.Op == "" && e.Val == nil }

// isLiteral 判断 Expr 是否为纯字面量叶子（Op 为空、无列引用），
// 用于 WHERE 右值回落为旧路径常量 c.Val，保持 matchWhere 语义。
func (e *Expr) isLiteral() bool { return e != nil && e.Op == "" && e.Val != nil }

// JoinClause JOIN 子句（INNER / LEFT，支持子查询右表）。
type JoinClause struct {
	Type     string // "INNER" | "LEFT"
	Table    string // 右表名（与 Subquery 二选一）
	Alias    string // 右表别名（空 = 表名）
	Subquery *SelectStmt
	On       []Cond // ON 条件（AND 组合）
}

// CTE 公共表表达式（M10）：WITH name AS (SELECT ...)。
type CTE struct {
	Name   string
	Select *SelectStmt
}

// WindowSpec 窗口函数规格（M10）：OVER(PARTITION BY ... ORDER BY ... [frame])。
// 无显式框架，默认为 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW。
type WindowSpec struct {
	PartitionBy []string     // 分区列（t.col / col）
	OrderBy     *OrderBy     // 窗口内排序（可为 nil；首列，保持既有单列路径兼容）
	OrderBys    []OrderBy    // T39：窗口内多列 ORDER BY（首列同步写入 OrderBy；逗号分隔任意 ASC/DESC 组合）
	Frame       *WindowFrame // P0-5：显式窗口帧（nil = 默认 ROWS UNBOUNDED PRECEDING..CURRENT ROW）
}

// WindowFrameBound 窗口帧边界（P0-5）。
// Kind 取值："UNBOUNDED_PRECEDING" | "PRECEDING" | "CURRENT_ROW" | "FOLLOWING" | "UNBOUNDED_FOLLOWING"。
// Offset 仅 PRECEDING / FOLLOWING 使用（n PRECEDING / n FOLLOWING）。
type WindowFrameBound struct {
	Kind   string
	Offset int64
}

// WindowFrame 显式窗口帧（P0-5）：ROWS 或 RANGE 帧。
// Start/End 均非空；单边界简写（如 ROWS UNBOUNDED PRECEDING）在解析时补齐另一端。
type WindowFrame struct {
	Type  string // "ROWS" | "RANGE"
	Start *WindowFrameBound
	End   *WindowFrameBound
}

// SelectStmt SELECT。
type SelectStmt struct {
	Columns   []SelectColumn
	Distinct  bool        // SELECT DISTINCT（T40）：投影行去重，先于 ORDER BY/LIMIT
	CTEs      []CTE       // M10 WITH 子句（可为空）
	From      string      // 表名（无 JOIN/子查询时）
	FromAlias string      // FROM 表别名（空 = 表名）
	Subquery  *SelectStmt // FROM 子查询
	SubAlias  string      // FROM 子查询别名（必填）
	Joins     []JoinClause
	Where     *Where
	Having    *Where   // P0-4：HAVING 过滤条件（AND 组合，作用于分组/聚合结果）
	GroupBy   []string // GROUP BY 列（t.col / col）
	OrderBy   *OrderBy
	OrderBys  []OrderBy // M18-C：多列 ORDER BY（首列同步写入 OrderBy 保持既有单列路径兼容）
	Limit     int       // <=0 不限
	Offset    int       // <=0 不偏移（M12-2 分页）
	ForUpdate bool      // FOR UPDATE（MySQL TPC-C 语句语法；无行锁语义，仅吞掉保证可解析）
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
	Col  string
	Val  Value // 字面量赋值（col = 1 / col = 'x' / col = ?）
	Expr *Expr // 表达式赋值（col = col + 1 / col = a * b - 1），非空时优先于 Val
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

// CreateViewStmt CREATE VIEW <name> AS <SELECT>（M9 视图）。
type CreateViewStmt struct {
	Name        string
	Select      *SelectStmt
	IfNotExists bool // T41：CREATE VIEW IF NOT EXISTS（视图已存在时静默跳过）
}

func (CreateViewStmt) stmt() {}

// DropViewStmt DROP VIEW [IF EXISTS] <name>。
type DropViewStmt struct {
	Name     string
	IfExists bool
}

func (DropViewStmt) stmt() {}

// ShowViewsStmt SHOW VIEWS。
type ShowViewsStmt struct{}

func (ShowViewsStmt) stmt() {}

// CreateMatViewStmt CREATE MATERIALIZED VIEW <name> AS <SELECT>（M15 物化视图）。
// 物化视图本质是一张"影子表"：定义以 SelectStmt 持久化于 m:matviews，
// SELECT 直接读同名影子表，REFRESH 全量重算写回。
type CreateMatViewStmt struct {
	Name        string
	Select      *SelectStmt
	IfNotExists bool // T41：CREATE MATERIALIZED VIEW IF NOT EXISTS（已存在时静默跳过）
}

func (CreateMatViewStmt) stmt() {}

// RefreshMatViewStmt REFRESH MATERIALIZED VIEW <name> [CONCURRENTLY]（M15）。
// Concurrently 仅做语法兼容，语义上全量重算（与 PostgreSQL 的并发刷新等价结果）。
type RefreshMatViewStmt struct {
	Name         string
	Concurrently bool
}

func (RefreshMatViewStmt) stmt() {}

// DropMatViewStmt DROP MATERIALIZED VIEW [IF EXISTS] <name>（M15）。
type DropMatViewStmt struct {
	Name     string
	IfExists bool
}

func (DropMatViewStmt) stmt() {}

// CreateTriggerStmt CREATE TRIGGER <name> BEFORE|AFTER INSERT|UPDATE|DELETE
// ON <table> FOR EACH ROW <body>（M15-2 触发器）。
// 触发器体为简单 SQL 语句（UPDATE 条件更新 / INSERT 单行插入），以原始 SQL 文本
// 持久化，DML 触发时重新解析执行。FOR EACH ROW 为语法兼容：体不支持 NEW/OLD
// 引用，触发粒度为语句级（受影响行数 > 0 时触发）。
type CreateTriggerStmt struct {
	Name        string
	Timing      string // "BEFORE" | "AFTER"
	Event       string // "INSERT" | "UPDATE" | "DELETE"
	Table       string // 源表
	Body        string // 触发器体原始 SQL 文本
	IfNotExists bool   // T41：CREATE TRIGGER IF NOT EXISTS（触发器已存在时静默跳过）
}

func (CreateTriggerStmt) stmt() {}

// DropTriggerStmt DROP TRIGGER [IF EXISTS] <name>（M15-2）。
type DropTriggerStmt struct {
	Name     string
	IfExists bool
}

func (DropTriggerStmt) stmt() {}

// ShowTriggersStmt SHOW TRIGGERS [FROM <table>]（M15-2）。
// Table 为空表示列出全部触发器。
type ShowTriggersStmt struct {
	Table string
}

func (ShowTriggersStmt) stmt() {}

// ParamMeta 存储过程参数定义（M15-3 / P1-3）。参数类型仅用于元数据展示，
// 调用时不强制类型检查（参数值按字面量代入过程体）。
// Mode 为参数方向："IN"（默认，可省略）、"OUT"（输出，实参须为 @var）、
// "INOUT"（双向，实参须为 @var）；空串等价于 IN。
type ParamMeta struct {
	Name string `json:"name"`
	Type string `json:"type"` // "INT" | "TEXT"
	Mode string `json:"mode,omitempty"` // "IN" | "OUT" | "INOUT"；空=IN
}

// ProcedureMeta 存储过程元数据（M15-3）：名称、IN 参数列表、过程体原文。
// 持久化于独立目录键 m:procedures，与表/视图/物化视图/触发器命名空间互相独立。
type ProcedureMeta struct {
	Name   string      `json:"name"`
	Params []ParamMeta `json:"params,omitempty"`
	Body   string      `json:"body"`
}

// CreateProcedureStmt CREATE PROCEDURE <name> [( [IN] <param> <TYPE> [, ...] )]
// BEGIN <stmt>; [<stmt>; ...] END（M15-3 存储过程）。
// 过程体为一条或多条受支持语句，以原始 SQL 文本持久化，CALL 时逐条解析执行；
// IN 关键字可省略（参数默认 IN 输入参数）。
type CreateProcedureStmt struct {
	Name        string
	Params      []ParamMeta
	Body        string
	IfNotExists bool // T41：CREATE PROCEDURE IF NOT EXISTS（存储过程已存在时静默跳过）
}

func (CreateProcedureStmt) stmt() {}

// DropProcedureStmt DROP PROCEDURE [IF EXISTS] <name>（M15-3）。
type DropProcedureStmt struct {
	Name     string
	IfExists bool
}

func (DropProcedureStmt) stmt() {}

// CallStmt CALL <name> [( <expr> [, <expr> ...] )]（M15-3 / P0-2）。
// Args 为实参表达式列表（支持字面量、@var 引用及算术表达式，如 CALL p(@n - 1)），
// 执行时求值为 Value 后与过程定义参数一一对应代入。
type CallStmt struct {
	Name string
	Args []*Expr
}

func (CallStmt) stmt() {}

// ShowProceduresStmt SHOW PROCEDURES（M15-3）。
type ShowProceduresStmt struct{}

func (ShowProceduresStmt) stmt() {}

// BeginStmt 开启会话级显式事务（BEGIN）。
type BeginStmt struct{}

func (BeginStmt) stmt() {}

// CommitStmt 提交会话级显式事务（COMMIT）。
type CommitStmt struct{}

func (CommitStmt) stmt() {}

// RollbackStmt 回滚会话级显式事务（ROLLBACK）。
type RollbackStmt struct{}

func (RollbackStmt) stmt() {}

// ExportStmt 导出表数据（EXPORT TABLE t [(cols)] TO 'path' [FORMAT CSV|JSON|PARQUET]）。
type ExportStmt struct {
	Table   string
	Columns []string // 空 = 全部列按定义序
	Path    string
	Format  string // ""=CSV（缺省）；"JSON" / "PARQUET"（M19）
}

func (ExportStmt) stmt() {}

// PrepareStmt PREPARE <name> FROM '<sql-text>'（M18 prepared statement）。
type PrepareStmt struct {
	Name string
	SQL  string
}

func (PrepareStmt) stmt() {}

// ExecuteStmt EXECUTE <name> [USING @a, @b]（M18 prepared statement）。
type ExecuteStmt struct {
	Name string
	Args []string // 用户变量名（含 @）或字面量文本
}

func (ExecuteStmt) stmt() {}

// DeallocateStmt DEALLOCATE PREPARE <name>（M18 prepared statement）。
type DeallocateStmt struct {
	Name string
}

func (DeallocateStmt) stmt() {}

// ImportStmt 从 CSV 导入建行（IMPORT INTO t FROM 'path' [BATCH n] [IGNORE ERRORS]）。
// Batch>0 时逐批提交（每 n 行一批）；IgnoreErrors 时坏行计数跳过不整体回滚。
// Concurrency>1（仅 MYSQLDUMP + BATCH 模式，P2-12）时启用表级并行导入：
// 每张表独立会话/独立事务并发灌入，保持 BATCH 的"部分完成可恢复"语义。
//
// P2-13 mysqldump 全选项扩展（仅 Format=MYSQLDUMP 生效）：
// NoData=跳过数据行；NoCreateInfo=不建表（目标库表须已存在）；CompleteInsert/
// ExtendedInsert=兼容 --complete-insert/--extended-insert 输出形态（解析层原生支持）；
// InsertIgnore=主键冲突忽略（幂等默认语义）；Replace=REPLACE INTO 覆盖语义；
// HexBlob=BLOB 列按十六进制字面量解析；SetNamesCharset=SET NAMES 识别字符集
// （数据按 UTF-8 处理）；DropTables=DROP TABLE IF EXISTS 先删后建（默认 true，NO DROP TABLES 关闭）。
type ImportStmt struct {
	Format       string // ""=CSV（IMPORT INTO t FROM 'path'）；"MYSQLDUMP"=mysqldump 兼容导入
	Table        string
	Path         string
	Batch        int
	IgnoreErrors bool
	Concurrency  int // P2-12：mysqldump 表级并行导入并发度（0/1=串行）

	// P2-13：mysqldump 全选项（仅 MYSQLDUMP）
	NoData          bool
	NoCreateInfo    bool
	CompleteInsert  bool
	ExtendedInsert  bool
	InsertIgnore    bool
	Replace         bool
	HexBlob         bool
	SetNamesCharset string
	DropTables      bool // 默认 true（parser 初始化；NO DROP TABLES 关闭）
}

func (ImportStmt) stmt() {}

// ---- P2：运维语句 ----

// CreateDatabaseStmt CREATE DATABASE [IF NOT EXISTS] <name>（T39 多库元数据）。
// 仓库为单库模型（默认库 default 恒存在），新增库仅登记数据库目录，不影响表目录。
type CreateDatabaseStmt struct {
	Name        string
	IfNotExists bool // 库已存在时静默跳过
}

func (CreateDatabaseStmt) stmt() {}

// DropDatabaseStmt DROP DATABASE [IF EXISTS] <name>（T39）：级联删除库内所有表。
type DropDatabaseStmt struct {
	Name     string
	IfExists bool // 库不存在时静默成功
}

func (DropDatabaseStmt) stmt() {}

// UseStmt USE <db>（T39）：切换会话级默认数据库。
type UseStmt struct {
	Name string
}

func (UseStmt) stmt() {}

// ShowDatabasesStmt SHOW DATABASES：列出全部数据库（默认库 default + 已创建库）。
type ShowDatabasesStmt struct{}

func (ShowDatabasesStmt) stmt() {}

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

// SlowQueryRecord 慢查询记录（内存保留最近 slowQueryMax 条，可落盘 data/slow.log）。
// M13：记录执行耗时超过阈值的 SQL，含用户、开始时间、耗时、SQL 文本、结果行数/错误。
type SlowQueryRecord struct {
	SQL      string // SQL 原文
	User     string // 执行用户（root / AUTH 会话用户；guest 为空串）
	StartAt  string // 开始时间 YYYY-MM-DD HH:MM:SS
	Duration int64  // 耗时（毫秒）
	Rows     int64  // 结果行数（SELECT 返回行数 / DML 影响行数；执行失败为 0）
	Err      string // 错误信息（执行成功为空串；慢且失败同样记录）
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

func (ShowStatsStmt) stmt() {}

// ShowConfigStmt SHOW CONFIG / SHOW PERSISTED CONFIG（M16-1 在线配置热更新）：
// 前者列出当前生效配置（含来源 default/persisted/session），后者仅列出
// 已持久化到配置文件的配置项（重启后仍生效的部分）。
type ShowConfigStmt struct {
	Persisted bool // true = SHOW PERSISTED CONFIG
}

func (ShowConfigStmt) stmt() {}

// ShowBackupStatusStmt SHOW BACKUP STATUS（BR 方向②）：输出备份调度配置
// （周期/保留份数/目录/远程目标）与最近执行状态（下次执行/最近结果/错误）。
type ShowBackupStatusStmt struct{}

func (ShowBackupStatusStmt) stmt() {}

// ShowAlertsStmt SHOW ALERTS（日志告警体系）：列出最近告警记录
// （时间/级别/模块/事件/消息，最多 alert_keep 条）。
type ShowAlertsStmt struct{}

func (ShowAlertsStmt) stmt() {}

// ShowAlertStatusStmt SHOW ALERT STATUS（日志告警体系）：查看告警开关与出口
// 配置（enabled/file/webhook/keep）。
type ShowAlertStatusStmt struct{}

func (ShowAlertStatusStmt) stmt() {}

// ShowAlertRulesStmt SHOW ALERT RULES（T37 监控告警）：列出告警规则与运行时
// 状态（id/metric/op/threshold/duration_sec/severity/enabled/state/last_value）。
type ShowAlertRulesStmt struct{}

func (ShowAlertRulesStmt) stmt() {}

// ShowBinlogEventsStmt SHOW BINLOG EVENTS [FROM <lsn>] [LIMIT <n>]（M14 CDC）：
// 只读展示 binlog 增量变更流；From 为起始位点（nil=从头），Limit<=0=不限。
type ShowBinlogEventsStmt struct {
	From  *uint64
	Limit int
}

func (ShowBinlogEventsStmt) stmt() {}

// ShowMasterStatusStmt SHOW MASTER STATUS（v1.6.0 P1-2 复制协议位点入口）：
// 返回 binlog 文件名与当前写位点（最后 LSN+1），供 MySQL 复制/订阅工具取位点。
type ShowMasterStatusStmt struct{}

func (ShowMasterStatusStmt) stmt() {}

// SetStmt 会话变量设置：SET [PERSIST] <name> = <value>（M9 查询缓存开关等；
// M16-1 起支持 PERSIST：应用后同时写入持久化配置文件，重启后仍生效）。
// M18-C：SET @var = value 为用户变量赋值（Var 非空时忽略 Name/Value 走 userVars）。
type SetStmt struct {
	Name    string
	Value   string
	Persist bool   // M16-1：true 表示 SET PERSIST <key> = <value>
	Var     string // M18-C：用户变量名（含 @），非空表示用户变量赋值
	VarVal  Value  // M18-C：用户变量值
	// P2-13：SET [SESSION] TRANSACTION ISOLATION LEVEL <level>。
	IsoSet   bool     // true 表示事务隔离级别设置语句（此时忽略 Name/Value/Var）
	IsoScope string   // "SESSION"（会话级默认）或 ""（语句级，仅影响下一事务）
	IsoLevel IsoLevel // 目标隔离级别
}

func (SetStmt) stmt() {}

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
// BackupStmt BACKUP TO '<path>' [CONCURRENCY <k>]：
// CONCURRENCY k（k>=1）指定备份数据段分片并行扫描的并发度；
// 未指定时使用默认自动并发度（min(GOMAXPROCS, 4)）。
type BackupStmt struct {
	Path        string
	Concurrency int
}

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
	ColTypes     []string // 平行于 Columns：列类型 "INT"/"DECIMAL"/"TEXT"/"BLOB"/"DATE"；空串=未知（协议层默认 VarString）
	ColNullable  []bool   // 平行于 Columns：true=列可空（聚合空结果集等）；nil=全部按不可空处理
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
