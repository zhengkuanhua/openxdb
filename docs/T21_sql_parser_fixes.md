# T21 SQL 解析器缺陷修复（DROP TABLE IF EXISTS / 行内 PRIMARY KEY）

> 版本：v5.0-P10-SQLFIX, v0.12.0-alpha
> 日期：2026-10-10
> 范围：`pkg/sql`（lexer / ast / parser / executor）+ 单元测试 + README 勾选
> 闭环：`go build ./...` 与 `go test ./... -count=1` 全绿，不 push

## 背景与缺陷

端到端冒烟测试暴露 SQL 解析器两个语法缺陷：

1. **`DROP TABLE IF EXISTS` 无法解析**
   `IF` / `EXISTS` 不在 lexer 关键字表，`DROP TABLE` 后直接按 `tokIdent` 取表名，
   把 `IF` 当作表名，剩余 `EXISTS` 无法消费，报 `unexpected token "EXISTS"`；
   即使 `DROP TABLE` 目标表不存在也会报错，不具备幂等删除语义。

2. **建表行内主键 `id INT PRIMARY KEY` 无法解析**
   列定义循环在列类型后只处理 `,` 与 `)` 两种 token，遇到 `PRIMARY` 关键字
   走默认分支报 `expected , or ) at 27, got "PRIMARY"`；
   此前仅支持表级写法 `PRIMARY KEY (col)`（表头列列表之后）。

## 修复方案

按 lexer → ast → parser → executor 顺序增量修改，每步 `go build ./pkg/sql/` 校验。

### 1. lexer.go

关键字表补充 `IF` 与 `EXISTS`（大小写不敏感，lexIdent 统一转大写判定）：

```go
"CREATE": true, "TABLE": true, "DROP": true, "IF": true, "EXISTS": true, "INSERT": true,
```

### 2. ast.go

`DropTableStmt` 增加 `IfExists` 标志：

```go
type DropTableStmt struct {
	Name     string
	IfExists bool // DROP TABLE IF EXISTS：表不存在时静默成功
}
```

### 3. parser.go

**parseDrop**：`DROP TABLE [IF EXISTS] <name>`，解析 `IF EXISTS` 后置位标志再取表名；
不带 `IF EXISTS` 时行为与原来完全一致。

**parseCreate 列定义循环**：`default` 分支识别行内 `PRIMARY KEY`，
将该列直接设为主键（`stmt.PK = col.text`），与表级 `PRIMARY KEY (col)` 等价；
支持行内主键位于任意列（首列 / 中间列 / 末尾列）：

- 主键后遇 `,` → 继续解析下一列（仍兼容后续表级 `PRIMARY KEY (col)`）；
- 主键后遇 `)` → 表定义结束，直接返回；
- 主键后遇其他 token → 维持原有 `expected , or )` 报错。

### 4. executor.go

`execDropTable`：目录中找不到目标表时，若 `IfExists` 为真返回空结果
（`AffectedRows: 0`，静默成功），否则维持原有 `table not exists` 报错。

## 测试

`pkg/sql/sql_test.go` 新增：

- `TestDropTableIfExists`
  - 表存在：`DROP TABLE IF EXISTS` 正常删除（AffectedRows = 3）；
  - 表不存在：静默成功（AffectedRows = 0）；
  - 回归：不带 `IF EXISTS` 且表不存在仍报 `table not exists`。
- `TestCreateInlinePrimaryKey`
  - 首列行内主键建表 + 主键点查；
  - 主键唯一约束生效（重复插入报 `duplicate primary key`）；
  - 末尾列行内主键建表 + 点查；
  - 行内主键与表级主键同列共存不冲突。

## 验证结果

| 命令 | 结果 |
|---|---|
| `go build ./pkg/sql/` | 通过 |
| `go test ./pkg/sql/ -count=1` | ok（含新增 2 用例，全量无回归） |
| `go build ./...` | 通过 |
| `go test ./... -count=1` | 全绿 |

## 修改文件清单

| 文件 | 变更 |
|---|---|
| `pkg/sql/lexer.go` | 关键字表新增 IF / EXISTS |
| `pkg/sql/ast.go` | DropTableStmt 新增 IfExists 字段 |
| `pkg/sql/parser.go` | parseDrop 支持 IF EXISTS；parseCreate 支持行内 PRIMARY KEY |
| `pkg/sql/executor.go` | execDropTable 支持 IfExists 静默成功 |
| `pkg/sql/sql_test.go` | 新增 TestDropTableIfExists / TestCreateInlinePrimaryKey |
| `README.md` | Status / Development log / Roadmap 勾选 |
