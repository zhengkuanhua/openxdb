# T22 M9 功能簇（5/5）：视图

> 版本：v5.0-P11-M9VIEW, v0.13.0-alpha
> 日期：2026-10-10
> 范围：`pkg/sql`（新增 `view.go` + lexer / ast / parser / executor）+ 单元测试（`m9_view_test.go`）+ README 勾选
> 闭环：`go build ./...` 与 `go test ./... -count=1` 全绿，不 push
> 提交：`M9: view (CREATE/DROP VIEW + SHOW VIEWS, definition expansion, cycle guard)`（本地，未 push）

## 背景

SQL 层只有物理表可查询，复杂查询（过滤、排序、多表 JOIN）需每次重复书写。
M9 视图把**命名查询定义**持久化：`CREATE VIEW` 保存展开式 SELECT 定义，
`SELECT ... FROM view` 时**定义展开执行**（视图即宏，实时反映底层数据变更，
无物化快照）；`DROP VIEW` 与 `SHOW VIEWS` 提供生命周期管理与盘点；
循环引用（自引用 / 间接环）在创建期静态拦截，执行期另有栈式防护兜底。

## 设计

### 语法

```sql
CREATE VIEW name AS SELECT ...;   -- 定义须引用已存在的表或视图
DROP VIEW [IF EXISTS] name;       -- 被其它视图引用时拒绝（RESTRICT）
SHOW VIEWS;                       -- 列出视图名 + 定义摘要
```

### 词法 / 语法层

- `lexer.go`：关键字表补充 `VIEW` / `VIEWS`；
- `ast.go`：新增 `CreateViewStmt{Name, Select}` / `DropViewStmt{Name, IfExists}`
  / `ShowViewsStmt`；
- `parser.go`：`parseCreate` 增加 `VIEW` 分支（`parseCreateView`：解析视图名
  与 `AS SELECT` 子句并断言为 `SelectStmt`）；`parseDrop` 增加 `VIEW` 分支
  （支持 `IF EXISTS`）；`parseShow` 增加 `VIEWS` 分支。

### 元数据层（view.go）

- `ViewMeta{Name string, Select *SelectStmt}` 视图定义随
  `viewsKey`（全局键，同表元数据模式）持久化；
- `loadViews` / `saveViews` 读写全部视图元数据；`findView` 按名查找；
- `viewRefNames` 提取定义中引用的全部表 / 视图名（From + JOIN 左右表）；
- `checkViewCycle` 静态循环防护：DFS 沿已有视图引用链展开，若到达
  新视图名自身即判定 `cyclic view reference`（直接自引用与经
  v1→v2→v1 的间接环均拦截）；
- `viewSummary` 生成 SHOW VIEWS 定义摘要。

### 执行层（executor）

- `execCreateView`：依次校验 ① 与表重名（`view name conflicts with table`）
  ② 重名视图（`view already exists`）③ 循环引用（先于存在性检查，
  保证自引用报循环错误）④ 引用名字存在性（`table not exists`），
  通过后持久化；
- `execDropView`：RESTRICT——被其它视图引用的视图禁止删除
  （`referenced by view`）；`IF EXISTS` 下不存在视图静默成功，否则报错；
- `execShowViews`：输出 `view` / `definition` 两列；
- 定义展开：`execSelect` 入口调用 `expandViews`——递归扫描 FROM /
  JOIN 中的视图名，替换为 `Subquery`（深拷贝定义，防元数据被修改），
  交由高级 SELECT 关系执行路径运行；嵌套视图（视图引用视图）逐层展开；
- 运行期防护：`cyclicView` 借助展开栈记录，异常路径下兜底拒绝循环展开
  避免死循环（静态防护为主，运行期为第二道防线）。

## 测试（pkg/sql/m9_view_test.go）

| 测试 | 覆盖 |
|---|---|
| `TestCreateViewBasic` | CREATE VIEW + SELECT FROM view 基本查询 |
| `TestCreateViewWithFilterAndOrder` | 视图定义含 WHERE / ORDER BY |
| `TestCreateViewWithJoin` | 视图定义含 JOIN 多表 |
| `TestCreateViewQualifiedQuery` | `SELECT vw.col FROM vw WHERE vw.id=...` 限定列查询 |
| `TestCreateViewDuplicate` | 重名视图报错 |
| `TestCreateViewNameConflictsTable` | 与表重名报错 |
| `TestCreateViewMissingTable` | 定义引用不存在表报错 |
| `TestCreateViewSelfCycle` | 自引用报 `cyclic view reference` |
| `TestCreateViewIndirectCycle` | v1→v2→v1 间接环：DROP v1 被 RESTRICT 拒绝 |
| `TestViewOverView` | 视图引用视图逐层展开查询 |
| `TestDropView` | DROP 后查询报错、可重建同名视图 |
| `TestDropViewIfExists` | IF EXISTS 静默成功 / 无 IF 报错 |
| `TestDropViewReferencedRestrict` | 被引用视图禁止删除 |
| `TestShowViews` | 列名与视图清单 + 定义摘要非空 |
| `TestViewReflectsUnderlyingChanges` | 展开执行：底层数据变更实时反映 |

## 取舍说明

- 视图是**展开式宏**而非物化表：每次查询实时执行底层 SQL，保证数据
  新鲜、无快照一致性负担；代价是复杂视图每次全量执行；
- 循环防护以**创建期静态检查**为主（用户可即时感知错误），运行期
  栈式防护兜底防止元数据被外部篡改后死循环；
- `DROP VIEW` 采用 RESTRICT 语义：被引用的视图不可删，须先删引用方；
- 视图不支持写操作（INSERT / UPDATE / DELETE 视图），后续可扩展
  可更新视图。

## README 勾选

- Status 行更新为 `M9 view: CREATE/DROP VIEW + SHOW VIEWS, definition expansion, cycle guard (v5.0-P11-M9VIEW), v0.13.0-alpha.`
- T 列表追加 `+ T22 M9: view (CREATE/DROP VIEW + SHOW VIEWS, definition expansion, cycle guard)`
