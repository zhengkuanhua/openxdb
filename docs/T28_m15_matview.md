---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_15c52666c57711f19063525400393707
    ReservedCode1: jgZEH9JNVXNTUwsgxsVY9hFvVNeBKy+nEyqJFGFDzcdbJcsC78SJkgzGW0pNvQRaW66nFW9HILRMAmAHoExTYNzV1xl7+wUn5evtiGh9NtVFWr+KnDH039sul74VtYecPRC0XlFz5o84EkX3OjqLuu1Rf+oB6MUEqnDQJ9DmwzECB2yC7OlmS1NEkVY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_15c52666c57711f19063525400393707
    ReservedCode2: jgZEH9JNVXNTUwsgxsVY9hFvVNeBKy+nEyqJFGFDzcdbJcsC78SJkgzGW0pNvQRaW66nFW9HILRMAmAHoExTYNzV1xl7+wUn5evtiGh9NtVFWr+KnDH039sul74VtYecPRC0XlFz5o84EkX3OjqLuu1Rf+oB6MUEqnDQJ9DmwzECB2yC7OlmS1NEkVY=
---

# T28 M15 SQL 功能增强簇（1/3）：物化视图

> 版本：v5.0-P16-M15, v0.18.0-alpha
> 日期：2026-10-12
> 范围：`pkg/sql`（ast.go 新增 CreateMatViewStmt/RefreshMatViewStmt/DropMatViewStmt、parser.go 新增 parseCreateMatView/parseRefreshMatView/parseDropMatView 与 CREATE/DROP 分发、lexer.go 新增 MATERIALIZED/REFRESH/CONCURRENTLY 关键字、executor.go 新增三个执行器分发与 visibleColumns/visibleRow 隐藏列过滤、新增 matview.go 与 matview_test.go）、`README.md`（Status 行同步 M15/v0.18.0-alpha/T1–T28 complete，Roadmap 新增 M15 行并勾选 T28）
> 闭环：CGO 环境下 `go build ./...` + `go test -count=1 ./pkg/sql/` 全绿（含 17 个 M15 物化视图用例），不 push
> 提交：`M15: materialized view`（本地，未 push）

## 背景
M15 SQL 功能增强簇按 Roadmap 依次落地三个子功能：物化视图（本批次）、触发器、存储过程。物化视图在现有**虚拟视图**机制上做增量扩展：虚拟视图是"每次查询即时展开定义"的轻量视图；物化视图则是"创建时执行 SELECT 并把结果**落盘为同名影子表**，查询直接读影子表快照，仅在显式 REFRESH 时全量重算"的快照视图，不随源表 DML 自动更新。两者可共存、可互相引用，命名空间统一隔离。

## 设计

### 存储层：独立目录 `m:matviews` + 同名影子表

- **元数据** `MatViewMeta{Name, Select, ShadowID}`：以 JSON 序列化持久化于独立目录 `m:matviews`（新增 `matViewsKey`），**不写入** `m:views`，因此 `expandViewsInner`（仅展开虚拟视图）不会展开物化视图名，SELECT 物化视图名时走普通表路径命中影子表；
- **影子表**：创建时在 `m:tables` 注册与物化视图同名的 `TableMeta`（含 ID），列由 SELECT 结果推断，末位追加内部隐藏主键列 `__mv_rid INT`（`mvHiddenPK`）。隐藏列对用户不可见：`SELECT *` 展开、`relFromScan` 等路径统一走 `visibleColumns` / `visibleRow` 过滤 `__mv_` 前缀列；列名保留 `__mv_` 前缀会被拒绝（`reserved column name`）；
- **原子创建**：单语句自动提交时包一层事务（`e.beginTx`/`commitTx`），影子表注册 + 快照落盘 + 元数据持久化要么全部生效、要么全部回滚。

### 执行器

| 语句 | 行为 |
|---|---|
| `CREATE MATERIALIZED VIEW <name> AS <SELECT>` | 校验名称非空、不与表/虚拟视图/物化视图重名、定义不自引用、引用对象存在 → 事务内 `execSelect` 取快照 → 推断 schema → 注册影子表 → 落盘 → 持久化元数据 |
| `REFRESH MATERIALIZED VIEW <name> [CONCURRENTLY]` | v1.8.0 前为全量重算：事务内重算 → 若列集/类型变化则重建影子表 schema（保留 ID/PK）→ 差异删除残留行 + 全量写回新快照 → 提交；重算失败（源表被 DROP/改列）整体回滚，旧快照保持可读。v1.9.0 起：普通 REFRESH 优先走**增量刷新**（仅重算基表变更键，无变更幂等跳过）；`CONCURRENTLY` 语义化为**并行刷新 + 快照交换**（事务内重算新快照，期间并发读仍服务旧影子表，提交后原子切换新快照）；依赖链按依赖序递归刷新（依赖 MV 先刷） |
| `DROP MATERIALIZED VIEW [IF EXISTS] <name>` | RESTRICT 语义：被虚拟视图或其它物化视图引用时拒绝；否则清空影子表行、从 `m:tables` 移除影子表、从 `m:matviews` 移除元数据，同名表/视图可重建 |

### 类型推断与归一化（`inferMatViewSchema`）

- 空结果集列定义全部 `TEXT`（REFRESH 后有数据时 schema 自动更新为实际类型）；
- 有数据时按列统计实际值 Kind：全 `INT`→`INT`；`INT+DECIMAL`→`DECIMAL`；全 `DATE`→`DATE`；全 `BLOB`→`BLOB`；`INT/BLOB/TEXT` 任意组合→`TEXT`（字符串化）；`DATE/DECIMAL` 与其它类型混合不支持并报错；
- 归一化写入时 `TEXT` 列统一 `StrVal(v.String())`，其余类型走 `coerceValue` 校验/转换。

### 列名归一化（`matViewColumnNames`）

JOIN 等场景结果列名可能带表/别名限定前缀（如 `users.name`、`orders.amount`），落盘列名去掉前缀与普通表一致；去前缀后重名则保留完整限定名避免歧义；空列名报 `column name missing`，`__mv_` 前缀报 `reserved column name`。

### 刷新写回与存储层 WriteBatch 语义

存储层 WriteBatch 按「先 puts 后 deletes」应用，同一 key 同批先 Delete 后 Put 时最终 Delete 生效，因此**不能**采用「先清空再全量写回」。`refreshMatViewRows` 改为：先构造新快照 key 集合，仅删除旧快照中不在新集合的残留行，再全量 Put 新快照（与 Delete 无同 key 冲突），避免批内覆盖失效。

### 与虚拟视图共存

- 命名冲突：物化视图与表、虚拟视图、已有物化视图互斥；
- 互相引用：物化视图可基于虚拟视图（`CREATE MATERIALIZED VIEW x AS SELECT * FROM v`），虚拟视图也可基于物化视图；虚拟视图即时反映源表，物化视图保持快照，语义各自独立；
- RESTRICT 删除：被虚拟视图或其它物化视图引用时，`DROP MATERIALIZED VIEW` 报 `depends on it` 拒绝（先删依赖方可删除）。

### v1.9.0 增量刷新（`matview_incremental.go`）

#### 基表变更跟踪

- 独立目录 `m:mv_dirty`（`mvDirtyKey`）持久化 `mvDirty{Ver uint64, Overflow bool, Rows []string}`：`Ver` 为全局变更版本（每个写事务 `recordMVDirtyKeys` 自增），`Rows` 为受影响主键 hex 键集，`Overflow` 表示写量超阈值（并发写聚合或批量插入超过单事务上限）时置位并清空 Rows，后续刷新走全量兜底；
- `execInsert` / `execUpdate` / `execDelete` 在数据写路径末尾挂变更跟踪钩子：解析物化视图依赖集合（`buildMVDependencySet`，创建时登记）与基表匹配，命中则记录受影响主键键；`execDropTable` / `execAlterTable` 对依赖该表的物化视图标记 Overflow（结构变更无法定位行级键）；
- 主键键集编码与影子表隐藏 PK 完全一致（`pkBytes`），保证增量定位与全量写回互不冲突。

#### 增量合并（Simple 行级）

- 定义满足 `simpleMatViewInfoOf`（单基表、无 JOIN/聚合/子查询/CTE、纯列投影含主键、基表主键 INT/DECIMAL/TEXT）时走增量：逐 dirty 主键重跑「定义 SELECT + 主键等值条件」，命中则按主键定位影子表行键 Put 新值，未命中（行已删/被 WHERE 过滤）则 Delete 影子表行；只重算变更键，AffectedRows = 变更键数；
- 依赖 MV / 虚拟视图 / 非 Simple 定义 / `Overflow` 置位一律走全量路径，语义不破坏。

#### CONCURRENTLY 并行刷新与快照交换

- `REFRESH ... CONCURRENTLY` 不再全量语义：强制走全量重建路径，在单个写事务内重算新快照 → 差异写回影子表 → 提交；事务提交前并发读（其它会话/语句）仍读旧影子表快照，提交后原子读到新快照，读不阻塞；
- 全量路径挂载会话透传事务（`Session.forcedTx` + `getTx`/`autocommit` 感知）：重算 SELECT 复用当前刷新事务，使依赖 MV 在同一事务内已写入的增量对重算可见（`execSelect` 新建临时事务会读到旧快照，导致依赖链结果不一致）。

#### 多物化视图依赖管理

- 创建时解析引用关系：`MatViewMeta.Deps`（依赖基表集合，经虚拟视图/物化视图定义递归展开）与 `DepMVs`（直接依赖的物化视图列表）；Executor 维护 `mvDepMu` / `mvDepSet` 依赖缓存供 DML 变更跟踪；
- 刷新顺序：`refreshMatViewRec` 深度优先先刷新 `DepMVs`（其自身依赖递归处理），再判定当前 MV 失效（`maxVer` = max(依赖基表 dirty.Ver, 依赖 MV 刷新后 Version) > 自身 Version 才刷新），整体单事务提交，任一环失败整体回滚；
- 失效传播：基表 DML 标记直接/间接依赖的物化视图为 stale；`DROP MATERIALIZED VIEW` 清空其 dirty 记录并重建依赖缓存。

## 测试清单（v1.9.0 增量刷新）

| 用例 | 覆盖点 |
|---|---|
| `TestM15CreateAndQueryMatView` | 创建/查询/SELECT * 隐藏列不可见/SHOW TABLES 可见影子表 |
| `TestM15MatViewSnapshotNotAutoUpdated` | 源表 INSERT 后快照不自动更新，REFRESH 后更新 |
| `TestM15RefreshConcurrentlySyntax` | `REFRESH MATERIALIZED VIEW CONCURRENTLY` 语法兼容 |
| `TestM15RefreshFailsAfterSourceDrop` | 源表 DROP 后 REFRESH 失败，旧快照仍可读 |
| `TestM15RefreshFailsAfterSourceAlter` | 源表删列后 REFRESH 失败，旧快照仍可读 |
| `TestM15DropMatView` | DROP / IF EXISTS / 同名表可重建 |
| `TestM15NameConflictsAndCoexist` | 与表/虚拟视图命名冲突、互相引用、虚拟视图即时 vs 物化快照 |
| `TestM15MatViewAggregate` | GROUP BY 聚合快照与 REFRESH |
| `TestM15MatViewDecimal` / `TestM15MatViewBlob` | DECIMAL / BLOB 列类型推断与读写 |
| `TestM15SelfReferenceAndDependencies` | 自引用/引用不存在对象报错、级联依赖删除保护 |
| `TestM15ReservedColumnName` | `__mv_` 保留列名拒绝 |
| `TestM15MatViewSchemaEvolution` | 源表加列不影响快照列集；删定义列后 REFRESH 失败保留旧快照 |
| `TestM15MatViewSelectWithLimit` / `TestM15MatViewJoin` | LIMIT / JOIN 定义落盘 |
| `TestM19MatViewIncrementalRefresh` | 增量刷新只重算变更键（AffectedRows=变更键数）；无变更幂等跳过；单行 UPDATE/DELETE 只重算 1 行；数据正确性 |
| `TestM19MatViewConcurrentlySnapshotSwap` | CONCURRENTLY 刷新期间并发读旧快照、提交后原子读到新快照（快照交换语义） |
| `TestM19MatViewDependencyRefresh` | 依赖链（mv_a 依赖 mv_b 依赖基表）：只刷上层时按依赖序先刷下层（失效传播）；基表变更在链上可见；整链幂等 |
| `TestM19MatViewFullRefreshRegression` | 非 Simple 定义（JOIN+聚合）走全量路径语义不破坏；幂等 |

## 闭环（v1.9.0 增量刷新）

- 代码：`pkg/sql/matview_incremental.go`（新增：`mvDirty` 持久化 / `recordMVDirtyKeys` 变更跟踪 / `buildMVDependencySet`+`collectMatViewDeps` 依赖解析 / `applyIncrementalRefresh` 增量合并 / `refreshMatViewRec` 依赖递归刷新 / `writeMatViewRowsPK`+`refreshMatViewRowsPK` 全量快照写回）、`pkg/sql/matview.go`（创建/刷新/删除改造与依赖缓存）、`pkg/sql/executor.go`（getTx/autocommit 感知 `Session.forcedTx`、DML 变更跟踪钩子、DROP/ALTER 溢出标记）、`pkg/sql/session.go`（`forcedTx` 字段）；
- 测试：`pkg/sql/matview_incremental_test.go`（4 个用例，与 M15 老用例共 21 个物化视图用例全绿）；
- 文档：本文档；`docs/DEV_PLAN_2026Q6.md` 3.3 节执行状态补完成说明（DEV_PLAN 不入库）。
*（内容由AI生成，仅供参考）*
