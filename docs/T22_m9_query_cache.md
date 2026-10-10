# T22 M9 功能簇（2/5）：查询缓存

> 版本：v5.0-P11-M9QC, v0.13.0-alpha
> 日期：2026-10-10
> 范围：`pkg/sql`（新增 `query_cache.go` + lexer / ast / parser / executor）+ 单元测试（`m9_query_cache_test.go`）+ README 勾选
> 闭环：`go build ./...` 与 `go test ./... -count=1` 全绿，不 push
> 提交：`M9: query cache (SET query_cache + SELECT result cache)`（本地，未 push）

## 背景

只读查询（SELECT）每次执行都重新走存储层扫描、解码与计算。对于重复执行的
相同 SQL（如监控轮询、报表模板），结果完全一致却反复付出全量 IO 代价。
M9 查询缓存为 SQL 层增加**按规范化 SQL 文本为 key** 的结果缓存：
命中直接返回上次结果，写语句（DML / DDL）成功后自动全量失效，保证缓存
与存储强一致；提供开关（`SET query_cache = on|off`）与统计
（`SHOW STATS` 输出命中 / 未命中 / 失效次数）。

## 设计

### 语法

```sql
SET query_cache = on|off|1|0|true|false   -- 会话开关
SHOW STATS                                 -- 输出 query_cache_enabled/hits/misses/invalidations
```

### 词法 / 语法层

- `lexer.go` 关键字已含 `SET`（新增值分支接收关键字 ON/OFF/TRUE/FALSE 作为变量值）；
- `ast.go` 新增 `SetStmt{Name, Value}`（会话变量设置语句节点）；
- `parser.go` `parseStmt` 新增 `SET` 分支，`parseSet` 解析
  `SET <name> = <value>`（标识符 / 关键字 / 数字 / 字符串，值统一小写）；
- `sql.go` `IsSQL` 关键字列表补充 `ALTER` / `SET`，协议层可路由这两类语句。

### 执行层（executor + query_cache.go）

- `QueryCache`（`pkg/sql/query_cache.go`，线程安全）：
  - 字段 `enabled / entries(map[normalizedSQL]→cols+rows) / hits / misses / invals`；
  - `Get` 命中返回**深拷贝**（防调用方修改污染缓存），未命中计 miss；
  - `Put` 仅缓存带结果集列的 SELECT 结果；
  - `Invalidate` 全量清空并计数（安全优先：不做表级细粒度跟踪）；
  - `SetEnabled` 关闭时清空条目（避免陈旧结果残留）；
  - `NormalizeSQL` 规范化 key：去首尾空白 / 尾分号、统一小写、折叠连续空白，
    使不同写法的同语义 SQL 共享缓存。
- `Executor` 新增 `qc *QueryCache` 字段，`NewExecutor` 初始化（默认关闭）；
- `ExecuteText` 缓存接入：仅 **SELECT + autocommit + 开关开启** 时走
  `Get`（命中直接返回）→ 未命中执行后 `Put`；写语句（DML / DDL 等，
  `isWriteStmt` 判定）成功后 `Invalidate`。显式事务（BEGIN…COMMIT）内
  不参与缓存，保持快照隔离语义；
- `execSet` 处理 `SET query_cache`（非法值 / 未知变量报错）；
- `execShowStats` 追加 4 项缓存指标（`query_cache_enabled` /
  `query_cache_hits` / `query_cache_misses` / `query_cache_invalidations`）。

**失效联动覆盖**：`INSERT / UPDATE / DELETE / CREATE TABLE / DROP TABLE /
CREATE INDEX / DROP INDEX / ALTER TABLE / IMPORT / region 运维写 / 备份恢复`
全部计入失效；`SET` 本身不计数（开关变更由 `SetEnabled` 管理缓存内容）。

## 测试（pkg/sql/m9_query_cache_test.go）

| 测试 | 覆盖 |
|---|---|
| `TestQueryCacheDefaultOff` | 默认关闭：不缓存、统计为 0 |
| `TestQueryCacheHitMiss` | SET on 后首查 miss、二查命中、SET off 清空 |
| `TestQueryCacheNormalizeKey` | 大小写 / 空白 / 尾分号差异命中同一缓存 |
| `TestQueryCacheInvalidateOnDML` | INSERT / UPDATE / DELETE 各自触发失效并重新读到新数据 |
| `TestQueryCacheInvalidateOnDDL` | ALTER / CREATE / DROP TABLE 触发失效 |
| `TestSetQueryCacheParse` | on/off/1/0/true/false 均可解析；非法值 / 未知变量报错 |

## 取舍说明

- 缓存粒度按**全量失效**（写语句后清空全部缓存），不做表级 / 行级细粒度
  跟踪：当前为单节点小数据量，安全优先、实现简单；后续可扩展按表依赖
  失效以提升命中率；
- 仅缓存 autocommit 下的 SELECT（显式事务内结果可能被未提交修改影响，
  不参与缓存）；
- 缓存条目不做 LRU 淘汰（命中即全量返回，写语句自然清空），后续可加
  容量上限与逐出策略。

## README 勾选

- Status 行更新为 `M9 query cache: SET query_cache + SELECT result cache with DML/DDL auto-invalidation (v5.0-P11-M9QC), v0.13.0-alpha.`
- T 列表追加 `+ T22 M9: query cache (SET query_cache + SELECT result cache, auto-invalidate on DML/DDL)`
