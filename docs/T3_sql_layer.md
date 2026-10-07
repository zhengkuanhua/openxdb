---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_bd484ae8c20111f18019525400248c00
    ReservedCode1: 6QenkJX0B0asm7iWQmi4iojlLaNIWtN6u+MWgohDyOwuTNLC6KozBgk8iws03y13LB5E95KCin+cBIh8Zbo5Ho3/jaDUGcSTBeZfMejS9C8xkIvvXC/nnAxEUsqpC1xX1De9nwv5tjxg3H4uaaghZpXHgTiXbkLs1hAIM60/b5r3cfWEcEB6fJhggTw=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_bd484ae8c20111f18019525400248c00
    ReservedCode2: 6QenkJX0B0asm7iWQmi4iojlLaNIWtN6u+MWgohDyOwuTNLC6KozBgk8iws03y13LB5E95KCin+cBIh8Zbo5Ho3/jaDUGcSTBeZfMejS9C8xkIvvXC/nnAxEUsqpC1xX1De9nwv5tjxg3H4uaaghZpXHgTiXbkLs1hAIM60/b5r3cfWEcEB6fJhggTw=
---

# T3 SQL 层实现记录

## 目标

在 T1 存储层（RocksDB）+ T2 WAL + T3 事务层之上，补齐 SQL 引擎：解析器、执行器、表目录（DDL/DML 最小集），并挂载进协议层（TCP/REPL）与 CLI，使 openxdb 可直接执行 SQL 语句。

## 模块结构

| 文件 | 职责 |
|------|------|
| `pkg/sql/parser.go` | 词法/语法分析，产出 AST（Stmt 各类型） |
| `pkg/sql/ast.go` | 语句与表达式结构：CreateTable/DropTable/Insert/Select/Update/Delete、Where、OrderBy、ColumnRef |
| `pkg/sql/executor.go` | 执行器：DDL/DML 以单语句隐式事务提交，自带主键约束、类型强制、WHERE 过滤、排序、LIMIT、聚合、投影、点查优化 |
| `pkg/sql/meta.go` | 表目录（TableMeta）元数据与 Key 编码：表 ID 分配、行 Key 编码（表 ID 前缀 + 主键）、行 JSON 编解码 |
| `pkg/sql/sql.go` | Engine 入口：New(tm) 装配执行器；IsSQL 判定协议层路由 |
| `pkg/sql/sql_test.go` | 集成测试（外部测试包，避免循环依赖） |

## 支持语法

- DDL：`CREATE TABLE name (col TYPE, ..., PRIMARY KEY (col))`；`DROP TABLE name`
  - 类型仅 INT / TEXT；主键缺省时取第一列
- DML：`INSERT INTO t [(cols)] VALUES (...), (...)`；`UPDATE t SET col=val, ... [WHERE ...]`；`DELETE FROM t [WHERE ...]`
- 查询：`SELECT cols|* FROM t [WHERE conds] [ORDER BY col [ASC|DESC]] [LIMIT n]`
  - WHERE 支持 `= != < > <= >=` 的 AND 组合
  - 聚合：`COUNT(*)` / `SUM(col)` / `AVG(col)`
  - 主键等值单条件 → 点查优化（直接 Get 而非全表扫描）

## 执行语义

- 所有语句经 `TxnManager.Begin/Commit/Rollback` 单事务执行（隐式事务，写即提交）
- 表目录存于 `metaKey`（`\x00meta`）单键 JSON；首次无目录时按空目录处理（`loadTables` 容忍 `storage.ErrNotFound`）
- 主键唯一性：Insert 前 `tx.Get` 判重，冲突报 `duplicate primary key`
- 类型强制：INT 列拒绝 TEXT 值，TEXT 列拒绝 INT 值（`coerceRow`）
- 行 Key 编码：`meta.ID`（4 字节 BE）+ 主键字节（INT 定长 8 字节 / TEXT 原字节），`TableRange` 用于整表扫描
- 聚合与投影：聚合结果单行多列输出；`SELECT *` 投影列名展开为表定义列

## 修复记录（本轮）

1. `loadTables` 对首次建表（目录键不存在）直接返回错误 → 容忍 `storage.ErrNotFound` 返回空目录
2. `finishSelect` 投影列名：`SELECT *` 仍输出 `"*"` 且行值为展开多列，导致协议层 `formatResult` 列宽数组越界 panic（`index out of range [1] with length 1`）→ 按表定义展开列名
3. `doAgg` 聚合结果逐列各成一行（竖排）→ 改为单行多列

## 验证

- 全仓 `go test ./... -count=1` 全绿（storage/rocksdb/wal/txn/sql/db/server 均通过）
- TCP 冒烟：PING、CREATE、INSERT、SELECT/WHERE/ORDER BY/聚合、UPDATE、DELETE、DROP、QUIT 全链路通过

## 命令示例

```
openxdb start --data-dir <dir> --port 17789
> CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))
> INSERT INTO users (id, name, age) VALUES (1, 'alice', 30), (2, 'bob', 25)
> SELECT name, age FROM users WHERE age > 25 ORDER BY age DESC
> SELECT COUNT(*), SUM(age), AVG(age) FROM users
```
*（内容由AI生成，仅供参考）*
