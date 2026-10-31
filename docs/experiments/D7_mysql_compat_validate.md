---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_050d39cbd2c811f18460525400393706
    ReservedCode1: GyHlDrY2FZko4e3FsAKsRS8NTHvUjlkvVFeHmGLao/gwPXpWsxLKL1PAQHgMkjaeBeME7T92rY+DoZKFEARL63riD5pdQYNZTBRGJCU0gqIAQaZzyJXK6KseFqLqQkUvhr0zTeOA3ebk5jTdpFsfr3TovzyW55yprp3Mr+On3vUzzE9hJmUjYQCuBRo=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_050d39cbd2c811f18460525400393706
    ReservedCode2: GyHlDrY2FZko4e3FsAKsRS8NTHvUjlkvVFeHmGLao/gwPXpWsxLKL1PAQHgMkjaeBeME7T92rY+DoZKFEARL63riD5pdQYNZTBRGJCU0gqIAQaZzyJXK6KseFqLqQkUvhr0zTeOA3ebk5jTdpFsfr3TovzyW55yprp3Mr+On3vUzzE9hJmUjYQCuBRo=
---

# D7 MySQL 兼容性补齐与客户端体验（mysql compat + client prompt）专项验证

版本：v1.17.0 D7（G1-G5）
日期：2026-10-31
范围：MySQL 握手版本串（G1）、`select version()`（G3）、`SHOW CREATE TABLE`（G4）、`SHOW INDEXES` 复数（G5）、`oxsql>` 提示符（G2，P2）五项兼容补齐的验收实测。

## 1. 功能实现

| 项 | commit | 说明 |
|---|---|---|
| G1 handshake version string | `3d43777` | `pkg/mysqlproto/handshake.go` 版本串由硬编码 `8.0.36-OpenXDB` 改为与 v1.17.0 版本常量同源注入，目标串 `8.0.36-OpenXDB v1.17.0`；同步握手版本断言测试 |
| G3 version() function | `9d8f968` | SQL 函数注册表登记 `version`，返回服务端版本串（与 G1 握手同源 v1.17.0），`select version();` 可用 |
| G5 SHOW INDEXES plural | `ac95473` + 本次 | `parseShow` 接受 `INDEX` / `INDEXES` 复数，复用 `ShowIndexStmt` / `execShowIndex` 单数实现，输出等价；验收发现小写 `show indexes` 未识别（`switch p.cur().text` 大小写敏感），补 `strings.ToUpper` 归一化 + 新增 `TestD7ShowIndexesCaseInsensitive` 覆盖大小写等价 |
| G4 SHOW CREATE TABLE | `33a7979` | `parseShow` 新增 `CREATE` 分支 + `ShowCreateStmt`，executor 基于表存储元数据重建 CREATE TABLE（列定义/类型/主键/二级索引），逐表与真实 schema 回归比对一致 |
| G2 client prompt (oxsql>) | `a00f522` | 自研交互 CLI `cmd/oxsql` 提示符 `oxsql>`（仓库无 `mysql>` 实现，`mysql>` 为第三方 mysql CLI 客户端侧固定，品牌提示符在自研客户端落地） |

## 2. 方案取舍

- **版本串同源**：G1 与 G3 版本串统一由 `OpenxdbVersion`（v1.17.0）注入，前缀 `8.0.36-OpenXDB` 保持 mysql CLI 对版本格式的识别兼容；避免多处置版本号漂移。
- **G5 大小写归一化**：`parseShow` 的 `switch p.cur().text` 改为 `strings.ToUpper(...)`，使 `show indexes` / `SHOW INDEXES` / `show index` / `SHOW INDEX` 四种大小写组合等价；仅 SHOW 作用域内识别复数，避免与保留字冲突。
- **G2 载体确认**：仓库全仓无 `mysql>` / `openxdb>` 提示符实现，第三方 mysql CLI 的 `mysql>` 无法从服务端改变；品牌提示符 `oxsql>` 落地在自研交互 CLI（`pkg/mysqlproto` 客户端 Dial 基座扩展）。第三方 mysql CLI 用户仍见 `mysql>`，属客户端侧展示，不阻塞验收。

## 3. 验证结果（5 项验收 + 服务监听）

环境：Windows 11，openxdb.exe / oxsql.exe 构建自 D7 代码（HEAD=83d562c + G5 大小写修复工作区）。服务端 `start --port 17791 --replica-port 17792 --mysql-port 13311`；客户端为自研 `bin/oxsql.exe`（G2 载体）与 mysql CLI 兼容协议（G1/G3-G5 均走 MySQL wire 兼容监听）。

### 3.1 G1 握手版本串
oxsql 连接首行显示：

```
Connected to 127.0.0.1:13311, server version: 8.0.36-OpenXDB v1.17.0
```

mysql CLI 登录握手显示同源版本串，PASS。

### 3.2 G3 select version()
```
mysql> select version();
+----------------------------+
| VERSION()                  |
+----------------------------+
| 8.0.36-OpenXDB v1.17.0     |
+----------------------------+
1 row(s) in set
```
返回 v1.17.0，PASS（注：oxsql 行协议输出为 `VERSION() | 8.0.36-OpenXDB v1.17.0`）。

### 3.3 G4 SHOW CREATE TABLE
建表 `CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, age INT, KEY idx_age (age))` 后：

```
mysql> show create table t;
+-------+---------------------------------------------------------------+
| Table | Create Table                                                 |
+-------+---------------------------------------------------------------+
| t     | CREATE TABLE `t` (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, age INT, KEY idx_age (age)) |
+-------+---------------------------------------------------------------+
1 row(s) in set
```

输出含 `CREATE TABLE`、主键与二级索引，可重建 schema，PASS。

### 3.4 G5 SHOW INDEXES 复数（四种大小写组合等价）
`show indexes from t;` / `SHOW INDEXES FROM t;` / `show index from t;` / `SHOW INDEX FROM t;` 四路输出逐字等价（`-ceq` 比较均 True），PASS。

### 3.5 G2 oxsql> 提示符
交互 REPL 注入 `select 1;` / `select version();` / `quit` 后，输出含多个 `oxsql>` 提示符与查询结果，PASS。

### 3.6 全仓构建与测试
- `go build ./...` 通过；
- `go test ./...` 全绿（含 D7 专项：`TestD7ShowIndexes*`、`TestD7ShowCreate*`、`TestD7VersionFunc`、`TestD7ClientHandshakeAndVersion`、`TestHandshakeV10Structure`，含新增 `TestD7ShowIndexesCaseInsensitive`）。

## 4. 复现方法

```powershell
# 1) 构建二进制
go build -o bin/openxdb.exe ./cmd/openxdb
go build -o bin/oxsql.exe ./cmd/oxsql

# 2) 运行 D7 专项单测
go test ./pkg/mysqlproto/... ./pkg/sql/... -run "TestD7|TestHandshakeV10" -count=1

# 3) 运行专项验证（自动 init、起服务、执行 G1-G5 断言，PASS=6/FAIL=0）
powershell -NoProfile -ExecutionPolicy Bypass -File docs/experiments/run_d7_mysql_compat_validate.ps1
```

> 脚本说明：查询统一走 `System.Diagnostics.Process` 直接重定向 stdout/stdin，规避 PowerShell 5.1 对原生程序 `2>&1` 的 CLIXML 包装与 cmd /c 引号歧义；脚本为 UTF-8 **带 BOM**（PS 5.1 对无 BOM 的 UTF-8 脚本按 ANSI 读取会破坏含中文注释的解析）。数据目录使用带时间戳的唯一临时路径，脚本不删除任何既有目录。

## 5. 边界声明

- 握手版本串与 `version()` 版本号硬编码为 v1.17.0（与版本常量同源），发版升版本需同步更新 `OpenxdbVersion`；
- `SHOW CREATE TABLE` 以存储元数据为唯一事实源，覆盖列定义/类型/主键/二级索引/约束；`SHOW INDEXES` 输出与 `SHOW INDEX` 完全等价（同一实现路径）；
- `oxsql>` 为自研 CLI 提示符；第三方 mysql CLI 的 `mysql>` 属客户端侧展示，不在服务端能力范围；
- 五项兼容补齐均为读路径/客户端体验改动，不触碰事务、复制与 WAL 语义，全仓回归全绿。
