
# T24 M11 功能簇（2/3）：权限模型（GRANT/REVOKE）

> 版本：v5.0-P13-M11, v0.14.0-alpha
> 日期：2026-10-11
> 范围：`pkg/sql`（lexer / ast / parser / executor / grant.go / sql.go + 测试 `m11_grant_test.go`）
> 闭环：`go test ./pkg/sql/ -count=1` 全绿，不 push（README 勾选由 M11 整簇收尾提交统一更新）
> 提交：`M11: privilege model (GRANT/REVOKE)`（本地，未 push）

## 背景
M11 子功能一（用户管理，`b137acd`）已建立用户实体与口令存储（`m:users`、`AuthenticateUser` 预留接口），但用户尚不具备任何可校验的权限：任何已登录用户可对任意表执行任意 DML。本子功能落地**表级权限模型**：`GRANT` / `REVOKE` SQL 语句、权限元数据持久化（`m:grants`）、`DROP USER` / `DROP TABLE` 联动清理、以及供 M11 子功能三（连接鉴权与执行期校验）调用的 `Engine.HasPrivilege` 查询接口。

## 设计
### 语法
```sql
GRANT <priv[,priv...]> ON <table> TO '<user>';   -- 授权（重复 GRANT 幂等）
REVOKE <priv[,priv...]> ON <table> FROM '<user>';-- 撤销（未授权 REVOKE 幂等）
-- priv ∈ {SELECT, INSERT, UPDATE, DELETE, ALL}
```
- 权限名大小写不敏感（词法层统一转大写关键字）；
- `ALL` 展开为 SELECT|INSERT|UPDATE|DELETE 全部四个权限位；
- `<user>` 必须为字符串字面量（与 CREATE USER 一致）；`<table>` 为标识符。

### 词法 / 语法层
- `lexer.go`：关键字表补充 `GRANT`、`REVOKE`、`ALL`（`SELECT`/`INSERT`/`UPDATE`/`DELETE`/`ON`/`TO`/`FROM` 已存在）；
- `ast.go`：新增 `GrantStmt{Privileges []string, Table, User}` / `RevokeStmt{Privileges, Table, User}` 并实现 `stmt()`；
- `parser.go`：`parseStmt` 增加 `GRANT` → `parseGrant`、`REVOKE` → `parseRevoke` 分支；共用 `parsePrivList` 解析逗号分隔权限列表，非法权限名（如 `DROP`）报 `unsupported privilege`。

### 执行层（executor）
- `isWriteStmt` 纳入 `GrantStmt` / `RevokeStmt`（写语句，走事务提交并使查询缓存失效）；
- `execGrant`：加载用户元数据，**授权用户必须存在**（否则 `user not exists`）→ 权限位 `OR` 合并写入（重复 GRANT 幂等）；
- `execRevoke`：用户必须存在（否则 `user not exists`）→ 清除对应权限位；用户/表无授权记录时幂等成功；位清零后删除表条目、空用户条目一并移除，保持目录无空洞；
- `execDropUser`（M11 子功能一，本提交扩展）：删除用户后联动 `removeUserGrants` 清理 `m:grants` 中该用户全部授权；
- `execDropTable`（本提交扩展）：删除表后联动 `removeTableGrants` 清理所有用户对该表的授权（空用户条目一并移除）。

### 持久化
- `grant.go`（新增）：
  - `Privilege` 位掩码：`PrivSelect=1<<0`、`PrivInsert=1<<1`、`PrivUpdate=1<<2`、`PrivDelete=1<<3`；`PrivAll` 为四者并集；
  - `grantsMetaKey`（元数据表固定键 `m:grants`），JSON 编码存储，结构 `map[user]map[table]int`（int 为权限位掩码）；
  - `loadGrants(get)` / `saveGrants`：遵循 `loadUsers`/`saveUsers` 的 `tx.Get` 注入模式；无记录时返回空 map，损坏 JSON 报 `corrupted grant catalog`；
  - `parsePrivilege`：权限名 → 位掩码（含 `ALL`）。
- `sql.go`：Engine 增加公开预留接口 `HasPrivilege(user, table, op) (bool, error)`，供 M11 子功能三执行期权限校验调用；`op` 为 `SELECT`/`INSERT`/`UPDATE`/`DELETE`，未知 op 报错，用户/表无授权返回 `false`。

## 测试（pkg/sql/m11_grant_test.go）
| 测试 | 覆盖 |
|---|---|
| `TestGrantAndHasPrivilege` | GRANT SELECT 后 `HasPrivilege(SELECT)=true`、INSERT/其他表/其他用户为 false |
| `TestGrantMultipleAndAll` | `GRANT SELECT, INSERT` 两权限生效；`GRANT ALL` 后四权限全 true |
| `TestGrantUserNotExists` | 授权不存在用户 → `user not exists` |
| `TestGrantTableLoose` | 表不存在宽松处理：授权成功且 `HasPrivilege` 可用（MySQL 语义说明见下） |
| `TestGrantIdempotent` | 重复 GRANT 同一权限不报错、权限保持 |
| `TestRevokePrivilege` | REVOKE 后 SELECT 失效、UPDATE 保留；全部撤销后记录消失 |
| `TestRevokeIdempotent` | 撤销未授权权限、无任何授权用户 REVOKE 均幂等成功 |
| `TestRevokeUserNotExists` | 撤销不存在用户 → `user not exists` |
| `TestDropUserCleansGrants` | DROP USER 联动清权限，`HasPrivilege` 返回 false |
| `TestDropTableCleansGrants` | DROP TABLE 联动清该表所有用户授权 |
| `TestGrantParseErrors` | 缺 `ON`/`TO`、用户名非字符串、非法权限名、REVOKE 缺 `ON`/用户名均报语法错误 |

## 边界说明（语义自洽）
- **授权/撤销不存在用户 → 报错**：与用户管理一致，避免向幽灵用户写入不可达的授权记录；
- **授权不存在的表 → 宽松成功**：对齐 MySQL 授权语义（MySQL 可授权给未创建的表，建表后权限自动生效）。本实现不校验表存在性，文档在此明示：`GRANT SELECT ON ghost TO 'alice'` 成功，`HasPrivilege` 可查询到该授权；`DROP TABLE ghost` 时该授权同样被联动清理；
- **重复 GRANT → 幂等**：权限位 `OR` 合并，不报错、不重复计数；
- **REVOKE 未授权权限 → 幂等**：无授权记录或权限位不存在时静默成功；
- **DROP USER / DROP TABLE 联动**：删除主体后权限记录一并清理，避免悬空授权；`DROP USER IF EXISTS` 命中不存在用户时不做任何清理（无记录可清）。

## 取舍说明
- **权限位掩码**：表级权限仅四类 DML，位掩码紧凑且支持 `ALL` 一次授权；扩展列级权限（MySQL `GRANT SELECT(col)`）需重构为更复杂结构，暂不引入；
- **宽松表授权**：选择 MySQL 语义而非严格校验，避免授权语句依赖表创建顺序；已在文档明示（见边界说明）；
- **HasPrivilege 仅查询**：预留接口不修改连接协议，也不在本子功能接入执行校验（M11 子功能三统一接入）；
- **元数据键独立**：`m:grants` 独立于 `m:users`/`m:tables`，与用户/表元数据解耦，联动清理通过 `execDropUser`/`execDropTable` 内显式调用完成，不引入触发器机制。

## README 勾选
由 M11 整簇收尾提交统一执行：
- Status 行更新为 M11 权限与用户管理簇；
- T 表格追加 `T24 M11 permission & user management cluster: user management + GRANT/REVOKE + connection auth`；
- 文档列表追加 `T24_m11_grant.md`（随本提交加入）。

*（内容由AI生成，仅供参考）*
