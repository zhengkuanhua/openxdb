---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_2de1db63c51a11f197b3525400248c00
    ReservedCode1: 2ehtsq9uBTG8zZdrllt9+06A2JwPmRNs1lLUOvODVN2nJogIlrsZ+qi3md51DAkT3DOhxB5NztjoyKEzWnVqT9q8Ry7aK9aJnNbNJ013842a7m/AAhUHjNX8Psrz6bbRkzJ0+U6pen5o/7FtroT/54ASgETgXoXPDUcloTPgBoleGErn+/lulcr6xCQ=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_2de1db63c51a11f197b3525400248c00
    ReservedCode2: 2ehtsq9uBTG8zZdrllt9+06A2JwPmRNs1lLUOvODVN2nJogIlrsZ+qi3md51DAkT3DOhxB5NztjoyKEzWnVqT9q8Ry7aK9aJnNbNJ013842a7m/AAhUHjNX8Psrz6bbRkzJ0+U6pen5o/7FtroT/54ASgETgXoXPDUcloTPgBoleGErn+/lulcr6xCQ=
---



# T24 M11 功能簇（1/3）：用户管理（CREATE/DROP/SHOW/ALTER USER）

> 版本：v5.0-P13-M11, v0.14.0-alpha
> 日期：2026-10-11
> 范围：`pkg/sql`（lexer / ast / parser / executor / user.go / sql.go + 测试 `m11_user_test.go`）
> 闭环：`go test ./pkg/sql/ -count=1` 全绿，不 push（README 勾选由 M11 整簇收尾提交统一更新）
> 提交：`M11: user management (CREATE/DROP/SHOW/ALTER USER)`（本地，未 push）

## 背景

OpenXDB 此前无任何用户概念：所有连接与语句执行均无身份与口令校验，server 层无法区分操作者。M11 权限与用户管理簇分三步落地：**用户管理**（本子功能，建立用户实体与口令存储）→ GRANT/REVOKE 权限模型 → 连接鉴权与执行期校验。本子功能先打通用户生命周期 SQL：`CREATE USER` / `DROP USER` / `SHOW USERS` / `ALTER USER`，口令以 **SHA-256 + 随机 salt** 哈希落库（禁止明文），元数据参考 TableMeta 的 JSON 持久化方式写入元数据表，并预留供 server 层鉴权握手的查询接口。

## 设计

### 语法

```sql
CREATE USER '<name>' IDENTIFIED BY '<pwd>';   -- 新建用户（重复建报错）
DROP USER [IF EXISTS] '<name>';               -- 删除用户（不存在报错；IF EXISTS 静默）
SHOW USERS;                                   -- 列出全部用户（按名称排序）
ALTER USER '<name>' IDENTIFIED BY '<pwd>';    -- 重置密码（用户不存在报错）
```

### 词法 / 语法层
- `lexer.go`：关键字表补充 `USER`、`IDENTIFIED`（`BY`/`IF`/`EXISTS`/`ALTER`/`DROP`/`CREATE` 已存在）；
- `ast.go`：新增 `CreateUserStmt{Name, Password}` / `DropUserStmt{Name, IfExists}` / `AlterUserStmt{Name, Password}` / `ShowUsersStmt` 并实现 `stmt()`；
- `parser.go`：
  - `parseCreate` 增加 `USER` 分支 → `parseCreateUser`（`IDENTIFIED BY` 必填，用户名/密码均须字符串字面量）；
  - `parseDrop` 增加 `USER` 分支 → `parseDropUser`（支持 `IF EXISTS`）；
  - `parseAlter` 开头识别 `USER` → `parseAlterUser`（`IDENTIFIED BY` 必填）；
  - `parseShow` 增加 `USERS` 分支：**USERS 不加入关键字表**（避免与合法表名 `users` 冲突），按标识符文本 `EqualFold` 匹配，`SHOW USERS` 与 `CREATE TABLE users` 可共存。

### 执行层（executor）
- `isWriteStmt` 纳入 `CreateUserStmt` / `DropUserStmt` / `AlterUserStmt`（写语句，走事务提交）；
- `execCreateUser`：加载用户元数据 → 重复名报 `user already exists` → `hashPassword` 生成 salt+哈希 → 追加保存；
- `execDropUser`：不存在时报 `user not exists`（`IF EXISTS` 静默成功），否则移除保存；
- `execAlterUser`：不存在时报 `user not exists`，否则重新哈希密码覆盖 `SaltHash`；
- `execShowUsers`：加载全部用户，按名称排序，返回单列 `user` 结果集。

### 持久化与口令安全
- `user.go`（新增）：
  - `UserMeta{Name string, SaltHash string}`；`usersMetaKey`（元数据表固定键）与 TableMeta 同用 JSON 编码存储；
  - `loadUsers` / `saveUsers` / `findUser`：读改写元数据，遵循 `loadTables`/`saveTables` 的 `tx.Get` 注入模式；
  - `hashPassword(pwd)`: 生成 16 字节随机 salt，`SHA-256(salt || pwd)` 得哈希，存储格式 `base64(salt):base64(hash)`，**落库无明文口令**；
  - `verifyPassword(pwd, saltHash)`: 用 `crypto/subtle.ConstantTimeCompare` 常数时间比对防时序侧信道；
  - `authenticateUser(name, pwd)`: 查用户 → 校验口令（M11 子功能三连接鉴权的内部入口）。
- `sql.go`：Engine 增加公开预留接口 `AuthenticateUser(name, password) (bool, error)`，供 server 层后续握手与执行期权限校验调用。

## 测试（pkg/sql/m11_user_test.go）

| 测试 | 覆盖 |
|---|---|
| `TestCreateUserAndShow` | 建用户 + SHOW USERS 列出；与表名 `users` 共存（`setupUsers` 先建 users 表） |
| `TestCreateUserDuplicate` | 重复建用户 → `user already exists` |
| `TestDropUser` | 删除成功、删除不存在报错、`DROP USER IF EXISTS` 静默、清空后 SHOW USERS 为 0 行 |
| `TestAlterUserPassword` | ALTER 重置密码后旧密码失效新密码生效；ALTER 不存在用户报错 |
| `TestAuthenticateUser` | 正确密码通过、错误密码拒绝、不存在用户拒绝 |
| `TestShowUsersSort` | 多用户按名称排序输出（alice/bob/carol） |
| `TestUserParseErrors` | 缺 `IDENTIFIED BY`、用户名非字符串字面量、缺密码、SHOW USERS 多余 token 均报语法错误 |

## 取舍说明

- **口令哈希**：采用 SHA-256 + 每用户随机 salt（`crypto/sha256`），避免彩虹表与相同口令同哈希；未引入 bcrypt/argon2（依赖外部库、增加 CGO 面），后续如需更高强度可在 `hashPassword` 单点替换；
- **USERS 不作关键字**：`SHOW USERS` 通过 `parseShow` 中标识符文本匹配实现，`users` 表名合法不冲突（测试显式覆盖）；
- **用户名/密码限定字符串字面量**：语法简单明确，暂不支持 `IDENTIFIED WITH ...` 等 MySQL 变体；
- **鉴权接口只读**：`AuthenticateUser` 为预留查询接口，不改动连接协议，server 层接入在 M11 子功能三进行；
- **元数据键**：用户元数据独立键（`usersMetaKey`）而非复用 TableMeta 目录，避免与表名耦合，也便于 GRANT/REVOKE 子功能扩展权限字段。

## README 勾选

由 M11 整簇收尾提交统一执行：
- Status 行更新为 M11 权限与用户管理簇；
- T 表格追加 `T24 M11 permission & user management cluster: user management + GRANT/REVOKE + connection auth`；
- 文档列表追加 `T24_m11_user.md`（随本提交加入）。

*（内容由AI生成，仅供参考）*
