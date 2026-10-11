# T24 M11 功能簇（3/3）：连接鉴权 + SQL 执行校验

## 设计

### 连接握手协议（server 层）
- 行协议新增握手命令 `AUTH <user> <password>`：
  - 成功：返回 `AUTH OK`，会话切换为对应用户；
  - 失败（用户不存在 / 密码错误）：返回 `ERR auth failed: invalid user or password`，会话保持未鉴权（允许重试）；
  - 参数缺失：返回 `ERR syntax: AUTH <user> <password>`；
  - 密码允许含空格（取剩余 token 拼接），与 `SET <key> <val...>` 一致；
- 会话状态**跨命令保持**（每 TCP 连接一个 `session{user, authed}`），与 SQL 会话事务类似；
- `Server.SetAuth(&AuthConfig{Engine, AllowLegacy})` 装配鉴权；`Auth == nil` 时保持旧行为（无握手，会话默认超级用户 `root`，协议与既有 JDBC/Python 驱动完全不变）。

### 旧客户端兼容（T19 降级策略）
T19 JDBC/Python 驱动 README 明示 **no server-side auth handshake**（旧协议无 `AUTH` 命令）。启用鉴权后按 `AllowLegacy` 二选一：

| 策略 | 行为 | 适用 |
|---|---|---|
| `AllowLegacy = true`（默认） | 未鉴权连接**降级为 guest**（user 为空串）：KV 命令（`PING/HELP/GET/SET/DEL/BEGIN/COMMIT/ROLLBACK/QUIT`）照常可用；SQL 语句照常路由，但经执行期权限校验后**一律拒绝**（guest 无任何授权，管理语句与无授权 DML 均返回 `permission denied`） | 向后兼容优先，旧客户端可继续用 KV 能力与只读元数据语句 |
| `AllowLegacy = false` | 未鉴权连接除 `AUTH/PING/HELP/QUIT` 外一律拒绝，返回 `ERR auth required: send AUTH <user> <password> first` | 安全优先，强制握手 |

> 取舍：默认开启降级而非硬拒绝——牺牲 SQL 能力换取旧驱动连接不中断；若部署侧要求强制鉴权，显式 `SetAuth(AllowLegacy=false)` 即可，两策略均已测试覆盖。

### SQL 执行期权限校验（SQL 层）
- `Engine.ExecuteAs(stmt, user)`：以指定用户身份执行；`Engine.Execute(stmt)` 保持不变，等价于 `ExecuteAs(stmt, root)`（向后兼容既有调用方与 CLI）；
- `checkPrivilege` 规则（`pkg/sql/authz.go`，新增）：
  - **超级用户 `root`**：全部语句放行（内置 root 用户，初始密码由首次 `CREATE USER 'root' IDENTIFIED BY '...'` 设定，见下）；
  - **管理语句**（DDL：`CREATE TABLE/INDEX/VIEW`、`DROP TABLE/INDEX/VIEW`、`ALTER TABLE`；用户管理：`CREATE/DROP/ALTER USER`；授权：`GRANT/REVOKE`；运维：`IMPORT/EXPORT/BACKUP/RESTORE/SPLIT/BALANCE` 等）：仅超级用户 `root` 可执行，其余用户返回 `permission denied: admin statement requires superuser`;
  - **DML**（`SELECT/INSERT/UPDATE/DELETE`）：执行前经 `Engine.HasPrivilege(user, table, op)` 校验目标表权限；无授权（或用户/表不存在）返回 `permission denied`；
  - **guest（空 user）**：无任何授权，DML 与管理语句一律拒绝；`SHOW TABLES`、`EXPLAIN`、`BEGIN/COMMIT/ROLLBACK` 等会话/元数据语句放行；表不存在与无授权统一返回权限拒绝（不泄露表存在性）；
  - `checkPrivilege` 失败**不得修改任何状态**（校验先于事务开始）。

### 内置 root 与初始密码策略
- 代码内定义 `SuperUser = "root"` 常量（`pkg/sql/authz.go`）；
- **不内置硬编码默认密码**：`CREATE USER 'root' IDENTIFIED BY '...'` 由管理员首启时显式创建（复用 M11 子功能一的 SHA-256+salt 存储），未创建 root 前无法通过握手进入超级用户会话；
- 未启用鉴权（`Auth == nil`）时，旧协议默认 root 会话等价于隐式管理员（维持旧行为）。

## 实现

### server 层（pkg/server/server.go，本提交扩展）
- `Server.Auth *AuthConfig`：`Engine *sql.Engine`（校验用）+ `AllowLegacy bool`；
- `SetAuth(cfg)`：装配鉴权配置；
- `session{user, authed}` + `defaultSession()`：无鉴权模式返回 root/已鉴权，鉴权模式返回 guest/未鉴权；
- `HandleSession(line, sess)`：携带会话的单条命令处理；`Handle(line)` 兼容旧调用（等价默认会话）；
- `handleLocked` 未鉴权拦截：`AUTH/PING/HELP/QUIT` 放行；其余按 `AllowLegacy` 拒绝（`auth required`）或降级 guest 继续；
- `doAuth`：调 `Engine.AuthenticateUser` 完成握手，成功回 `AUTH OK`；
- `doSQL(line, sess)`：改走 `Engine.ExecuteAs(line, sess.user)`（执行期权限校验统一生效）；
- `REPL`（旧接口）委托 `REPLsSession(r, w, defaultSession())`；`ServeListener` 每连接创建独立会话。

### SQL 层（pkg/sql/，本提交扩展）
- `authz.go`（新增）：`SuperUser` 常量、`permissionDenied`、`isAdminStmt(stmt)`、`tablePrivOp(stmt)`（取 `SelectStmt.From` / `InsertStmt.Table` / `UpdateStmt.Table` / `DeleteStmt.Table`）、`checkPrivilege(eng, stmt, user)`；
- `executor.go`：`Execute(stmt)` / `ExecuteText(stmt, sqlText)` 统一委托新增 `ExecuteTextAs(stmt, sqlText, user)`，先 `checkPrivilege` 再 `execTimed`；
- `sql.go`：`Execute(stmt)` 委托 `ExecuteAs(stmt, SuperUser)`；新增公开 `ExecuteAs(stmt, user)` → 解析 AST → `ExecuteTextAs(ast, stmt, user)`；
- `sql.go`：`IsSQL` 关键字表补 `GRANT`/`REVOKE`（子功能二已支持，server 路由需识别）。

## 测试

### SQL 层（pkg/sql/m11_authz_test.go，新增 4 函数）
| 测试 | 覆盖 |
|---|---|
| `TestExecuteAsAdminOnlySuperuser` | 12 类管理语句（CREATE/DROP TABLE、INDEX、VIEW、USER、ALTER、GRANT/REVOKE）普通用户拒绝、root 放行；`Execute` 默认 root 兼容 |
| `TestExecuteAsDMLPrivilege` | 无授权四类 DML 拒绝；GRANT SELECT 后仅 SELECT 放行；GRANT ALL 后四类全放行；其他表仍拒绝 |
| `TestExecuteAsGuest` | 空用户 DML/管理语句拒绝；SHOW/EXPLAIN/BEGIN 放行；表不存在与无授权统一权限拒绝 |
| `TestExecuteAsRootPasswordRoundTrip` | `CREATE USER root` → `AuthenticateUser(root, pw)` 通过 → root 全权限执行 |

### server 层（pkg/server/server_auth_test.go，新增 6 函数）
| 测试 | 覆盖 |
|---|---|
| `TestAuthHandshakeTCP` | 真实 TCP 握手闭环：guest 拒绝 → 错密码 `auth failed` → AUTH OK → root 管理语句+DML 放行 |
| `TestAuthRejectWithoutLegacy` | `AllowLegacy=false`：未鉴权 KV/SQL/事务全拒（`auth required`），AUTH 后恢复 |
| `TestAuthLegacyGuestKV` | `AllowLegacy=true`：旧客户端 KV 照常、SQL 受限（降级策略） |
| `TestAuthAlicePrivilegedSQL` | 普通用户 AUTH 后：有授权 SELECT 放行、未授权 DML/管理语句拒绝 |
| `TestAuthSyntax` | AUTH 缺参返回 `ERR syntax` |
| `TestAuthUnavailableWithoutConfig` | `Auth==nil` 时 AUTH 返回 `auth unavailable`，旧行为不变 |

`go test ./...` 全绿（15 个包 ok）。

## 边界说明（语义自洽）
- **校验先于事务**：权限拒绝不开启事务、不改任何状态；
- **表不存在与无授权统一权限拒绝**：避免向客户端泄露表是否存在（`SELECT * FROM ghost` 与无授权 `SELECT * FROM users` 同报 `permission denied`）；
- **降级 guest 可执行只读元数据**：`SHOW TABLES`/`EXPLAIN`/事务控制放行，保证旧客户端连接健康检查与只读管理可用；
- **错误密码后保持未鉴权**：AUTH 失败不锁会话、不切断连接，允许重试（未引入失败计数，防爆破留待后续）；
- **root 无内置密码**：必须显式 `CREATE USER 'root'`，避免默认口令风险。

## 取舍说明（兼容策略结论）
1. **握手帧不做二进制改造，复用行协议 `AUTH` 命令**：协议零升级，旧客户端发送任意行都能得到可读响应；若未来引入二进制协议（预编译语句），再考虑握手帧内嵌鉴权字段；
2. **降级默认开启**：T19 JDBC/Python 驱动（无 AUTH 能力）连上后 KV 能力可用、SQL 受限，而非直接断连——兼容优先；强制鉴权场景用 `AllowLegacy=false`；
3. **guest 不映射到任何真实用户**：避免"匿名用户可执行未授权 SQL"的误解；guest 唯一能力是显式放行的会话/元数据语句；
4. **管理语句白名单按 AST 类型判定**（`isAdminStmt` 基于 `stmt()` 分发），与解析器同步演进，不依赖文本正则；
5. **`Execute` 默认 root**：既有 CLI/测试/驱动调用链零改动，权限语义只在显式传入 user 的 `ExecuteAs` 生效。

## README 勾选
由 M11 整簇收尾提交统一执行：
- Status 行更新为 M11 权限与用户管理簇；
- T 表格追加 `T24 M11 permission & user management cluster: user management + GRANT/REVOKE + connection auth`；
- 文档列表追加 `T24_m11_auth.md`（随本提交加入）。

*（内容由AI生成，仅供参考）*
