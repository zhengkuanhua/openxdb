# T30 M17-2：细粒度权限与角色体系（RBAC）

## 概述

M17 安全增强里程碑的第二个子功能：在 M11 表级 GRANT 基础上扩展

1. **库级权限**：`GRANT <priv> ON DATABASE <name|*> TO ...`
   （OpenXDB 单库模型：`*` 或任意具名库的授权对该库全部表生效）
2. **列级权限**：`GRANT <priv> ON <table> (col, ...) TO ...`
3. **角色体系**：`CREATE ROLE / DROP ROLE / SHOW ROLES` +
   `GRANT <priv> ... TO ROLE '<role>'` +
   `GRANT ROLE '<role>' TO '<user>'` / `REVOKE ROLE '<role>' FROM '<user>'`

并接入连接认证与 DML 执行时的权限校验（authz.go 统一聚合直授 + 角色授权）。

## 数据模型（元数据键）

| 键 | 结构 | 说明 |
|----|------|------|
| `m:grants` | user → table → mask | M11 表级直授（兼容，不动） |
| `m:colgrants` | user → table → col → mask | 列级直授（新） |
| `m:dbgrants` | user → db → mask | 库级直授（新；db=`*` 或具名） |
| `m:roles` | role → RoleMeta | 角色定义与角色内授权（新） |
| `m:userroles` | user → []role | 用户被授予的角色（新） |

RoleMeta：

```json
{"name":"analyst","grants":{"t1":15},"col_grants":{"t1":{"salary":4}},"db_grants":{"*":1}}
```

权限位沿用 M11：SELECT=1 / INSERT=2 / UPDATE=4 / DELETE=8 / ALL=15。

## SQL 语法扩展

```sql
-- 列级 / 库级授权与回收
GRANT  SELECT (col1, col2) ON t TO 'u';
GRANT  SELECT ON t (col1, col2) TO 'u';        -- 两种写法等价
GRANT  SELECT ON DATABASE * TO 'u';
GRANT  ALL   ON DATABASE sales TO 'u';
REVOKE UPDATE (col1) ON t FROM 'u';
REVOKE SELECT ON DATABASE * FROM 'u';

-- 角色管理
CREATE ROLE 'analyst';
DROP ROLE [IF EXISTS] 'analyst';
SHOW ROLES;

-- 角色内授权（角色名用 ROLE 关键字区分用户）
GRANT SELECT, UPDATE ON t TO ROLE 'analyst';
GRANT SELECT (col1) ON t TO ROLE 'analyst';
GRANT SELECT ON DATABASE * TO ROLE 'analyst';
REVOKE SELECT ON t FROM ROLE 'analyst';

-- 角色授予用户（幂等：重复授予静默成功）
GRANT ROLE 'analyst' TO 'alice';
REVOKE ROLE 'analyst' FROM 'alice';
```

解析器扩展：`GRANT ROLE '<r>' TO '<u>'` 与
`GRANT <priv> ON ... TO ROLE '<r>'` 两条路径；
`DATABASE` / `ROLE` 加入 lexer 关键字表。

## 权限判定：effectivePrivilege（聚合直授 + 角色）

校验顺序（任一命中即通过，M17 与 M11 行为向后兼容）：

1. `root` 超级用户全放行（不变）
2. 直授表级：`grants[user][table] & bit`
3. 直授库级：`dbgrants[user]` 非空且任一条目命中 bit（单库模型，`*`/具名等价）
4. 角色聚合：遍历 `userroles[user]` 中的每个角色，重复 2/3（角色的表级/库级）
5. 列级直授/角色列级：见下

### 列级校验规则（V1 务实边界，文档声明）

- SELECT：所需列 = SELECT 列表中显式裸列名（`Ref=="" && Agg=="" &&
  Case==nil && Window==nil` 的 `Col`）。若列表含 `*` / 聚合 / 表达式列，
  所需列无法完整确定 → 必须表级权限（列级不兜底）
- INSERT：所需列 = 显式 `Columns`（空 = 全列 → 需表级）；`ON DUPLICATE KEY
  UPDATE` 的赋值列并入所需列
- UPDATE：所需列 = `SET` 赋值列 + WHERE 条件列
- DELETE：不支持列级，仅表级
- 命中规则：所需列非空且全部在（直授 ∪ 角色）列级授权中有对应权限位
- WHERE/ORDER BY/GROUP BY 引用的列也计入所需列（`tablePrivOp` 提取）

## 级联清理（DROP 联动）

| 操作 | 清理 |
|------|------|
| DROP USER | 该用户表级/列级/库级直授（现有）+ 该用户角色关系（m:userroles 中移除）+ 角色内不清理（角色独立存在） |
| DROP TABLE | 该表所有用户/角色的表级、列级授权（现有 removeTableGrants 扩展） |
| DROP ROLE | m:roles 删除该角色 + 所有用户的 userroles 中移除该角色；不触碰其他角色 |

## 执行期接入

- `authz.go isAdminStmt`：角色语句（CREATE/DROP ROLE、GRANT/REVOKE ROLE、
  SHOW ROLES）与 `SHOW AUDITLOG` 归入 admin 语句（仅 root）
- `checkPrivilege`：DML 前先做库级/列级/角色聚合判定；权限不足返回
  `permission denied`（M11 同款文案）
- 审计：GRANT/REVOKE/角色语句经 PRIVILEGE 事件审计（T30_m17_audit.md）

## 测试要点

- 库级：`ON DATABASE *` 授权后新表自动可访问；REVOKE 后失效
- 列级：SELECT 显式列命中放行、未授权列拒绝；`SELECT *` 需表级；
  UPDATE 未授权列拒绝；INSERT 显式列命中放行
- 角色：CREATE ROLE → GRANT ROLE TO user → 用户获得角色权限；
  角色 REVOKE 权限/解除用户角色关系后失效；DROP ROLE 级联
- 权限聚合：直授 + 角色并存取并集；重复 GRANT ROLE 幂等
- DROP USER/TABLE 级联清理不残留孤儿授权
- M11 既有表级授权行为零回归（go test ./... 全绿）

## 兼容性

- `m:grants` 结构与序列化格式不变，旧数据可直接读取
- 无鉴权模式（Auth==nil）默认 root，全部新语法仅 root 可执行（admin）
- 库级/列级/角色全部为增量能力，未使用时不进入校验路径（短路）

## 实现状态（2026-10-13 闭环）

已实现并接线：

1. **executor 分发**：`executor.go` stmt 分发 switch 新增
   `case *CreateRoleStmt / *DropRoleStmt / *ShowRolesStmt / *ShowGrantsStmt`，
   分别调用 `execCreateRole/execDropRole/execShowRoles/execShowGrants`
   （`pkg/sql/rbac.go`），`CREATE ROLE / DROP ROLE [IF EXISTS] / SHOW ROLES /
   SHOW GRANTS FOR '<user>'` 可执行。
2. **解析器**：`parseCreate` 新增 ROLE 分支（接线 `parseCreateRole`）；
   `parseGrant/parseRevoke` 的 DATABASE 分支补齐 `TO/FROM ROLE '<role>'` 支持
   （库级授权可授予角色，与表级/列级对称）。
3. **权限聚合校验（修复聚合短路）**：`effectiveTableMaskTx` 原先在表级直授
   存在时提前返回（忽略库级/角色来源），已改为三路并集
   `tableMask | dbMask | roleMask`——用户直授只覆盖部分操作时，角色/库级
   授予的其余权限仍生效；`hasPrivilege`（grant.go）复用
   `hasEffectivePrivilege` 完成聚合；`checkPrivilege`（authz.go）先表级聚合、
   未过时逐列 `checkColumnPrivilege` 校验。
4. **级联清理**：`DROP USER` 清理表级（m:grants）+ 库级（m:dbgrants）+
   列级（m:colgrants）+ 用户-角色映射（m:userroles），角色目录保留；
   `DROP TABLE` 清理列级 + 角色内表授权（`removeRoleTableGrants`，角色保留）；
   `DROP ROLE` 清理角色目录 + 全部用户映射。
5. **测试**：新增 `pkg/sql/m17_rbac_test.go`（7 个测试函数，execTimed
   合成执行风格，沿用 m11 辅助函数）：
   - `TestM17RoleLifecycleShowDrop`：CREATE/DROP ROLE、SHOW ROLES、
     SHOW GRANTS 输出与 DROP ROLE 级联
   - `TestM17GrantRevokeDatabase`：库级授权/回收与拒绝
   - `TestM17GrantRevokeColumn`：列级授权/回收与拒绝（全列 INSERT 语义）
   - `TestM17RoleInheritanceAggregation`：角色表级/库级继承、直授+角色
     并集、REVOKE 表级/解除角色后拒绝
   - `TestM17DropUserCleansRbac`：DROP USER 后重建同名用户无残留权限
   - `TestM17DropTableCleansRbac`：DROP TABLE 后列级/角色表授权清理
   - `TestM17DropRoleInvalidatesPrivileges`：DROP ROLE 后用户权限失效
6. **全量验证**：`go test ./...` 全仓全绿，测试总数 **544**（含 M17 RBAC
   新增用例；基线 509）。
