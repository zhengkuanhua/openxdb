package sql

import "fmt"

// M11 子功能 3：SQL 执行期权限校验。
//
// 与 server 层连接鉴权（AUTH 握手）配合构成完整鉴权链路：
//   - 超级用户（内置 root 标识）拥有全部权限，跳过一切校验；
//   - 管理语句（CREATE/DROP/ALTER/GRANT/REVOKE/BACKUP/RESTORE/IMPORT/EXPORT/
//     ADD NODE/SPLIT REGION/ASSIGN REGION/BALANCE 等）仅 root 可执行；
//   - 非管理语句（SELECT/INSERT/UPDATE/DELETE）执行前经 HasPrivilege 检查
//     对应表级权限，无授权返回权限拒绝错误；
//   - SHOW/EXPLAIN/SET 与事务控制（BEGIN/COMMIT/ROLLBACK）不做权限门槛
//     （仅元数据/会话级操作，不触碰业务数据）。
//
// 取舍（详见 docs/T24_m11_auth.md）：
//   - 权限校验作用于 SELECT 的 FROM 主表；JOIN/子查询/视图展开内部的辅助表
//     不做逐表校验（V1 简化，后续可扩展为逐表展开校验）；
//   - 表不存在与无授权统一返回权限拒绝，避免通过错误差异泄露元数据；
//   - 降级 guest（user 为空串）视为无任何授权：管理语句与 DML 一律拒绝。

// SuperUser 超级用户标识：管理语句仅该用户可执行。连接鉴权仍以 m:users
// 为准（root 需存在且密码匹配才可登录）；首次部署需在无鉴权/降级模式下
// 执行 CREATE USER 'root' IDENTIFIED BY '...' 建立（见 docs/T24_m11_auth.md）。
const SuperUser = "root"

// permissionDenied 构造权限拒绝错误（统一前缀，便于协议层透出与测试断言）。
func permissionDenied(msg string) error {
	return &SQLError{Msg: "permission denied: " + msg}
}

// checkPrivilege 执行期权限校验：user 为当前会话用户。
// root 全放行；管理语句仅 root；DML 按目标表与操作查 HasPrivilege；
// 其余语句（SHOW/EXPLAIN/SET/事务控制）放行。
func (e *Executor) checkPrivilege(stmt Stmt, user string) error {
	if user == SuperUser {
		return nil
	}
	if isAdminStmt(stmt) {
		return permissionDenied(fmt.Sprintf("user %q is not superuser (admin statement requires %q)", user, SuperUser))
	}
	if table, op, ok := tablePrivOp(stmt); ok {
		has, err := e.hasPrivilege(user, table, op)
		if err != nil {
			return err
		}
		if !has {
			return permissionDenied(fmt.Sprintf("user %q has no %s privilege on table %q", user, op, table))
		}
	}
	return nil
}

// isAdminStmt 判断语句是否属于管理语句（仅超级用户可执行）。
// 覆盖 DDL（表/索引/视图）、用户管理、授权、导入导出与运维写操作。
func isAdminStmt(stmt Stmt) bool {
	switch stmt.(type) {
	case *CreateTableStmt, *DropTableStmt, *CreateIndexStmt, *DropIndexStmt,
		*AlterTableStmt, *CreateViewStmt, *DropViewStmt,
		*CreateUserStmt, *DropUserStmt, *AlterUserStmt,
		*GrantStmt, *RevokeStmt,
		*ImportStmt, *ExportStmt, *BackupStmt, *RestoreStmt,
		*AddNodeStmt, *SplitRegionStmt, *AssignRegionStmt, *BalanceStmt:
		return true
	}
	return false
}

// tablePrivOp 返回 DML 语句的目标表与所需权限；非 DML 返回 ok=false。
// SELECT 以 FROM 主表为准（FROM 为空即子查询/纯表达式查询时不做表校验）。
func tablePrivOp(stmt Stmt) (table, op string, ok bool) {
	switch s := stmt.(type) {
	case *SelectStmt:
		if s.From != "" {
			return s.From, "SELECT", true
		}
	case *InsertStmt:
		return s.Table, "INSERT", true
	case *UpdateStmt:
		return s.Table, "UPDATE", true
	case *DeleteStmt:
		return s.Table, "DELETE", true
	}
	return "", "", false
}
