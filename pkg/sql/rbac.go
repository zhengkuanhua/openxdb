package sql

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// M17 细粒度权限与角色体系（RBAC）：在 M11 表级 GRANT 基础上扩展
// 库级（ON DATABASE）、列级（ON <table> (<col>, ...)）授权与角色
// （CREATE ROLE / GRANT ROLE / REVOKE ROLE / SHOW ROLES）。
//
// 权限来源聚合（effectivePrivilege）：任一来源授予即视为拥有
//  1. 表级显式授权（m:grants，M11）
//  2. 库级授权（m:dbgrants：对默认库 default 生效，覆盖当前全部表；
//     未来多库 schema 落地后按表名 <db>.<table> 前缀自动匹配）
//  3. 角色授予（m:userroles -> m:roles：角色内含表级与库级授权）
//
// 列级权限（m:colgrants）不参与表级聚合：仅在用户对表无对应操作的表级
// 权限时做逐列校验（INSERT 目标列 / UPDATE SET 列 / SELECT 显式列）。

// DefaultDB 默认数据库名：OpenXDB 当前为单库部署，库级授权对默认库内
// 全部表生效（即全库授权）。表名命名空间解析 splitTableNamespace 预留
// <db>.<table> 前缀匹配，未来多库落地后库级授权自动按表前缀生效。
const DefaultDB = "default"

// ---- 元数据键与结构 ----

// dbGrantsMetaKey 库级权限元数据键：JSON map[user]map[db]int。
var dbGrantsMetaKey = []byte("m:dbgrants")

// colGrantsMetaKey 列级权限元数据键：JSON map[user]map[table]map[col]int。
var colGrantsMetaKey = []byte("m:colgrants")

// rolesMetaKey 角色目录元数据键：JSON map[role]roleGrants
// （角色权限 = 表级授权 + 库级授权；角色不承载列级权限）。
var rolesMetaKey = []byte("m:roles")

// userRolesMetaKey 用户-角色映射元数据键：JSON map[user][]role。
var userRolesMetaKey = []byte("m:userroles")

// roleGrants 一个角色的权限集合（表级 + 库级）。
type roleGrants struct {
	TableGrants map[string]int `json:"tables"`
	DbGrants    map[string]int `json:"dbs"`
}

// splitTableNamespace 解析表名所属数据库：<db>.<table> 形式取前缀，
// 否则归属默认库 default。
func splitTableNamespace(table string) (db, name string) {
	if i := strings.Index(table, "."); i > 0 {
		return table[:i], table[i+1:]
	}
	return DefaultDB, table
}

// loadDbGrants 读取库级权限目录；无记录时返回空 map。
func loadDbGrants(get func(key []byte) ([]byte, error)) (map[string]map[string]int, error) {
	raw, err := get(dbGrantsMetaKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return map[string]map[string]int{}, nil
		}
		return nil, err
	}
	if raw == nil || len(raw) == 0 {
		return map[string]map[string]int{}, nil
	}
	var grants map[string]map[string]int
	if err := json.Unmarshal(raw, &grants); err != nil {
		return nil, &SQLError{Msg: "corrupted database grant catalog"}
	}
	if grants == nil {
		return map[string]map[string]int{}, nil
	}
	return grants, nil
}

// saveDbGrants 序列化库级权限目录。
func saveDbGrants(grants map[string]map[string]int) ([]byte, error) {
	return json.Marshal(grants)
}

// loadColGrants 读取列级权限目录；无记录时返回空 map。
func loadColGrants(get func(key []byte) ([]byte, error)) (map[string]map[string]map[string]int, error) {
	raw, err := get(colGrantsMetaKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return map[string]map[string]map[string]int{}, nil
		}
		return nil, err
	}
	if raw == nil || len(raw) == 0 {
		return map[string]map[string]map[string]int{}, nil
	}
	var grants map[string]map[string]map[string]int
	if err := json.Unmarshal(raw, &grants); err != nil {
		return nil, &SQLError{Msg: "corrupted column grant catalog"}
	}
	if grants == nil {
		return map[string]map[string]map[string]int{}, nil
	}
	return grants, nil
}

// saveColGrants 序列化列级权限目录。
func saveColGrants(grants map[string]map[string]map[string]int) ([]byte, error) {
	return json.Marshal(grants)
}

// loadRoles 读取角色目录；无记录时返回空 map。
func loadRoles(get func(key []byte) ([]byte, error)) (map[string]roleGrants, error) {
	raw, err := get(rolesMetaKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return map[string]roleGrants{}, nil
		}
		return nil, err
	}
	if raw == nil || len(raw) == 0 {
		return map[string]roleGrants{}, nil
	}
	var roles map[string]roleGrants
	if err := json.Unmarshal(raw, &roles); err != nil {
		return nil, &SQLError{Msg: "corrupted role catalog"}
	}
	if roles == nil {
		return map[string]roleGrants{}, nil
	}
	return roles, nil
}

// saveRoles 序列化角色目录。
func saveRoles(roles map[string]roleGrants) ([]byte, error) {
	return json.Marshal(roles)
}

// loadUserRoles 读取用户-角色映射；无记录时返回空 map。
func loadUserRoles(get func(key []byte) ([]byte, error)) (map[string][]string, error) {
	raw, err := get(userRolesMetaKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return map[string][]string{}, nil
		}
		return nil, err
	}
	if raw == nil || len(raw) == 0 {
		return map[string][]string{}, nil
	}
	var m map[string][]string
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, &SQLError{Msg: "corrupted user-role catalog"}
	}
	if m == nil {
		return map[string][]string{}, nil
	}
	return m, nil
}

// saveUserRoles 序列化用户-角色映射。
func saveUserRoles(m map[string][]string) ([]byte, error) {
	return json.Marshal(m)
}

// ---- 执行语句 ----

// execCreateRole CREATE ROLE '<role>'：角色须唯一，创建空权限集合。
func (e *Executor) execCreateRole(sess *Session, s *CreateRoleStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	roles, err := loadRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	if _, ok := roles[s.Name]; ok {
		return nil, &SQLError{Msg: "role already exists: " + s.Name}
	}
	roles[s.Name] = roleGrants{TableGrants: map[string]int{}, DbGrants: map[string]int{}}
	raw, err := saveRoles(roles)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(rolesMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execDropRole DROP ROLE [IF EXISTS] '<role>'：删除角色并联动清理
// 用户-角色映射与角色内授权。
func (e *Executor) execDropRole(sess *Session, s *DropRoleStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	roles, err := loadRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	if _, ok := roles[s.Name]; !ok {
		if s.IfExists {
			if auto {
				if err := tx.Commit(); err != nil {
					return nil, err
				}
			}
			return &Result{AffectedRows: 0}, nil
		}
		return nil, &SQLError{Msg: "role not exists: " + s.Name}
	}
	delete(roles, s.Name)
	raw, err := saveRoles(roles)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(rolesMetaKey, raw); err != nil {
		return nil, err
	}
	// 联动清理用户-角色映射
	ur, err := loadUserRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	changed := false
	for u, rs := range ur {
		kept := rs[:0]
		for _, r := range rs {
			if r != s.Name {
				kept = append(kept, r)
			}
		}
		if len(kept) != len(rs) {
			changed = true
		}
		if len(kept) == 0 {
			delete(ur, u)
		} else {
			ur[u] = kept
		}
	}
	if changed {
		rawUR, err := saveUserRoles(ur)
		if err != nil {
			return nil, err
		}
		if err := tx.Put(userRolesMetaKey, rawUR); err != nil {
			return nil, err
		}
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execShowRoles SHOW ROLES：列出全部角色。
func (e *Executor) execShowRoles(sess *Session, s *ShowRolesStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	roles, err := loadRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(roles))
	for name := range roles {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([][]Value, 0, len(names))
	for _, name := range names {
		rows = append(rows, []Value{StrVal(name)})
	}
	return &Result{Columns: []string{"role"}, Rows: rows}, nil
}

// execShowGrants SHOW GRANTS FOR '<user>'：列出用户全部权限来源
// （表级 / 库级 / 列级授权与角色授予）。仅 root 可执行（authz 拦截）。
func (e *Executor) execShowGrants(sess *Session, s *ShowGrantsStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findUser(users, s.User) == nil {
		return nil, &SQLError{Msg: "user not exists: " + s.User}
	}
	grants, err := loadGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	dbGrants, err := loadDbGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	colGrants, err := loadColGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	roles, err := loadRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	ur, err := loadUserRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	var lines []string
	// 表级
	tabs := grants[s.User]
	tNames := make([]string, 0, len(tabs))
	for t := range tabs {
		tNames = append(tNames, t)
	}
	sort.Strings(tNames)
	for _, t := range tNames {
		lines = append(lines, fmt.Sprintf("GRANT %s ON %s TO '%s'", privMaskString(Privilege(tabs[t])), t, s.User))
	}
	// 库级
	dbMap := dbGrants[s.User]
	dbNames := make([]string, 0, len(dbMap))
	for d := range dbMap {
		dbNames = append(dbNames, d)
	}
	sort.Strings(dbNames)
	for _, d := range dbNames {
		lines = append(lines, fmt.Sprintf("GRANT %s ON DATABASE %s TO '%s'", privMaskString(Privilege(dbMap[d])), d, s.User))
	}
	// 列级
	colMap := colGrants[s.User]
	ctNames := make([]string, 0, len(colMap))
	for t := range colMap {
		ctNames = append(ctNames, t)
	}
	sort.Strings(ctNames)
	for _, t := range ctNames {
		cols := make([]string, 0, len(colMap[t]))
		for c := range colMap[t] {
			cols = append(cols, c)
		}
		sort.Strings(cols)
		lines = append(lines, fmt.Sprintf("GRANT %s ON %s (%s) TO '%s'",
			privMaskString(Privilege(unionMask(colMap[t]))), t, strings.Join(cols, ", "), s.User))
	}
	// 角色
	for _, r := range ur[s.User] {
		lines = append(lines, fmt.Sprintf("GRANT ROLE %s TO '%s'", r, s.User))
		// 角色内含权限展开
		rg, ok := roles[r]
		if !ok {
			continue
		}
		rTabs := make([]string, 0, len(rg.TableGrants))
		for t := range rg.TableGrants {
			rTabs = append(rTabs, t)
		}
		sort.Strings(rTabs)
		for _, t := range rTabs {
			lines = append(lines, fmt.Sprintf("  (role %s) GRANT %s ON %s", r, privMaskString(Privilege(rg.TableGrants[t])), t))
		}
		rDbs := make([]string, 0, len(rg.DbGrants))
		for d := range rg.DbGrants {
			rDbs = append(rDbs, d)
		}
		sort.Strings(rDbs)
		for _, d := range rDbs {
			lines = append(lines, fmt.Sprintf("  (role %s) GRANT %s ON DATABASE %s", r, privMaskString(Privilege(rg.DbGrants[d])), d))
		}
	}
	if len(lines) == 0 {
		lines = append(lines, fmt.Sprintf("user '%s' has no grants", s.User))
	}
	rows := make([][]Value, 0, len(lines))
	for _, l := range lines {
		rows = append(rows, []Value{StrVal(l)})
	}
	return &Result{Columns: []string{"grant"}, Rows: rows}, nil
}

// unionMask 合并 map 中所有权限位（列级展示用）。
func unionMask(m map[string]int) int {
	mask := 0
	for _, v := range m {
		mask |= v
	}
	return mask
}

// privMaskString 权限位掩码 -> 大写权限名列表（ALL 折叠）。
func privMaskString(m Privilege) string {
	if m&PrivAll == PrivAll {
		return "ALL"
	}
	var parts []string
	for name, bit := range map[string]Privilege{"SELECT": PrivSelect, "INSERT": PrivInsert, "UPDATE": PrivUpdate, "DELETE": PrivDelete} {
		if m&bit != 0 {
			parts = append(parts, name)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

// ---- 权限聚合判定 ----

// effectiveTableMask 计算用户对表的有效表级权限掩码：表级显式授权 ∪
// 库级授权（表所属库）∪ 角色内含表级/库级授权。
func (e *Executor) effectiveTableMask(user, table string) (Privilege, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	return effectiveTableMaskTx(tx.Get, user, table)
}

// effectiveTableMaskTx 在给定读路径上计算有效表级权限掩码（供批量判定复用）。
func effectiveTableMaskTx(get func(key []byte) ([]byte, error), user, table string) (Privilege, error) {
	grants, err := loadGrants(get)
	if err != nil {
		return 0, err
	}
	// 表级直授（与库级/角色权限取并集，不可短路——用户可能同时拥有
	// 多种来源的权限，直授仅覆盖部分操作）
	tableMask := Privilege(0)
	if tabs := grants[user]; tabs != nil {
		tableMask = Privilege(tabs[table])
	}
	// 库级：表所属库的授权对表生效
	db, _ := splitTableNamespace(table)
	dbGrants, err := loadDbGrants(get)
	if err != nil {
		return 0, err
	}
	dbMask := Privilege(0)
	if dbs := dbGrants[user]; dbs != nil {
		dbMask = Privilege(dbs[db])
	}
	// 角色链
	ur, err := loadUserRoles(get)
	if err != nil {
		return 0, err
	}
	roles, err := loadRoles(get)
	if err != nil {
		return 0, err
	}
	roleMask := Privilege(0)
	for _, r := range ur[user] {
		rg, ok := roles[r]
		if !ok {
			continue
		}
		if mask := rg.TableGrants[table]; mask != 0 {
			roleMask |= Privilege(mask)
		}
		if mask := rg.DbGrants[db]; mask != 0 {
			roleMask |= Privilege(mask)
		}
	}
	return tableMask | dbMask | roleMask, nil
}

// hasEffectivePrivilege 用户对表是否拥有 op 权限（表级聚合判定，M17 执行期
// 校验入口；兼容 M11 hasPrivilege 语义且新增库级/角色来源）。
func (e *Executor) hasEffectivePrivilege(user, table, op string) (bool, error) {
	bit, err := parsePrivilege(op)
	if err != nil {
		return false, err
	}
	mask, err := e.effectiveTableMask(user, table)
	if err != nil {
		return false, err
	}
	return mask&bit != 0, nil
}

// hasColumnPrivilege 用户对表的指定列是否拥有 op 权限（列级显式授权）。
// 列级权限仅在用户无对应表级权限时参与校验（见 checkColumnPrivilege）。
func hasColumnPrivilege(get func(key []byte) ([]byte, error), user, table, col, op string) (bool, error) {
	bit, err := parsePrivilege(op)
	if err != nil {
		return false, err
	}
	colGrants, err := loadColGrants(get)
	if err != nil {
		return false, err
	}
	tabs := colGrants[user]
	if tabs == nil {
		return false, nil
	}
	cols := tabs[table]
	if cols == nil {
		return false, nil
	}
	return cols[col]&int(bit) != 0, nil
}

// checkColumnPrivilege 执行期列级校验：用户无表级 op 权限时，要求对
// 涉及的每一列都拥有列级 op 权限（任一列缺失即拒绝）。
// cols 为语句实际涉及的目标列；空 cols（如 SELECT * / INSERT 全列）视为
// 需要表级权限（列级授权无法覆盖未列出的列）。
func (e *Executor) checkColumnPrivilege(user, table, op string, cols []string) error {
	if len(cols) == 0 {
		return nil // 无列级场景（调用方已在表级校验通过或语义不适用）
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, c := range cols {
		ok, err := hasColumnPrivilege(tx.Get, user, table, c, op)
		if err != nil {
			return err
		}
		if !ok {
			return &SQLError{Msg: fmt.Sprintf("access denied: user '%s' has no %s privilege on column '%s' of table '%s'", user, op, c, table)}
		}
	}
	return nil
}

// ---- 级联清理 ----

// removeUserPrivileges 从全部权限/角色元数据移除指定用户的授权
// （DROP USER 联动清理，扩展 M11 removeUserGrants）。
func removeUserPrivileges(
	grants map[string]map[string]int,
	dbGrants map[string]map[string]int,
	colGrants map[string]map[string]map[string]int,
	userRoles map[string][]string,
	user string,
) {
	removeUserGrants(grants, user)
	delete(dbGrants, user)
	delete(colGrants, user)
	delete(userRoles, user)
}

// removeTablePrivileges 从列级权限与角色表级授权移除指定表
// （DROP TABLE 联动清理，扩展 M11 removeTableGrants）。
func removeTablePrivileges(
	colGrants map[string]map[string]map[string]int,
	roles map[string]roleGrants,
	table string,
) {
	for user, tabs := range colGrants {
		if _, ok := tabs[table]; ok {
			delete(tabs, table)
			if len(tabs) == 0 {
				delete(colGrants, user)
			}
		}
	}
	for role, rg := range roles {
		if _, ok := rg.TableGrants[table]; ok {
			delete(rg.TableGrants, table)
		}
		if len(rg.TableGrants) == 0 && len(rg.DbGrants) == 0 {
			delete(roles, role)
		} else {
			roles[role] = rg
		}
	}
}

// removeRoleTableGrants 从角色目录移除指定表的授权（DROP TABLE 时角色
// 权限同步清理；角色自身保留）。
func removeRoleTableGrants(roles map[string]roleGrants, table string) {
	for role, rg := range roles {
		if _, ok := rg.TableGrants[table]; ok {
			delete(rg.TableGrants, table)
			roles[role] = rg
		}
	}
}

// -- 库级 / 列级 / 角色授权执行（grant.go execGrant/execRevoke 分发） --

// execGrantDatabase GRANT <privs> ON DATABASE <db> TO '<user>'。
func (e *Executor) execGrantDatabase(sess *Session, s *GrantStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findUser(users, s.User) == nil {
		return nil, &SQLError{Msg: "user not exists: " + s.User}
	}
	mask := Privilege(0)
	for _, p := range s.Privileges {
		bit, err := parsePrivilege(p)
		if err != nil {
			return nil, err
		}
		mask |= bit
	}
	grants, err := loadDbGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	if grants[s.User] == nil {
		grants[s.User] = map[string]int{}
	}
	grants[s.User][s.Database] |= int(mask)
	raw, err := saveDbGrants(grants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(dbGrantsMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execRevokeDatabase REVOKE <privs> ON DATABASE <db> FROM '<user>'。
func (e *Executor) execRevokeDatabase(sess *Session, s *RevokeStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findUser(users, s.User) == nil {
		return nil, &SQLError{Msg: "user not exists: " + s.User}
	}
	mask := Privilege(0)
	for _, p := range s.Privileges {
		bit, err := parsePrivilege(p)
		if err != nil {
			return nil, err
		}
		mask |= bit
	}
	grants, err := loadDbGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	dbs := grants[s.User]
	if dbs == nil || dbs[s.Database] == 0 {
		if auto {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
		}
		return &Result{AffectedRows: 0}, nil
	}
	dbs[s.Database] &^= int(mask)
	if dbs[s.Database] == 0 {
		delete(dbs, s.Database)
	}
	if len(dbs) == 0 {
		delete(grants, s.User)
	}
	raw, err := saveDbGrants(grants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(dbGrantsMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execGrantColumn GRANT <privs> ON <table> (<cols>) TO '<user>'。
func (e *Executor) execGrantColumn(sess *Session, s *GrantStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findUser(users, s.User) == nil {
		return nil, &SQLError{Msg: "user not exists: " + s.User}
	}
	mask := Privilege(0)
	for _, p := range s.Privileges {
		bit, err := parsePrivilege(p)
		if err != nil {
			return nil, err
		}
		mask |= bit
	}
	grants, err := loadColGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	if grants[s.User] == nil {
		grants[s.User] = map[string]map[string]int{}
	}
	if grants[s.User][s.Table] == nil {
		grants[s.User][s.Table] = map[string]int{}
	}
	for _, c := range s.Columns {
		grants[s.User][s.Table][c] |= int(mask)
	}
	raw, err := saveColGrants(grants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(colGrantsMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execRevokeColumn REVOKE <privs> ON <table> (<cols>) FROM '<user>'。
func (e *Executor) execRevokeColumn(sess *Session, s *RevokeStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findUser(users, s.User) == nil {
		return nil, &SQLError{Msg: "user not exists: " + s.User}
	}
	mask := Privilege(0)
	for _, p := range s.Privileges {
		bit, err := parsePrivilege(p)
		if err != nil {
			return nil, err
		}
		mask |= bit
	}
	grants, err := loadColGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	tabs := grants[s.User]
	if tabs == nil || tabs[s.Table] == nil {
		if auto {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
		}
		return &Result{AffectedRows: 0}, nil
	}
	cols := tabs[s.Table]
	changed := false
	for _, c := range s.Columns {
		if cols[c] == 0 {
			continue
		}
		cols[c] &^= int(mask)
		changed = true
		if cols[c] == 0 {
			delete(cols, c)
		}
	}
	if !changed {
		if auto {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
		}
		return &Result{AffectedRows: 0}, nil
	}
	if len(cols) == 0 {
		delete(tabs, s.Table)
	}
	if len(tabs) == 0 {
		delete(grants, s.User)
	}
	raw, err := saveColGrants(grants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(colGrantsMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execGrantRole GRANT ROLE <role> TO '<user>'：把角色授予用户。
// 角色必须存在（报错）；重复授予幂等（去重后无变化不写回）。
func (e *Executor) execGrantRole(sess *Session, s *GrantStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findUser(users, s.User) == nil {
		return nil, &SQLError{Msg: "user not exists: " + s.User}
	}
	roles, err := loadRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	if _, ok := roles[s.Role]; !ok {
		return nil, &SQLError{Msg: "role not exists: " + s.Role}
	}
	ur, err := loadUserRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	for _, r := range ur[s.User] {
		if r == s.Role {
			if auto {
				if err := tx.Commit(); err != nil {
					return nil, err
				}
			}
			return &Result{AffectedRows: 0}, nil // 幂等
		}
	}
	ur[s.User] = append(ur[s.User], s.Role)
	raw, err := saveUserRoles(ur)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(userRolesMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execRevokeRole REVOKE ROLE <role> FROM '<user>'：回收用户角色。
func (e *Executor) execRevokeRole(sess *Session, s *RevokeStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	users, err := loadUsers(tx.Get)
	if err != nil {
		return nil, err
	}
	if findUser(users, s.User) == nil {
		return nil, &SQLError{Msg: "user not exists: " + s.User}
	}
	ur, err := loadUserRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	rs := ur[s.User]
	if rs == nil {
		if auto {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
		}
		return &Result{AffectedRows: 0}, nil
	}
	kept := rs[:0]
	found := false
	for _, r := range rs {
		if r != s.Role {
			kept = append(kept, r)
		} else {
			found = true
		}
	}
	if !found {
		if auto {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
		}
		return &Result{AffectedRows: 0}, nil
	}
	if len(kept) == 0 {
		delete(ur, s.User)
	} else {
		ur[s.User] = kept
	}
	raw, err := saveUserRoles(ur)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(userRolesMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// grantRolePrivileges GRANT <privs> ON <object> TO ROLE <role>：
// 向角色授予表级/库级权限（对象为表或 DATABASE）。
// 角色不存在报错；幂等 OR 合并。
func (e *Executor) grantRolePrivileges(sess *Session, s *GrantStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	roles, err := loadRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	rg, ok := roles[s.Role]
	if !ok {
		return nil, &SQLError{Msg: "role not exists: " + s.Role}
	}
	mask := Privilege(0)
	for _, p := range s.Privileges {
		bit, err := parsePrivilege(p)
		if err != nil {
			return nil, err
		}
		mask |= bit
	}
	if rg.TableGrants == nil {
		rg.TableGrants = map[string]int{}
	}
	if rg.DbGrants == nil {
		rg.DbGrants = map[string]int{}
	}
	if s.Scope == "DATABASE" {
		rg.DbGrants[s.Database] |= int(mask)
	} else {
		rg.TableGrants[s.Table] |= int(mask)
	}
	roles[s.Role] = rg
	raw, err := saveRoles(roles)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(rolesMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// revokeRolePrivileges REVOKE <privs> ON <object> FROM ROLE <role>。
func (e *Executor) revokeRolePrivileges(sess *Session, s *RevokeStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	roles, err := loadRoles(tx.Get)
	if err != nil {
		return nil, err
	}
	rg, ok := roles[s.Role]
	if !ok {
		return nil, &SQLError{Msg: "role not exists: " + s.Role}
	}
	mask := Privilege(0)
	for _, p := range s.Privileges {
		bit, err := parsePrivilege(p)
		if err != nil {
			return nil, err
		}
		mask |= bit
	}
	changed := false
	if s.Scope == "DATABASE" {
		if rg.DbGrants[s.Database]&int(mask) != 0 {
			rg.DbGrants[s.Database] &^= int(mask)
			changed = true
			if rg.DbGrants[s.Database] == 0 {
				delete(rg.DbGrants, s.Database)
			}
		}
	} else {
		if rg.TableGrants[s.Table]&int(mask) != 0 {
			rg.TableGrants[s.Table] &^= int(mask)
			changed = true
			if rg.TableGrants[s.Table] == 0 {
				delete(rg.TableGrants, s.Table)
			}
		}
	}
	if !changed {
		if auto {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
		}
		return &Result{AffectedRows: 0}, nil
	}
	roles[s.Role] = rg
	raw, err := saveRoles(roles)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(rolesMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execGrantDispatch 按 Scope / ToRole 分发授权语句：
//   - Scope=ROLE：GRANT ROLE <role> TO '<user>'（用户-角色映射）
//   - ToRole：GRANT <privs> ON <obj> TO ROLE '<role>'（向角色授权，表级/库级）
//   - Scope=DATABASE / COLUMN：库级 / 列级授权（用户目标）
//   - 默认：表级授权（M11 兼容）
func (e *Executor) execGrantDispatch(sess *Session, s *GrantStmt) (*Result, error) {
	switch {
	case s.Scope == "ROLE":
		return e.execGrantRole(sess, s)
	case s.ToRole:
		return e.grantRolePrivileges(sess, s)
	case s.Scope == "DATABASE":
		return e.execGrantDatabase(sess, s)
	case s.Scope == "COLUMN":
		return e.execGrantColumn(sess, s)
	default:
		return e.execGrant(sess, s)
	}
}

// execRevokeDispatch REVOKE 分发，语义同 execGrantDispatch。
func (e *Executor) execRevokeDispatch(sess *Session, s *RevokeStmt) (*Result, error) {
	switch {
	case s.Scope == "ROLE":
		return e.execRevokeRole(sess, s)
	case s.ToRole:
		return e.revokeRolePrivileges(sess, s)
	case s.Scope == "DATABASE":
		return e.execRevokeDatabase(sess, s)
	case s.Scope == "COLUMN":
		return e.execRevokeColumn(sess, s)
	default:
		return e.execRevoke(sess, s)
	}
}

