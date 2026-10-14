package sql

import (
	"encoding/json"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// Privilege 表级权限位（M11 权限模型，GRANT/REVOKE ... ON <table>）。
type Privilege int

const (
	// PrivSelect SELECT 权限。
	PrivSelect Privilege = 1 << iota
	// PrivInsert INSERT 权限。
	PrivInsert
	// PrivUpdate UPDATE 权限。
	PrivUpdate
	// PrivDelete DELETE 权限。
	PrivDelete
)

// PrivAll 全部表级权限（GRANT ALL ON ...）。
const PrivAll = PrivSelect | PrivInsert | PrivUpdate | PrivDelete

// grantsMetaKey 权限元数据键（全局元数据，与 m:users / m:tables 同级）。
// 存储结构：JSON map[user]map[table]int（表级权限位掩码，位见 Privilege）。
var grantsMetaKey = []byte("m:grants")

// loadGrants 读取权限目录；无记录时返回空 map（便于直接写入）。
func loadGrants(get func(key []byte) ([]byte, error)) (map[string]map[string]int, error) {
	raw, err := get(grantsMetaKey)
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
		return nil, &SQLError{Msg: "corrupted grant catalog"}
	}
	if grants == nil {
		return map[string]map[string]int{}, nil
	}
	return grants, nil
}

// saveGrants 序列化权限目录。
func saveGrants(grants map[string]map[string]int) ([]byte, error) {
	return json.Marshal(grants)
}

// parsePrivilege 将大写权限名转为位掩码（SELECT/INSERT/UPDATE/DELETE/ALL）。
func parsePrivilege(priv string) (Privilege, error) {
	switch priv {
	case "SELECT":
		return PrivSelect, nil
	case "INSERT":
		return PrivInsert, nil
	case "UPDATE":
		return PrivUpdate, nil
	case "DELETE":
		return PrivDelete, nil
	case "ALL":
		return PrivAll, nil
	}
	return 0, &SQLError{Msg: "unsupported privilege: " + priv}
}

// execGrant GRANT <priv[,priv...]> ON <table> TO '<user>'。
// 语义：授权用户必须存在（否则报错）；表存在性宽松处理（不校验，
// 见 T24_m11_grant.md 边界说明）；重复 GRANT 幂等（权限位 OR 合并）。
func (e *Executor) execGrant(sess *Session, s *GrantStmt) (*Result, error) {
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
	grants, err := loadGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	if grants[s.User] == nil {
		grants[s.User] = map[string]int{}
	}
	grants[s.User][s.Table] |= int(mask)
	raw, err := saveGrants(grants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(grantsMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// execRevoke REVOKE <priv[,priv...]> ON <table> FROM '<user>'。
// 语义：用户必须存在（否则报错）；未授予的权限 REVOKE 幂等成功
// （清除对应位；位清零后移除表条目与空用户条目，无变化也不报错）。
func (e *Executor) execRevoke(sess *Session, s *RevokeStmt) (*Result, error) {
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
	grants, err := loadGrants(tx.Get)
	if err != nil {
		return nil, err
	}
	tabs := grants[s.User]
	if tabs == nil {
		// 该用户尚无任何授权：幂等成功，无需写回。
		if auto {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
		}
		return &Result{AffectedRows: 0}, nil
	}
	if _, ok := tabs[s.Table]; !ok {
		// 该表未授权：幂等成功，无需写回。
		if auto {
			if err := tx.Commit(); err != nil {
				return nil, err
			}
		}
		return &Result{AffectedRows: 0}, nil
	}
	tabs[s.Table] &^= int(mask)
	if tabs[s.Table] == 0 {
		delete(tabs, s.Table)
	}
	if len(tabs) == 0 {
		delete(grants, s.User)
	}
	raw, err := saveGrants(grants)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(grantsMetaKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 0}, nil
}

// hasPrivilege 查询用户对某表是否拥有指定权限（执行期校验预留，M17 聚合）。
// op 为 "SELECT"/"INSERT"/"UPDATE"/"DELETE"；用户/表无授权记录返回 false。
// M17 扩展：聚合直授表级/库级授权与角色内含授权（同 effectiveTableMask）。
func (e *Executor) hasPrivilege(user, table, op string) (bool, error) {
	return e.hasEffectivePrivilege(user, table, op)
}

// removeUserGrants 从权限目录移除指定用户的全部授权（DROP USER 联动清理）。
func removeUserGrants(grants map[string]map[string]int, user string) {
	delete(grants, user)
}

// removeTableGrants 从权限目录移除指定表的所有用户授权（DROP TABLE 联动清理）。
// 表条目清空后连带移除空用户条目，保持目录无空洞。
func removeTableGrants(grants map[string]map[string]int, table string) {
	for user, tabs := range grants {
		if _, ok := tabs[table]; ok {
			delete(tabs, table)
			if len(tabs) == 0 {
				delete(grants, user)
			}
		}
	}
}
