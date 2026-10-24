package sql

// T39 多库元数据：CREATE DATABASE / SHOW DATABASES 支持。
//
// 仓库为单库模型（默认库 default 恒存在，表目录单一 meta 键 m:tables）。
// 新增库仅登记在数据库目录键 m:databases（库名列表），不参与表目录；
// default 库恒存在但不在目录中落盘（SHOW DATABASES 时动态前置）。
// 数据库目录经 WAL 原子持久化；跨节点目录广播不在 T34 表级 DDL 广播范围内
// （库目录属全局命名元数据，本节点为权威，单机/多机部署均零回归）。

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// defaultDB 默认库名（恒存在，禁止删除语义；单库模型兼容入口）。
const defaultDB = "default"

// dbKey 数据库目录键。
var dbKey = []byte("m:databases")

// loadDatabases 读取数据库目录（无目录时返回空）。
func loadDatabases(get func(key []byte) ([]byte, error)) ([]string, error) {
	raw, err := get(dbKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	if raw == nil || len(raw) == 0 {
		return nil, nil
	}
	var dbs []string
	if err := json.Unmarshal(raw, &dbs); err != nil {
		return nil, &SQLError{Msg: "corrupted database catalog"}
	}
	return dbs, nil
}

// saveDatabases 序列化数据库目录（排序后落盘，保证目录确定性）。
func saveDatabases(dbs []string) ([]byte, error) {
	sort.Strings(dbs)
	return json.Marshal(dbs)
}

// findDatabase 按名查找（区分大小写，与表名语义一致）。
func findDatabase(dbs []string, name string) bool {
	for _, d := range dbs {
		if d == name {
			return true
		}
	}
	return false
}

// allDatabases 返回 SHOW DATABASES 可见目录：默认库 default 前置 + 已创建库排序。
func allDatabases(dbs []string) []string {
	out := []string{defaultDB}
	for _, d := range dbs {
		if d != defaultDB {
			out = append(out, d)
		}
	}
	sort.Strings(out[1:])
	return out
}

// normalizeDBName 校验库名合法性：非空、不含点号与空白。
func normalizeDBName(name string) error {
	if name == "" {
		return &SQLError{Msg: "invalid database name"}
	}
	if strings.ContainsAny(name, ". \t\r\n") {
		return &SQLError{Msg: "invalid database name: " + name}
	}
	return nil
}
