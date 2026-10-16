package sql_test

// T34 节点侧 DDL apply 单元测试（sql 层）。
// 覆盖：远端 CREATE/ALTER 元数据 upsert 与 DROP 删除（按 TableID 幂等）、
// 非法 Meta / 未知 op 拒绝。

import (
	"encoding/json"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// mkRemoteMeta 构造 TableMeta JSON（TableID 固定 1，测试幂等复用）。
func mkRemoteMeta(cols ...sql.ColumnDef) []byte {
	b, err := json.Marshal(sql.TableMeta{ID: 1, Name: "rt", Columns: cols, PK: "id"})
	if err != nil {
		panic(err)
	}
	return b
}

func TestT34ApplyRemoteDDLUpsertAndDrop(t *testing.T) {
	e := newEngine(t)

	// CREATE 广播：节点侧注入新表
	createReq := cluster.DDLPushReq{DDLID: "c1", Op: cluster.DDLOpCreate, TableID: 1, Name: "rt",
		Meta: mkRemoteMeta(sql.ColumnDef{Name: "id", Type: "INT"}, sql.ColumnDef{Name: "name", Type: "TEXT"})}
	if err := e.ApplyRemoteDDL(createReq); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	mustExec(t, e, "INSERT INTO rt (id, name) VALUES (1, 'a')")
	rows := mustRows(t, e, "SELECT name FROM rt WHERE id = 1")
	if len(rows) != 1 || rows[0][0].S != "a" {
		t.Fatalf("row after remote create: %+v", rows)
	}

	// 幂等：同 TableID 重复应用不报错、不产生重复目录项
	if err := e.ApplyRemoteDDL(createReq); err != nil {
		t.Fatalf("re-apply create: %v", err)
	}
	mustExec(t, e, "INSERT INTO rt (id, name) VALUES (3, 'c')") // 重复应用后表仍可用

	// ALTER 广播：按 TableID 覆盖为新列集合
	alterReq := cluster.DDLPushReq{DDLID: "c2", Op: cluster.DDLOpAlter, TableID: 1, Name: "rt",
		Meta: mkRemoteMeta(
			sql.ColumnDef{Name: "id", Type: "INT"},
			sql.ColumnDef{Name: "name", Type: "TEXT"},
			sql.ColumnDef{Name: "score", Type: "INT"},
		)}
	if err := e.ApplyRemoteDDL(alterReq); err != nil {
		t.Fatalf("apply alter: %v", err)
	}
	mustExec(t, e, "INSERT INTO rt (id, name, score) VALUES (2, 'b', 9)")
	rows = mustRows(t, e, "SELECT score FROM rt WHERE id = 2")
	if len(rows) != 1 || rows[0][0].I != 9 {
		t.Fatalf("score after remote alter: %+v", rows)
	}

	// DROP 广播：按 TableID 删除目录项
	if err := e.ApplyRemoteDDL(cluster.DDLPushReq{DDLID: "c3", Op: cluster.DDLOpDrop, TableID: 1, Name: "rt"}); err != nil {
		t.Fatalf("apply drop: %v", err)
	}
	mustErr(t, e, "SELECT * FROM rt", "table not exists")
}

func TestT34ApplyRemoteDDLRejectsBadMetaAndUnknownOp(t *testing.T) {
	e := newEngine(t)
	if err := e.ApplyRemoteDDL(cluster.DDLPushReq{Op: cluster.DDLOpCreate, TableID: 1, Name: "rt", Meta: []byte("not-json")}); err == nil {
		t.Fatalf("bad meta: want error")
	}
	if err := e.ApplyRemoteDDL(cluster.DDLPushReq{Op: "explode", TableID: 1, Name: "rt"}); err == nil {
		t.Fatalf("unknown op: want error")
	}
}
