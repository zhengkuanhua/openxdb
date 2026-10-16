package cluster_test

// T34 跨节点 DDL 广播协议测试（cluster 层）。
// 覆盖：DDL_PUSH 载荷 JSON 往返、协调者 -> 节点推送成功（节点 apply 回调收到请求）、
// apply 回调失败回执错误、未注册节点推送失败。

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
)

func TestDDLPushReqJSONRoundTrip(t *testing.T) {
	req := cluster.DDLPushReq{DDLID: "ddl-1", Op: cluster.DDLOpCreate, TableID: 42, Name: "t1", Meta: []byte(`{"id":42,"name":"t1"}`)}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got cluster.DDLPushReq
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.DDLID != req.DDLID || got.Op != req.Op || got.TableID != req.TableID || got.Name != req.Name || string(got.Meta) != string(req.Meta) {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, req)
	}
}

func TestManagerPushDDLAppliesOnNode(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	_, srvB, _, _ := newTestNode(t, "node-b", nil)
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	var got cluster.DDLPushReq
	srvB.SetDDLApply(func(req cluster.DDLPushReq) error {
		got = req
		return nil
	})

	req := cluster.DDLPushReq{DDLID: "ddl-x", Op: cluster.DDLOpAlter, TableID: 7, Name: "users", Meta: []byte(`{"id":7}`)}
	if err := mgrA.PushDDL("node-b", req); err != nil {
		t.Fatalf("PushDDL: %v", err)
	}
	if got.DDLID != "ddl-x" || got.Op != cluster.DDLOpAlter || got.TableID != 7 || got.Name != "users" || string(got.Meta) != `{"id":7}` {
		t.Fatalf("applied req mismatch: %+v", got)
	}
}

func TestManagerPushDDLRejectsApplyError(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	_, srvB, _, _ := newTestNode(t, "node-b", nil)
	defer srvB.Close()
	if _, err := mgrA.AddNode(srvB.SelfAddr); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	srvB.SetDDLApply(func(cluster.DDLPushReq) error { return errors.New("apply boom") })

	req := cluster.DDLPushReq{DDLID: "ddl-y", Op: cluster.DDLOpCreate, TableID: 8, Name: "t8", Meta: []byte(`{"id":8}`)}
	err := mgrA.PushDDL("node-b", req)
	if err == nil || !strings.Contains(err.Error(), "apply boom") {
		t.Fatalf("want apply error propagated, got %v", err)
	}
}

func TestManagerPushDDLToUnknownNodeFails(t *testing.T) {
	mgrA, srvA, _, _ := newTestNode(t, "node-a", nil)
	defer srvA.Close()
	if err := mgrA.PushDDL("ghost", cluster.DDLPushReq{Op: cluster.DDLOpCreate, TableID: 1, Name: "t"}); err == nil {
		t.Fatalf("PushDDL to unknown node: want error")
	}
}
