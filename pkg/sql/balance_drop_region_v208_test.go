package sql_test

// v2.0.8 Q3 回归：DROP REGION 回收孤儿 region 的路由/归属。
// 覆盖：语法解析（含 FORCE/EXECUTE）、默认 dry-run 不改状态、空 region 可回收、
// 有数据 region 未加 FORCE 拒绝执行且路由不变、FORCE EXECUTE 后该 region 路由消失
// （物理数据永不删除）。

import (
	"fmt"
	"strings"
	"testing"
)

// TestV208DropRegionParseSyntax 解析层：DROP REGION <id> / FORCE / EXECUTE 组合
// 均能被解析并进入执行路径（region 不存在时报 region not found，而非语法错误）。
func TestV208DropRegionParseSyntax(t *testing.T) {
	e := newEngine(t)
	for _, stmt := range []string{
		"DROP REGION 987654321",
		"DROP REGION 987654321 FORCE",
		"DROP REGION 987654321 EXECUTE",
		"DROP REGION 987654321 FORCE EXECUTE",
		"DROP REGION 987654321 EXECUTE FORCE",
	} {
		_, err := e.Execute(stmt)
		if err == nil || !strings.Contains(err.Error(), "region not found") {
			t.Fatalf("%s: want region not found, got %v", stmt, err)
		}
	}
}

// TestV208DropRegionDryRunIsDefault 缺省 dry-run：回显预览但 AffectedRows=0，
// 且路由不发生任何变化（高危命令第一道闸）。
func TestV208DropRegionDryRunIsDefault(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_drop_dry (id INT, name TEXT, PRIMARY KEY (id))")
	rs, err := e.ListRegions("t_drop_dry")
	if err != nil || len(rs) != 1 {
		t.Fatalf("before: rs=%v err=%v", rs, err)
	}
	rid := uint64(rs[0].RegionID)
	res := mustExec(t, e, fmt.Sprintf("DROP REGION %d", rid))
	if res.AffectedRows != 0 {
		t.Fatalf("dry-run 不应改变状态: want AffectedRows=0, got %d", res.AffectedRows)
	}
	if len(res.Rows) != 1 || len(res.Columns) == 0 {
		t.Fatalf("dry-run 应回显预览行: rows=%v cols=%v", res.Rows, res.Columns)
	}
	rs2, _ := e.ListRegions("t_drop_dry")
	if len(rs2) != 1 || uint64(rs2[0].RegionID) != rid {
		t.Fatalf("dry-run 后 region 路由应保持: %+v", rs2)
	}
}

// TestV208DropRegionEmptyRegion 空 region（无行）加 EXECUTE 后可直接回收路由。
func TestV208DropRegionEmptyRegion(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_drop_empty (id INT, name TEXT, PRIMARY KEY (id))")
	rs, err := e.ListRegions("t_drop_empty")
	if err != nil || len(rs) != 1 {
		t.Fatalf("before: rs=%v err=%v", rs, err)
	}
	rid := uint64(rs[0].RegionID)
	res := mustExec(t, e, fmt.Sprintf("DROP REGION %d EXECUTE", rid))
	if res.AffectedRows != 1 {
		t.Fatalf("want AffectedRows=1, got %d", res.AffectedRows)
	}
}

// TestV208DropRegionRefusesLiveData 显式 region 仍有数据且未 FORCE：dry-run 提示会拒绝，
// EXECUTE 直接报错，且路由不被改动（region 数不变）。
func TestV208DropRegionRefusesLiveData(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_drop_live (id INT, name TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t_drop_live (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	rs, _ := e.ListRegions("t_drop_live")
	rid := uint64(rs[0].RegionID)
	mustExec(t, e, fmt.Sprintf("SPLIT REGION %d", rid))
	rs, _ = e.ListRegions("t_drop_live")
	if len(rs) != 2 {
		t.Fatalf("after split: want 2 regions, got %d", len(rs))
	}
	target := uint64(rs[0].RegionID)

	// dry-run 应把拒绝原因写进 action 列。
	dry := mustExec(t, e, fmt.Sprintf("DROP REGION %d", target))
	if dry.AffectedRows != 0 || len(dry.Rows) != 1 {
		t.Fatalf("dry-run 期待预览且不改变状态: %+v", dry)
	}
	action := fmt.Sprint(dry.Rows[0][len(dry.Rows[0])-1])
	if !strings.Contains(action, "refused") {
		t.Fatalf("有数据时 dry-run 应提示 refused, got %q", action)
	}

	_, err := e.Execute(fmt.Sprintf("DROP REGION %d EXECUTE", target))
	if err == nil || !strings.Contains(err.Error(), "still holds") {
		t.Fatalf("有数据的 region 未加 FORCE 应被拒绝, got %v", err)
	}
	rs2, _ := e.ListRegions("t_drop_live")
	if len(rs2) != 2 {
		t.Fatalf("被拒绝后路由不应变化: want 2 regions, got %d", len(rs2))
	}
}

// TestV208DropRegionForceDropsRoutingOnly FORCE EXECUTE：允许摘掉仍在用的 region 路由，
// 仅清元数据（物理数据不删，语句语义为回收孤儿 region 的路由/归属）。
func TestV208DropRegionForceDropsRoutingOnly(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_drop_force (id INT, name TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO t_drop_force (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	rs, _ := e.ListRegions("t_drop_force")
	rid := uint64(rs[0].RegionID)
	mustExec(t, e, fmt.Sprintf("SPLIT REGION %d", rid))
	rs, _ = e.ListRegions("t_drop_force")
	target := uint64(rs[0].RegionID)

	res := mustExec(t, e, fmt.Sprintf("DROP REGION %d FORCE EXECUTE", target))
	if res.AffectedRows != 1 {
		t.Fatalf("FORCE EXECUTE: want AffectedRows=1, got %d", res.AffectedRows)
	}
	rs, _ = e.ListRegions("t_drop_force")
	if len(rs) != 1 || uint64(rs[0].RegionID) == target {
		t.Fatalf("FORCE 后目标 region 路由应消失: %+v", rs)
	}
}
