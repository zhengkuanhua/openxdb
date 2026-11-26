package sql_test

// v2.0.8 Q1 回归：多行 INSERT 的"误报 duplicate primary key"。
// 修复要点：
//  1. 主键冲突判定只认"主键键真实存在"（磁盘或语句内 pending 同主键），
//     不再用 conflicts[0]==pk 推断，避免唯一索引项反解出的 pk 冒充主键冲突；
//  2. 磁盘唯一索引冲突扫描必须用完整索引键前缀精确匹配，避免 TEXT 变长索引值
//     的前缀关系把相邻索引值项扫进来，也避免 pk 后缀解析受值内 'p' 字节干扰；
//  3. 写路径唯一性判定走严格读，不允许 allow_partial_read 回退本地副本。
//
// 本测试锁定的是"错误码语义正确"这一可本地确定的部分；真机三节点误报复现仍需
// 按 DEV_PLAN_v2.0.8.md 第二/三周复验。

import (
	"strings"
	"testing"
)

func TestV208InsertTextUniquePrefixNoFalsePositive(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_u (id INT, u TEXT, PRIMARY KEY (id), UNIQUE KEY uk_u (u))")

	// 'a' 与 'ab' 存在字节前缀关系：范围扫描会互相覆盖，但索引值不同，不构成冲突
	mustExec(t, e, "INSERT INTO t_u (id, u) VALUES (1, 'a')")
	mustExec(t, e, "INSERT INTO t_u (id, u) VALUES (2, 'ab')")
	mustExec(t, e, "INSERT INTO t_u (id, u) VALUES (3, 'abc')")
	got := mustExec(t, e, "SELECT COUNT(*) FROM t_u")
	if len(got.Rows) != 1 || got.Rows[0][0].String() != "3" {
		t.Fatalf("prefix-distinct rows: want 3 rows, got %+v", got.Rows)
	}

	// 真正的唯一索引冲突：报 unique key，而不是 primary key
	_, err := e.Execute("INSERT INTO t_u (id, u) VALUES (9, 'ab')")
	if err == nil {
		t.Fatalf("dup unique key: want error, got nil")
	}
	msg := err.Error()
	if strings.Contains(msg, "duplicate primary key") {
		t.Fatalf("dup unique key: must not report primary key conflict, got %q", msg)
	}
	if !strings.Contains(msg, "duplicate unique key") {
		t.Fatalf("dup unique key: want duplicate unique key, got %q", msg)
	}
	// 失败语句不得留下半写入的行
	got = mustExec(t, e, "SELECT COUNT(*) FROM t_u")
	if len(got.Rows) != 1 || got.Rows[0][0].String() != "3" {
		t.Fatalf("dup unique key: rows changed to %+v", got.Rows)
	}
}

func TestV208InsertRealPrimaryKeyConflictStillReportsPrimary(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_pk (id INT, u TEXT, PRIMARY KEY (id), UNIQUE KEY uk_u (u))")
	mustExec(t, e, "INSERT INTO t_pk (id, u) VALUES (112, 'ap')")

	// 主键真实重复（112 的低字节为 0x70，即索引键分隔符 'p'）
	_, err := e.Execute("INSERT INTO t_pk (id, u) VALUES (112, 'ap2')")
	if err == nil || !strings.Contains(err.Error(), "duplicate primary key") {
		t.Fatalf("real pk conflict: want duplicate primary key, got %v", err)
	}

	// 不同主键 + 相同唯一值：仍应报 unique key
	mustExec(t, e, "INSERT INTO t_pk (id, u) VALUES (113, 'ap1')")
	_, err = e.Execute("INSERT INTO t_pk (id, u) VALUES (240, 'ap')")
	if err == nil || !strings.Contains(err.Error(), "duplicate unique key") {
		t.Fatalf("unique conflict with pk 240: want duplicate unique key, got %v", err)
	}
}

func TestV208MultiRowInsertWithinStatementDupPK(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_m (id INT, u TEXT, PRIMARY KEY (id), UNIQUE KEY uk_u (u))")

	// 语句内同主键：pending 主键命中，报 primary key
	_, err := e.Execute("INSERT INTO t_m (id, u) VALUES (1, 'x'), (1, 'y')")
	if err == nil || !strings.Contains(err.Error(), "duplicate primary key") {
		t.Fatalf("in-statement dup pk: want duplicate primary key, got %v", err)
	}

	// 语句内不同主键、唯一值相同：报 unique key
	_, err = e.Execute("INSERT INTO t_m (id, u) VALUES (1, 'same'), (2, 'same')")
	if err == nil || !strings.Contains(err.Error(), "duplicate unique key") {
		t.Fatalf("in-statement dup unique: want duplicate unique key, got %v", err)
	}

	// 多行合法插入：不应有任何误报，且全部落库
	res := mustExec(t, e, "INSERT INTO t_m (id, u) VALUES (10, 'a'), (11, 'ab'), (12, 'abc'), (112, 'ap')")
	if res.AffectedRows != 4 {
		t.Fatalf("multi-row insert: want AffectedRows=4, got %d", res.AffectedRows)
	}
	got := mustExec(t, e, "SELECT COUNT(*) FROM t_m")
	if len(got.Rows) != 1 || got.Rows[0][0].String() != "4" {
		t.Fatalf("multi-row insert: want 4 rows, got %+v", got.Rows)
	}
}
