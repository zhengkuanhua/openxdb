package sql

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// execTextAs 白盒测试执行：Parse 后走 ExecuteTextAsSession 统一入口（含权限校验与审计埋点）。
func execTextAs(e *Executor, sqlText, user string) (*Result, error) {
	ast, err := Parse(sqlText)
	if err != nil {
		return nil, err
	}
	return e.ExecuteTextAsSession(ast, sqlText, user, e.globalSess)
}

// execTimedAs 白盒审计执行：经 execTimed 统一埋点，注入合成执行结果
// （不依赖真实存储事务，NewExecutor(nil) 构造即可）。execErr 非 nil 模拟
// 执行失败（success=false + Err 记录）；stmt 为审计分类依据。
func execTimedAs(e *Executor, sql string, stmt Stmt, user string, execErr error) (*Result, error) {
	return e.execTimed(e.globalSess, user, sql, stmt, func() (*Result, error) {
		if execErr != nil {
			return nil, execErr
		}
		return &Result{AffectedRows: 1}, nil
	})
}


// TestM17AuditEventClassification 事件分类：DML / DDL / PRIVILEGE / 非审计语句。
func TestM17AuditEventClassification(t *testing.T) {
	cases := []struct {
		stmt  Stmt
		event string
		obj   string
		ok    bool
	}{
		{&InsertStmt{Table: "users"}, AuditDML, "users", true},
		{&UpdateStmt{Table: "users"}, AuditDML, "users", true},
		{&DeleteStmt{Table: "users"}, AuditDML, "users", true},
		{&CreateTableStmt{Name: "t1"}, AuditDDL, "t1", true},
		{&DropTableStmt{Name: "t1"}, AuditDDL, "t1", true},
		{&CreateIndexStmt{Table: "t1", Name: "idx"}, AuditDDL, "t1.idx", true},
		{&AlterTableStmt{Table: "t1"}, AuditDDL, "t1", true},
		{&CreateUserStmt{Name: "u1"}, AuditPrivilege, "user:u1", true},
		{&DropUserStmt{Name: "u1"}, AuditPrivilege, "user:u1", true},
		{&GrantStmt{Table: "t1"}, AuditPrivilege, "t1", true},
		{&GrantStmt{Scope: "DATABASE", Database: "db1"}, AuditPrivilege, "database:db1", true},
		{&GrantStmt{Scope: "COLUMN", Table: "t1", Columns: []string{"a", "b"}}, AuditPrivilege, "t1(a,b)", true},
		{&GrantStmt{Scope: "ROLE", Role: "r1"}, AuditPrivilege, "role:r1", true},
		{&CreateRoleStmt{Name: "r1"}, AuditPrivilege, "role:r1", true},
		{&SelectStmt{}, "", "", false},
		{&ShowTablesStmt{}, "", "", false},
	}
	for _, c := range cases {
		event, obj, ok := auditEventOf(c.stmt)
		if ok != c.ok || (ok && (event != c.event || obj != c.obj)) {
			t.Fatalf("auditEventOf(%T): got (%q,%q,%v), want (%q,%q,%v)",
				c.stmt, event, obj, ok, c.event, c.obj, c.ok)
		}
	}
}

// TestM17AuditDefaultOn 默认审计总开关开启（安全默认），内存上限 1000。
func TestM17AuditDefaultOn(t *testing.T) {
	e := newExecutorInternal()
	if !e.auditEnabled {
		t.Fatal("audit should be enabled by default")
	}
	if e.auditMax != auditMaxDefault {
		t.Fatalf("default auditMax: got %d, want %d", e.auditMax, auditMaxDefault)
	}
}

// TestM17AuditRecordsDMLDDL 执行 DML/DDL/PRIVILEGE 语句产生审计记录，
// SELECT/SHOW 不产生；记录含用户、对象、成功标记、SQL 原文。
func TestM17AuditRecordsDMLDDL(t *testing.T) {
	e := newExecutorInternal()
	e.SetAuditEnabled(true)

	mustExec := func(sql string, stmt Stmt) {
		t.Helper()
		if _, err := execTimedAs(e, sql, stmt, "alice", nil); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	mustExec("CREATE TABLE t1 (id INT PRIMARY KEY, v TEXT)", &CreateTableStmt{Name: "t1"})
	mustExec("INSERT INTO t1 VALUES (1, 'a')", &InsertStmt{Table: "t1"})
	mustExec("UPDATE t1 SET v = 'b' WHERE id = 1", &UpdateStmt{Table: "t1"})
	mustExec("DELETE FROM t1 WHERE id = 1", &DeleteStmt{Table: "t1"})
	mustExec("CREATE USER 'u2' IDENTIFIED BY 'pw'", &CreateUserStmt{Name: "u2"})
	mustExec("GRANT SELECT ON t1 TO 'u2'", &GrantStmt{Table: "t1"})
	// SELECT / SHOW 不产生审计记录
	mustExec("SELECT * FROM t1", &SelectStmt{})
	mustExec("SHOW TABLES", &ShowTablesStmt{})

	recs := e.AuditRecords()
	events := make([]string, 0, len(recs))
	for _, r := range recs {
		events = append(events, r.Event)
	}
	want := []string{"DDL", "DML", "DML", "DML", "PRIVILEGE", "PRIVILEGE"}
	if len(events) != len(want) {
		t.Fatalf("audit events: got %v, want %v", events, want)
	}
	// 按顺序校验（记录按发生序追加；SHOW AUDITLOG 才倒序）
	obj := make([]string, 0, len(recs))
	for _, r := range recs {
		obj = append(obj, r.Object)
	}
	wantObj := []string{"t1", "t1", "t1", "t1", "user:u2", "t1"}
	for i := range want {
		if events[i] != want[i] || obj[i] != wantObj[i] {
			t.Fatalf("rec %d: got (%s,%s), want (%s,%s)", i, events[i], obj[i], want[i], wantObj[i])
		}
		if recs[i].User != "alice" || !recs[i].Success || recs[i].Detail == "" {
			t.Fatalf("rec %d fields: %+v", i, recs[i])
		}
	}
}

// TestM17AuditFailureRecorded 权限拒绝/执行失败同样记录（success=false + err）。
func TestM17AuditFailureRecorded(t *testing.T) {
	e := newExecutorInternal()
	e.SetAuditEnabled(true)
	_, err := execTimedAs(e, "DELETE FROM missing_tbl WHERE id = 1", &DeleteStmt{Table: "missing_tbl"}, "u1", errors.New("table not found"))
	if err == nil {
		t.Fatal("want error")
	}
	recs := e.AuditRecords()
	if len(recs) != 1 {
		t.Fatalf("audit records: got %d, want 1", len(recs))
	}
	r := recs[0]
	if r.Event != AuditDML || r.Object != "missing_tbl" || r.Success || r.Err == "" {
		t.Fatalf("failure record: %+v", r)
	}
}

// TestM17AuditSwitchConfig 审计总开关 SET audit_log 即时生效：关闭后不再记录，
// 重新开启恢复。
func TestM17AuditSwitchConfig(t *testing.T) {
	e := newExecutorInternal()
	e.SetAuditEnabled(true)
	// 关闭审计后不再记录（配置即时生效）
	e.SetAuditEnabled(false)
	if e.AuditEnabled() {
		t.Fatal("audit should be off after disable")
	}
	if _, err := execTimedAs(e, "INSERT INTO t1 VALUES (1, 'alice')", &InsertStmt{Table: "t1"}, "alice", nil); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if n := len(e.AuditRecords()); n != 0 {
		t.Fatalf("audit records while off: got %d, want 0", n)
	}
	// 重新开启恢复记录
	e.SetAuditEnabled(true)
	if !e.AuditEnabled() {
		t.Fatal("audit should be on after re-enable")
	}
	if _, err := execTimedAs(e, "INSERT INTO t1 VALUES (2, 'alice')", &InsertStmt{Table: "t1"}, "alice", nil); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if n := len(e.AuditRecords()); n != 1 {
		t.Fatalf("audit records after re-enable: got %d, want 1", n)
	}
}

// TestM17AuditShowQuery SHOW AUDITLOG [LIMIT n]：倒序返回、列完整、LIMIT 生效。
func TestM17AuditShowQuery(t *testing.T) {
	e := newExecutorInternal()
	e.SetAuditEnabled(true)
	for i := 1; i <= 5; i++ {
		if _, err := execTimedAs(e, "INSERT INTO t VALUES (1, 'alice')", &InsertStmt{Table: "t"}, "alice", nil); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	res, err := e.execShowAuditLog(e.globalSess, &ShowAuditLogStmt{})
	if err != nil {
		t.Fatalf("SHOW AUDITLOG: %v", err)
	}
	wantCols := []string{"id", "time", "user", "event", "object", "success", "duration_ms", "slow", "error", "detail"}
	if len(res.Columns) != len(wantCols) {
		t.Fatalf("columns: got %v", res.Columns)
	}
	for i := range wantCols {
		if res.Columns[i] != wantCols[i] {
			t.Fatalf("column %d: got %q, want %q", i, res.Columns[i], wantCols[i])
		}
	}
	if len(res.Rows) != 5 {
		t.Fatalf("rows: got %d, want 5", len(res.Rows))
	}
	// 倒序：最新 INSERT（id 最大）在最前
	firstID := res.Rows[0][0].I
	lastID := res.Rows[len(res.Rows)-1][0].I
	if firstID != 5 || lastID != 1 {
		t.Fatalf("order: first=%v last=%v, want 5..1", firstID, lastID)
	}
	// LIMIT 2
	res2, err := e.execShowAuditLog(e.globalSess, &ShowAuditLogStmt{Limit: 2})
	if err != nil {
		t.Fatalf("SHOW AUDITLOG LIMIT 2: %v", err)
	}
	if len(res2.Rows) != 2 {
		t.Fatalf("limit rows: got %d, want 2", len(res2.Rows))
	}
}

// TestM17AuditPersistence 落盘：JSON 行追加、关闭后停止写入。
func TestM17AuditPersistence(t *testing.T) {
	e := newExecutorInternal()
	logPath := filepath.Join(t.TempDir(), "audit.log")
	e.SetAuditLog(logPath, true)
	e.RecordAuditConnect("alice", true)
	e.RecordAuditDisconnect("alice")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read audit.log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines: got %d, want 2: %q", len(lines), string(data))
	}
	var r AuditRecord
	if err := json.Unmarshal([]byte(lines[0]), &r); err != nil {
		t.Fatalf("line not json: %v", err)
	}
	if r.Event != AuditConnect || r.User != "alice" || !r.Success || r.Detail != "AUTH" {
		t.Fatalf("connect record: %+v", r)
	}
	// 关闭后不再写入
	e.SetAuditLog("", false)
	e.RecordAuditDisconnect("bob")
	data2, _ := os.ReadFile(logPath)
	if string(data) != string(data2) {
		t.Fatalf("file changed after disable: %q vs %q", string(data), string(data2))
	}
}

// TestM17AuditConnectEvents 连接鉴权成功/失败/登出事件。
func TestM17AuditConnectEvents(t *testing.T) {
	e := newExecutorInternal()
	e.SetAuditEnabled(true)
	e.RecordAuditConnect("root", true)
	e.RecordAuditConnect("bob", false)
	e.RecordAuditDisconnect("root")
	recs := e.AuditRecords()
	if len(recs) != 3 {
		t.Fatalf("records: got %d, want 3", len(recs))
	}
	if recs[0].Event != AuditConnect || recs[0].User != "root" {
		t.Fatalf("rec0: %+v", recs[0])
	}
	if recs[1].Event != AuditAuthFailed || recs[1].User != "bob" || recs[1].Success {
		t.Fatalf("rec1: %+v", recs[1])
	}
	if recs[2].Event != AuditDisconnect || recs[2].User != "root" {
		t.Fatalf("rec2: %+v", recs[2])
	}
}

// TestM17AuditRingBuffer 环缓冲上限：超过 auditMax 只保留最近 auditMax 条。
func TestM17AuditRingBuffer(t *testing.T) {
	e := newExecutorInternal()
	e.auditMax = 3
	for i := 0; i < 10; i++ {
		e.RecordAuditConnect("u", true)
	}
	recs := e.AuditRecords()
	if len(recs) != 3 {
		t.Fatalf("ring size: got %d, want 3", len(recs))
	}
	if recs[0].ID != 8 || recs[2].ID != 10 {
		t.Fatalf("ring ids: %d %d %d (want 8,9,10)", recs[0].ID, recs[1].ID, recs[2].ID)
	}
}

// TestM17AuditThresholdSlow 阈值 >0 时超时语句标记 slow；<=0 不标记。
func TestM17AuditThresholdSlow(t *testing.T) {
	e := newExecutorInternal()
	e.SetAuditEnabled(true)
	e.SetAuditThreshold(1)
	_, err := e.execTimed(e.globalSess, "alice", "SLOW_STMT", &InsertStmt{Table: "t1"}, func() (*Result, error) {
		time.Sleep(2 * time.Millisecond)
		return &Result{}, nil
	})
	if err != nil {
		t.Fatalf("execTimed: %v", err)
	}
	recs := e.AuditRecords()
	if len(recs) != 1 || !recs[0].Slow || recs[0].DurationMs <= 0 {
		t.Fatalf("slow record: %+v", recs)
	}
	// 阈值关闭后不再标记
	e.SetAuditThreshold(0)
	_, _ = e.execTimed(e.globalSess, "alice", "FAST_STMT", &InsertStmt{Table: "t1"}, func() (*Result, error) {
		time.Sleep(2 * time.Millisecond)
		return &Result{}, nil
	})
	recs2 := e.AuditRecords()
	if recs2[1].Slow {
		t.Fatalf("record should not be slow: %+v", recs2[1])
	}
}
