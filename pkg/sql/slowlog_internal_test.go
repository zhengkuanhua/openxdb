package sql

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newExecutorInternal 白盒测试用执行器：慢查询路径不依赖真实存储
// （execTimed 闭包注入合成结果），直接 NewExecutor(nil) 即可。
func newExecutorInternal() *Executor { return NewExecutor(nil) }

// TestM13SlowThresholdDefault 默认阈值 100ms、内存上限 64（M13 规格）。
func TestM13SlowThresholdDefault(t *testing.T) {
	e := newExecutorInternal()
	if e.slowThreshold != 100 {
		t.Fatalf("default slowThreshold: got %d, want 100", e.slowThreshold)
	}
	if e.slowQueryMax != 64 {
		t.Fatalf("default slowQueryMax: got %d, want 64", e.slowQueryMax)
	}
}

// TestM13ExecTimedErr 慢且失败的语句同样记录：Err 字段 + 落盘 err=。
func TestM13ExecTimedErr(t *testing.T) {
	e := newExecutorInternal()
	e.SetSlowThreshold(1)
	logPath := filepath.Join(t.TempDir(), "slow.log")
	e.SetSlowLog(logPath, true)
	_, err := e.execTimed(e.globalSess, "alice", "SELECT FAIL", func() (*Result, error) {
		time.Sleep(2 * time.Millisecond)
		return nil, errors.New("boom")
	})
	if err == nil {
		t.Fatal("want error from fn")
	}
	recs := e.SlowQueries()
	if len(recs) != 1 {
		t.Fatalf("slow queries: got %d, want 1", len(recs))
	}
	r := recs[0]
	if r.SQL != "SELECT FAIL" || r.User != "alice" || r.Err != "boom" || r.Rows != 0 {
		t.Fatalf("record: got %+v", r)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read slow.log: %v", err)
	}
	if !strings.Contains(string(data), "err=boom") || !strings.Contains(string(data), "SELECT FAIL") ||
		!strings.Contains(string(data), "| alice |") {
		t.Fatalf("slow.log line: %q", string(data))
	}
}

// TestM13SlowQueryMax 内存保留条数上限即时生效（环形裁剪）。
func TestM13SlowQueryMax(t *testing.T) {
	e := newExecutorInternal()
	e.SetSlowThreshold(1)
	e.SetSlowQueryMax(2)
	for i := 0; i < 5; i++ {
		_, _ = e.execTimed(e.globalSess, "u", "SLOW", func() (*Result, error) {
			time.Sleep(2 * time.Millisecond)
			return &Result{}, nil
		})
	}
	recs := e.SlowQueries()
	if len(recs) != 2 {
		t.Fatalf("slow queries capped: got %d, want 2", len(recs))
	}
}

// TestM13SlowLogFormat 落盘行格式化：单行、换行转义、字段齐全。
func TestM13SlowLogFormat(t *testing.T) {
	line := formatSlowLine(SlowQueryRecord{
		SQL:      "SELECT 1\n-- c",
		User:     "bob",
		StartAt:  "2026-10-11 12:00:00",
		Duration: 123,
		Rows:     5,
		Err:      "oops\nline2",
	})
	if strings.Contains(line, "\n") {
		t.Fatalf("slow line must be single line: %q", line)
	}
	if !strings.Contains(line, "123ms") || !strings.Contains(line, "rows=5") ||
		!strings.Contains(line, "err=oops\\nline2") || !strings.Contains(line, "SELECT 1\\n-- c") ||
		!strings.Contains(line, "| bob |") {
		t.Fatalf("slow line fields: %q", line)
	}
}

// TestM13SlowLogConfigure 默认关闭；空路径不开启；开启后追加、关闭后不再追加。
func TestM13SlowLogConfigure(t *testing.T) {
	e := newExecutorInternal()
	e.SetSlowThreshold(1)
	if e.SlowLogEnabled() {
		t.Fatal("slow log should default off")
	}
	e.SetSlowLog("", true)
	if e.SlowLogEnabled() {
		t.Fatal("slow log with empty path should stay off")
	}
	path := filepath.Join(t.TempDir(), "slow.log")
	e.SetSlowLog(path, true)
	if !e.SlowLogEnabled() || e.SlowLogPath() != path {
		t.Fatalf("slow log config: enabled=%v path=%q", e.SlowLogEnabled(), e.SlowLogPath())
	}
	_, err := e.execTimed(e.globalSess, "u", "SLOW", func() (*Result, error) {
		time.Sleep(2 * time.Millisecond)
		return &Result{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "SLOW") {
		t.Fatalf("slow.log missing entry: %q", string(data))
	}
	e.SetSlowLog(path, false)
	_, _ = e.execTimed(e.globalSess, "u", "SLOW2", func() (*Result, error) {
		time.Sleep(2 * time.Millisecond)
		return &Result{}, nil
	})
	data2, _ := os.ReadFile(path)
	if strings.Contains(string(data2), "SLOW2") {
		t.Fatalf("slow log should be off: %q", string(data2))
	}
}

// TestM13ExecTimedLowOverhead 未超阈值的语句不产生记录（阈值判断先于格式化）。
func TestM13ExecTimedLowOverhead(t *testing.T) {
	e := newExecutorInternal()
	e.SetSlowThreshold(100000) // 大阈值：正常语句必然不触发
	res, err := e.execTimed(e.globalSess, "u", "SELECT 1", func() (*Result, error) {
		return &Result{Rows: [][]Value{{IntVal(1)}}}, nil
	})
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("exec: %v %+v", err, res)
	}
	if n := len(e.SlowQueries()); n != 0 {
		t.Fatalf("expected no slow records, got %d", n)
	}
}
