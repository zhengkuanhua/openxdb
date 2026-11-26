package sql

// v2.0.8 Q3 回归：孤儿 region "no live copy" 日志限频。
// 覆盖：首次全量输出、窗口内抑制且计数、窗口外输出汇总行（含抑制条数）、
// 不同 region 独立计数、region 恢复（clear）后重新全量输出。

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

func TestV208NoLiveCopyLogRateLimit(t *testing.T) {
	var buf bytes.Buffer
	oldOut := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	oldWin := noLiveCopyLogWindow
	noLiveCopyLogWindow = 60 * time.Millisecond
	t.Cleanup(func() {
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
		noLiveCopyLogWindow = oldWin
	})

	var e Executor
	rid := storage.ID(9223372036854775825)
	errNoLive := errors.New("no live copy for region")

	// 1) 首次异常：全量输出一条。
	e.logNoLiveCopyDeferred(rid, errNoLive)
	if n := strings.Count(buf.String(), "deferred (no live copy, will retry)"); n != 1 {
		t.Fatalf("first occurrence: want 1 log line, got %d (%q)", n, buf.String())
	}
	if !strings.Contains(buf.String(), "no live copy for region") {
		t.Fatalf("first occurrence must carry full cause, got %q", buf.String())
	}

	// 2) 窗口内重复 20 次：只计数，不输出。
	for i := 0; i < 20; i++ {
		e.logNoLiveCopyDeferred(rid, errNoLive)
	}
	if n := strings.Count(buf.String(), "still no live copy"); n != 0 {
		t.Fatalf("within window: want 0 summary lines, got %d", n)
	}

	// 3) 窗口到期：输出一条汇总行，含被抑制条数。
	time.Sleep(70 * time.Millisecond)
	e.logNoLiveCopyDeferred(rid, errNoLive)
	if !strings.Contains(buf.String(), "still no live copy: 20 entries suppressed") {
		t.Fatalf("after window: want summary with 20 suppressed, got %q", buf.String())
	}

	// 4) 另一 region 独立计数，不受上者影响。
	base := strings.Count(buf.String(), "deferred (no live copy, will retry)")
	e.logNoLiveCopyDeferred(storage.ID(42), errNoLive)
	if got := strings.Count(buf.String(), "deferred (no live copy, will retry)"); got != base+1 {
		t.Fatalf("independent region: want %d first-line logs, got %d", base+1, got)
	}

	// 5) 恢复后清除状态：下次异常重新全量输出。
	e.clearNoLiveCopyLog(rid)
	before := strings.Count(buf.String(), "deferred (no live copy, will retry)")
	e.logNoLiveCopyDeferred(rid, errNoLive)
	if got := strings.Count(buf.String(), "deferred (no live copy, will retry)"); got != before+1 {
		t.Fatalf("after clear: want full log again (%d), got %d", before+1, got)
	}
}

// TestV208NoLiveCopyB1LogRateLimit B1 副本维护路径的同类日志独立限频：
// 与 failover 路径分开计数，首次全量、窗口内抑制、窗口到期汇总。
func TestV208NoLiveCopyB1LogRateLimit(t *testing.T) {
	var buf bytes.Buffer
	oldOut := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	oldWin := noLiveCopyLogWindow
	noLiveCopyLogWindow = 60 * time.Millisecond
	t.Cleanup(func() {
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
		noLiveCopyLogWindow = oldWin
	})

	var e Executor
	rid := storage.ID(9223372036854775825)

	// 1) failover 与 B1 同 region 各自首次都要全量输出（互不吞日志）。
	e.logNoLiveCopyDeferred(rid, errors.New("no live copy for region"))
	e.logNoLiveCopyB1(rid)
	if n := strings.Count(buf.String(), "sql: B1 region 9223372036854775825 has no live copy in cluster"); n != 1 {
		t.Fatalf("B1 first occurrence: want 1 line, got %d (%q)", n, buf.String())
	}

	// 2) 窗口内重复 30 轮：只计数不输出（真实 3s 心跳场景）。
	for i := 0; i < 30; i++ {
		e.logNoLiveCopyB1(rid)
	}
	if n := strings.Count(buf.String(), "B1 region"); n != 1 {
		t.Fatalf("within window: want only the first line, got %d", n)
	}

	// 3) 窗口到期：输出汇总并带抑制条数。
	time.Sleep(70 * time.Millisecond)
	e.logNoLiveCopyB1(rid)
	if !strings.Contains(buf.String(), "B1 region 9223372036854775825 still has no live copy: 30 entries suppressed") {
		t.Fatalf("after window: want summary with 30 suppressed, got %q", buf.String())
	}

	// 4) clear 后重新全量输出。
	e.clearNoLiveCopyLog(rid)
	before := strings.Count(buf.String(), "B1 region 9223372036854775825 has no live copy in cluster")
	e.logNoLiveCopyB1(rid)
	if got := strings.Count(buf.String(), "B1 region 9223372036854775825 has no live copy in cluster"); got != before+1 {
		t.Fatalf("after clear: want full log again (%d), got %d", before+1, got)
	}
}

// TestV208NoLiveCopyLogTracksRegions 抑制状态按 region 记录，可被观测。
func TestV208NoLiveCopyLogTracksRegions(t *testing.T) {
	var e Executor
	if n := e.NoLiveCopySuppressed(); n != 0 {
		t.Fatalf("empty executor: want 0 tracked regions, got %d", n)
	}
	oldWin := noLiveCopyLogWindow
	noLiveCopyLogWindow = time.Hour
	t.Cleanup(func() { noLiveCopyLogWindow = oldWin })
	e.logNoLiveCopyDeferred(storage.ID(1), errors.New("x"))
	e.logNoLiveCopyDeferred(storage.ID(2), errors.New("y"))
	if n := e.NoLiveCopySuppressed(); n != 2 {
		t.Fatalf("want 2 tracked regions, got %d", n)
	}
	e.clearNoLiveCopyLog(storage.ID(1))
	if n := e.NoLiveCopySuppressed(); n != 1 {
		t.Fatalf("after clear: want 1 tracked region, got %d", n)
	}
}
