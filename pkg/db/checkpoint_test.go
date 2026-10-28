package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// walSizeBytes 返回数据目录 wal.log 当前字节数（测试辅助）。
func walSizeBytes(t *testing.T, d *DB) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(d.Dir, WALFile))
	if err != nil {
		t.Fatalf("stat wal.log: %v", err)
	}
	return fi.Size()
}

// putCommit 通过事务管理器写入一条提交（测试辅助）。
func putCommit(t *testing.T, d *DB, k, v string) {
	t.Helper()
	tx, err := d.Txn.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put([]byte(k), []byte(v)); err != nil {
		t.Fatalf("Put(%s): %v", k, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit(%s): %v", k, err)
	}
}

func mustGet(t *testing.T, d *DB, k string) string {
	t.Helper()
	v, err := d.Storage.Get([]byte(k))
	if err != nil {
		t.Fatalf("Get(%s): %v", k, err)
	}
	return string(v)
}

// TestCheckpointTruncatesWAL 手动触发一次 checkpoint：WAL 体积下降、数据不丢。
func TestCheckpointTruncatesWAL(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	d := mustOpen(t, dir)
	defer d.Close()

	for i := 0; i < 30; i++ {
		putCommit(t, d, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}
	szBefore := walSizeBytes(t, d)
	cc := d.checkpoint
	// 默认阈值 64MB 下不应截断（验证大小阈值生效）。
	cc.SetCheckpointMaxMB(64)
	if cc.checkpointOnce() {
		t.Fatalf("checkpointOnce: want false (WAL below 64MB), got true")
	}
	// 关闭大小阈值：每次检查都截断。
	cc.SetCheckpointMaxMB(0)
	if !cc.checkpointOnce() {
		t.Fatalf("checkpointOnce: want true, got false")
	}
	szAfter := walSizeBytes(t, d)
	if szAfter >= szBefore {
		t.Fatalf("WAL not truncated: before=%d after=%d", szBefore, szAfter)
	}
	st := cc.Stats()
	if st.Truncations != 1 || st.LastTruncated == 0 {
		t.Fatalf("stats: want Truncations=1 LastTruncated>0, got %+v", st)
	}
	// 数据仍在（截断不丢已提交数据）。
	for i := 0; i < 30; i++ {
		if got := mustGet(t, d, fmt.Sprintf("k%02d", i)); got != fmt.Sprintf("v%02d", i) {
			t.Fatalf("k%02d: want v%02d, got %s", i, i, got)
		}
	}
}

// TestCheckpointAutoLoop 后台循环自动截断：小周期 + 每次检查截断，WAL 收敛。
func TestCheckpointAutoLoop(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	d := mustOpen(t, dir)
	defer d.Close()

	cc := d.checkpoint
	cc.SetCheckpointInterval(30)
	cc.SetCheckpointMaxMB(0) // 每次检查都截断

	for i := 0; i < 20; i++ {
		putCommit(t, d, fmt.Sprintf("ak%02d", i), fmt.Sprintf("av%02d", i))
	}
	// 轮询等待后台协程至少截断一次且 WAL 收敛。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := cc.Stats()
		if st.Truncations > 0 && walSizeBytes(t, d) < 2048 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	st := cc.Stats()
	if st.Truncations == 0 {
		t.Fatalf("auto loop: no truncation happened, stats=%+v", st)
	}
	if sz := walSizeBytes(t, d); sz >= 2048 {
		t.Fatalf("auto loop: WAL not converged, size=%d", sz)
	}
	for i := 0; i < 20; i++ {
		if got := mustGet(t, d, fmt.Sprintf("ak%02d", i)); got != fmt.Sprintf("av%02d", i) {
			t.Fatalf("ak%02d: want av%02d, got %s", i, i, got)
		}
	}
}

// TestCheckpointDisabled 关闭总开关后后台循环不截断（可回退）。
func TestCheckpointDisabled(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	d := mustOpen(t, dir)
	defer d.Close()

	cc := d.checkpoint
	cc.SetCheckpointInterval(30)
	cc.SetCheckpointMaxMB(0)
	cc.SetCheckpointEnabled(false)

	for i := 0; i < 20; i++ {
		putCommit(t, d, fmt.Sprintf("dk%02d", i), "dv")
	}
	time.Sleep(300 * time.Millisecond)
	if st := cc.Stats(); st.Truncations != 0 {
		t.Fatalf("disabled: want no truncation, got %+v", st)
	}
	// 数据仍可读（关闭只是不截断，不影响提交路径）。
	for i := 0; i < 20; i++ {
		_ = mustGet(t, d, fmt.Sprintf("dk%02d", i))
	}
}

// TestCheckpointCrashRecoveryConsistency 截断后重开（Recover 重放）数据严格一致，
// 且截断后继续写入 + 再次截断仍一致（长期运行不丢数据）。
func TestCheckpointCrashRecoveryConsistency(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	d := mustOpen(t, dir)
	cc := d.checkpoint
	cc.SetCheckpointMaxMB(0) // 每次检查都截断

	const batch = 40
	for i := 0; i < batch; i++ {
		putCommit(t, d, fmt.Sprintf("r%03d", i), fmt.Sprintf("rv%03d", i))
	}
	if !cc.checkpointOnce() {
		t.Fatalf("checkpointOnce: want true")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	d2 := mustOpen(t, dir)
	defer d2.Close()
	for i := 0; i < batch; i++ {
		if got := mustGet(t, d2, fmt.Sprintf("r%03d", i)); got != fmt.Sprintf("rv%03d", i) {
			t.Fatalf("after reopen r%03d: want rv%03d, got %s", i, i, got)
		}
	}
	// 截断后继续写入并再次截断，长期运行一致性。
	cc2 := d2.checkpoint
	cc2.SetCheckpointMaxMB(0)
	for i := batch; i < batch*2; i++ {
		putCommit(t, d2, fmt.Sprintf("r%03d", i), fmt.Sprintf("rv%03d", i))
	}
	if !cc2.checkpointOnce() {
		t.Fatalf("second checkpointOnce: want true")
	}
	for i := 0; i < batch*2; i++ {
		if got := mustGet(t, d2, fmt.Sprintf("r%03d", i)); got != fmt.Sprintf("rv%03d", i) {
			t.Fatalf("after second checkpoint r%03d: want rv%03d, got %s", i, i, got)
		}
	}
}

// TestCheckpointLongRunBoundedWAL 长期运行验证：持续写入 + 后台自动
// checkpoint 期间，wal.log 大小始终受控（不随累计提交单调增长），数据不丢。
func TestCheckpointLongRunBoundedWAL(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	d := mustOpen(t, dir)
	defer d.Close()

	cc := d.checkpoint
	cc.SetCheckpointInterval(20)
	cc.SetCheckpointMaxMB(0) // 每次检查都截断（模拟长期运行收敛）

	const (
		rounds   = 40
		perRound = 5
		bounded  = 256 * 1024 // WAL 任何时刻不得超过 256KB
	)
	for r := 0; r < rounds; r++ {
		for i := 0; i < perRound; i++ {
			k := fmt.Sprintf("l%04d", r*perRound+i)
			putCommit(t, d, k, "lv")
		}
		// 每轮提交后给后台协程截断窗口，并断言体积受控。
		time.Sleep(30 * time.Millisecond)
		if sz := walSizeBytes(t, d); sz > bounded {
			t.Fatalf("round %d: WAL size %d exceeds bound %d", r, sz, bounded)
		}
	}
	if st := cc.Stats(); st.Truncations == 0 {
		t.Fatalf("long run: no truncation happened, stats=%+v", st)
	}
	// 全部 200 条数据不丢。
	for i := 0; i < rounds*perRound; i++ {
		if got := mustGet(t, d, fmt.Sprintf("l%04d", i)); got != "lv" {
			t.Fatalf("l%04d: want lv, got %s", i, got)
		}
	}
	// 重开一致性（WAL 已多轮截断，恢复重放仍完整）。
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d2 := mustOpen(t, dir)
	defer d2.Close()
	for i := 0; i < rounds*perRound; i++ {
		if got := mustGet(t, d2, fmt.Sprintf("l%04d", i)); got != "lv" {
			t.Fatalf("after reopen l%04d: want lv, got %s", i, got)
		}
	}
}

// TestCheckpointConcurrentWrites checkpoint 与并发提交并行安全：数据完整不丢。
func TestCheckpointConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	d := mustOpen(t, dir)
	defer d.Close()

	cc := d.checkpoint
	cc.SetCheckpointInterval(20)
	cc.SetCheckpointMaxMB(0) // 每次检查都截断（频繁触发）

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 30; i++ {
			putCommit(t, d, fmt.Sprintf("c%03d", i), fmt.Sprintf("cv%03d", i))
		}
	}()
	// 并发触发多次手动 checkpoint（模拟后台协程与写并发）。
	for i := 0; i < 5; i++ {
		cc.checkpointOnce()
		time.Sleep(5 * time.Millisecond)
	}
	<-done

	for i := 0; i < 30; i++ {
		if got := mustGet(t, d, fmt.Sprintf("c%03d", i)); got != fmt.Sprintf("cv%03d", i) {
			t.Fatalf("c%03d: want cv%03d, got %s", i, i, got)
		}
	}
	// 最后一次显式截断后重开仍一致。
	if !cc.checkpointOnce() {
		t.Fatalf("final checkpointOnce: want true")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d2 := mustOpen(t, dir)
	defer d2.Close()
	for i := 0; i < 30; i++ {
		if got := mustGet(t, d2, fmt.Sprintf("c%03d", i)); got != fmt.Sprintf("cv%03d", i) {
			t.Fatalf("after reopen c%03d: want cv%03d, got %s", i, i, got)
		}
	}
}
