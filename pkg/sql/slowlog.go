package sql

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

// SlowLog 慢查询落盘（M13）：线程安全、低开销。
// 调用方保证仅在"超阈值判定通过后"才调用 Write（格式化/打开文件都在判定之后，
// 正常语句零开销）；Write 内部以互斥锁串行化追加写，多并发执行器安全。
// 配置（路径 + 开关）经 Configure 变更即时生效，无锁读字段、写时互斥。
type SlowLog struct {
	mu      sync.Mutex
	path    string // 落盘文件路径；空串表示关闭
	enabled bool
}

// NewSlowLog 创建关闭状态的慢查询落盘器（db/cmd 装配开启）。
func NewSlowLog() *SlowLog { return &SlowLog{} }

// Configure 即时更新落盘配置：path 空串或 enabled=false 关闭落盘。
// 变更对后续 Write 立即生效，无需重启。
func (l *SlowLog) Configure(path string, enabled bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.path = path
	l.enabled = enabled && path != ""
}

// Enabled 返回当前是否已开启落盘（读路径无锁，供 SHOW 等展示）。
func (l *SlowLog) Enabled() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.enabled
}

// Path 返回当前落盘路径（关闭时为空串）。
func (l *SlowLog) Path() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.path
}

// Write 追加写入一条慢查询记录。仅在慢查询判定后调用；开启落盘时
// 以追加模式打开文件写一行（慢查询低频，重复打开开销可忽略且避免长句柄）。
// 写入失败静默忽略（慢查询记录不阻塞 SQL 执行），返回错误供测试断言。
func (l *SlowLog) Write(r SlowQueryRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.enabled {
		return nil
	}
	line := formatSlowLine(r)
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(line + "\n")
	return err
}

// formatSlowLine 格式化一条慢查询落盘行（低开销：仅超阈值后才调用）：
//
//	2026-10-11 12:00:00 | user | 123ms | rows=10 | err= | SELECT ...
//
// SQL 文本中的换行/回车转义为 \n / \r，保证一行一条记录。
func formatSlowLine(r SlowQueryRecord) string {
	errPart := "err="
	if r.Err != "" {
		errPart = "err=" + strings.ReplaceAll(strings.ReplaceAll(r.Err, "\n", "\\n"), "\r", "\\r")
	}
	sqlText := strings.ReplaceAll(strings.ReplaceAll(r.SQL, "\n", "\\n"), "\r", "\\r")
	return fmt.Sprintf("%s | %s | %dms | rows=%d | %s | %s",
		r.StartAt, r.User, r.Duration, r.Rows, errPart, sqlText)
}
