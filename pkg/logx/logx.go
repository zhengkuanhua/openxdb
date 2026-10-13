// Package logx 提供 OpenXDB 结构化日志（JSON 行）与告警体系。
//
// 结构化日志：单行 JSON，固定字段 time/level/module/event/msg，可按级别过滤
// （debug < info < warn < error），支持运行时动态调整级别（SET log_level /
// openxdb start --log-level / 配置文件的 [server] log_level）。
//
// 告警：WAL/存储/网络/备份等关键模块异常通过 AlertManager 产生可配置告警
// （内存环形缓冲 + 可选 JSON 行文件落盘 + 可选 HTTP webhook 推送），
// 查看命令 SHOW ALERTS / SHOW ALERT STATUS。零第三方依赖，仅标准库。
package logx

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Level 结构化日志级别（数值越大越严重）。
type Level int32

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// String 返回级别文本（JSON 行 level 字段取值）。
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "unknown"
	}
}

// ParseLevel 解析级别文本（大小写不敏感），支持 debug/info/warn/warning/error。
func ParseLevel(s string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug, nil
	case "info":
		return LevelInfo, nil
	case "warn", "warning":
		return LevelWarn, nil
	case "error":
		return LevelError, nil
	}
	return LevelInfo, fmt.Errorf("invalid log level %q (expected debug/info/warn/error)", s)
}

// Logger 结构化 JSON 行日志器（并发安全）。输出单行 JSON：
//
//	{"time":"2026-10-13T12:00:00.000+08:00","level":"info","module":"db",
//	 "event":"open","msg":"data directory opened","key":"value",...}
type Logger struct {
	level atomic.Int32
	mu    sync.Mutex
	out   io.Writer
}

// New 创建日志器；w 为 nil 时输出丢弃。
func New(w io.Writer) *Logger {
	l := &Logger{}
	if w == nil {
		w = io.Discard
	}
	l.out = w
	l.level.Store(int32(LevelInfo))
	return l
}

// SetOutput 替换输出目标（nil = 丢弃）。
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if w == nil {
		w = io.Discard
	}
	l.out = w
}

// SetLevel 动态调整级别；低于该级别的日志不再输出。
func (l *Logger) SetLevel(lv Level) { l.level.Store(int32(lv)) }

// Level 返回当前级别。
func (l *Logger) Level() Level { return Level(l.level.Load()) }

// log 输出一条结构化日志；kv 为可选的平铺键值对（奇数个时忽略末尾孤值）。
func (l *Logger) log(lv Level, module, event, msg string, kv ...any) {
	if lv < Level(l.level.Load()) {
		return
	}
	m := make(map[string]any, 8+len(kv)/2)
	m["time"] = time.Now().Format("2006-01-02T15:04:05.000Z07:00")
	m["level"] = lv.String()
	m["module"] = module
	m["event"] = event
	m["msg"] = msg
	for i := 0; i+1 < len(kv); i += 2 {
		m[fmt.Sprint(kv[i])] = fmt.Sprint(kv[i+1])
	}
	b, _ := json.Marshal(m)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.out != nil {
		_, _ = l.out.Write(append(b, '\n'))
	}
}

// Debug 输出 debug 级日志。
func (l *Logger) Debug(module, event, msg string, kv ...any) { l.log(LevelDebug, module, event, msg, kv...) }

// Info 输出 info 级日志。
func (l *Logger) Info(module, event, msg string, kv ...any) { l.log(LevelInfo, module, event, msg, kv...) }

// Warn 输出 warn 级日志。
func (l *Logger) Warn(module, event, msg string, kv ...any) { l.log(LevelWarn, module, event, msg, kv...) }

// Error 输出 error 级日志。
func (l *Logger) Error(module, event, msg string, kv ...any) { l.log(LevelError, module, event, msg, kv...) }

// Default 全局结构化日志器（服务启动时输出到 stderr；测试可 SetOutput 接管）。
var Default = New(os.Stderr)
