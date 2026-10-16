package sql

// T33 同步/半同步复制（M20）的 SQL 配置入口。
//
// 新增三个热更新配置键（configSpecs 注册表）：
//   - replication_mode：async / semi-sync / sync（默认 async，M2 兼容）
//   - replication_ack：半同步最少确认副本数（默认 1）
//   - replication_timeout：ACK 等待超时毫秒（默认 10000）
//
// 通过 ReplicationModeSetter 接口转发至 db.DB（sql 包不依赖 db 包，
// 由 db.Open 装配注入；未装配时 SET 返回明确错误）。

import (
	"strconv"
)

// ReplicationModeSetter 复制模式热切换接口（由 db.DB 实现并注入 Executor）。
type ReplicationModeSetter interface {
	// SetMode 切换复制模式：mode 为 async / semi-sync / sync；
	// ackN 半同步最少确认副本数；timeoutMs ACK 等待超时毫秒。
	SetMode(mode string, ackN int, timeoutMs int64) error
	// ModeStatus 返回当前模式文本（async / semi-sync / sync）。
	ModeStatus() string
	// AckStatus 返回半同步最少确认副本数（async 模式返回默认值 1）。
	AckStatus() int
	// TimeoutStatus 返回 ACK 等待超时毫秒。
	TimeoutStatus() int64
}

// SetReplicationModeSetter 装配复制模式控制器（db.Open 调用）。
func (e *Executor) SetReplicationModeSetter(c ReplicationModeSetter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.replCtl = c
}

// applyReplicationMode 解析并转发复制模式配置。
func applyReplicationMode(e *Executor, v string) error {
	if e.replCtl == nil {
		return &SQLError{Msg: "replication_mode requires replication enabled (start master replication first)"}
	}
	mode, ackN, timeoutMs, err := e.currentReplicationParams()
	if err != nil {
		return err
	}
	switch v {
	case "async", "semi-sync", "semisync", "semi_sync", "sync":
		mode = v
	default:
		return &SQLError{Msg: "invalid value for replication_mode: " + v + " (expected async/semi-sync/sync)"}
	}
	return e.replCtl.SetMode(mode, ackN, timeoutMs)
}

// applyReplicationAck 解析并转发半同步最少确认副本数。
func applyReplicationAck(e *Executor, v string) error {
	if e.replCtl == nil {
		return &SQLError{Msg: "replication_ack requires replication enabled (start master replication first)"}
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return &SQLError{Msg: "invalid value for replication_ack: " + v + " (expected positive integer)"}
	}
	mode, _, timeoutMs, err := e.currentReplicationParams()
	if err != nil {
		return err
	}
	return e.replCtl.SetMode(mode, n, timeoutMs)
}

// applyReplicationTimeout 解析并转发 ACK 等待超时（毫秒）。
func applyReplicationTimeout(e *Executor, v string) error {
	if e.replCtl == nil {
		return &SQLError{Msg: "replication_timeout requires replication enabled (start master replication first)"}
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil || ms < 0 {
		return &SQLError{Msg: "invalid value for replication_timeout: " + v + " (expected non-negative integer ms)"}
	}
	mode, ackN, _, err := e.currentReplicationParams()
	if err != nil {
		return err
	}
	return e.replCtl.SetMode(mode, ackN, ms)
}

// currentReplicationParams 读取当前复制参数（read 回调）。
func (e *Executor) currentReplicationParams() (string, int, int64, error) {
	if e.replCtl == nil {
		return "async", 1, 10000, nil
	}
	return e.replCtl.ModeStatus(), e.replCtl.AckStatus(), e.replCtl.TimeoutStatus(), nil
}

// 三个配置项在 configSpecs 注册表中的实现。
func initReplicationConfigSpecs() {
	configSpecs = append(configSpecs,
		&configSpec{
			key:  "replication_mode",
			desc: "主端复制模式（T33）：async/semi-sync/sync，默认 async（M2 兼容）；需先启用复制",
			def:  "async",
			apply: func(e *Executor, v string) error {
				return applyReplicationMode(e, v)
			},
			read: func(e *Executor) string {
				m, _, _, _ := e.currentReplicationParams()
				return m
			},
		},
		&configSpec{
			key:  "replication_ack",
			desc: "半同步最少确认副本数（T33）：正整数，默认 1；async 下忽略",
			def:  "1",
			apply: func(e *Executor, v string) error {
				return applyReplicationAck(e, v)
			},
			read: func(e *Executor) string {
				_, n, _, _ := e.currentReplicationParams()
				return strconv.Itoa(n)
			},
		},
		&configSpec{
			key:  "replication_timeout",
			desc: "同步/半同步 ACK 等待超时毫秒（T33）：非负整数，默认 10000；async 下忽略",
			def:  "10000",
			apply: func(e *Executor, v string) error {
				return applyReplicationTimeout(e, v)
			},
			read: func(e *Executor) string {
				_, _, ms, _ := e.currentReplicationParams()
				return strconv.FormatInt(ms, 10)
			},
		},
	)
}

// 幂等挂接 T33 配置项（init 调用一次）。
var replicationConfigInited = false

func ensureReplicationConfigSpecs() {
	if replicationConfigInited {
		return
	}
	replicationConfigInited = true
	initReplicationConfigSpecs()
}

func init() {
	ensureReplicationConfigSpecs()
}
