package sql

import (
	"strconv"
)

// WAL 自动 checkpoint 的 SQL 层装配（v1.16.0）：
//   - CheckpointSetter：WAL 自动 checkpoint 配置接口（由 db.checkpointController
//     实现并注入；sql 包不依赖 db 包，由 db.Open 装配，nil = 未装配零回归）。
//
// 新增三个热更新配置键（configSpecs 注册表）：
//   - wal_auto_checkpoint：总开关（默认 on；off 关闭后台检查，仅保留手动
//     Truncate 路径，与 v1.15.0 及以前行为一致，可回退）
//   - wal_checkpoint_interval_ms：周期检查间隔毫秒（默认 60000）
//   - wal_checkpoint_max_mb：WAL 大小阈值（默认 64；WAL 超过该大小才截断；
//     0 = 仅按周期截断，每次检查都收敛）

// CheckpointSetter WAL 自动 checkpoint 配置接口（由 db.checkpointController 实现）。
type CheckpointSetter interface {
	SetCheckpointEnabled(on bool)
	SetCheckpointInterval(ms int64)
	SetCheckpointMaxMB(mb int64)
	CheckpointConfig() (enabled, interval, maxMB string)
}

// SetCheckpointController 装配 WAL 自动 checkpoint 控制器（db.Open 调用；
// nil = 不启用，SET wal_* 返回明确错误，零回归）。
func (e *Executor) SetCheckpointController(cc CheckpointSetter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.chkCtl = cc
}

// applyWalCheckpointEnabled 解析并转发 wal_auto_checkpoint。
func applyWalCheckpointEnabled(e *Executor, v string) error {
	on := false
	switch v {
	case "on", "true", "1":
		on = true
	case "off", "false", "0":
		on = false
	default:
		return &SQLError{Msg: "invalid value for wal_auto_checkpoint: " + v + " (expected on/off/1/0)"}
	}
	e.mu.Lock()
	cc := e.chkCtl
	e.mu.Unlock()
	if cc == nil {
		return &SQLError{Msg: "wal_auto_checkpoint requires checkpoint controller (db.Open wiring)"}
	}
	cc.SetCheckpointEnabled(on)
	return nil
}

// applyWalCheckpointIntervalMs 解析并转发 wal_checkpoint_interval_ms。
func applyWalCheckpointIntervalMs(e *Executor, v string) error {
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil || ms < 0 {
		return &SQLError{Msg: "invalid value for wal_checkpoint_interval_ms: " + v + " (expected non-negative integer ms)"}
	}
	e.mu.Lock()
	cc := e.chkCtl
	e.mu.Unlock()
	if cc == nil {
		return &SQLError{Msg: "wal_checkpoint_interval_ms requires checkpoint controller (db.Open wiring)"}
	}
	cc.SetCheckpointInterval(ms)
	return nil
}

// applyWalCheckpointMaxMB 解析并转发 wal_checkpoint_max_mb。
func applyWalCheckpointMaxMB(e *Executor, v string) error {
	mb, err := strconv.ParseInt(v, 10, 64)
	if err != nil || mb < 0 {
		return &SQLError{Msg: "invalid value for wal_checkpoint_max_mb: " + v + " (expected non-negative integer MB, 0=checkpoint on every interval)"}
	}
	e.mu.Lock()
	cc := e.chkCtl
	e.mu.Unlock()
	if cc == nil {
		return &SQLError{Msg: "wal_checkpoint_max_mb requires checkpoint controller (db.Open wiring)"}
	}
	cc.SetCheckpointMaxMB(mb)
	return nil
}

// 三个 WAL checkpoint 配置项在 configSpecs 注册表中的实现。
func initWalCheckpointConfigSpecs() {
	configSpecs = append(configSpecs,
		&configSpec{
			key:  "wal_auto_checkpoint",
			desc: "WAL 自动 checkpoint 总开关（v1.16.0）：on/off/1/0，周期检查并截断已持久化 WAL 记录防止 wal.log 单调增长，默认 on；off 回退 v1.15.0 仅手动 Truncate 行为",
			def:  "on",
			apply: func(e *Executor, v string) error {
				return applyWalCheckpointEnabled(e, v)
			},
			read: func(e *Executor) string {
				e.mu.Lock()
				cc := e.chkCtl
				e.mu.Unlock()
				if cc == nil {
					return "on"
				}
				enabled, _, _ := cc.CheckpointConfig()
				return enabled
			},
		},
		&configSpec{
			key:  "wal_checkpoint_interval_ms",
			desc: "WAL 自动 checkpoint 周期检查间隔毫秒（v1.16.0）：正整数，默认 60000；0=关闭后台检查（等价 wal_auto_checkpoint=off）",
			def:  "60000",
			apply: func(e *Executor, v string) error {
				return applyWalCheckpointIntervalMs(e, v)
			},
			read: func(e *Executor) string {
				e.mu.Lock()
				cc := e.chkCtl
				e.mu.Unlock()
				if cc == nil {
					return "60000"
				}
				_, interval, _ := cc.CheckpointConfig()
				return interval
			},
		},
		&configSpec{
			key:  "wal_checkpoint_max_mb",
			desc: "WAL 大小阈值 MB（v1.16.0）：正整数，WAL 超过该大小才截断，默认 64；0=仅按周期截断（每次检查都收敛）",
			def:  "64",
			apply: func(e *Executor, v string) error {
				return applyWalCheckpointMaxMB(e, v)
			},
			read: func(e *Executor) string {
				e.mu.Lock()
				cc := e.chkCtl
				e.mu.Unlock()
				if cc == nil {
					return "64"
				}
				_, _, maxMB := cc.CheckpointConfig()
				return maxMB
			},
		},
	)
}

// 幂等挂接 WAL checkpoint 配置项（init 调用一次）。
var walCheckpointConfigInited = false

func ensureWalCheckpointConfigSpecs() {
	if walCheckpointConfigInited {
		return
	}
	walCheckpointConfigInited = true
	initWalCheckpointConfigSpecs()
}

func init() {
	ensureWalCheckpointConfigSpecs()
}
