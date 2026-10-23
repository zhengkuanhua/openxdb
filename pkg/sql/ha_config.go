package sql

import "fmt"

// P0 HA 脑裂防护（最小防双主）的 SQL 层装配：
//   - WriteFence：写入 fencing 检查接口（由 db.DB.AllowWrite 实现并注入）。
//     execInsert/execUpdate/execDelete 在写路径入口调用 writeFenceCheck；
//     epoch 开关关闭或本节点未授权为 master 时放行（零回归），已被更高
//     clusterEpoch 取代的陈旧主返回 ErrFenced 拒绝写入。
//   - HAConfigSetter：ha_epoch_enabled / ha_quorum_enabled 开关节流接口
//     （由 db.DB 实现并注入；sql 包不依赖 db 包，由 db.Open 装配）。
//
// 新增两个热更新配置键（configSpecs 注册表），默认 off（M6 旧行为零回归）：
//   - ha_epoch_enabled：epoch 授权与写入 fencing（升主取新 epoch + 广播 +
//     陈旧主写路径拒绝）
//   - ha_quorum_enabled：升主多数派仲裁（N/2+1 可达才允许 promote）

// WriteFence 写入 fencing 检查接口（由 db.DB.AllowWrite 实现）。
// 返回 nil 放行；返回 ErrFenced 表示本节点为陈旧主（已被更高 epoch 取代）。
type WriteFence interface {
	AllowWrite() error
}

// HAConfigSetter HA 开关热切换接口（由 db.DB 实现并注入 Executor）。
type HAConfigSetter interface {
	SetEpochEnabled(on bool)
	SetQuorumEnabled(on bool)
	EpochEnabled() bool
	QuorumEnabled() bool
}

// SetWriteFence 装配写入 fencing 检查（db.Open 调用；nil = 不启用，零回归）。
func (e *Executor) SetWriteFence(wf WriteFence) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.writeFence = wf
}

// P1 HA 防路由分裂装配接口：
//   - EpochSource：clusterEpoch 取号源（由 db.DB 实现并注入；Epoch() 返回当前
//     已知最大纪元，AcquireEpoch() 在 quorum 授权后单调递增取新号）。
//   - QuorumArbiter：FailoverDownNode 前置多数派仲裁（由 db.DB 实现并注入；
//     返回 nil 表示多数派可达，可执行重指派；返回错误表示 quorum 不足，降级只读）。
//
// 两者均由 db.Open 装配；nil = 未装配（M6 旧行为零回归）。

// EpochSource clusterEpoch 取号源（由 db.DB 实现）。
type EpochSource interface {
	Epoch() uint64
	AcquireEpoch() uint64
}

// QuorumArbiter FailoverDownNode 前置多数派仲裁（由 db.DB 实现）。
type QuorumArbiter interface {
	QuorumOK() error
}

// SetEpochSource 装配 clusterEpoch 取号源（db.Open 调用；nil = 不启用，零回归）。
func (e *Executor) SetEpochSource(src EpochSource) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.epochSrc = src
}

// SetQuorumArbiter 装配 quorum 仲裁器（db.Open 调用；nil = 不启用，零回归）。
func (e *Executor) SetQuorumArbiter(qa QuorumArbiter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.quorumArb = qa
}

// SetHAConfigSetter 装配 HA 开关节流控制器（db.Open 调用）。
func (e *Executor) SetHAConfigSetter(hc HAConfigSetter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.haCfg = hc
}

// writeFenceCheck 写路径入口 fencing 检查：未装配或放行返回 nil。
func (e *Executor) writeFenceCheck() error {
	if e.writeFence == nil {
		return nil
	}
	return e.writeFence.AllowWrite()
}

// applyHaEpochEnabled 解析并转发 ha_epoch_enabled。
func applyHaEpochEnabled(e *Executor, v string) error {
	if e.haCfg == nil {
		return &SQLError{Msg: "ha_epoch_enabled requires cluster HA configuration (start cluster first)"}
	}
	on, err := haParseBool(v)
	if err != nil {
		return &SQLError{Msg: "invalid value for ha_epoch_enabled: " + v + " (expected on/off)"}
	}
	e.haCfg.SetEpochEnabled(on)
	return nil
}

// applyHaQuorumEnabled 解析并转发 ha_quorum_enabled。
func applyHaQuorumEnabled(e *Executor, v string) error {
	if e.haCfg == nil {
		return &SQLError{Msg: "ha_quorum_enabled requires cluster HA configuration (start cluster first)"}
	}
	on, err := haParseBool(v)
	if err != nil {
		return &SQLError{Msg: "invalid value for ha_quorum_enabled: " + v + " (expected on/off)"}
	}
	e.haCfg.SetQuorumEnabled(on)
	return nil
}

// haParseBool 解析 on/off/true/false/1/0 布尔配置文本。
func haParseBool(v string) (bool, error) {
	switch v {
	case "on", "true", "1":
		return true, nil
	case "off", "false", "0":
		return false, nil
	}
	return false, fmt.Errorf("invalid bool: %s", v)
}

// 两个 HA 配置项在 configSpecs 注册表中的实现。
func initHAConfigSpecs() {
	configSpecs = append(configSpecs,
		&configSpec{
			key:  "ha_epoch_enabled",
			desc: "P0 HA：clusterEpoch 授权与写入 fencing（默认 off，M6 兼容零回归）；需先启动集群",
			def:  "off",
			apply: func(e *Executor, v string) error {
				return applyHaEpochEnabled(e, v)
			},
			read: func(e *Executor) string {
				if e.haCfg == nil {
					return "off"
				}
				return boolValStr(e.haCfg.EpochEnabled())
			},
		},
		&configSpec{
			key:  "ha_quorum_enabled",
			desc: "P0 HA：升主多数派仲裁 N/2+1（默认 off，M6 兼容零回归）；需先启动集群",
			def:  "off",
			apply: func(e *Executor, v string) error {
				return applyHaQuorumEnabled(e, v)
			},
			read: func(e *Executor) string {
				if e.haCfg == nil {
					return "off"
				}
				return boolValStr(e.haCfg.QuorumEnabled())
			},
		},
	)
}

// 幂等挂接 P0 HA 配置项（init 调用一次）。
var haConfigInited = false

func ensureHAConfigSpecs() {
	if haConfigInited {
		return
	}
	haConfigInited = true
	initHAConfigSpecs()
}

func init() {
	ensureHAConfigSpecs()
}
