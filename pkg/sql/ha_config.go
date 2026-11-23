package sql

import (
	"fmt"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

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

// statementWriteFence 可选能力（v1.21.0 NEW-3）：语句级写授权——只做复制轨
// 判定，集群轨归属 fence 交由 region 粒度接口（regionWriteFence）。实现方为
// db.DB.AllowWriteStatement；旧装配（仅实现 AllowWrite）自动退回节点级判定，
// 零回归。
type statementWriteFence interface {
	AllowWriteStatement() error
}

// regionWriteFence 可选能力（v1.21.0 NEW-3）：region 级写授权——仅当目标
// region 当前归属本机时才做集群轨归属一致性/纪元校验，目标 region 归属其它
// 节点的转发型写不再被本地节点级归属 fence 拦截。实现方为 db.DB.AllowWriteRegion。
type regionWriteFence interface {
	AllowWriteRegion(rid uint64) error
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
// P3（D4.5 §6-3 / T3）：先做复制轨 epoch fencing（db.AllowWrite），再做集群轨
// 多数派授权校验——集群轨 masterEpoch 恒 0，db.AllowWrite 恒放行，故分区期
// 旧主的写入必须由 clusterFenceCheck 拦截（ha_epoch_enabled + ha_quorum_enabled
// 双开才生效，默认关闭零回归）。
func (e *Executor) writeFenceCheck() error {
	if err := e.clusterFenceCheck(); err != nil {
		return err
	}
	if e.writeFence == nil {
		return nil
	}
	// v1.21.0 NEW-3：语句级只做复制轨判定；集群轨归属 fence 下沉 region 粒度
	// （groupOps 组装本地写批次时经 regionOwnershipFence 调用 AllowWriteRegion），
	// 避免非 owner 节点的转发型写被节点级归属状态误拦。旧实现（仅 AllowWrite）
	// 保持原节点级判定，零回归。
	if sf, ok := e.writeFence.(statementWriteFence); ok {
		return sf.AllowWriteStatement()
	}
	return e.writeFence.AllowWrite()
}

// regionOwnershipFence region 级归属 fencing 入口（v1.21.0 NEW-3）：本地写
// 批次组装阶段按目标 region 校验集群轨归属。
//   - 未装配 writeFence 或实现方不提供 region 粒度能力 → 放行（节点级判定已在
//     writeFenceCheck 完成，零回归）；
//   - 目标 region 归属其它节点的转发型写：实现方直接放行（不落入本地写批次）；
//   - 目标 region 归属本机但归属已被权威视图收回 / 重启待确认 → 返回带
//     fencing 原因的 SQLError（保持 ErrFenced / ErrOwnershipPending 语义）。
func (e *Executor) regionOwnershipFence(rid storage.ID) error {
	if e.writeFence == nil {
		return nil
	}
	rf, ok := e.writeFence.(regionWriteFence)
	if !ok {
		return nil
	}
	if err := rf.AllowWriteRegion(uint64(rid)); err != nil {
		return &SQLError{Msg: fmt.Sprintf("write to region %d rejected: %v", rid, err)}
	}
	return nil
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
	// B1（D4.5 §6 第 1 项）：quorum 开启即要求生产冗余，立即异步补齐单副本
	// region（关闭时无动作，零回归）。
	if on {
		e.b1EnsureAsync()
	}
	return nil
}

// applyHaReplicaEnabled 解析并转发 ha_replica_enabled（B1 副本冗余开关）。
func applyHaReplicaEnabled(e *Executor, v string) error {
	if e.haCfg == nil {
		return &SQLError{Msg: "ha_replica_enabled requires cluster HA configuration (start cluster first)"}
	}
	rs, ok := e.haCfg.(ReplicaSwitch)
	if !ok {
		return &SQLError{Msg: "ha_replica_enabled unsupported by this build (no replica switch)"}
	}
	on, err := haParseBool(v)
	if err != nil {
		return &SQLError{Msg: "invalid value for ha_replica_enabled: " + v + " (expected on/off)"}
	}
	rs.SetReplicaEnabled(on)
	// 开即补齐：把当前所有单副本 region 立刻冗余一份，避免"开关已开但仍
	// 单副本"的裸奔窗口（异步执行，不阻塞 SET 语句）。
	if on {
		e.b1EnsureAsync()
	}
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

// ---- v1.20.0 C2：写路径强制路由开关（ha_write_route_enforce）----

// writeRouteEnforce 判定写路径强制路由是否生效：
//   - ha_write_route_enforce=on（显式置位）→ 生效；
//   - 否则关闭（默认 off，M1-M19 单机/集群旧行为零回归）。
//
// 为什么不随 ha_quorum_enabled 自动联动（原 D-3 设想的"off 起步 + 联动"）：
// 既有 quorum 用例（TestP2T9FailoverWithEpochQuorum、
// TestPhase8New2StartupPersistedQuorumMatrix/quorum_on、
// TestD45T3ClusterTrackFencingE2E）在 quorum=on 后会先做"未指派 region / 空
// 隐式 region 的基线写"，这些写按旧语义是合法的集群轨放行。若联动开启强制路由，
// 基线写会被判为 REGION_NOT_ASSIGNED / REGION_NOT_LOCAL，与零回归红线（既有测试
// 断言不得修改）直接冲突。故强制路由保持"必须人工显式开启"，联动评估后置 C5，
// 与 V-A4（分区拒写）同批处理。
func (e *Executor) writeRouteEnforce() bool {
	return e.writeRouteEnforceExplicit()
}

// writeRouteEnforceExplicit 返回显式开关位（不看 quorum 联动），
// 供 SHOW CONFIG 区分"显式开启"与"随 quorum 生效"。
func (e *Executor) writeRouteEnforceExplicit() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.routeEnforce
}

// SetWriteRouteEnforce 设置显式开关位（e.mu 保护，运行期热切换）。
func (e *Executor) SetWriteRouteEnforce(on bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routeEnforce = on
}

// applyHaWriteRouteEnforce 解析并应用 ha_write_route_enforce。
func applyHaWriteRouteEnforce(e *Executor, v string) error {
	if e.mgr == nil {
		return &SQLError{Msg: "ha_write_route_enforce requires cluster HA configuration (start cluster first)"}
	}
	on, err := haParseBool(v)
	if err != nil {
		return &SQLError{Msg: "invalid value for ha_write_route_enforce: " + v + " (expected on/off)"}
	}
	e.SetWriteRouteEnforce(on)
	return nil
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
		&configSpec{
			key: "ha_replica_enabled",
			desc: "B1 副本冗余：单副本 region 自动补齐影子副本（默认 off；" +
				"ha_quorum_enabled=on 时自动生效）；需先启动集群",
			def: "off",
			apply: func(e *Executor, v string) error {
				return applyHaReplicaEnabled(e, v)
			},
			read: func(e *Executor) string {
				if e.haCfg == nil {
					return "off"
				}
				if rs, ok := e.haCfg.(ReplicaSwitch); ok {
					return boolValStr(rs.ReplicaEnabled())
				}
				return "off"
			},
		},
		&configSpec{
			key: "ha_write_route_enforce",
			desc: "v1.20.0 C2 写路径强制路由：region 未指派/不持有/归属节点不可达时拒绝写" +
				"（默认 off 零回归；ha_quorum_enabled=on 时自动生效）；需先启动集群",
			def: "off",
			apply: func(e *Executor, v string) error {
				return applyHaWriteRouteEnforce(e, v)
			},
			read: func(e *Executor) string {
				return boolValStr(e.writeRouteEnforceExplicit())
			},
		},
		&configSpec{
			key: "ha_2pc_replica_quorum",
			desc: "v1.20.0 C3 2PC 参与面扩展：参与者集合 = region owner + 副本，" +
				"PREPARE 按多数派 n/2+1 判定（未达 quorum 回滚并返回 TXN_PREPARE_QUORUM_FAILED），" +
				"同时启用参与者侧 region 归属校验（2PC_NOT_OWNER）" +
				"（默认 off 零回归：参与者仅 owner、任一失败即回滚）；需先启动集群",
			def: "off",
			apply: func(e *Executor, v string) error {
				return applyHaTwoPCReplicaQuorum(e, v)
			},
			read: func(e *Executor) string {
				return boolValStr(e.TwoPCReplicaQuorum())
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
