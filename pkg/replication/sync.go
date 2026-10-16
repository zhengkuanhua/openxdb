package replication

// T33 同步/半同步复制（M20）：在主端为 M2 异步复制增加 ACK 聚合等待器。
//
// 模式语义：
//   - ModeAsync：M2 原样，提交后不等待从节点 ACK（默认，零开销）。
//   - ModeSemiSync：提交返回客户端前等待至少 min(semiAckN, 在线副本数) 个
//     副本确认该位点已应用落盘；ACK 超时或在线副本不足时降级为异步继续，
//     记录降级事件（保证可用性）。
//   - ModeSync：提交返回客户端前等待全部在线副本确认；任一在线副本超时
//     未确认或没有在线副本时返回错误（不静默降级），保证已确认事务不丢。
//
// 协议复用 M2 的 ACK 帧（ACK 语义即"该 LSN 已应用落盘"），不新增帧类型；
// HELLO_OK 追加 1 字节 mode（长度兼容旧版 10B 帧）。详见 docs/T33。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ReplMode 主端复制模式。
type ReplMode uint8

const (
	// ModeAsync 异步复制（M2 默认）：提交不等待副本确认。
	ModeAsync ReplMode = 0
	// ModeSemiSync 半同步复制：等待至少 N 个副本 ACK，超时降级异步。
	ModeSemiSync ReplMode = 1
	// ModeSync 同步复制：等待全部在线副本 ACK，超时提交报错。
	ModeSync ReplMode = 2
)

// ParseReplMode 解析复制模式文本（大小写不敏感）。
func ParseReplMode(s string) (ReplMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "async", "":
		return ModeAsync, nil
	case "semi-sync", "semisync", "semi_sync":
		return ModeSemiSync, nil
	case "sync":
		return ModeSync, nil
	default:
		return ModeAsync, fmt.Errorf("replication: unknown mode %q (expected async/semi-sync/sync)", s)
	}
}

// String 返回模式文本（SHOW CONFIG / SyncStatus 展示用）。
func (m ReplMode) String() string {
	switch m {
	case ModeSemiSync:
		return "semi-sync"
	case ModeSync:
		return "sync"
	default:
		return "async"
	}
}

// 默认参数。
const (
	DefaultSemiAckN   = 1
	DefaultAckTimeout = 10 * time.Second
)

// SyncStatus 主端复制同步状态快照。
type SyncStatus struct {
	Mode           ReplMode      // 当前模式
	SemiAckN       int           // 半同步最少确认副本数
	Timeout        time.Duration // ACK 等待超时
	Degraded       bool          // 半同步当前是否处于降级异步状态
	DegradedLSN    storage.LSN   // 最近一次触发降级的位点
	DegradedAt     time.Time     // 最近一次降级时间
	DegradedCount  int64         // 累计降级次数
	LastWaitMs     int64         // 最近一次 WaitAck 实际等待毫秒
	Waiting        bool          // 当前是否有提交在等待 ACK
	OnlineSlaves   int           // 当前在线副本数
	AckedSlaves    int           // 当前已确认到最新位点的在线副本数
}

// 标准错误（T33 新增）。
var (
	// ErrInvalidMode 非法复制模式。
	ErrInvalidMode = errors.New("replication: invalid mode")
	// ErrBadAckN 半同步最少确认副本数非法（<1）。
	ErrBadAckN = errors.New("replication: semi-sync ack count must be >= 1")
	// ErrBadTimeout ACK 等待超时非法（<0）。
	ErrBadTimeout = errors.New("replication: ack timeout must be >= 0")
	// ErrAckTimeout 同步复制等待全部副本确认超时（事务已本地提交，客户端报错）。
	ErrAckTimeout = errors.New("replication: sync ack timeout, transaction committed locally but not confirmed by all replicas")
	// ErrNoSlave 同步复制没有在线副本，无法达成全确认（拒绝伪同步提交）。
	ErrNoSlave = errors.New("replication: sync requires at least one online replica")
	// ErrReplicationNotStarted 复制未启用时设置模式返回。
	ErrReplicationNotStarted = errors.New("replication: replication not started")
)

// SetReplicationMode 热切换复制模式与参数。
//
//	mode  ModeAsync / ModeSemiSync / ModeSync
//	ackN  半同步最少确认副本数（>=1；async 下忽略）
//	timeout ACK 等待超时（<=0 使用默认 10s；async 下忽略）
func (m *MasterReplicator) SetReplicationMode(mode ReplMode, ackN int, timeout time.Duration) error {
	if mode != ModeAsync && mode != ModeSemiSync && mode != ModeSync {
		return ErrInvalidMode
	}
	if ackN < 1 {
		return ErrBadAckN
	}
	if timeout < 0 {
		return ErrBadTimeout
	}
	if timeout == 0 {
		timeout = DefaultAckTimeout
	}
	m.mu.Lock()
	m.mode = mode
	if mode == ModeSemiSync {
		m.semiAckN = ackN
		m.ackTimeout = timeout
	} else {
		m.semiAckN = DefaultSemiAckN
		m.ackTimeout = DefaultAckTimeout
	}
	m.mu.Unlock()
	return nil
}

// ReplicationMode 返回当前复制模式。
func (m *MasterReplicator) ReplicationMode() ReplMode {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mode
}

// SyncStatus 返回主端同步状态快照。
func (m *MasterReplicator) SyncStatus() SyncStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := SyncStatus{
		Mode:          m.mode,
		SemiAckN:      m.semiAckN,
		Timeout:       m.ackTimeout,
		Degraded:      m.degraded,
		DegradedLSN:   m.degLSN,
		DegradedAt:    m.degAt,
		DegradedCount: m.degCount,
		LastWaitMs:    m.lastWaitMs,
		Waiting:       m.waiting,
	}
	last := m.store.LastLSN()
	for _, info := range m.known {
		if info.online {
			st.OnlineSlaves++
			if info.lastAck >= last {
				st.AckedSlaves++
			}
		}
	}
	return st
}

// WaitAck 按当前模式等待副本确认位点 lsn。
//
//   - ModeAsync：立即返回 nil（M2 兼容，零开销）。
//   - ModeSemiSync：等待至少 min(semiAckN, 在线副本数) 个副本 lastAck>=lsn；
//     在线副本不足立即降级；超时降级异步并返回 nil（提交成功）。
//   - ModeSync：等待全部在线副本 lastAck>=lsn；无在线副本返回 ErrNoSlave；
//     超时返回 ErrAckTimeout。
//
// ctx 用于外部取消（如 DB Close），取消返回 ctx.Err()。
func (m *MasterReplicator) WaitAck(ctx context.Context, lsn storage.LSN) error {
	m.mu.Lock()
	mode := m.mode
	ackN := m.semiAckN
	timeout := m.ackTimeout
	if mode == ModeAsync {
		m.mu.Unlock()
		return nil
	}
	// 快照需要等待的在线副本数（只统计尚未确认到位点的在线副本）。
	need := 0
	for _, info := range m.known {
		if info.online && info.lastAck < lsn {
			need++
		}
	}
	if mode == ModeSemiSync && need > ackN {
		need = ackN
	}
	// 快照边界：sync 必须至少有一个在线副本（拒绝伪同步）；semi-sync 无在线
	// 副本立即降级异步（提交成功，记录降级）。
	if mode == ModeSync && need == 0 {
		m.mu.Unlock()
		return ErrNoSlave
	}
	if mode == ModeSemiSync && need == 0 {
		m.degraded = true
		m.degLSN = lsn
		m.degAt = time.Now()
		m.degCount++
		m.mu.Unlock()
		return nil
	}
	m.waiting = true
	start := time.Now()
	m.mu.Unlock()

	deadline := start.Add(timeout)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	check := func() (int, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		done := 0
		for _, info := range m.known {
			if info.online && info.lastAck >= lsn {
				done++
			}
		}
		return done, nil
	}

	finish := func(err error, degraded bool) error {
		m.mu.Lock()
		m.waiting = false
		m.lastWaitMs = time.Since(start).Milliseconds()
		if degraded {
			m.degraded = true
			m.degLSN = lsn
			m.degAt = time.Now()
			m.degCount++
		}
		m.mu.Unlock()
		return err
	}

	for {
		done, _ := check()
		if done >= need {
			return finish(nil, false)
		}
		select {
		case <-ctx.Done():
			return finish(ctx.Err(), false)
		case <-tick.C:
			if time.Now().After(deadline) {
				if mode == ModeSemiSync {
					return finish(nil, true) // 降级异步，提交成功
				}
				return finish(ErrAckTimeout, false)
			}
		}
	}
}
