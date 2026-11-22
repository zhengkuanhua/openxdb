package db

// v1.20.0 C4：持续复制 region 化（影子副本侧）。
//
// 背景（C1_M8_gap_report.md §4.4 V-C7 基线缺口）：
//   v1.19.0 的 EnsureRegionReplicas 是"静态一次性 bootstrap"——只在被调用那一刻
//   把 owner 当前存量推给影子副本；此后 owner 的新写入不再同步，影子副本永久
//   停在 bootstrap 时刻。
//
// 本文件的改造：
//   1. owner 侧：开启新开关后把提交写入落成"仅落盘 binlog"（不监听复制端口、
//      不改变既有 M2 主从链路），条目携带 region 维度（见 replication.RegionOfBatch）；
//   2. 副本侧：后台循环按 region 拉取 owner 的增量并应用到本地存储；
//   3. 位点：m:repl:region:<rid>:lsn 单调推进（同一事务内与数据一起落盘）；
//   4. 默认 off：开关 ha_replica_region_sync 未开启时，整条路径不存在
//      （不建 binlog 文件、不起协程、不写位点键），旧行为完全不变。
//
// 复用决策（DEV_PLAN §4 D-1）：复用 M2 复制轨的 binlog 与条目结构，不新建
// region 级同步通道；副本集合仍走独立元数据键 m:repl:replicas:<rid>（D-2，C3 已落地）。

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// RegionBinlogFile 集群轨 region 增量日志文件名（与 M2 的 binlog.log 分离，
// 避免与 StartReplicationTLS 争用同一文件锁）。
const RegionBinlogFile = "region.binlog"

// regionLSNPrefix 副本位点键前缀：m:repl:region:<rid>:lsn。
const regionLSNPrefix = "m:repl:region:"

// DefaultRegionSyncInterval 副本跟随 owner 的拉取周期。
// 红线 4（超时参数不得放大）针对的是既有 3 秒复制 / 5 秒 2PC；本周期为 C4 新增，
// 取 200ms 以满足 V-C2「延迟 < 500ms」。
const DefaultRegionSyncInterval = 200 * time.Millisecond

// regionSyncBatchLimit 单轮单 region 最多拉取条目数（防止大 backlog 卡住循环）。
const regionSyncBatchLimit = 256

// ErrRegionSyncNoSource 本机没有可用的 region 增量日志（未开启或未落盘 binlog）。
var ErrRegionSyncNoSource = errors.New("db: region sync log unavailable")

// ErrRegionSyncNotOwnerSource 目标节点未注册 region 增量日志源。
var ErrRegionSyncNotOwnerSource = errors.New("db: region sync log source not registered")

// RegionLogSource 按 region 读取增量的日志源（owner 侧能力）。
// 生产实现是 *DB（包装本地 binlog）；测试可注入内存实现模拟断连与积压。
type RegionLogSource interface {
	ReadRegion(from storage.LSN, regionID storage.ID, limit int) ([]*replication.BinlogEntry, error)
}

// RegionSyncSwitch 由 pkg/sql 的 HA 配置项回调（ha_replica_region_sync）。
// 放在 db 包是为了让 pkg/sql 不反向依赖 db（与既有 HA 开关同构）。
type RegionSyncSwitch interface {
	SetRegionSyncEnabled(on bool) error
	RegionSyncEnabled() bool
}

// 编译期确认 *DB 同时提供两类能力（副本侧开关 + owner 侧日志源）。
var (
	_ RegionSyncSwitch = (*DB)(nil)
	_ RegionLogSource  = (*DB)(nil)
)

// ---------------------------------------------------------------------------
// owner 侧：region 日志源注册表（进程内寻址）
// ---------------------------------------------------------------------------

var (
	regionLogMu   sync.RWMutex
	regionLogSrcs = map[string]RegionLogSource{}
)

// RegisterRegionLogSource 注册某节点的 region 增量日志源。
// 同一进程内多节点（测试 / 单机多实例）靠 nodeID 区分，不依赖网络端口。
func RegisterRegionLogSource(nodeID string, src RegionLogSource) {
	if nodeID == "" || src == nil {
		return
	}
	regionLogMu.Lock()
	regionLogSrcs[nodeID] = src
	regionLogMu.Unlock()
}

// UnregisterRegionLogSource 注销某节点的日志源（节点关闭时调用）。
func UnregisterRegionLogSource(nodeID string) {
	if nodeID == "" {
		return
	}
	regionLogMu.Lock()
	delete(regionLogSrcs, nodeID)
	regionLogMu.Unlock()
}

// LookupRegionLogSource 查找某节点的日志源。
func LookupRegionLogSource(nodeID string) (RegionLogSource, bool) {
	regionLogMu.RLock()
	src, ok := regionLogSrcs[nodeID]
	regionLogMu.RUnlock()
	return src, ok
}

// SetRegionLogSource 注入日志源（测试用；等价于 RegisterRegionLogSource）。
func SetRegionLogSource(nodeID string, src RegionLogSource) {
	RegisterRegionLogSource(nodeID, src)
}

// ---------------------------------------------------------------------------
// owner 侧：binlog 落盘（条目携带 region 维度）
// ---------------------------------------------------------------------------

// appendBinlogEntry 把一次提交落成 binlog 条目。
// 目标优先级：既有的 M2 主端（Replicator，含 StartReplicationTLS）> region 日志。
// 两者皆无时静默返回（单机零回归：不产生 binlog 文件）。
func (d *DB) appendBinlogEntry(lsn storage.LSN, batch *storage.WriteBatch) error {
	r := d.binlogTarget()
	if r == nil {
		return nil
	}
	return r.Append(&replication.BinlogEntry{
		CommitLSN: lsn,
		Batch:     batch,
		SchemaVer: 0,
		RegionID:  replication.RegionOfBatch(batch),
	})
}

// binlogTarget 返回当前提交写入应落的 binlog 目标。
func (d *DB) binlogTarget() *replication.MasterReplicator {
	if d.Replicator != nil {
		return d.Replicator
	}
	d.regionSyncMu.Lock()
	r := d.regionSyncLog
	d.regionSyncMu.Unlock()
	return r
}

// commitHookAppend 生成提交钩子（M2 复制挂接点，C1 引入）。
func (d *DB) commitHookAppend() func(lsn storage.LSN, seq uint64, batch *storage.WriteBatch) error {
	return func(lsn storage.LSN, seq uint64, batch *storage.WriteBatch) error {
		return d.appendBinlogEntry(lsn, batch)
	}
}

// ensureRegionLog 保证本机存在 region 增量日志（仅在开关开启时调用）。
func (d *DB) ensureRegionLog() error {
	if d.Replicator != nil {
		return nil // 已有 M2 主端，直接复用其 binlog
	}
	d.regionSyncMu.Lock()
	if d.regionSyncLog != nil {
		d.regionSyncMu.Unlock()
		return nil
	}
	d.regionSyncMu.Unlock()
	r, err := replication.NewBinlogOnly(filepath.Join(d.Dir, RegionBinlogFile))
	if err != nil {
		return fmt.Errorf("db: open region binlog: %w", err)
	}
	d.regionSyncMu.Lock()
	if d.regionSyncLog != nil {
		d.regionSyncMu.Unlock()
		_ = r.Close()
		return nil
	}
	d.regionSyncLog = r
	d.regionSyncMu.Unlock()
	d.Txn.SetCommitHook(d.commitHookAppend())
	return nil
}

// ReadRegion 实现 RegionLogSource：从本机 binlog 读取 LSN>=from 的 region 增量。
func (d *DB) ReadRegion(from storage.LSN, regionID storage.ID, limit int) ([]*replication.BinlogEntry, error) {
	r := d.binlogTarget()
	if r == nil {
		return nil, ErrRegionSyncNoSource
	}
	return r.ReadRegionFrom(from, regionID, limit)
}

// ---------------------------------------------------------------------------
// 副本侧：位点与增量应用
// ---------------------------------------------------------------------------

// regionSyncLSNKey 副本位点键：m:repl:region:<rid>:lsn（值 8B 大端 LSN）。
func regionSyncLSNKey(regionID storage.ID) []byte {
	return []byte(fmt.Sprintf("%s%d:lsn", regionLSNPrefix, uint64(regionID)))
}

// RegionSyncLSN 读取本机记录的 region 复制位点（未推进过返回 0）。
func (d *DB) RegionSyncLSN(regionID storage.ID) (storage.LSN, error) {
	v, err := d.Storage.Get(regionSyncLSNKey(regionID))
	if err != nil {
		// 位点键缺失表示"该 region 尚未跟随过任何增量"，起始位点即 0；
		// 若向上抛 ErrNotFound，跟随循环会在首轮就判定失败并永久停滞。
		if errors.Is(err, storage.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if len(v) < 8 {
		return 0, nil
	}
	return storage.LSN(binary.BigEndian.Uint64(v[:8])), nil
}

// applyRegionEntry 应用一条增量到本地存储，并推进位点键。
//
// 裁剪语义：只应用物理键属于 rid 的部分（单 region 批次即全量）。
// 旧条目 RegionID==0（v1 文件 / 跨 region 整批）走同一裁剪逻辑：
// 键前缀已带 region，因此"整批应用"在副本侧等价于按 region 裁剪，
// 既满足 V-C4 的向下兼容，又不会把 owner 的元数据键（m:2pc:*/m:regions）
// 覆盖到副本自己的元数据上。
//
// 幂等：位点键与数据同批落盘，重复投递的条目因 LSN<=位点被跳过。
func (d *DB) applyRegionEntry(regionID storage.ID, e *replication.BinlogEntry) (storage.LSN, error) {
	if e == nil {
		return 0, nil
	}
	if e.RegionID != 0 && e.RegionID != regionID {
		return 0, nil // 不属于本 region（防御：日志源已过滤）
	}
	batch := &storage.WriteBatch{}
	if e.Batch != nil {
		for _, p := range e.Batch.Puts {
			if !keyInRegion(p.Key, regionID) {
				continue
			}
			batch.Puts = append(batch.Puts, p)
		}
		for _, k := range e.Batch.Deletes {
			if !keyInRegion(k, regionID) {
				continue
			}
			batch.Deletes = append(batch.Deletes, k)
		}
	}
	batch.Puts = append(batch.Puts, storage.KVPair{Key: regionSyncLSNKey(regionID), Value: encodeRegionLSN(e.LSN)})
	if err := d.Storage.Write(batch); err != nil {
		return 0, err
	}
	return e.LSN, nil
}

// encodeRegionLSN 位点键值编码：8 字节大端（单调可比）。
func encodeRegionLSN(lsn storage.LSN) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(lsn))
	return b
}

// keyInRegion 判断物理键是否属于指定 region。
func keyInRegion(k []byte, regionID storage.ID) bool {
	if len(k) < sharding.RegionKeyPrefixLen || k[0] != 'r' {
		return false
	}
	rid, _ := sharding.DecodeRegionKey(k)
	return rid == regionID
}

// ---------------------------------------------------------------------------
// 副本侧：跟随循环
// ---------------------------------------------------------------------------

// SetRegionSyncEnabled 打开 / 关闭 region 增量复制（新开关，默认 off）。
//   - 打开：确保 region 日志落盘 + 启动跟随循环（需集群已装配）；
//   - 关闭：停止循环（幂等）。
func (d *DB) SetRegionSyncEnabled(on bool) error {
	d.regionSyncMu.Lock()
	if d.regionSyncEnabled == on {
		d.regionSyncMu.Unlock()
		return nil
	}
	d.regionSyncMu.Unlock()

	if on {
		if err := d.ensureRegionLog(); err != nil {
			return err
		}
	}
	d.regionSyncMu.Lock()
	d.regionSyncEnabled = on
	d.regionSyncMu.Unlock()
	if on {
		d.startRegionSyncLoop()
	} else {
		d.stopRegionSync()
	}
	return nil
}

// RegionSyncEnabled 报告 region 增量复制开关状态。
func (d *DB) RegionSyncEnabled() bool {
	d.regionSyncMu.Lock()
	defer d.regionSyncMu.Unlock()
	return d.regionSyncEnabled
}

// maybeStartRegionSync 在集群装配完成后按开关状态启动循环（StartCluster 调用）。
func (d *DB) maybeStartRegionSync() {
	if !d.RegionSyncEnabled() || d.Cluster == nil {
		return
	}
	d.registerRegionLogSource()
	d.startRegionSyncLoop()
}

// registerRegionLogSource 把本机注册为 region 增量日志源（供其他节点拉取）。
func (d *DB) registerRegionLogSource() {
	if d.Cluster == nil {
		return
	}
	id := d.Cluster.SelfNode().ID
	if id == "" {
		return
	}
	d.regionSyncMu.Lock()
	d.regionSyncSelfID = id
	d.regionSyncMu.Unlock()
	RegisterRegionLogSource(id, d)
}

// unregisterRegionLogSource 注销本机日志源（Close 调用）。
func (d *DB) unregisterRegionLogSource() {
	d.regionSyncMu.Lock()
	id := d.regionSyncSelfID
	d.regionSyncSelfID = ""
	d.regionSyncMu.Unlock()
	UnregisterRegionLogSource(id)
}

// startRegionSyncLoop 启动跟随循环（幂等）。
func (d *DB) startRegionSyncLoop() {
	if d.Cluster == nil {
		return
	}
	d.regionSyncMu.Lock()
	if d.regionSyncStop != nil {
		d.regionSyncMu.Unlock()
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	d.regionSyncStop = stop
	d.regionSyncDone = done
	d.regionSyncMu.Unlock()

	d.registerRegionLogSource()
	interval := d.regionSyncIntervalOrDefault()
	go func() {
		defer close(done)
		tk := time.NewTicker(interval)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				d.regionSyncOnce()
			}
		}
	}()
}

// stopRegionSync 停止跟随循环并等待退出（幂等）。
func (d *DB) stopRegionSync() {
	d.regionSyncMu.Lock()
	stop, done := d.regionSyncStop, d.regionSyncDone
	d.regionSyncStop, d.regionSyncDone = nil, nil
	d.regionSyncMu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	if done != nil {
		<-done
	}
}

// closeRegionLog 关闭 region 增量日志（Close 调用）。
func (d *DB) closeRegionLog() {
	d.regionSyncMu.Lock()
	r := d.regionSyncLog
	d.regionSyncLog = nil
	d.regionSyncMu.Unlock()
	if r != nil {
		_ = r.Close()
	}
}

// regionSyncIntervalOrDefault 返回拉取周期（测试可设 regionSyncInterval 加速）。
func (d *DB) regionSyncIntervalOrDefault() time.Duration {
	d.regionSyncMu.Lock()
	iv := d.regionSyncInterval
	d.regionSyncMu.Unlock()
	if iv <= 0 {
		return DefaultRegionSyncInterval
	}
	return iv
}

// SetRegionSyncInterval 调整拉取周期（测试用；<=0 表示恢复默认）。
func (d *DB) SetRegionSyncInterval(iv time.Duration) {
	d.regionSyncMu.Lock()
	d.regionSyncInterval = iv
	d.regionSyncMu.Unlock()
}

// regionSyncOnce 执行一轮"跟随 owner 增量"：遍历本机路由视图中的远端 region，
// 从各自的位点拉取增量并应用。owner 缺失 / 日志源不可达时跳过（不报错，
// 下一轮重试），因此天然支持断连后追平（V-C3）。
func (d *DB) regionSyncOnce() int {
	if d.Cluster == nil || d.SQL == nil {
		return 0
	}
	self := d.Cluster.SelfNode().ID
	if self == "" {
		return 0
	}
	applied := 0
	for _, rg := range d.SQL.Regions() {
		if rg.Node == "" || rg.Node == self {
			continue // 无归属已知 / 本机即为 owner（无需影子复制）
		}
		src, ok := LookupRegionLogSource(rg.Node)
		if !ok {
			continue // owner 未开启 region 日志，下一轮再看
		}
		pos, err := d.RegionSyncLSN(rg.RegionID)
		if err != nil {
			d.noteRegionSyncErr(rg.RegionID, err)
			continue
		}
		entries, err := src.ReadRegion(pos+1, rg.RegionID, regionSyncBatchLimit)
		if err != nil {
			d.noteRegionSyncErr(rg.RegionID, err)
			continue
		}
		for _, e := range entries {
			if e == nil || e.LSN <= pos {
				continue
			}
			next, err := d.applyRegionEntry(rg.RegionID, e)
			if err != nil {
				d.noteRegionSyncErr(rg.RegionID, err)
				break
			}
			if next > pos {
				pos = next
				applied++
				d.noteRegionSyncErr(rg.RegionID, nil)
			}
		}
	}
	return applied
}

// noteRegionSyncErr 记录/清除某 region 的最近一次同步错误（观测用）。
func (d *DB) noteRegionSyncErr(regionID storage.ID, err error) {
	d.regionSyncMu.Lock()
	defer d.regionSyncMu.Unlock()
	if err == nil {
		delete(d.regionSyncErrs, regionID)
		return
	}
	if d.regionSyncErrs == nil {
		d.regionSyncErrs = map[storage.ID]error{}
	}
	d.regionSyncErrs[regionID] = err
}

// RegionSyncError 返回某 region 最近一次同步错误（nil 表示最近一轮成功）。
func (d *DB) RegionSyncError(regionID storage.ID) error {
	d.regionSyncMu.Lock()
	defer d.regionSyncMu.Unlock()
	return d.regionSyncErrs[regionID]
}

// RegionSyncActive 报告跟随协程是否在运行（V-C1 断言无协程用）。
func (d *DB) RegionSyncActive() bool {
	d.regionSyncMu.Lock()
	defer d.regionSyncMu.Unlock()
	return d.regionSyncStop != nil
}
