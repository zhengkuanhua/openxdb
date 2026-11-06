package db

// B1（D4.5 报告 §6 第 1 项）：单副本不可恢复 → 副本冗余与无损恢复（db 层装配）。
//
// 职责：
//  1) 持有 ha_replica_enabled 开关（SetReplicaEnabled/ReplicaEnabled），并实现
//     sql.ReplicaSwitch 供 SET 语句读写；
//  2) StartCluster 时按开关启动 ha_replica_maintainer 维护循环，周期调用
//     SQL.EnsureRegionReplicas 补齐单副本 region（quorum 开启时自动生效）；
//  3) Close 时停止循环，避免关闭竞态。
//
// 默认 off：不启动协程、不产生任何补齐动作，单机/未启用路径零回归。

import (
	"log"
	"time"
)

// defaultReplicaMaintainInterval 副本维护周期：兼顾"单副本裸奔窗口足够短"
// 与"扫描/推送开销可控"（每次仅对未达标 region 推送）。
const defaultReplicaMaintainInterval = 3 * time.Second

// SetReplicaEnabled 开关副本冗余维护（ha_replica_enabled）。
// on 为真时若集群已启动且循环未运行，则立即启动维护循环并异步补齐一次。
func (d *DB) SetReplicaEnabled(on bool) {
	d.replicaMu.Lock()
	d.replicaEnabled = on
	running := d.replicaStop != nil
	needStart := on && !running && d.Cluster != nil && d.SQL != nil
	if !on && running {
		close(d.replicaStop)
		d.replicaStop = nil
	}
	d.replicaMu.Unlock()

	if !on {
		return
	}
	if needStart {
		d.startReplicaMaintainer()
	}
	// 开即补齐（不阻塞 SET；由 SQL 内部异步执行）。
	if d.SQL != nil {
		go func() {
			if _, err := d.SQL.EnsureRegionReplicas(); err != nil {
				log.Printf("db: B1 replica ensure: %v", err)
			}
		}()
	}
}

// ReplicaEnabled 返回副本冗余开关状态（默认 false）。
func (d *DB) ReplicaEnabled() bool {
	d.replicaMu.Lock()
	defer d.replicaMu.Unlock()
	return d.replicaEnabled
}

// ReplicaMaintaining 返回维护循环是否在运行（观测/测试用）。
func (d *DB) ReplicaMaintaining() bool {
	d.replicaMu.Lock()
	defer d.replicaMu.Unlock()
	return d.replicaStop != nil
}

// startReplicaMaintainer 启动副本维护循环（幂等：已运行则直接返回）。
func (d *DB) startReplicaMaintainer() {
	d.replicaMu.Lock()
	if d.replicaStop != nil {
		d.replicaMu.Unlock()
		return
	}
	interval := d.replicaInterval
	if interval <= 0 {
		interval = defaultReplicaMaintainInterval
	}
	stop := make(chan struct{})
	d.replicaStop = stop
	d.replicaMu.Unlock()

	go func() {
		tk := time.NewTicker(interval)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				d.replicaMaintainOnce()
			}
		}
	}()
}

// stopReplicaMaintainer 停止副本维护循环（Close 装配）。
func (d *DB) stopReplicaMaintainer() {
	d.replicaMu.Lock()
	defer d.replicaMu.Unlock()
	if d.replicaStop == nil {
		return
	}
	close(d.replicaStop)
	d.replicaStop = nil
}

// replicaMaintainOnce 执行一轮副本补齐（单轮失败不终止循环：网络抖动/
// 节点短暂不可达后下一轮自愈）。
func (d *DB) replicaMaintainOnce() {
	if d.SQL == nil {
		return
	}
	n, err := d.SQL.EnsureRegionReplicas()
	if err != nil {
		log.Printf("db: B1 replica maintain: %v", err)
		return
	}
	if n > 0 {
		log.Printf("db: B1 replica maintain: %d region(s) replicated", n)
	}
}

// SetReplicaMaintainInterval 配置副本维护周期（测试/运维调优用；<=0 用默认 3s）。
// 必须在 SetReplicaEnabled(true) 之前或循环重启后生效。
func (d *DB) SetReplicaMaintainInterval(interval time.Duration) {
	d.replicaMu.Lock()
	defer d.replicaMu.Unlock()
	d.replicaInterval = interval
}

// B1Stats 返回 B1 修复的三项观测指标（供报告/回归验收读取）：
// pending = 因无存活副本待重试的 region 数；
// lost = 集群内已无任何副本的 region 数（无法从集群内恢复，需备份介入）；
// ensured = 累计补齐的副本数。
func (d *DB) B1Stats() (pending, lost int, ensured uint64) {
	if d.SQL == nil {
		return 0, 0, 0
	}
	return len(d.SQL.B1PendingRegions()), len(d.SQL.B1LostRegions()), d.SQL.B1EnsureCount()
}
