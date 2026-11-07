package db

// P9（D4.5 Phase 9 / 候选 B）：NEW-1 与 T3 同源的集群轨归属纪元收口。
//
// 缺陷（D4.5 §6 / Phase7 复测）：写路径的 masterEpoch 只由复制轨
// （StartReplication / PromoteToMaster）推进。纯集群轨（no replication）没有
// 升主动作，masterEpoch 恒 0，AllowWrite 的 masterEpoch>0 判定永不成立 →
//   - T3：分区期少数派节点（旧主）写入不受 epoch fencing 约束；
//   - NEW-1：节点重启装载旧 m:regions 视图后仍认为自己是 owner（local=true），
//     陈旧写覆盖多数派新 owner 已确认的数据。
//
// 修复（集群轨归属纪元 cluster term，与 clusterEpoch 同源单调）：
//  1. 授予：节点经 failover 接管（REGION_PUSH ClaimOwner）或权威全量路由视图
//     确认本机持有 region 时，记 clusterOwnTerm=term，并把已知纪元 clusterTerm
//     推到该 term——集群轨由此获得可比较的"授权纪元"。
//  2. 收回：权威全量视图（Authoritative=true）显示本机不再持有任何 region 时，
//     置 clusterOwned=false（归属在更高纪元被取代）。
//  3. 持久化：clusterTerm / clusterOwnTerm / clusterOwned 落盘 cluster_term.meta。
//     重启装载后若此前仍持有归属，先置 clusterPend=true：归属待权威视图确认，
//     确认前 AllowWrite 拒绝写入（ErrOwnershipPending）。
//  4. 拦截：AllowWrite 在 clusterOwnTerm>0 时按"待确认"与"已收回"两种状态拒绝
//     写入（ErrOwnershipPending / ErrFenced），陈旧主与重启旧视图由此无法写入。
//
// 与既有能力的关系：本文件不修改 DB.AllowWrite 的复制轨分支（masterEpoch /
// epoch 判定原样保留），不修改 T3 的多数派 fencing（sql.clusterFenceCheck）与
// 0303c13 的 region 视图未确认栅栏（sql.regionOwnerFence），只在其上补齐
// 集群轨归属纪元这一缺失环节。

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

// ErrOwnershipPending 重启后归属尚未被权威路由视图确认，拒绝写入。
// 语义区别于 ErrFenced 的"已被更高纪元取代"：这是"归属状态未知，先拒写直到
// 确认"，用于封住 NEW-1 的"重启后按旧视图抢先写"窗口。
var ErrOwnershipPending = fmt.Errorf("%w: region ownership unconfirmed after restart", ErrFenced)

// clusterOwnershipFenceLocked 集群轨归属 fencing（调用方须持有 epochMu）。
//   - 重启后归属待权威视图确认（clusterPend）：拒绝，避免按旧视图抢先写——
//     该分支不要求 clusterOwnTerm>0，因为"从未经过 failover 授权、但本地视图
//     仍持有 region"的重启节点正是 NEW-1 的陈旧主（Phase7 实测 A 重启后
//     owner=A/local=true 仍可写）；
//   - 从未获得过归属纪元（clusterOwnTerm==0）：放行（单机/从未持归属的节点零回归）；
//   - 归属已被权威视图收回（!clusterOwned）：拒绝，本节点为陈旧主。
func (d *DB) clusterOwnershipFenceLocked() error {
	if d.clusterPend {
		d.conflictCount++ // 数据冲突标记：重启未确认期写尝试被拒
		return fmt.Errorf("%w: cluster term %d, awaiting authoritative route view", ErrOwnershipPending, d.clusterOwnTerm)
	}
	if d.clusterOwnTerm == 0 {
		return nil
	}
	if !d.clusterOwned {
		d.conflictCount++ // 数据冲突标记：陈旧主写尝试被 fence
		return fmt.Errorf("%w: cluster ownership revoked at term %d (known term %d)", ErrFenced, d.clusterOwnTerm, d.clusterTerm)
	}
	return nil
}

// loadClusterTermMeta 装载 cluster_term.meta（Open 时调用，紧接 loadEpochMeta）。
// 文件缺失 = 从未获得集群轨归属（零回归）；内容损坏按零值处理（不阻断启动）。
func (d *DB) loadClusterTermMeta() {
	if d.clusterTermFilePath == "" {
		return
	}
	b, err := os.ReadFile(d.clusterTermFilePath)
	if err != nil {
		return
	}
	var term, own uint64
	var owned, pending int
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) != 2 {
			continue
		}
		v, perr := strconv.ParseUint(fields[1], 10, 64)
		if perr != nil {
			continue
		}
		switch fields[0] {
		case "term":
			term = v
		case "own":
			own = v
		case "owned":
			owned = int(v)
		case "pending":
			pending = int(v)
		}
	}
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	d.clusterTerm = term
	d.clusterOwnTerm = own
	d.clusterOwned = owned == 1
	// 重启前仍持有归属：先按"待确认"处理，等权威路由视图确认（NEW-1 关键）。
	// 上一次运行已确认收回的（owned=0 且 own>0）保持收回，重启后仍拒绝写入。
	d.clusterPend = d.clusterOwned || pending == 1
	if own > d.epoch {
		d.persistEpochLocked(own) // 归属纪元与已知最大 epoch 同源单调，取并集
	}
}

// persistClusterTermLocked 落盘集群轨归属纪元状态（尽力而为；调用方须持有
// epochMu）。失败仅影响重启后的 fencing 下限，不阻断运行期判定。
func (d *DB) persistClusterTermLocked() {
	if d.clusterTermFilePath == "" {
		return
	}
	body := fmt.Sprintf("term %d\nown %d\nowned %d\npending %d\n",
		d.clusterTerm, d.clusterOwnTerm, boolToInt(d.clusterOwned), boolToInt(d.clusterPend))
	if err := os.WriteFile(d.clusterTermFilePath, []byte(body), 0o644); err != nil {
		log.Printf("db: persist cluster term meta failed: %v", err)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// NoteOwnershipView 集群轨归属纪元推进入口。
//
// 由 cluster.Server 在应用完路由视图/数据装载消息后回调（db.StartCluster 经
// SetOwnershipNotify 装配），承载 failover 接管、重启刷新、增量广播三类路径。
//   - term：消息携带的集群纪元（0 = 源端无纪元信息，只学归属不推进纪元）；
//   - selfOwns：本次应用后本节点是否仍持有任一 region 归属；
//   - authoritative：是否为权威全量视图——只有此类视图才能据"本机不再持有
//     任何 region"判定归属被收回（增量广播只含本次变更条目，不得据此收回，
//     否则会误 fence 仍持有其它 region 的节点）。
//
// 语义：
//   - term 单调推进（取并集，回退的陈旧消息不降低已知纪元）；
//   - 授予：selfOwns 时 clusterOwnTerm 取 max(term, 现值)，clusterOwned=true，
//     并解除重启后的"待确认"状态；
//   - 收回：authoritative && !selfOwns && 此前持有 → clusterOwned=false，
//     写路径随即被 ErrFenced 拒绝（陈旧主/重启旧视图不能再写）；
//   - 任何在线归属消息都会解除"待确认"，避免重启后永久只读。
func (d *DB) NoteOwnershipView(term uint64, selfOwns, authoritative bool) {
	d.epochMu.Lock()
	if term > d.clusterTerm {
		d.clusterTerm = term
	}
	prevPend := d.clusterPend
	d.clusterPend = false
	var granted, revoked bool
	if selfOwns {
		grant := term
		if grant < d.clusterOwnTerm {
			grant = d.clusterOwnTerm // 归属纪元不回退（更大 term 的消息可前进）
		}
		if grant != d.clusterOwnTerm || !d.clusterOwned {
			granted = true
		}
		d.clusterOwnTerm = grant
		d.clusterOwned = true
		if grant > d.epoch {
			d.persistEpochLocked(grant)
		}
	} else if authoritative && d.clusterOwned {
		d.clusterOwned = false
		revoked = true
	}
	d.persistClusterTermLocked()
	ownTerm, known := d.clusterOwnTerm, d.clusterTerm
	d.epochMu.Unlock()

	if prevPend {
		log.Printf("db: cluster term %d: ownership confirmed by route view (self_owns=%v)", known, selfOwns)
	}
	if granted {
		log.Printf("db: cluster term %d: region ownership granted to local node (own_term=%d)", known, ownTerm)
	}
	if revoked {
		log.Printf("db: cluster term %d: local ownership revoked (own_term=%d) -> write path fenced", known, ownTerm)
	}
}

// noteRestartOwnershipView 重启装载后判定归属是否处于"待权威视图确认"（在
// StartCluster 装配完路由视图后调用）。
//
// NEW-1 陈旧主的直接成因：节点重启后沿用磁盘上的旧 m:regions 快照（owner=本机、
// local=true），在没有任何权威视图确认的情况下继续接受写入，覆盖多数派新
// owner 已确认的数据。因此只要满足下列任一条件即置 clusterPend：
//   - 磁盘记录"重启前仍持有归属"（见 loadClusterTermMeta）；
//   - 本地路由视图显示本机仍持有 region，且集群里存在其它节点（另有权威源）。
//
// 单节点集群（peerCount==0）本地视图即最终视图，不置 pending（零回归）。
func (d *DB) noteRestartOwnershipView(selfOwns bool, peerCount int) {
	d.epochMu.Lock()
	should := (d.clusterOwned && d.clusterOwnTerm > 0) || (selfOwns && peerCount > 0)
	if should && !d.clusterPend {
		d.clusterPend = true
		d.persistClusterTermLocked()
		log.Printf("db: cluster order: restart ownership pending authoritative view (self_owns=%v peers=%d own_term=%d)",
			selfOwns, peerCount, d.clusterOwnTerm)
	}
	d.epochMu.Unlock()
}

// ConfirmOwnershipLocal 单节点/无权威源场景下确认本机归属（解除"待确认"）。
//
// 重启节点在无从拉取权威视图时（单节点集群），本地视图即最终视图，不能永久
// 停留在"待确认"导致只读。由 db.refreshRouteViewFromPeers 的单节点分支调用。
func (d *DB) ConfirmOwnershipLocal() {
	d.epochMu.Lock()
	was := d.clusterPend
	d.clusterPend = false
	own := d.clusterOwnTerm
	if was {
		d.persistClusterTermLocked()
	}
	d.epochMu.Unlock()
	if was {
		log.Printf("db: cluster order: no peer to confirm ownership, accept local view (own_term=%d)", own)
	}
}

// ClusterTerm 返回本节点已知最新集群轨归属纪元（0 = 无纪元信息）。
func (d *DB) ClusterTerm() uint64 {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	return d.clusterTerm
}

// ClusterOwnTerm 返回本节点获授权持有归属的纪元（0 = 从未授权）。
func (d *DB) ClusterOwnTerm() uint64 {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	return d.clusterOwnTerm
}

// ClusterOwned 返回本节点当前是否仍持有集群轨 region 归属。
func (d *DB) ClusterOwned() bool {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	return d.clusterOwned
}

// OwnershipPending 返回重启后归属是否仍待权威视图确认。
func (d *DB) OwnershipPending() bool {
	d.epochMu.Lock()
	defer d.epochMu.Unlock()
	return d.clusterPend
}
