package db

// D4.5 §6-2（B2）TSO HA 装配层：把 TSO 从"指向单一固定节点"升级为
// "组内主备 + epoch 选主"。
//
// 修复前（B2 根因）：SetTSONode 让全集群客户端的 TSO 客户端固定指向 nodeID，
// 该节点宕机后 fetchBatch 直接拨号失败，事务取戳返回 tso unavailable，
// 跨节点写全失败（读不受影响）。
//
// 修复后（本文件装配的三段式，全部落在"发号权"上）：
//  1. 服务端：每个副本节点持本地状态机 tsoHAState（epoch/last/window），
//     只有"已授权主"响应 TSO_REQ，follower 回执 not tso leader；
//  2. 客户端：HATSOClient 按组优先序选主，主不可达自动切换，组内全不可达
//     才报 tso unavailable；
//  3. 护栏：服务端 last 只增不减（选主时提升到观测值之上）、窗口先落盘再发号；
//     客户端拒绝任何 start<=maxSeen 的回退批；epoch 单调不减。
//
// 关停/切换：EnableTSOHA 未调用时 TSO 仍为单点或本地发号，零回归。

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/tso"
)

// TSOHAMetaFile TSO HA 持久化文件：记录 (epoch, reserved) 两行。
// epoch 防选主纪元回退；reserved 为已落盘的发号窗口上界，重启后从
// reserved 续发（跳过窗口内未用完的号），保证重启不回退、不重复。
const TSOHAMetaFile = "tso_ha.meta"

// EnableTSOHA 启用本节点 TSO 主备（B2）：
//   - group：TSO 副本节点 ID 列表（客户端按此顺序选主，通常按配置稳定性排序）；
//   - batch：客户端单次拉批大小（<=0 用默认 64）。
//
// 前置条件：集群已 StartCluster（需要服务端帧处理与节点链路地址）。
// 全部节点都应以相同 group 调用本方法；未调用的节点继续走单点/本地发号。
func (d *DB) EnableTSOHA(group []string, batch uint64) error {
	if len(group) == 0 {
		return errors.New("db: tso ha group is empty")
	}
	if d.Cluster == nil {
		return errors.New("db: cluster manager not initialized")
	}
	if d.ClusterSrv == nil {
		return errors.New("db: cluster not started (call StartCluster first)")
	}
	metaPath := filepath.Join(d.Dir, TSOHAMetaFile)
	load := func() (uint64, uint64) { return loadTSOHAMeta(metaPath) }
	save := func(epoch, reserved uint64) { saveTSOHAMeta(metaPath, epoch, reserved) }
	// 服务端保持本地发号器：HA 分支只让"已授权主"调用它（不发号时不用），
	// 同进程客户端命中自身时走本地状态机，不产生自指网络请求。
	d.ClusterSrv.SetTSO(tso.New())
	d.ClusterSrv.EnableTSOHA(group, load, save)
	c := cluster.NewHATSOClient(d.Cluster, group, batch)
	d.ClusterSrv.BindTSOHAClient(c)
	d.TSO = c
	d.SQL.SetTSO(c)
	return nil
}

// TSOHALeader 返回本节点当前认定的 TSO 主节点 ID（空 = 尚未选主）。
// 未启用 HA 时返回空串。
func (d *DB) TSOHALeader() string {
	if c, ok := d.TSO.(*cluster.HATSOClient); ok {
		return c.Leader()
	}
	return ""
}

// TSOHAClient 返回 TSO HA 客户端（未启用返回 nil），供测试/运维观测。
func (d *DB) TSOHAClient() *cluster.HATSOClient {
	if c, ok := d.TSO.(*cluster.HATSOClient); ok {
		return c
	}
	return nil
}

// loadTSOHAMeta 读取 (epoch, reserved)。文件缺失/损坏按 (0,0) 处理（首启）。
func loadTSOHAMeta(path string) (uint64, uint64) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, 0
	}
	lines := strings.Fields(strings.TrimSpace(string(b)))
	if len(lines) < 2 {
		return 0, 0
	}
	epoch, err1 := strconv.ParseUint(lines[0], 10, 64)
	reserved, err2 := strconv.ParseUint(lines[1], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0
	}
	return epoch, reserved
}

// saveTSOHAMeta 落盘 (epoch, reserved)：先写临时文件再 rename，避免半截文件。
// 由状态机在窗口推进/epoch 变更时调用（低频），失败仅影响重启防回退。
func saveTSOHAMeta(path string, epoch, reserved uint64) {
	if path == "" {
		return
	}
	body := strconv.FormatUint(epoch, 10) + "\n" + strconv.FormatUint(reserved, 10) + "\n"
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o644); err != nil {
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
	}
}
