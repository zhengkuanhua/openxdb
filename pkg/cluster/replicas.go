/*
---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_78c959d3e30d11f1913b525400393706
    ReservedCode1: nucB5a131X3JrmedPbHJSm4SFUQg6VPltCR6o7d+PJBnH9b7zqcPacu2QqkV3F1iVcAvduytT6x04jC5v3MYKUn8G28tZV75pMAK69t91q8DGc7l7hBBweQl+ynDEcMYk/1H8p8kQ8drpqYryCgUvEoBKOMAMGI2XKHM0jpeyKfc5COoXZcpIjSCNYY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_78c959d3e30d11f1913b525400393706
    ReservedCode2: nucB5a131X3JrmedPbHJSm4SFUQg6VPltCR6o7d+PJBnH9b7zqcPacu2QqkV3F1iVcAvduytT6x04jC5v3MYKUn8G28tZV75pMAK69t91q8DGc7l7hBBweQl+ynDEcMYk/1H8p8kQ8drpqYryCgUvEoBKOMAMGI2XKHM0jpeyKfc5COoXZcpIjSCNYY=
---
*/

package cluster

// v1.20.0 C3：2PC 参与面扩展（缺口 B-2 / B-3 / B-4）。
//
// 本文件提供两件事：
//  1. region 副本集合的独立元数据键 m:repl:replicas:<rid>（DEV_PLAN D-2 决策的
//     MVP 形态：不扩展 sharding.RegionInfo.Replicas，避免改动既有路由元数据
//     编解码与全部既有读路径）。记录缺失 = 未声明副本，参与者集合退化为仅
//     owner，与 v1.19.0 行为逐字一致；
//  2. 参与者侧 PREPARE 的 region 归属校验（B-3）：本节点既非 owner 也非副本时
//     拒绝 prepare，且不落盘 m:2pc:p:<txid> 暂存。
//
// 开关（ha_2pc_replica_quorum，默认 off）位于 SQL 层，由 db.StartCluster 通过
// SetParticipantOwnershipCheck 把"开关是否开启"注入参与者 Server：开关关闭时
// checkParticipantOwnership 直接放行，参与者 PREPARE 与 v1.19.0 完全一致。

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ReplicasMetaPrefix region 副本集合元数据键前缀（m:repl:replicas:<rid>）。
const ReplicasMetaPrefix = "m:repl:replicas:"

// ErrCode2PCNotOwner 参与者不具备 region 参与资格的错误码（v1.20.0 C3 B-3）。
// 参与者 PREPARE 回执文本以该码开头，协调者据此识别"发错了归属节点"，
// 与网络故障/超时类失败区分开。
const ErrCode2PCNotOwner = "2PC_NOT_OWNER"

// ReplicasKey 返回 region 副本集合元数据键 m:repl:replicas:<rid>。
func ReplicasKey(rid storage.ID) []byte {
	return []byte(fmt.Sprintf("%s%d", ReplicasMetaPrefix, uint64(rid)))
}

// PutRegionReplicas 覆盖式写入 region 副本集合（不含 owner，仅副本节点）。
// 空集合等价于删除记录（参与者集合退化为仅 owner）。
func PutRegionReplicas(st storage.Storage, rid storage.ID, nodes []string) error {
	if st == nil {
		return fmt.Errorf("cluster: nil storage for replicas of region %d", rid)
	}
	uniq := make([]string, 0, len(nodes))
	seen := map[string]bool{}
	for _, n := range nodes {
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		uniq = append(uniq, n)
	}
	sort.Strings(uniq)
	if len(uniq) == 0 {
		return DeleteRegionReplicas(st, rid)
	}
	raw, err := json.Marshal(uniq)
	if err != nil {
		return err
	}
	batch := &storage.WriteBatch{}
	batch.Puts = append(batch.Puts, storage.KVPair{Key: ReplicasKey(rid), Value: raw})
	return st.Write(batch)
}

// DeleteRegionReplicas 删除 region 副本集合记录（幂等）。
func DeleteRegionReplicas(st storage.Storage, rid storage.ID) error {
	if st == nil {
		return fmt.Errorf("cluster: nil storage for replicas of region %d", rid)
	}
	batch := &storage.WriteBatch{}
	batch.Deletes = append(batch.Deletes, ReplicasKey(rid))
	return st.Write(batch)
}

// LoadRegionReplicas 读取 region 副本集合。
// 记录缺失或存储未装配时返回 (nil, nil)——调用方据此退化为"仅 owner"。
func LoadRegionReplicas(st storage.Storage, rid storage.ID) ([]string, error) {
	if st == nil {
		return nil, nil
	}
	raw, err := st.Get(ReplicasKey(rid))
	if err == storage.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var nodes []string
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil, fmt.Errorf("cluster: decode replicas of region %d: %w", rid, err)
	}
	return nodes, nil
}

// ContainsNode 判断节点集合是否包含 node。
func ContainsNode(nodes []string, node string) bool {
	for _, n := range nodes {
		if n == node {
			return true
		}
	}
	return false
}

// SetParticipantOwnershipCheck 装配"参与者归属校验是否生效"的开关提供者
// （v1.20.0 C3，由 db.StartCluster 注入 sql.Executor.TwoPCReplicaQuorum）。
// 传 nil 或提供者返回 false 时，checkParticipantOwnership 直接放行——参与者
// PREPARE 与 v1.19.0 逐字一致（默认 off 零回归）。
func (s *Server) SetParticipantOwnershipCheck(fn func() bool) {
	s.muOwner.Lock()
	defer s.muOwner.Unlock()
	s.participantOwnerCheck = fn
}

// participantOwnershipEnabled 返回归属校验是否生效（未装配提供者视为关闭）。
// 使用独立锁 muOwner：校验在 io 线程（prepare2pc）入口执行，不能与 2PC 状态
// 锁 mu2pc 混用（校验函数本体又会读 SQL 层开关，避免任何锁序倒置）。
func (s *Server) participantOwnershipEnabled() bool {
	s.muOwner.Lock()
	defer s.muOwner.Unlock()
	return s.participantOwnerCheck != nil && s.participantOwnerCheck()
}

// checkParticipantOwnership 校验 ops 中每个键所属 region 的参与资格（B-3）：
//   - 无 region 前缀的键（DDL/元数据类）不参与校验；
//   - 本机为 owner 或本机在副本集合内 -> 通过；
//   - 路由视图内归属其它节点且本机不在副本集合内 -> 返回 2PC_NOT_OWNER
//     （调用方在落盘 m:2pc:p:<txid> 之前失败，保证无暂存残留）。
//
// 未装配路由视图（单机/旧节点）时无法判定归属，一律放行。
func (s *Server) checkParticipantOwnership(ops []TxnOp) error {
	if !s.participantOwnershipEnabled() {
		return nil
	}
	if s.router == nil {
		return nil
	}
	checked := map[storage.ID]bool{}
	for _, op := range ops {
		rid, ok := regionIDOfKey(op.Key)
		if !ok || checked[rid] {
			continue
		}
		checked[rid] = true
		owner := s.router.RegionNode(rid)
		if owner == "" || owner == s.SelfID {
			continue
		}
		reps, err := LoadRegionReplicas(s.st, rid)
		if err != nil {
			return err
		}
		if ContainsNode(reps, s.SelfID) {
			continue
		}
		return fmt.Errorf("%s: region %d owned by node %q, this node %q is neither owner nor replica",
			ErrCode2PCNotOwner, rid, owner, s.SelfID)
	}
	return nil
}

// regionIDOfKey 解析物理键中的 region ID；非 region 前缀键返回 false。
func regionIDOfKey(key []byte) (storage.ID, bool) {
	if len(key) <= sharding.RegionKeyPrefixLen || key[0] != 'r' {
		return 0, false
	}
	rid, _ := sharding.DecodeRegionKey(key)
	return rid, true
}
