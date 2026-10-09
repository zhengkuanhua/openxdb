package sql

// M5 分布式写路径 2PC（协调者侧）。
//
// 形态：SQL 写语句（INSERT / UPDATE / DELETE / CSV IMPORT）先产出
// "物理键操作列表"（行键 / 索引键的 Put / Delete），按 region 归属分组：
// 本地 region 写入协调者本地事务缓冲；远端 region 经 cluster.Manager 与
// 归属节点执行两阶段提交——
//
//	PREPARE（参与者落盘暂存）-> 全部就绪 -> 协调者本地 COMMIT
//	-> 远端 COMMIT（幂等重试）；任一 PREPARE 失败 -> 本地回滚
//	+ 已就绪参与者 ABORT。
//
// 单机模式（mgr == nil）零触发：仅 applyOps + 本地提交，路径与 M1-M4
// 完全一致。显式事务（BEGIN..COMMIT）内的跨节点写返回明确错误
// （M5 支持 autocommit 2PC，见 docs/T14_m5_2pc.md）。

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// kvOp 物理键写操作（SQL 写路径的统一中间表示）。
type kvOp struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// applyOps 将操作列表应用进协调者事务缓冲（本地路径）。
func applyOps(tx txn.Txn, ops []kvOp) error {
	for _, op := range ops {
		if op.Delete {
			if err := tx.Delete(op.Key); err != nil {
				return err
			}
		} else {
			if err := tx.Put(op.Key, op.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

// hasRemoteOps 是否存在远端 region 的写操作（显式事务内不支持的判定）。
func (e *Executor) hasRemoteOps(ops []kvOp) bool {
	if e.mgr == nil {
		return false
	}
	for _, op := range ops {
		rid, _ := sharding.DecodeRegionKey(op.Key)
		if !e.regionLocal(rid) {
			return true
		}
	}
	return false
}

// groupOps 按 region 归属分组：本地组 + 远端组（按归属节点 ID）。
func (e *Executor) groupOps(ops []kvOp) ([]kvOp, map[string][]kvOp, error) {
	var local []kvOp
	remote := map[string][]kvOp{}
	for _, op := range ops {
		rid, _ := sharding.DecodeRegionKey(op.Key)
		if e.regionLocal(rid) {
			local = append(local, op)
		} else {
			node := e.regionNode(rid)
			remote[node] = append(remote[node], op)
		}
	}
	return local, remote, nil
}

var _2pcSeq uint64

// new2pcID 生成协调者侧事务 ID（时间戳 + 进程内递增序列；参与者侧按
// txID 幂等标记保证跨节点唯一语义）。
func (e *Executor) new2pcID() string {
	seq := atomic.AddUint64(&_2pcSeq, 1)
	return fmt.Sprintf("2pc-%d-%d", time.Now().UnixNano(), seq)
}

// toClusterTxnOps 转换 SQL 内部操作为 cluster 协议单元。
func toClusterTxnOps(ops []kvOp) []cluster.TxnOp {
	out := make([]cluster.TxnOp, 0, len(ops))
	for _, op := range ops {
		out = append(out, cluster.TxnOp{Key: op.Key, Value: op.Value, Delete: op.Delete})
	}
	return out
}

// exec2pcWrite 执行一批写入并提交协调者本地事务。
// 调用前提：autocommit 语句或自建事务（CSV IMPORT）；不得在显式会话
// 事务内调用（调用方先经 hasRemoteOps 拦截）。
//
// 语义：
//   - mgr == nil（单机）：applyOps + tx.Commit()（M1-M4 原路径）；
//   - 全本地：applyOps + tx.Commit()；
//   - 有远端：本地组 applyOps -> 远端全部 PREPARE -> tx.Commit()
//     -> 远端全部 COMMIT；PREPARE 任一失败则 abort 已就绪参与者并返回
//     TXN_PREPARE_FAILED；协调者本地提交失败返回 TXN_COMMIT_ABORTED
//     （已 abort 参与者）；远端 COMMIT 失败返回 TXN_COMMIT_UNCERTAIN
//     （本地已提交，参与者可重试幂等 COMMIT）。
func (e *Executor) exec2pcWrite(tx txn.Txn, ops []kvOp) error {
	if len(ops) == 0 {
		return nil
	}
	if e.mgr == nil {
		if err := applyOps(tx, ops); err != nil {
			return err
		}
		return tx.Commit()
	}
	local, remote, err := e.groupOps(ops)
	if err != nil {
		return err
	}
	if err := applyOps(tx, local); err != nil {
		return err
	}
	if len(remote) == 0 {
		return tx.Commit()
	}
	// 两阶段提交
	txID := e.new2pcID()
	prepared := make([]string, 0, len(remote))
	for node, nops := range remote {
		if err := e.mgr.PrepareTxn(node, txID, toClusterTxnOps(nops)); err != nil {
			for _, n := range prepared {
				_ = e.mgr.AbortTxn(n, txID)
			}
			return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: 2pc %s participant %s: %v", txID, node, err)}
		}
		prepared = append(prepared, node)
	}
	// 全部就绪：提交协调者本地事务
	if err := tx.Commit(); err != nil {
		for _, n := range prepared {
			_ = e.mgr.AbortTxn(n, txID)
		}
		return &SQLError{Msg: fmt.Sprintf("TXN_COMMIT_ABORTED: local commit failed for 2pc %s: %v", txID, err)}
	}
	// 远端提交（幂等；网络失败返回不确定状态，重试 CommitTxn 安全）
	for _, n := range prepared {
		if err := e.mgr.CommitTxn(n, txID); err != nil {
			return &SQLError{Msg: fmt.Sprintf("TXN_COMMIT_UNCERTAIN: 2pc %s participant %s: %v (CommitTxn idempotent, retry safe)", txID, n, err)}
		}
	}
	return nil
}
