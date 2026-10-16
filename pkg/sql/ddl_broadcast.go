package sql

// T34 跨节点 DDL 元数据广播：协调者侧广播 + 节点侧 apply + pending 重放。
//
// 设计（详见 docs/T34_m20_cross_node_ddl.md）：
//   - 协调者在 DDL 本地提交成功后调用 broadcastDDL：把表目录变更（按表粒度
//     upsert/delete）推送到全部在线注册节点；单机（mgr==nil / 无远端节点）零触发；
//   - 推送失败/节点离线：请求记入 pending（内存 + 落盘 m:ddl:pending），DDL
//     仍返回成功（本地已提交为权威，目录收敛为最终一致）；下次广播前/显式
//     ReplayPendingDDL 时按序重放；
//   - 节点侧 ApplyRemoteDDL 按 TableID upsert/delete 幂等收敛，与 binlog
//     复制重放通道（commitHook 全量 batch 含 metaKey）并发安全。

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// pendingDDLKey 落盘的待重放 DDL 广播列表键。
var pendingDDLKey = []byte("m:ddl:pending")

// ddlIDSeq 广播 ID 序号（进程内递增，配合时间戳生成唯一 ID）。
var ddlIDSeq int64

// newDDLID 生成广播 ID：ddl-<unixnano>-<seq>。
func newDDLID() string {
	seq := atomic.AddInt64(&ddlIDSeq, 1)
	return fmt.Sprintf("ddl-%d-%d", time.Now().UnixNano(), seq)
}

// pendingDDL 待重放条目：目标节点 + 广播请求。
type pendingDDL struct {
	NodeID string             `json:"node_id"`
	Req    cluster.DDLPushReq `json:"req"`
}

// ApplyRemoteDDL 节点侧应用远端 DDL 目录变更（T34）。
// 幂等：TableID 由 nextTableID 单调分配不复用，按 ID upsert/delete 重复应用安全；
// 目标节点无该表时直接追加，保证后续 ASSIGN REGION 时目录已就绪。
func (e *Executor) ApplyRemoteDDL(req cluster.DDLPushReq) error {
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return err
	}
	idx := -1
	for i, t := range tabs {
		if t.ID == req.TableID {
			idx = i
			break
		}
	}
	switch req.Op {
	case cluster.DDLOpDrop:
		if idx >= 0 {
			tabs = append(tabs[:idx], tabs[idx+1:]...)
		}
	case cluster.DDLOpCreate, cluster.DDLOpAlter:
		var meta TableMeta
		if err := json.Unmarshal(req.Meta, &meta); err != nil {
			return fmt.Errorf("ddl broadcast: bad meta for %s: %w", req.Op, err)
		}
		if idx >= 0 {
			tabs[idx] = &meta
		} else {
			tabs = append(tabs, &meta)
		}
	default:
		return fmt.Errorf("ddl broadcast: unknown op %q", req.Op)
	}
	raw, err := saveTables(tabs)
	if err != nil {
		return err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return err
	}
	return tx.Commit()
}

// broadcastDDL 协调者侧：DDL 本地提交成功后把目录变更广播到在线节点。
// 调用方须在 DDL autocommit 提交成功后调用（显式事务内 DDL 不广播，见文档限制）。
func (e *Executor) broadcastDDL(op string, meta *TableMeta, dropID uint64, dropName string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mgr == nil || !e.mgr.HasRemoteNodes() {
		return
	}
	// 先收敛历史失败项，再广播新变更（保持节点按序收敛）
	e.replayPendingDDLLocked()
	var req cluster.DDLPushReq
	switch op {
	case cluster.DDLOpCreate, cluster.DDLOpAlter:
		raw, err := json.Marshal(meta)
		if err != nil {
			return
		}
		req = cluster.DDLPushReq{DDLID: newDDLID(), Op: op, TableID: meta.ID, Name: meta.Name, Meta: raw}
	case cluster.DDLOpDrop:
		req = cluster.DDLPushReq{DDLID: newDDLID(), Op: op, TableID: dropID, Name: dropName}
	default:
		return
	}
	for _, n := range e.mgr.ListNodes() {
		if n.ID == e.selfID {
			continue
		}
		if e.mgr.IsDown(n.ID) {
			e.recordPendingDDLLocked(pendingDDL{NodeID: n.ID, Req: req})
			continue
		}
		if err := e.mgr.PushDDL(n.ID, req); err != nil {
			e.recordPendingDDLLocked(pendingDDL{NodeID: n.ID, Req: req})
		}
	}
}

// ReplayPendingDDL 重放所有待同步的 DDL 广播（启动/运维/测试调用）。
// 返回重放后仍剩余待同步条数（0 = 全部收敛）。
func (e *Executor) ReplayPendingDDL() (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.replayPendingDDLLocked()
}

// replayPendingDDLLocked 按记录顺序重放 pending 列表；目标离线保留待下次。
// 调用方须已持有 e.mu。
func (e *Executor) replayPendingDDLLocked() (int, error) {
	if e.mgr == nil || !e.mgr.HasRemoteNodes() {
		return 0, nil
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return 0, err
	}
	raw, err := tx.Get(pendingDDLKey)
	if err != nil {
		tx.Rollback()
		if err == storage.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	tx.Rollback()
	if len(raw) == 0 {
		return 0, nil
	}
	var list []pendingDDL
	if err := json.Unmarshal(raw, &list); err != nil {
		return len(list), err
	}
	var kept []pendingDDL
	for _, p := range list {
		if p.NodeID == e.selfID || e.mgr.IsDown(p.NodeID) {
			kept = append(kept, p)
			continue
		}
		if err := e.mgr.PushDDL(p.NodeID, p.Req); err != nil {
			kept = append(kept, p)
		}
	}
	if len(kept) == len(list) {
		return len(kept), nil
	}
	if err := e.savePendingDDLLocked(kept); err != nil {
		return len(kept), err
	}
	return len(kept), nil
}

// recordPendingDDLLocked 把失败广播追加到 pending 列表（内存由调用方持有 e.mu 保护；
// 落盘独立事务）。
func (e *Executor) recordPendingDDLLocked(p pendingDDL) {
	list, err := e.loadPendingDDLLocked()
	if err != nil {
		return
	}
	list = append(list, p)
	_ = e.savePendingDDLLocked(list)
}

// loadPendingDDLLocked 读取当前 pending 列表（调用方须已持有 e.mu）。
func (e *Executor) loadPendingDDLLocked() ([]pendingDDL, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	raw, err := tx.Get(pendingDDLKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var list []pendingDDL
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	return list, nil
}

// savePendingDDLLocked 覆写 pending 列表（独立事务；调用方须已持有 e.mu）。
func (e *Executor) savePendingDDLLocked(list []pendingDDL) error {
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if len(list) == 0 {
		if err := tx.Delete(pendingDDLKey); err != nil {
			return err
		}
	} else {
		raw, err := json.Marshal(list)
		if err != nil {
			return err
		}
		if err := tx.Put(pendingDDLKey, raw); err != nil {
			return err
		}
	}
	return tx.Commit()
}
