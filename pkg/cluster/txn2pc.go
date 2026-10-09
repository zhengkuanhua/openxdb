package cluster

// M5 分布式写路径 2PC 协议（协调者-参与者两阶段提交）。
//
// 帧（在 M4 帧 20/21 REGION_PUSH 之后扩展）：
//
//	22 PREPARE_REQ  -> 23 PREPARE_RESP
//	24 COMMIT_REQ   -> 25 COMMIT_RESP
//	26 ABORT_REQ    -> 27 ABORT_RESP
//
// 参与者状态机（持久化于引擎元键，崩溃可恢复）：
//
//	m:2pc:p:<txid>  prepared：prepare 落盘（值 = ops JSON；崩溃恢复时一律回滚）
//	m:2pc:c:<txid>  committed：commit 幂等标记（重试不再重复应用）
//	m:2pc:a:<txid>  aborted：abort 幂等标记（重复 abort 幂等；已提交则拒绝）
//
// 生命周期：PREPARE(prepared) -> COMMIT(committed) | ABORT(aborted)。
// 参与者崩溃恢复：启动时扫描 m:2pc:* 全部清理——prepared 未决事务回滚
// （数据从未写入，删除标记即丢弃，无残留）；committed/aborted 为运行期
// 幂等标记，重启后不再需要。协调者本地提交先行，远端 COMMIT 幂等可重试；
// 协调者崩溃后未决 prepare 由参与者启动恢复回滚（见 docs/T14_m5_2pc.md）。

import (
	"errors"
	"net"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

const (
	msg2pcPrepareReq  byte = 22
	msg2pcPrepareResp byte = 23
	msg2pcCommitReq   byte = 24
	msg2pcCommitResp  byte = 25
	msg2pcAbortReq    byte = 26
	msg2pcAbortResp   byte = 27
)

// TxnOp 单个物理键写操作（跨节点 2PC 传输单元；JSON 中 []byte 为 base64）。
type TxnOp struct {
	Key    []byte `json:"key"`
	Value  []byte `json:"value,omitempty"`
	Delete bool   `json:"delete,omitempty"`
}

// PrepareReq PREPARE_REQ：协调者 -> 参与者（待提交物理键操作批次）。
type PrepareReq struct {
	TxID string  `json:"txid"`
	Ops  []TxnOp `json:"ops"`
}

// PrepareResp PREPARE_RESP。
type PrepareResp struct {
	OK  bool   `json:"ok"`
	Err string `json:"err,omitempty"`
}

// CommitReq COMMIT_REQ。
type CommitReq struct {
	TxID string `json:"txid"`
}

// CommitResp COMMIT_RESP。
type CommitResp struct {
	OK  bool   `json:"ok"`
	Err string `json:"err,omitempty"`
}

// AbortReq ABORT_REQ。
type AbortReq struct {
	TxID string `json:"txid"`
}

// AbortResp ABORT_RESP。
type AbortResp struct {
	OK  bool   `json:"ok"`
	Err string `json:"err,omitempty"`
}

// 参与者 2PC 元键前缀（引擎文本键，与 m:nodes / m:regions 同层，无 region 前缀）。
const (
	key2pcPrefix    = "m:2pc:"
	key2pcPrepared  = "m:2pc:p:"
	key2pcCommitted = "m:2pc:c:"
	key2pcAborted   = "m:2pc:a:"
)

func prepared2pcKey(txID string) []byte  { return []byte(key2pcPrepared + txID) }
func committed2pcKey(txID string) []byte { return []byte(key2pcCommitted + txID) }
func aborted2pcKey(txID string) []byte   { return []byte(key2pcAborted + txID) }

// ---- 协调者侧：Manager / Client ----

// PrepareTxn 向参与者发送 PREPARE（待提交 ops 落盘，幂等重放安全）。
func (m *Manager) PrepareTxn(nodeID, txID string, ops []TxnOp) error {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return err
	}
	return m.clientFor(n.ID).prepareTxn(txID, ops)
}

// CommitTxn 向参与者发送 COMMIT（幂等：已提交返回 OK，可安全重试）。
func (m *Manager) CommitTxn(nodeID, txID string) error {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return err
	}
	return m.clientFor(n.ID).commitTxn(txID)
}

// AbortTxn 向参与者发送 ABORT（幂等：已中止返回 OK）。
func (m *Manager) AbortTxn(nodeID, txID string) error {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return err
	}
	return m.clientFor(n.ID).abortTxn(txID)
}

// 2pc 请求读写超时（参与者无响应视为失败，协调者转入 abort 决策）。
const txn2pcTimeout = 5 * time.Second

func (c *Client) prepareTxn(txID string, ops []TxnOp) error {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return err
	}
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(txn2pcTimeout))
	payload, err := marshalJSON(PrepareReq{TxID: txID, Ops: ops})
	if err != nil {
		return err
	}
	if err := writeFrame(conn, msg2pcPrepareReq, payload); err != nil {
		return err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return err
	}
	if typ != msg2pcPrepareResp {
		return errors.New("cluster: unexpected prepare reply")
	}
	var resp PrepareResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return err
	}
	if !resp.OK || resp.Err != "" {
		return errors.New("cluster: prepare rejected: " + resp.Err)
	}
	return nil
}

func (c *Client) commitTxn(txID string) error {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return err
	}
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(txn2pcTimeout))
	payload, err := marshalJSON(CommitReq{TxID: txID})
	if err != nil {
		return err
	}
	if err := writeFrame(conn, msg2pcCommitReq, payload); err != nil {
		return err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return err
	}
	if typ != msg2pcCommitResp {
		return errors.New("cluster: unexpected commit reply")
	}
	var resp CommitResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return err
	}
	if !resp.OK || resp.Err != "" {
		return errors.New("cluster: commit rejected: " + resp.Err)
	}
	return nil
}

func (c *Client) abortTxn(txID string) error {
	addr, err := c.mgr.NodeAddr(c.nodeID)
	if err != nil {
		return err
	}
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(txn2pcTimeout))
	payload, err := marshalJSON(AbortReq{TxID: txID})
	if err != nil {
		return err
	}
	if err := writeFrame(conn, msg2pcAbortReq, payload); err != nil {
		return err
	}
	typ, raw, err := readFrame(conn)
	if err != nil {
		return err
	}
	if typ != msg2pcAbortResp {
		return errors.New("cluster: unexpected abort reply")
	}
	var resp AbortResp
	if err := unmarshalJSON(raw, &resp); err != nil {
		return err
	}
	if !resp.OK || resp.Err != "" {
		return errors.New("cluster: abort rejected: " + resp.Err)
	}
	return nil
}

// ---- 参与者侧：Server ----

// handle2pcPrepare 处理 PREPARE_REQ。
func (s *Server) handle2pcPrepare(conn net.Conn, payload []byte) {
	var req PrepareReq
	if err := unmarshalJSON(payload, &req); err != nil {
		writeFrame(conn, msg2pcPrepareResp, mustJSON(PrepareResp{OK: false, Err: "bad prepare request"}))
		return
	}
	if err := s.prepare2pc(req.TxID, req.Ops); err != nil {
		writeFrame(conn, msg2pcPrepareResp, mustJSON(PrepareResp{OK: false, Err: err.Error()}))
		return
	}
	writeFrame(conn, msg2pcPrepareResp, mustJSON(PrepareResp{OK: true}))
}

// handle2pcCommit 处理 COMMIT_REQ。
func (s *Server) handle2pcCommit(conn net.Conn, payload []byte) {
	var req CommitReq
	if err := unmarshalJSON(payload, &req); err != nil {
		writeFrame(conn, msg2pcCommitResp, mustJSON(CommitResp{OK: false, Err: "bad commit request"}))
		return
	}
	if err := s.commit2pc(req.TxID); err != nil {
		writeFrame(conn, msg2pcCommitResp, mustJSON(CommitResp{OK: false, Err: err.Error()}))
		return
	}
	writeFrame(conn, msg2pcCommitResp, mustJSON(CommitResp{OK: true}))
}

// handle2pcAbort 处理 ABORT_REQ。
func (s *Server) handle2pcAbort(conn net.Conn, payload []byte) {
	var req AbortReq
	if err := unmarshalJSON(payload, &req); err != nil {
		writeFrame(conn, msg2pcAbortResp, mustJSON(AbortResp{OK: false, Err: "bad abort request"}))
		return
	}
	if err := s.abort2pc(req.TxID); err != nil {
		writeFrame(conn, msg2pcAbortResp, mustJSON(AbortResp{OK: false, Err: err.Error()}))
		return
	}
	writeFrame(conn, msg2pcAbortResp, mustJSON(AbortResp{OK: true}))
}

// prepare2pc 参与者 PREPARE：校验幂等（已提交/已中止/已 prepared）后，
// 将 ops 落盘为 m:2pc:p:<txid>（崩溃可恢复）并缓存内存。
func (s *Server) prepare2pc(txID string, ops []TxnOp) error {
	if txID == "" {
		return errors.New("cluster: empty txid")
	}
	s.mu2pc.Lock()
	defer s.mu2pc.Unlock()
	if s.prepareFail != nil {
		if err := s.prepareFail(txID); err != nil {
			return err
		}
	}
	// 已提交 -> 幂等 OK（协调者重试/迟到 prepare 重放）
	if _, err := s.st.Get(committed2pcKey(txID)); err == nil {
		return nil
	} else if err != storage.ErrNotFound {
		return err
	}
	// 已中止 -> 拒绝（协调者不应在 abort 决策后重试 prepare）
	if _, err := s.st.Get(aborted2pcKey(txID)); err == nil {
		return errors.New("cluster: txn already aborted")
	} else if err != storage.ErrNotFound {
		return err
	}
	// 已 prepared（内存或落盘）-> 幂等 OK
	if _, ok := s.pending2pc[txID]; ok {
		return nil
	}
	if _, err := s.st.Get(prepared2pcKey(txID)); err == nil {
		s.pending2pc[txID] = ops
		return nil
	} else if err != storage.ErrNotFound {
		return err
	}
	raw, err := marshalJSON(PrepareReq{TxID: txID, Ops: ops})
	if err != nil {
		return err
	}
	batch := &storage.WriteBatch{}
	batch.Puts = append(batch.Puts, storage.KVPair{Key: prepared2pcKey(txID), Value: raw})
	if err := s.st.Write(batch); err != nil {
		return err
	}
	s.pending2pc[txID] = ops
	return nil
}

// commit2pc 参与者 COMMIT：把 prepare 暂存 ops 原子应用进存储
// （数据 + 删除 prepared 标记 + 写 committed 标记同一 WriteBatch），
// 幂等：已提交直接 OK；未 prepare 但落盘记录存在时按记录应用（协调者
// 重试 COMMIT 时内存已丢，读盘恢复 ops 后重放，同键同值幂等）。
func (s *Server) commit2pc(txID string) error {
	if txID == "" {
		return errors.New("cluster: empty txid")
	}
	s.mu2pc.Lock()
	defer s.mu2pc.Unlock()
	if _, err := s.st.Get(committed2pcKey(txID)); err == nil {
		return nil // 幂等重放
	} else if err != storage.ErrNotFound {
		return err
	}
	if _, err := s.st.Get(aborted2pcKey(txID)); err == nil {
		return errors.New("cluster: txn already aborted")
	} else if err != storage.ErrNotFound {
		return err
	}
	ops, ok := s.pending2pc[txID]
	if !ok {
		raw, err := s.st.Get(prepared2pcKey(txID))
		if err != nil {
			if err == storage.ErrNotFound {
				return errors.New("cluster: commit for unknown txn")
			}
			return err
		}
		var rec PrepareReq
		if err := unmarshalJSON(raw, &rec); err != nil {
			return err
		}
		ops = rec.Ops
	}
	batch := &storage.WriteBatch{}
	for _, op := range ops {
		if op.Delete {
			batch.Deletes = append(batch.Deletes, op.Key)
		} else {
			batch.Puts = append(batch.Puts, storage.KVPair{Key: op.Key, Value: op.Value})
		}
	}
	batch.Deletes = append(batch.Deletes, prepared2pcKey(txID))
	batch.Puts = append(batch.Puts, storage.KVPair{Key: committed2pcKey(txID), Value: []byte("1")})
	if err := s.st.Write(batch); err != nil {
		return err
	}
	delete(s.pending2pc, txID)
	return nil
}

// abort2pc 参与者 ABORT：丢弃 prepare 暂存（数据从未写入），
// 删除 prepared 标记并写 aborted 标记；幂等。
func (s *Server) abort2pc(txID string) error {
	if txID == "" {
		return errors.New("cluster: empty txid")
	}
	s.mu2pc.Lock()
	defer s.mu2pc.Unlock()
	if _, err := s.st.Get(aborted2pcKey(txID)); err == nil {
		return nil // 幂等重放
	} else if err != storage.ErrNotFound {
		return err
	}
	if _, err := s.st.Get(committed2pcKey(txID)); err == nil {
		return errors.New("cluster: txn already committed")
	} else if err != storage.ErrNotFound {
		return err
	}
	batch := &storage.WriteBatch{}
	batch.Deletes = append(batch.Deletes, prepared2pcKey(txID))
	batch.Puts = append(batch.Puts, storage.KVPair{Key: aborted2pcKey(txID), Value: []byte("1")})
	if err := s.st.Write(batch); err != nil {
		return err
	}
	delete(s.pending2pc, txID)
	return nil
}

// Recover2PC 启动恢复：清理全部 2PC 元键。
// 语义：prepared 未决事务一律回滚（数据从未写入，删除标记即丢弃，无残留）；
// committed/aborted 历史标记仅用于运行期幂等重试，重启后不再需要。
// 由 db.StartCluster 在节点链路启动前调用。
func (s *Server) Recover2PC() error {
	s.mu2pc.Lock()
	defer s.mu2pc.Unlock()
	// 上界：任何 m:2pc:* 键都小于 "m:2pcq"（第 6 字节 ':'(0x3a) < 'q'(0x71)）
	rows, err := s.st.Scan(storage.KeyRange{Start: []byte(key2pcPrefix), End: []byte("m:2pcq")}, -1)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	batch := &storage.WriteBatch{}
	for _, kv := range rows {
		batch.Deletes = append(batch.Deletes, kv.Key)
	}
	s.pending2pc = map[string][]TxnOp{}
	return s.st.Write(batch)
}
