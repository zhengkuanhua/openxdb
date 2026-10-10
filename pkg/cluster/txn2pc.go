package cluster

// M5 分布式写路径 2PC 协议（协调者-参与者两阶段提交）。
//
// 帧（在 M4 帧 20/21 REGION_PUSH 之后扩展）：
//
//	22 PREPARE_REQ  -> 23 PREPARE_RESP
//	24 COMMIT_REQ   -> 25 COMMIT_RESP
//	26 ABORT_REQ    -> 27 ABORT_RESP
//
// M8 扩展（分布式事务增强，docs/T17_m8_dist_txn.md）：
//   - PREPARE_REQ 携带 BeginTS：协调者 begin 时从全局 TSO 获取的版本号，
//     参与者落盘保存（崩溃恢复时可获知事务起始版本）；
//   - COMMIT_REQ 携带 CommitTS：协调者 commit 时从全局 TSO 获取的提交版本号，
//     参与者提交数据的同时为每个写入键登记版本记录（m:ver:<key>:<commit_ts>，
//     行键在前、时间戳在后，保证同键版本记录前缀分段连续）；
//   - 版本记录支撑分布式快照隔离：任何读取（本地/远端）按读取者 begin_ts
//     过滤 commit_ts > begin_ts 的写入行（见 VersionVisible）；
//   - 协调者持久化 prepare 记录（m:2pc:coord:<txid>，含参与者列表 / begin_ts /
//     commit_ts / 本地 ops / 状态机），崩溃后 RecoverCoordinated2PC 依据
//     prepare 状态决定 commit（重放本地 + 驱动参与者幂等完成）或 abort。
//
// 参与者状态机（持久化于引擎元键，崩溃可恢复）：
//
//	m:2pc:p:<txid>  prepared：prepare 落盘（值 = PrepareReq JSON；崩溃恢复时一律回滚）
//	m:2pc:c:<txid>  committed：commit 幂等标记（重试不再重复应用）
//	m:2pc:a:<txid>  aborted：abort 幂等标记（重复 abort 幂等；已提交则拒绝）
//	m:2pc:coord:<txid> 协调者 prepare 记录（State 见 CoordRecord）
//
// 生命周期：PREPARE(prepared) -> COMMIT(committed) | ABORT(aborted)。
// 参与者崩溃恢复：启动时扫描 m:2pc:* 全部清理——prepared 未决事务回滚
// （数据从未写入，删除标记即丢弃，无残留）；committed/aborted 为运行期
// 幂等标记，重启后不再需要。协调者崩溃恢复：RecoverCoordinated2PC 依据
// 持久化 coord 记录决策（prepared/committing -> commit；preparing/aborted
// -> abort）并驱动参与者幂等完成。版本记录 m:ver:* 永久保留（提交数据
// 的可见性依据，恢复不清）。

import (
	"encoding/binary"
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
// BeginTS 为该分布式事务 begin 时获取的全局版本号（M8 快照隔离起点）。
type PrepareReq struct {
	TxID    string  `json:"txid"`
	BeginTS uint64  `json:"begin_ts,omitempty"`
	Ops     []TxnOp `json:"ops"`
}

// PrepareResp PREPARE_RESP。
type PrepareResp struct {
	OK  bool   `json:"ok"`
	Err string `json:"err,omitempty"`
}

// CommitReq COMMIT_REQ：CommitTS 为协调者 commit 时获取的全局提交版本号
// （M8：参与者据此登记 m:ver 版本记录，供分布式快照隔离过滤）。
type CommitReq struct {
	TxID     string `json:"txid"`
	CommitTS uint64 `json:"commit_ts,omitempty"`
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
	key2pcCoord     = "m:2pc:coord:"
)

func prepared2pcKey(txID string) []byte  { return []byte(key2pcPrepared + txID) }
func committed2pcKey(txID string) []byte { return []byte(key2pcCommitted + txID) }
func aborted2pcKey(txID string) []byte   { return []byte(key2pcAborted + txID) }

// CoordKey 协调者 prepare 记录键（供 sql 包持久化/删除协调者状态）。
func CoordKey(txID string) []byte { return []byte(key2pcCoord + txID) }

// CoordState 协调者 prepare 记录状态机。
type CoordState string

const (
	// CoordPreparing 准备阶段：参与者 prepare 尚未全部成功（恢复决策 = abort）。
	CoordPreparing CoordState = "preparing"
	// CoordPrepared 全部参与者已 prepare 就绪（恢复决策 = commit）。
	CoordPrepared CoordState = "prepared"
	// CoordCommitting 本地已提交、参与者 COMMIT 进行中（恢复决策 = commit）。
	CoordCommitting CoordState = "committing"
	// CoordAborted 已决策 abort（恢复决策 = abort）。
	CoordAborted CoordState = "aborted"
)

// CoordRecord 协调者侧持久化 prepare 记录（M8 2PC 恢复强化）：
// 包含恢复决策所需的全部信息（参与者列表、begin_ts、commit_ts、本地 ops、状态）。
type CoordRecord struct {
	TxID         string     `json:"txid"`
	BeginTS      uint64     `json:"begin_ts"`
	CommitTS     uint64     `json:"commit_ts,omitempty"`
	Participants []string   `json:"participants,omitempty"`
	LocalOps     []TxnOp    `json:"local_ops,omitempty"`
	State        CoordState `json:"state"`
}

// MarshalCoord 序列化协调者 prepare 记录。
func MarshalCoord(rec CoordRecord) ([]byte, error) { return marshalJSON(rec) }

// UnmarshalCoord 解析协调者 prepare 记录。
func UnmarshalCoord(raw []byte, rec *CoordRecord) error { return unmarshalJSON(raw, rec) }

// ---- M8 版本记录（分布式快照隔离的可见性索引） ----
//
// 键格式：m:ver:<物理行键>:<commit_ts 8B 大端>。
// 物理行键在前、commit_ts 在后，保证同一行键的版本记录按 commit_ts 升序
// 连续存放（前缀 [m:ver:key] 精确分段，任意 key 的区间扫描互不串扰）；
// 提交一个写入键即登记一条版本记录（覆盖写同键同值，幂等）；
// 删除操作不登记（行已不存在，无需过滤）。
// 读过滤规则：key 存在 commit_ts > 读事务 begin_ts 的版本记录 -> 不可见；
// 否则可见（无记录的行视为旧数据/未参与版本管理，恒可见——单机零回归）。

const verPrefix = "m:ver:"

// verKey 构造版本记录键。
func verKey(commitTS uint64, key []byte) []byte {
	b := make([]byte, 0, len(verPrefix)+len(key)+8)
	b = append(b, verPrefix...)
	b = append(b, key...)
	var t [8]byte
	binary.BigEndian.PutUint64(t[:], commitTS)
	return append(b, t[:]...)
}

// VersionKey 构造版本记录键（导出版，供 sql 包在协调者提交路径登记版本）。
func VersionKey(commitTS uint64, key []byte) []byte { return verKey(commitTS, key) }

// verRangeStart 某 key 版本记录区间下界（commit_ts = 0）。
func verRangeStart(key []byte) []byte {
	b := make([]byte, 0, len(verPrefix)+len(key)+8)
	b = append(b, verPrefix...)
	b = append(b, key...)
	return append(b, 0, 0, 0, 0, 0, 0, 0, 0)
}

// verRangeEnd 某 key 版本记录区间上界（commit_ts = 0xFFFF...）。
func verRangeEnd(key []byte) []byte {
	b := make([]byte, 0, len(verPrefix)+len(key)+8)
	b = append(b, verPrefix...)
	b = append(b, key...)
	return append(b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
}

// versionPuts 为一批 ops 的写入键登记版本记录（追加到 batch）。
func versionPuts(batch *storage.WriteBatch, commitTS uint64, ops []TxnOp) {
	for _, op := range ops {
		if op.Delete {
			continue // 删除键不登记：行已不存在，无需版本过滤
		}
		batch.Puts = append(batch.Puts, storage.KVPair{Key: verKey(commitTS, op.Key), Value: []byte("1")})
	}
}

// VersionVisible 判断 key 对 begin_ts 快照是否可见：
//   - beginTS==0：不过滤（旧路径/物理搬迁），恒可见；
//   - 无版本记录：可见（单机数据/旧数据零回归）；
//   - 存在最大 commit_ts > beginTS 的版本记录：不可见（后提交写入）。
//
// scanFn 抽象读取器（事务扫描或存储扫描均可）。
func VersionVisible(scanFn func(r storage.KeyRange, limit int) ([]storage.KVPair, error), key []byte, beginTS uint64) (bool, error) {
	if beginTS == 0 {
		return true, nil
	}
	rows, err := scanFn(storage.KeyRange{Start: verRangeStart(key), End: verRangeEnd(key)}, 0)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return true, nil
	}
	last := rows[len(rows)-1].Key // 升序，最后一条 = 最大 commit_ts（ts 在键末尾）
	ts := binary.BigEndian.Uint64(last[len(last)-8:])
	return ts <= beginTS, nil
}

// ---- 协调者侧：Manager / Client ----

// PrepareTxn 向参与者发送 PREPARE（待提交 ops 落盘，幂等重放安全）。
// beginTS 为事务全局起始版本号（M8）。
func (m *Manager) PrepareTxn(nodeID, txID string, beginTS uint64, ops []TxnOp) error {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return err
	}
	return m.clientFor(n.ID).prepareTxn(txID, beginTS, ops)
}

// CommitTxn 向参与者发送 COMMIT（幂等：已提交返回 OK，可安全重试）。
// commitTS 为事务全局提交版本号（M8，参与者登记版本记录）。
func (m *Manager) CommitTxn(nodeID, txID string, commitTS uint64) error {
	n, err := m.ensureRemote(nodeID)
	if err != nil {
		return err
	}
	return m.clientFor(n.ID).commitTxn(txID, commitTS)
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

func (c *Client) prepareTxn(txID string, beginTS uint64, ops []TxnOp) error {
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
	payload, err := marshalJSON(PrepareReq{TxID: txID, BeginTS: beginTS, Ops: ops})
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

func (c *Client) commitTxn(txID string, commitTS uint64) error {
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
	payload, err := marshalJSON(CommitReq{TxID: txID, CommitTS: commitTS})
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
	if err := s.prepare2pc(req.TxID, req.BeginTS, req.Ops); err != nil {
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
	if err := s.commit2pc(req.TxID, req.CommitTS); err != nil {
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
// 将 ops 与 begin_ts 落盘为 m:2pc:p:<txid>（崩溃可恢复）并缓存内存。
func (s *Server) prepare2pc(txID string, beginTS uint64, ops []TxnOp) error {
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
	raw, err := marshalJSON(PrepareReq{TxID: txID, BeginTS: beginTS, Ops: ops})
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
// （数据 + 版本记录 + 删除 prepared 标记 + 写 committed 标记同一 WriteBatch），
// 幂等：已提交直接 OK；未 prepare 但落盘记录存在时按记录应用（协调者
// 重试 COMMIT 时内存已丢，读盘恢复 ops 后重放，同键同值幂等）。
func (s *Server) commit2pc(txID string, commitTS uint64) error {
	if txID == "" {
		return errors.New("cluster: empty txid")
	}
	s.mu2pc.Lock()
	defer s.mu2pc.Unlock()
	if s.commitFail != nil {
		if err := s.commitFail(txID); err != nil {
			return err
		}
	}
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
	// M8：为写入键登记版本记录（删除键不登记）。
	versionPuts(batch, commitTS, ops)
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

// Recover2PC 启动恢复：清理全部参与者侧 2PC 元键。
// 语义：prepared 未决事务一律回滚（数据从未写入，删除标记即丢弃，无残留）；
// committed/aborted 历史标记仅用于运行期幂等重试，重启后不再需要。
// 版本记录 m:ver:* 不在清理范围（已提交数据的可见性依据）。
// 由 db.StartCluster 在节点链路启动前调用。
func (s *Server) Recover2PC() error {
	s.mu2pc.Lock()
	defer s.mu2pc.Unlock()
	// M8 恢复强化：仅清理参与者幂等标记（p: prepared / c: committed / a: aborted）。
	// m:2pc:coord:* 是协调者 prepare 记录，留给 RecoverCoordinated2PC 依据
	// 持久化状态决策 commit/abort，绝不能在协调者崩溃恢复前被清掉。
	//
	// 逐前缀精确扫描：下界 = 前缀，上界 = 前缀 + 0xff（大于任意 ASCII txid），
	// 只命中该前缀自身键。coord 键（m:2pc:coord:*，第 8 字节 'o'）大于
	// "m:2pc:c:\xff"（第 8 字节 ':'），天然落在 c: 扫描范围之外。
	batch := &storage.WriteBatch{}
	for _, prefix := range []string{key2pcPrefix + "p:", key2pcPrefix + "c:", key2pcPrefix + "a:"} {
		rows, err := s.st.Scan(storage.KeyRange{Start: []byte(prefix), End: append([]byte(prefix), 0xff)}, -1)
		if err != nil {
			return err
		}
		for _, kv := range rows {
			batch.Deletes = append(batch.Deletes, kv.Key)
		}
	}
	s.pending2pc = map[string][]TxnOp{}
	return s.st.Write(batch)
}

// RecoverCoordinated2PC 协调者崩溃恢复（M8 2PC 恢复强化）：
// 扫描持久化协调者 prepare 记录 m:2pc:coord:*，依据 prepare 状态决策：
//   - prepared / committing：全部参与者已就绪 -> 决策 commit：
//     重放本地 ops 与版本记录（幂等，同键同值），驱动参与者幂等 COMMIT；
//   - preparing / aborted：未全部就绪或已决策中止 -> 决策 abort：
//     驱动参与者幂等 ABORT（本地从未写入，无需回滚）。
//
// 参与者 COMMIT/ABORT 全部成功后删除 coord 记录；任一参与者失败则保留
// 该记录并返回错误（下次启动重试，避免决策丢失导致数据不一致）。
func (s *Server) RecoverCoordinated2PC() error {
	s.mu2pc.Lock()
	defer s.mu2pc.Unlock()
	// 上界：m:2pc:coord:* 均小于 "m:2pc:coorq"。
	rows, err := s.st.Scan(storage.KeyRange{Start: []byte(key2pcCoord), End: []byte("m:2pc:coorq")}, -1)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	var pendingErr error
	for _, kv := range rows {
		var rec CoordRecord
		if err := unmarshalJSON(kv.Value, &rec); err != nil {
			pendingErr = errors.Join(pendingErr, err)
			continue
		}
		commit := rec.State == CoordPrepared || rec.State == CoordCommitting
		if commit {
			// 防御：极端情况下记录缺失 commitTS（旧版本记录/写入竞态），
			// 且本节点已装配 TSO 时，重新获取一个时间戳作为提交版本。
			commitTS := rec.CommitTS
			if commitTS == 0 && s.tso != nil {
				if ts, err := s.tso.Get(); err == nil {
					commitTS = ts
				}
			}
			// 本地重放：数据 + 版本记录（与协调者提交路径同 batch 语义）。
			batch := &storage.WriteBatch{}
			for _, op := range rec.LocalOps {
				if op.Delete {
					batch.Deletes = append(batch.Deletes, op.Key)
				} else {
					batch.Puts = append(batch.Puts, storage.KVPair{Key: op.Key, Value: op.Value})
				}
			}
			versionPuts(batch, commitTS, rec.LocalOps)
			if err := s.st.Write(batch); err != nil {
				pendingErr = errors.Join(pendingErr, err)
				continue
			}
			for _, p := range rec.Participants {
				if err := s.mgr.CommitTxn(p, rec.TxID, commitTS); err != nil {
					pendingErr = errors.Join(pendingErr, err)
				}
			}
		} else {
			for _, p := range rec.Participants {
				if err := s.mgr.AbortTxn(p, rec.TxID); err != nil {
					pendingErr = errors.Join(pendingErr, err)
				}
			}
		}
		// 决策执行完成（或 abort 路径无需本地动作）后删除 coord 记录。
		batch := &storage.WriteBatch{}
		batch.Deletes = append(batch.Deletes, kv.Key)
		if err := s.st.Write(batch); err != nil {
			pendingErr = errors.Join(pendingErr, err)
		}
	}
	return pendingErr
}
