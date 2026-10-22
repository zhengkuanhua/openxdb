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
// 完全一致。
//
// P0-1（显式事务跨 region 写）：显式事务（BEGIN..COMMIT/ROLLBACK）内的
// 跨节点写不再拦截——写语句执行时本地 ops 立即进入会话事务缓冲，远端 ops
// 按归属节点暂存会话（pendingRemote，未 PREPARE 前参与者无任何状态）；
// COMMIT 时经 execExplicitCommit2PC 统一走与 autocommit 完全相同的 2PC
// 状态机（延迟 PREPARE -> commit_ts -> 本地 COMMIT（含版本记录）-> 远端
// COMMIT -> 清理记录；崩溃由 CoordRecord 重放 / 后台补提交消除不确定窗口），
// ROLLBACK 时直接丢弃暂存。详见 docs/T14_m5_2pc.md。

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cluster"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// M17 协调者侧行锁：串行化同 key 的跨节点读改写（UPDATE col = col + x 类）。
// 背景：2PC 写路径在协调者本地求值（读旧行 -> 计算新值 -> 以最终值 Put 到
// 参与者），两个并发协调者事务若同时读到同一旧值，会各自算出相同新值、
// 后者覆盖前者，造成丢更新。单机模式引擎事务自带行锁；分布式读改写由
// 协调者进程内行锁串行化——后到事务等待前一事务 2PC 提交后再读，读到
// 的是包含先前写的新值。
// 限制：锁仅协调者进程内生效（当前验收为单协调者入口负载）；多协调者
// 并发写同 key 需后续全局行锁服务。
type rowLock struct {
	ch  chan struct{}
	ref int
}

var (
	rowLockMu sync.Mutex
	rowLocks  = make(map[string]*rowLock)
)

// lockRowKeys 按物理 key（或表级哨兵 key）获取互斥并返回解锁函数。
// 多 key 按字典序获取避免死锁；单机模式（mgr == nil）为 no-op。
// 注意：autocommit 语句在语句结束（2PC 提交后）解锁；显式事务写语句
// 由 takeRowLocks 挂到会话，统一在 COMMIT/ROLLBACK 后释放（M19）。
func (e *Executor) lockRowKeys(keys ...string) func() {
	dbgLog("LOCK enter mgr=%v n=%d keys=%q", e.mgr != nil, len(keys), keys)
	if e.mgr == nil {
		return func() {}
	}
	sort.Strings(keys)
	for _, k := range keys {
		rowLockMu.Lock()
		lk, ok := rowLocks[k]
		if !ok {
			lk = &rowLock{ch: make(chan struct{}, 1)}
			rowLocks[k] = lk
		}
		lk.ref++
		rowLockMu.Unlock()
		dbgLog("LOCK want key=%q ref=%d", k, lk.ref)
		lk.ch <- struct{}{} // 阻塞直至可获取（容量 1 即互斥量）
		dbgLog("LOCK got key=%q", k)
	}
	return func() {
		for _, k := range keys {
			rowLockMu.Lock()
			lk, ok := rowLocks[k]
			if !ok {
				rowLockMu.Unlock()
				continue
			}
			lk.ref--
			if lk.ref == 0 {
				delete(rowLocks, k)
			}
			rowLockMu.Unlock()
			<-lk.ch
		}
	}
}

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

// hasRemoteOps 是否存在远端 region 的写操作。
// P0-1 后显式事务 DML 不再走此判定（改用 splitExplicitOps 分组暂存）；
// 保留给 mysqldump_import 等仍需整体拒绝远端写的调用方。
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

var _txSeq uint64

// newTxID 生成会话级显式事务 ID（M18-A）：时间戳 + 进程内递增序列，
// 跨节点唯一，随 QUERY_REQ 等集群帧传播，供参与者识别写请求所属会话事务。
func (e *Executor) newTxID() string {
	seq := atomic.AddUint64(&_txSeq, 1)
	return fmt.Sprintf("tx-%d-%d", time.Now().UnixNano(), seq)
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
// 调用前提：autocommit 语句或自建事务（CSV IMPORT）。显式会话事务内的
// 跨 region 写由写语句 splitExplicitOps 暂存、COMMIT 时经
// execExplicitCommit2PC 提交（P0-1，本函数不直接服务显式事务）。
//
// M8 语义（分布式快照隔离 + 2PC 恢复强化）：
//   - mgr == nil 且未装配 TSO：applyOps + tx.Commit()（M1-M4 原路径零回归）；
//   - mgr == nil 但装配 TSO：applyOps + 版本记录登记 + tx.Commit()
//     （单机也满足快照过滤，begin_ts 由本地 TSO 发号）；
//   - 全本地 + TSO：applyOps -> 取 commit_ts -> 版本记录登记 -> tx.Commit()；
//   - 有远端：本地组 applyOps -> 持久化 CoordRecord(preparing) -> 远端全部
//     PREPARE（携带 begin_ts）-> 取 commit_ts -> 更新 CoordRecord(prepared,
//     参与者列表) -> 本地 COMMIT（含版本记录）-> 更新 CoordRecord(committing)
//     -> 远端全部 COMMIT（携带 commit_ts，幂等）-> 删除 CoordRecord。
//     PREPARE/TSO 失败：abort 已就绪参与者 + 删除记录；本地提交失败：
//     abort 参与者；远端 COMMIT 失败：返回 TXN_COMMIT_UNCERTAIN（记录保留
//     committing，重启后 RecoverCoordinated2PC 决策 commit 完成收尾）。
func (e *Executor) exec2pcWrite(sess *Session, tx txn.Txn, ops []kvOp) error {
	if len(ops) == 0 {
		return nil
	}
	if e.mgr == nil && e.ts == nil {
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
	beginTS := e.snapshotTS(sess)
	if len(remote) == 0 {
		// 全本地：装配了 TSO 时登记版本记录（保证快照过滤覆盖本地提交）。
		commitTS, err := e.tsoGet()
		if err != nil {
			return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: tso unavailable: %v", err)}
		}
		if err := e.putVersionRecords(tx, commitTS, local); err != nil {
			return err
		}
		return tx.Commit()
	}
	// 两阶段提交（M8 恢复强化）
	txID := e.new2pcID()
	rec := cluster.CoordRecord{TxID: txID, BeginTS: beginTS, LocalOps: toClusterTxnOps(local), State: cluster.CoordPreparing}
	if err := e.writeCoord(rec); err != nil {
		return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: persist coord %s: %v", txID, err)}
	}
	prepared := make([]string, 0, len(remote))
	for node, nops := range remote {
		if err := e.mgr.PrepareTxn(node, txID, beginTS, toClusterTxnOps(nops)); err != nil {
			for _, n := range prepared {
				_ = e.mgr.AbortTxn(n, txID)
			}
			_ = e.deleteCoord(txID)
			return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: 2pc %s participant %s: %v", txID, node, err)}
		}
		prepared = append(prepared, node)
	}
	commitTS, err := e.tsoGet()
	if err != nil {
		for _, n := range prepared {
			_ = e.mgr.AbortTxn(n, txID)
		}
		_ = e.deleteCoord(txID)
		return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: tso unavailable: %v", err)}
	}
	rec.CommitTS = commitTS
	rec.Participants = prepared
	rec.State = cluster.CoordPrepared
	if err := e.writeCoord(rec); err != nil {
		for _, n := range prepared {
			_ = e.mgr.AbortTxn(n, txID)
		}
		return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: persist coord %s: %v", txID, err)}
	}
	// 版本记录入本地事务（与本地数据同批次提交）
	if err := e.putVersionRecords(tx, commitTS, local); err != nil {
		return err
	}
	// 全部就绪：提交协调者本地事务
	if err := tx.Commit(); err != nil {
		for _, n := range prepared {
			_ = e.mgr.AbortTxn(n, txID)
		}
		return &SQLError{Msg: fmt.Sprintf("TXN_COMMIT_ABORTED: local commit failed for 2pc %s: %v", txID, err)}
	}
	// 本地已提交：记录 committing（崩溃后恢复决策 commit）。
	rec.State = cluster.CoordCommitting
	_ = e.writeCoord(rec)
	// 远端提交（幂等；网络失败返回不确定状态，重试 CommitTxn 安全）。
	// P0 治本修复：对同一 txID 有限次短退避重试，尽量在请求窗口内完成
	// 全部参与者提交，避免过早返回 UNCERTAIN 诱发客户端以新事务重试、
	// 远端对新 txID 无幂等保护而重复应用 +1。
	for _, n := range prepared {
		if err := e.commitWithRetry(n, txID, commitTS); err != nil {
			return &SQLError{Msg: fmt.Sprintf("TXN_COMMIT_UNCERTAIN: 2pc %s participant %s: %v (retry exhausted; coord committing record kept, background replay will finish)", txID, n, err)}
		}
	}
	_ = e.deleteCoord(txID)
	return nil
}

// commitWithRetry 对同一参与者按同一 txID 幂等提交，失败后短退避重试
// （50ms / 150ms / 450ms，最多 3 次）。参与者 commit2pc 以
// committed2pcKey 去重，重试仅补发 COMMIT、绝不重复应用数据，安全。
func (e *Executor) commitWithRetry(node, txID string, commitTS uint64) error {
	err := e.mgr.CommitTxn(node, txID, commitTS)
	if err == nil {
		return nil
	}
	for i, delay := range []time.Duration{50 * time.Millisecond, 150 * time.Millisecond, 450 * time.Millisecond} {
		time.Sleep(delay)
		dbgLog("2PC COMMIT retry %d node=%s tx=%s after %v: %v", i+1, node, txID, delay, err)
		if err = e.mgr.CommitTxn(node, txID, commitTS); err == nil {
			return nil
		}
	}
	return err
}

// ---- P0 后台补提交：committing 状态协调者记录周期扫描重放 ----
// 协调者本地已提交、远端 COMMIT 未全部成功时 CoordRecord 保留在
// committing 状态。除 exec2pcWrite 内短退避重试外，本扫描器周期性
// 幂等重放未完成提交（CommitTxn 以 committed2pcKey 去重，安全），
// 成功后清理记录，彻底消除"不确定窗口"残留。

// StartCoordReplay 启动后台补提交扫描循环（db.StartCluster 在
// RecoverCoordinated2PC 之后装配）。单机模式（mgr==nil）或未装配
// storage 时 no-op；重复调用 no-op。
func (e *Executor) StartCoordReplay(interval time.Duration) {
	if e.mgr == nil || e.st == nil {
		return
	}
	e.mu.Lock()
	if e.coordReplayStop != nil {
		e.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	e.coordReplayStop = stop
	e.coordReplayDone = done
	e.mu.Unlock()
	go e.coordReplayLoop(interval, stop, done)
}

// StopCoordReplay 停止后台补提交循环（db.Close 流程装配，避免关闭竞态）。
func (e *Executor) StopCoordReplay() {
	e.mu.Lock()
	stop := e.coordReplayStop
	done := e.coordReplayDone
	e.coordReplayStop = nil
	e.coordReplayDone = nil
	e.mu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	<-done
}

func (e *Executor) coordReplayLoop(interval time.Duration, stop, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			e.replayCommittingCoords()
		}
	}
}

// replayCommittingCoords 扫描本地 committing 状态协调者记录，对全部
// 参与者按同 txID 幂等重放 COMMIT；全部成功后删除记录，任一失败保留
// 记录供下轮扫描重试（与 RecoverCoordinated2PC 相同"决策不丢失"语义）。
func (e *Executor) replayCommittingCoords() {
	prefix := cluster.CoordPrefix()
	rows, err := e.st.Scan(storage.KeyRange{Start: prefix, End: append(prefix, 0xff)}, -1)
	if err != nil {
		dbgLog("2PC coord replay scan failed: %v", err)
		return
	}
	for _, kv := range rows {
		var rec cluster.CoordRecord
		if err := cluster.UnmarshalCoord(kv.Value, &rec); err != nil {
			dbgLog("2PC coord replay unmarshal %s: %v", kv.Key, err)
			continue
		}
		if rec.State != cluster.CoordCommitting {
			continue
		}
		allOK := true
		for _, p := range rec.Participants {
			if err := e.commitWithRetry(p, rec.TxID, rec.CommitTS); err != nil {
				dbgLog("2PC coord replay tx=%s node=%s failed: %v", rec.TxID, p, err)
				allOK = false
				break
			}
		}
		if allOK {
			_ = e.deleteCoord(rec.TxID)
		}
	}
}

// writeCoord 持久化协调者 prepare 记录（独立事务，M8 恢复依据）。
func (e *Executor) writeCoord(rec cluster.CoordRecord) error {
	raw, err := cluster.MarshalCoord(rec)
	if err != nil {
		return err
	}
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.Put(cluster.CoordKey(rec.TxID), raw); err != nil {
		return err
	}
	return tx.Commit()
}

// deleteCoord 删除协调者 prepare 记录（独立事务；2PC 收尾后调用）。
func (e *Executor) deleteCoord(txID string) error {
	tx, err := e.tm.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.Delete(cluster.CoordKey(txID)); err != nil {
		return err
	}
	return tx.Commit()
}

// putVersionRecords 为本地写键登记 M8 版本记录（commit_ts 版本号）。
// 删除键不登记（行已不存在，无需过滤）。
func (e *Executor) putVersionRecords(tx txn.Txn, commitTS uint64, ops []kvOp) error {
	if commitTS == 0 {
		return nil // 未装配 TSO：不登记（旧路径零回归）
	}
	for _, op := range ops {
		if op.Delete {
			continue
		}
		if err := tx.Put(cluster.VersionKey(commitTS, op.Key), []byte("1")); err != nil {
			return err
		}
	}
	return nil
}

// ---- P0-1：显式事务内跨 region 写支持 ----
//
// 设计（延迟 PREPARE）：
//   - 写语句执行：本地 ops 立即 applyOps 进会话事务缓冲（沿用 M1-M4 语义，
//     同语句内可被后续行求值读到）；远端 ops 仅按归属节点暂存，不 PREPARE
//     （参与者侧此时无任何 prepared 状态，回滚零成本）；
//   - COMMIT：execExplicitCommit2PC 与 autocommit exec2pcWrite 复用同一
//     2PC 状态机——延迟 PREPARE -> commit_ts -> 本地 COMMIT（含版本记录）
//     -> 远端 COMMIT -> 清理记录；崩溃/不确定窗口由 CoordRecord 重放与
//     后台补提交消除（既有机制零改动）；
//   - ROLLBACK：暂存直接丢弃，参与者无状态需清理。

// splitExplicitOps 显式事务写语句分组：本地 ops 立即应用进会话事务缓冲，
// 远端 ops 按归属节点返回（未挂载、未 PREPARE）。行键锁仍按全部 ops 由
// 调用方在语句前统一获取（持锁至 COMMIT/ROLLBACK，M19 语义不变）。
func (e *Executor) splitExplicitOps(sess *Session, tx txn.Txn, ops []kvOp) ([]kvOp, map[string][]kvOp, error) {
	if len(ops) == 0 {
		return nil, nil, nil
	}
	local, remote, err := e.groupOps(ops)
	if err != nil {
		return nil, nil, err
	}
	if err := applyOps(tx, local); err != nil {
		return nil, nil, err
	}
	return local, remote, nil
}

// stagePendingExplicit 语句成功后把本次（本地/远端）写挂载到会话暂存：
// 本地累积供 COMMIT 恢复重放与版本记录（挂载前会话事务缓冲已含这些键，
// 提交时本地 Txn 直接落盘）；远端按节点合并。显式事务外调用为空操作。
func (e *Executor) stagePendingExplicit(sess *Session, local []kvOp, remote map[string][]kvOp) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !sess.inTx {
		return
	}
	sess.pendingLocal = append(sess.pendingLocal, local...)
	for node, nops := range remote {
		sess.pendingRemote[node] = append(sess.pendingRemote[node], nops...)
	}
}

// clearPendingExplicit 清空会话显式事务 2PC 暂存（COMMIT/ROLLBACK 后调用，
// 防御会话复位残留）。
func (e *Executor) clearPendingExplicit(sess *Session) {
	e.mu.Lock()
	defer e.mu.Unlock()
	sess.pendingLocal = nil
	sess.pendingRemote = map[string][]kvOp{}
}

// execExplicitCommit2PC 显式事务 COMMIT 时的 2PC 提交路径（P0-1）。
// 调用前提：会话已出事务态（commitTx 已置 inTx=false）、pendingLocal/
// pendingRemote 尚未清空。语义与 exec2pcWrite 的远端分支一致：
//   - 无远端：本地 Txn 直接提交（本地 ops 已随语句 applyOps；版本记录
//     按快照起点登记，保证跨节点读一致）；
//   - 有远端：CoordRecord(preparing) -> 远端 PREPARE（携带 begin_ts）
//     -> commit_ts -> CoordRecord(prepared) -> 本地 COMMIT（含版本记录）
//     -> CoordRecord(committing) -> 远端 COMMIT（幂等重试）-> 清理记录；
//     任一失败路径与 autocommit 完全相同的 abort/UNCERTAIN 语义，
//     崩溃恢复由 RecoverCoordinated2PC / 后台补提交兜底。
func (e *Executor) execExplicitCommit2PC(sess *Session, tx txn.Txn, beginTS uint64) error {
	local := sess.pendingLocal
	remote := sess.pendingRemote
	if e.mgr == nil && e.ts == nil {
		// 单机未装配 TSO：本地 Txn 直接提交（远端暂存理论为空）。
		return tx.Commit()
	}
	if beginTS == 0 && e.ts != nil {
		// 事务快照起点未取号（显式事务只写未读）：以提交时权威 TSO 号
		// 作为 begin_ts，保证 commit_ts 之后读到此事务全部写入。
		var err error
		beginTS, err = e.tsoGet()
		if err != nil {
			_ = tx.Rollback()
			return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: tso unavailable: %v", err)}
		}
	}
	if len(remote) == 0 {
		// 全本地：登记版本记录后提交（快照过滤覆盖显式事务本地提交）。
		commitTS, err := e.tsoGet()
		if err != nil {
			_ = tx.Rollback()
			return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: tso unavailable: %v", err)}
		}
		if err := e.putVersionRecords(tx, commitTS, local); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	// 跨 region：与 autocommit 完全相同的 2PC 状态机。
	txID := e.new2pcID()
	rec := cluster.CoordRecord{TxID: txID, BeginTS: beginTS, LocalOps: toClusterTxnOps(local), State: cluster.CoordPreparing}
	if err := e.writeCoord(rec); err != nil {
		_ = tx.Rollback()
		return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: persist coord %s: %v", txID, err)}
	}
	prepared := make([]string, 0, len(remote))
	for node, nops := range remote {
		if err := e.mgr.PrepareTxn(node, txID, beginTS, toClusterTxnOps(nops)); err != nil {
			for _, n := range prepared {
				_ = e.mgr.AbortTxn(n, txID)
			}
			_ = e.deleteCoord(txID)
			_ = tx.Rollback()
			return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: 2pc %s participant %s: %v", txID, node, err)}
		}
		prepared = append(prepared, node)
	}
	commitTS, err := e.tsoGet()
	if err != nil {
		for _, n := range prepared {
			_ = e.mgr.AbortTxn(n, txID)
		}
		_ = e.deleteCoord(txID)
		_ = tx.Rollback()
		return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: tso unavailable: %v", err)}
	}
	rec.CommitTS = commitTS
	rec.Participants = prepared
	rec.State = cluster.CoordPrepared
	if err := e.writeCoord(rec); err != nil {
		for _, n := range prepared {
			_ = e.mgr.AbortTxn(n, txID)
		}
		_ = tx.Rollback()
		return &SQLError{Msg: fmt.Sprintf("TXN_PREPARE_FAILED: persist coord %s: %v", txID, err)}
	}
	// 版本记录入本地事务（与本地数据同批次提交）
	if err := e.putVersionRecords(tx, commitTS, local); err != nil {
		_ = tx.Rollback()
		return err
	}
	// 全部就绪：提交协调者本地事务
	if err := tx.Commit(); err != nil {
		for _, n := range prepared {
			_ = e.mgr.AbortTxn(n, txID)
		}
		_ = tx.Rollback()
		return &SQLError{Msg: fmt.Sprintf("TXN_COMMIT_ABORTED: local commit failed for 2pc %s: %v", txID, err)}
	}
	// 本地已提交：记录 committing（崩溃后恢复决策 commit）。
	rec.State = cluster.CoordCommitting
	_ = e.writeCoord(rec)
	// 远端提交（幂等；网络失败返回不确定状态，重试 CommitTxn 安全）。
	for _, n := range prepared {
		if err := e.commitWithRetry(n, txID, commitTS); err != nil {
			return &SQLError{Msg: fmt.Sprintf("TXN_COMMIT_UNCERTAIN: 2pc %s participant %s: %v (retry exhausted; coord committing record kept, background replay will finish)", txID, n, err)}
		}
	}
	_ = e.deleteCoord(txID)
	return nil
}
