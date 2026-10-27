package txn

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/wal"
)

type txnManager struct {
	mu        sync.Mutex
	st        storage.Storage
	wal       wal.WAL
	nextID    uint64
	seqNext   uint64     // 下一个待分配的提交序号（锁内分配；仅 Commit 事务占位）
	applied   uint64     // 已成功应用到存储的最大提交序号
	applyCond *sync.Cond // 关联 mu，等待轮到自己按序应用

	commitHook    func(lsn storage.LSN, seq uint64, batch *storage.WriteBatch) error // M2 复制挂接点（锁内，异步推送）
	hookErr       error                                                              // 最近一次 hook 错误
	commitWaitHook func(lsn storage.LSN, seq uint64) error // T33 复制确认挂接点（锁外，同步/半同步等待 ACK）
}

// New 创建事务管理器并执行启动恢复（重放 WAL 已 Commit 记录）。
func New(st storage.Storage, w wal.WAL) (TxnManager, error) {
	m := &txnManager{st: st, wal: w}
	m.applyCond = sync.NewCond(&m.mu)
	if err := m.Recover(); err != nil {
		return nil, err
	}
	return m, nil
}

// Begin 创建新事务。
// v1.16.0 C2 快照创建锁外化：锁内仅分配事务 ID（保持单调递增、与旧版一致），
// 快照创建移出锁外——RocksDB Snapshot 线程安全，锁外创建显著缩短 Begin 临界区，
// 缓解点查/autocommit 密集场景的 Begin 锁竞争。
// 语义等价性：快照创建时刻 = Begin 生效时刻（返回前固定一致性视图），锁外创建
// 仅把视图固定点从「持锁时刻」推迟到「解锁后瞬间」，对已提交数据仍是一致快照；
// TSO/beginTS 由调用方（executor）另行取号，与本层 ID/序号解耦，保持不变。
// ID 消耗语义不变：快照创建失败时 ID 已分配、不回收（与旧版锁内失败行为一致）。
func (m *txnManager) Begin() (Txn, error) {
	m.mu.Lock()
	m.nextID++
	id := m.nextID
	m.mu.Unlock()

	snap, err := m.st.Snapshot()
	if err != nil {
		return nil, err
	}
	return &txnImpl{
		id:    id,
		mgr:   m,
		batch: &storage.WriteBatch{},
		snap:  snap,
		state: TxnActive,
	}, nil
}

// Recover 重放 WAL 中所有 StateCommit 记录到存储。
// 提交顺序为 WAL 先行：崩溃于「WAL 已落盘、存储未应用」时由本方法补齐；重复应用幂等。
// 恢复后提交序号从已重放 Commit 数之后继续（Rollback 记录只审计、不占提交序号）。
func (m *txnManager) Recover() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var applyErr error
	var commits uint64
	// 路由元数据键：m:regions / m:tables 以落盘快照为准，WAL 重放不得覆盖。
	// 根因：SplitTable/Failover 的 m:regions 写入、DDL/表目录变更（m:tables）
	// 会进入 WAL，崩溃重启后 Recover 重放会把历史/被污染的快照覆盖回 storage，
	// 造成"启动即漂移"（例如表 ID 被旧 WAL 记录回退，导致分片路由失效）。
	skipKeys := [][]byte{[]byte("m:regions"), []byte("m:tables")}
	skipMeta := func(k []byte) bool {
		for _, sk := range skipKeys {
			if bytes.Equal(k, sk) {
				return true
			}
		}
		return false
	}
	err := m.wal.Replay(func(e *wal.WalEntry) {
		if e.State == wal.StateCommit && applyErr == nil {
			commits++
			if e.Batch != nil && (len(e.Batch.Puts) > 0 || len(e.Batch.Deletes) > 0) {
				hasMeta := false
				fb := &storage.WriteBatch{}
				for _, p := range e.Batch.Puts {
					if skipMeta(p.Key) {
						hasMeta = true
						continue
					}
					fb.Puts = append(fb.Puts, p)
				}
				for _, d := range e.Batch.Deletes {
					if skipMeta(d) {
						hasMeta = true
						continue
					}
					fb.Deletes = append(fb.Deletes, d)
				}
				if hasMeta {
					if len(fb.Puts) > 0 || len(fb.Deletes) > 0 {
						applyErr = m.st.Write(fb)
					}
					return
				}
			}
			applyErr = m.st.Write(e.Batch)
		}
	})
	if err != nil {
		return err
	}
	if applyErr == nil {
		m.seqNext = commits + 1
		m.applied = commits
	}
	return applyErr
}

func (m *txnManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.wal.Close()
}

// SetCommitHook 注册提交后回调（M2 复制挂接点；传 nil 清除）。
func (m *txnManager) SetCommitHook(hook func(lsn storage.LSN, seq uint64, batch *storage.WriteBatch) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitHook = hook
	m.hookErr = nil
}

// SetCommitWaitHook 注册提交后置确认回调（T33 同步/半同步复制挂接点；
// 在锁外、Commit 返回前调用，可阻塞等待副本 ACK；传 nil 清除）。
func (m *txnManager) SetCommitWaitHook(hook func(lsn storage.LSN, seq uint64) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitWaitHook = hook
}

// HookErr 返回最近一次提交后回调的错误（无错误返回 nil）。
func (m *txnManager) HookErr() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hookErr
}

// SetGroupCommitWindow 设置 WAL 组提交定时刷盘窗口（v1.14.0 P1），透传底层 WAL。
// d<=0 关闭定时窗口（回退 v1.13.0 被动攒批行为）；调用方需持 m.mu 或串行访问。
func (m *txnManager) SetGroupCommitWindow(d time.Duration) {
	m.wal.SetGroupCommitWindow(d)
}

type txnImpl struct {
	id    uint64
	mgr   *txnManager
	batch *storage.WriteBatch
	ops   []opEntry // 事务内操作序列（按时间序，read-your-writes 判定依据）
	snap  storage.Snapshot
	state TxnState
	// P2-13 Serializable 冲突检测：EnableSerializable 开启后记录
	// 读集（Get/MGet/Scan 命中的 key）与写集（Put/Delete 的 key），
	// 提交时由协调者做 rw-antidependency 检测。
	serEnabled bool
	readSet    map[string]bool
	writeSet   map[string]bool
}

type opEntry struct {
	isDelete bool
	key      []byte
	value    []byte
}

func (t *txnImpl) ID() uint64 { return t.id }

func (t *txnImpl) Get(key []byte) ([]byte, error) {
	if t.state != TxnActive {
		return nil, ErrTxnClosed
	}
	// P2-13：Serializable 读集记录（含读不存在，供 phantom 检测）。
	if t.serEnabled {
		t.readSet[string(key)] = true
	}
	// read-your-writes：按操作时间序反向扫描，最近一次操作决定结果
	for i := len(t.ops) - 1; i >= 0; i-- {
		o := t.ops[i]
		if bytes.Equal(o.key, key) {
			if o.isDelete {
				return nil, storage.ErrNotFound
			}
			return o.value, nil
		}
	}
	// 快照隔离读（BEGIN 时刻一致性）
	return t.snap.Get(key)
}

// MGet 批量读取：read-your-writes 命中优先，其余键走快照批量读（单次底层调用）。
func (t *txnImpl) MGet(keys [][]byte) (map[string][]byte, error) {
	if t.state != TxnActive {
		return nil, ErrTxnClosed
	}
	// P2-13：Serializable 读集记录（含读不存在，供 phantom 检测）。
	if t.serEnabled {
		for _, key := range keys {
			t.readSet[string(key)] = true
		}
	}
	got := make(map[string][]byte, len(keys))
	missing := make([][]byte, 0, len(keys))
	for _, key := range keys {
		found := false
		for i := len(t.ops) - 1; i >= 0; i-- {
			o := t.ops[i]
			if bytes.Equal(o.key, key) {
				found = true
				if !o.isDelete {
					got[string(key)] = o.value
				}
				break
			}
		}
		if !found {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		snapGot, err := t.snap.MGet(missing)
		if err != nil {
			return nil, err
		}
		for k, v := range snapGot {
			got[k] = v
		}
	}
	return got, nil
}

// Scan 合并快照数据与事务内未提交写（read-your-writes），结果按 Key 升序。
func (t *txnImpl) Scan(r storage.KeyRange, limit int) ([]storage.KVPair, error) {
	if t.state != TxnActive {
		return nil, ErrTxnClosed
	}
	base, err := t.snap.Scan(r, 0)
	if err != nil {
		return nil, err
	}
	// v1.15.0 W1 快路径：事务内无未提交写（ops 为空）时，快照结果即为最终结果
	// （无 read-your-writes 合并需求），直接透传快照有序结果并应用 limit 截断，
	// 跳过 byKey/dirty/merged map 全量物化 + sort 重建（SQL 层全量物化+重复排序
	// 瓶颈的主要来源）。前提：快照 Scan 保证按 Key 升序（RocksDB/btree 迭代器语义）。
	if len(t.ops) == 0 {
		out := base
		if limit > 0 && len(out) > limit {
			out = out[:limit]
		}
		// P2-13：Serializable 读集记录（以实际返回行为准，与旧路径等价；
		// limit 截断的未读行不记）。
		if t.serEnabled {
			for _, kv := range out {
				t.readSet[string(kv.Key)] = true
			}
		}
		return out, nil
	}
	// 1) 快照结果映射 + 记录被事务写覆盖的键
	byKey := make(map[string]storage.KVPair, len(base))
	dirty := make(map[string]bool, len(base))
	for _, kv := range base {
		keyStr := string(kv.Key)
		byKey[keyStr] = kv
		if t.dirtyContains(keyStr) {
			dirty[keyStr] = true
		}
	}
	// 2) 事务内写按时间序覆盖：先删掉快照中被覆盖项，再整体重新落一次
	merged := make(map[string]storage.KVPair, len(byKey)+len(t.ops))
	for ks, kv := range byKey {
		if !dirty[ks] {
			merged[ks] = kv
		}
	}
	for i := len(t.ops) - 1; i >= 0; i-- {
		o := t.ops[i]
		ks := string(o.key)
		if !inRange(o.key, r) {
			continue
		}
		if o.isDelete {
			delete(merged, ks)
		} else {
			merged[ks] = storage.KVPair{Key: o.key, Value: o.value}
		}
	}
	// P2-13：Serializable 读集记录（范围读返回的键均纳入，供 phantom 检测）。
	if t.serEnabled {
		for ks := range merged {
			t.readSet[ks] = true
		}
	}
	// 3) 升序输出 + limit
	keys := make([][]byte, 0, len(merged))
	for _, kv := range merged {
		keys = append(keys, kv.Key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	out := make([]storage.KVPair, 0, len(keys))
	for _, k := range keys {
		out = append(out, merged[string(k)])
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	// P2-13：Serializable 读集记录（以实际返回行为准；limit 截断的未读行不记）。
	if t.serEnabled {
		for _, kv := range out {
			t.readSet[string(kv.Key)] = true
		}
	}
	return out, nil
}

// CountRange 范围计数 [Start, End)：storage.RangeCounter 可选接口实现
// （v1.15.0 W2 COUNT(*) 快速路径）。不写入 Txn 接口，调用方以类型断言
// （tx.(storage.RangeCounter)）探测使用。
//
// 快速路径：ops 为空且非 Serializable 时直接走快照 CountRange（存储层
// 只数 key 不拷贝 value）；快照不支持 CountRange 时回退 Scan 计数。
// 回退路径：ops 非空或 Serializable 时调用 t.Scan 后计数，保证
// read-your-writes 合并与 phantom 读集记录与既有 Scan 语义完全等价。
// 结果与「同范围 Scan 的行数」一致（limit<=0 不限制；limit>0 至多返回 limit）。
func (t *txnImpl) CountRange(r storage.KeyRange, limit int) (int, error) {
	if t.state != TxnActive {
		return 0, ErrTxnClosed
	}
	if len(t.ops) == 0 && !t.serEnabled {
		if rc, ok := storage.SnapshotCountRangeOf(t.snap); ok {
			return rc.CountRange(r, limit)
		}
	}
	kvs, err := t.Scan(r, limit)
	if err != nil {
		return 0, err
	}
	return len(kvs), nil
}

// 编译期断言：txnImpl 实现 storage.RangeCounter（可选接口，不改动 Txn 契约）。
var _ storage.RangeCounter = (*txnImpl)(nil)

// RefreshSnapshot 锁内刷新一致性快照（读改写读旧值前调用）：
// 丢弃旧快照，重新固定为当前已提交视图，保证后拿锁事务
// 能看到前序事务提交的数据（修复并发 UPDATE 丢更新：取号虽已延迟到
// 锁内，但引擎快照若仍固定在 BEGIN（早于行锁）会读到旧值覆盖）。
// 使用方：autocommit 单语句路径，以及显式事务写语句的锁内当前读
// （M19：executor.refreshWriteSnapshot 在调用本方法后推进 curBeginTS，
// 使 VersionVisible 对前序已提交版本可见）。
func (t *txnImpl) RefreshSnapshot() error {
	if t.state != TxnActive {
		return ErrTxnClosed
	}
	snap, err := t.mgr.st.Snapshot()
	if err != nil {
		return err
	}
	old := t.snap
	t.snap = snap
	if old != nil {
		old.Release()
	}
	return nil
}

func (t *txnImpl) dirtyContains(ks string) bool {
	for i := len(t.ops) - 1; i >= 0; i-- {
		if string(t.ops[i].key) == ks {
			return true
		}
	}
	return false
}

func inRange(key []byte, r storage.KeyRange) bool {
	if len(r.Start) > 0 && bytes.Compare(key, r.Start) < 0 {
		return false
	}
	if len(r.End) > 0 && bytes.Compare(key, r.End) >= 0 {
		return false
	}
	return true
}

func (t *txnImpl) Put(key, value []byte) error {
	if t.state != TxnActive {
		return ErrTxnClosed
	}
	// P2-13：Serializable 写集记录。
	if t.serEnabled {
		t.writeSet[string(key)] = true
	}
	t.batch.Puts = append(t.batch.Puts, storage.KVPair{Key: key, Value: value})
	t.ops = append(t.ops, opEntry{key: key, value: value})
	return nil
}

func (t *txnImpl) Delete(key []byte) error {
	if t.state != TxnActive {
		return ErrTxnClosed
	}
	// P2-13：Serializable 写集记录。
	if t.serEnabled {
		t.writeSet[string(key)] = true
	}
	t.batch.Deletes = append(t.batch.Deletes, key)
	t.ops = append(t.ops, opEntry{isDelete: true, key: key})
	return nil
}

// EnableSerializable 开启 Serializable 读/写集记录（P2-13）。
// 由协调者（executor.beginTx）在事务注册前调用；未开启时零开销。
func (t *txnImpl) EnableSerializable() {
	t.serEnabled = true
	if t.readSet == nil {
		t.readSet = map[string]bool{}
	}
	if t.writeSet == nil {
		t.writeSet = map[string]bool{}
	}
}

// ReadSet 返回 Serializable 读集（协调者提交检测用；仅 EnableSerializable 后非空）。
func (t *txnImpl) ReadSet() map[string]bool { return t.readSet }

// WriteSet 返回 Serializable 写集（协调者提交检测用；仅 EnableSerializable 后非空）。
func (t *txnImpl) WriteSet() map[string]bool { return t.writeSet }

// Commit：WAL 先行 → 存储原子应用 → 释放快照。
// B2 组提交流水线：①锁内 Append 分配 LSN（WAL 顺序 = 锁获取顺序 = 提交顺序）；
// ②释放锁后 SyncUpTo(lsn)，并发提交在此合并 fsync；③重新加锁，按提交序号排队应用存储，
// 保持「最后提交者胜」串行语义且 st.Write 顺序与 WAL 顺序严格一致（崩溃恢复重放结果等价）。
// 说明：提交序号由本层在锁内分配（仅 Commit 事务占位），与 LSN 解耦——
// Rollback 审计记录也消耗 LSN 但不参与应用队列，避免 LSN 缺口导致的排队死锁。
func (t *txnImpl) Commit() error {
	if t.state != TxnActive {
		return ErrTxnClosed
	}
	m := t.mgr

	// 空批次：无需写 WAL / 存储，直接结束（与旧版行为一致，不消耗 LSN 与序号）。
	if len(t.batch.Puts) == 0 && len(t.batch.Deletes) == 0 {
		m.mu.Lock()
		t.finish(TxnCommitted)
		m.mu.Unlock()
		return nil
	}

	// 阶段 1：锁内顺序 Append 拿 LSN，并分配提交序号（快速返回，组提交模式下不 fsync）。
	m.mu.Lock()
	entry := &wal.WalEntry{TxnID: t.id, Batch: t.batch, State: wal.StateCommit}
	if err := m.wal.Append(entry); err != nil {
		m.mu.Unlock()
		return err
	}
	lsn := entry.LSN
	seq := m.seqNext
	m.seqNext++
	m.mu.Unlock()

	// 阶段 2：锁外 SyncUpTo —— 并发事务在此合并为一次 leader fsync。
	if err := m.wal.SyncUpTo(lsn); err != nil {
		// fsync 失败：该事务视为未提交成功。跳过自己的序号，避免后续事务永久等待。
		m.mu.Lock()
		m.applied++
		m.applyCond.Broadcast()
		m.mu.Unlock()
		return err
	}

	// 阶段 3：重新加锁，按提交序号串行应用存储（避免锁重新获取顺序打乱 WAL 顺序）。
	m.mu.Lock()
	for seq != m.applied+1 {
		m.applyCond.Wait()
	}
	if err := m.st.Write(t.batch); err != nil {
		// 应用失败：跳过该序号让后续事务继续（该记录已落 WAL，崩溃恢复会补齐）。
		m.applied++
		m.applyCond.Broadcast()
		m.mu.Unlock()
		return err
	}
	m.applied++
	m.applyCond.Broadcast()
	if m.commitHook != nil {
		if herr := m.commitHook(lsn, seq, t.batch); herr != nil {
			m.hookErr = herr
		}
	}
	t.finish(TxnCommitted)
	m.mu.Unlock()
	// T33 同步/半同步复制（M20）：锁外、Commit 返回前执行复制 ACK 等待。
	// 错误（如 sync 超时）回传调用者——此时事务已本地提交，属于分布式
	// "提交不确定"窗口，客户端重试幂等。
	if m.commitWaitHook != nil {
		if herr := m.commitWaitHook(lsn, seq); herr != nil {
			return herr
		}
	}
	return nil
}

// Rollback：丢弃缓冲并释放快照。未写存储，恢复时不会被重放。
// 空批次直接结束（无写入可回滚，审计无可记录内容）；非空批次写一条 StateRollback
// 记录留审计，追加后同步 SyncUpTo 确保审计记录落盘（失败不阻断回滚语义，审计非关键路径）。
func (t *txnImpl) Rollback() error {
	if t.state != TxnActive {
		return ErrTxnClosed
	}
	m := t.mgr
	m.mu.Lock()
	defer m.mu.Unlock()
	// 空批次：与空批次 Commit 对称，跳过 WAL。恢复语义不变——Rollback 记录本就不重放，
	// 崩溃时缺失的审计记录等同从未存在的事务，无任何可恢复状态。
	if len(t.batch.Puts) == 0 && len(t.batch.Deletes) == 0 {
		t.finish(TxnRolledBack)
		return nil
	}
	en := &wal.WalEntry{
		TxnID: t.id,
		Batch: t.batch,
		State: wal.StateRollback,
	}
	if m.wal.Append(en) == nil {
		_ = m.wal.SyncUpTo(en.LSN)
	}
	t.finish(TxnRolledBack)
	return nil
}

func (t *txnImpl) finish(s TxnState) {
	t.state = s
	if t.snap != nil {
		t.snap.Release()
		t.snap = nil
	}
}
