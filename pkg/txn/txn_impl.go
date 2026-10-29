package txn

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/wal"
)

// pendingApply 提交应用组提交（C4）的待应用事务：事务完成 WAL SyncUpTo 后
// 在阶段 3 注册；leader 合并整批 st.Write 后填写 err/errFilled，follower
// 醒来据此读取结果（nil=合并成功，非 nil=整批应用失败）。
type pendingApply struct {
	seq       uint64
	lsn       storage.LSN
	batch     *storage.WriteBatch
	err       error
	errFilled bool
}

// defaultApplyWindow 提交应用聚合窗口默认值：0 = 不主动等待候补注册，
// 依赖 C1 组提交的天然聚簇（一批 SyncUpTo 完成的事务几乎同时注册，leader
// 直接收集合并）。实测在 2 conns OLTP 下主动等待窗口（20-200µs）会引入
// 固定延迟与时序抖动（探针：window=0 → 1884 TPS，20µs → 1048 TPS），
// 故默认关闭；SetApplyGroupWindow 保留供高并发/长事务场景按需调优。
const defaultApplyWindow = 0

type txnManager struct {
	mu        sync.Mutex
	st        storage.Storage
	wal       wal.WAL
	nextID    uint64
	seqNext   uint64     // 下一个待分配的提交序号（锁内分配；仅 Commit 事务占位）
	applied   uint64     // 已成功应用到存储的最大提交序号
	applyCond *sync.Cond // 关联 mu，等待轮到自己按序应用
	// appliedLSN 已成功应用到存储的最大提交记录 LSN（v1.16.0 WAL 自动
	// checkpoint 用）。与 applied（处理序号）不同：fsync/st.Write 失败的
	// 事务会跳号推进 applied，但 appliedLSN 只记录真正落到存储的提交位点，
	// checkpoint 依据 min(appliedLSN, WAL.LastSyncedLSN) 截断，失败事务的
	// 记录永不截断（崩溃恢复可补齐）。
	appliedLSN storage.LSN

	// C4 提交应用组提交（流水线化）：消除阶段 3 st.Write 全局单写者逐事务
	// 串行瓶颈——leader 把按 seq 连续就绪的多个事务合并为一次原子 st.Write，
	// 合并顺序 = seq 顺序 = WAL 顺序，崩溃恢复重放结果与逐事务串行等价。
	applyPending map[uint64]*pendingApply // seq -> 已就绪待应用事务（锁内读写）
	syncDone     uint64                   // 完成 WAL SyncUpTo 的累计事务数（atomic，锁外写）
	applyWindow  time.Duration            // 应用聚合窗口；<=0 关闭（回退逐事务串行 st.Write）
	applyWrites  uint64                   // 累计 st.Write 次数（观测）
	applyTxns    uint64                   // 累计应用事务数（观测）
	applyBypass  uint64                   // 累计直通（无候补零开销）应用事务数（观测，返工回归用）

	commitHook    func(lsn storage.LSN, seq uint64, batch *storage.WriteBatch) error // M2 复制挂接点（锁内，异步推送）
	hookErr       error                                                              // 最近一次 hook 错误
	commitWaitHook func(lsn storage.LSN, seq uint64) error // T33 复制确认挂接点（锁外，同步/半同步等待 ACK）

	// C6 全局提交序列：产生存储可见写的成功提交（st.Write 应用成功）时递增
	// （锁内递增）。executor 的 autocommit 只读共享快照窗口复用前比对，
	// 窗口期间检测到新提交即关闭窗口重新取快照，保持写后读语义。
	commitSeq uint64

	// v1.17.0 D1.2：autocommit 写 pipeline 延迟持久化水位（默认零回归）。
	// 置位 defer 的事务在 Commit 阶段 2 跳过 WAL SyncUpTo，仅把最大 LSN
	// 归并到本水位；调用方在写批尾统一 FlushDeferredSync 一次性 SyncUpTo
	// 覆盖整批，把 N 次 fsync 合并为 1 次。安全前提：pipeline 窗口内
	// RocksDB 存储必须禁用自身 WAL（disable_wal），使 OpenXDB WAL 成为
	// 唯一持久化通道——崩溃恢复完全由 OpenXDB WAL 重放决定，未 fsync 的
	// 未 ack 事务在存储中不存在，杜绝"RocksDB 自身 WAL 恢复出 OpenXDB
	// WAL 未记录事务"的幽灵数据窗口。
	deferredLSN storage.LSN  // 已 Append 但尚未 SyncUpTo 的最大 LSN（defer 事务归并）
}

// CommitSeq 返回产生可见写的成功提交次数（C6，见 CommitSeqProvider 注释）。
func (m *txnManager) CommitSeq() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitSeq
}

// AppliedLSN 返回已成功应用到存储的最大提交记录 LSN（txn.AppliedLSNProvider）。
func (m *txnManager) AppliedLSN() storage.LSN {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appliedLSN
}

// notifyCommittedLocked 在产生存储可见写的成功提交后递增全局提交序列
// （调用方须持有 m.mu）。纯读事务（空批次提交）与应用失败不递增——只读
// 不改变对外可见状态，失败提交未落存储。
func (m *txnManager) notifyCommittedLocked() {
	m.commitSeq++
}

// New 创建事务管理器并执行启动恢复（重放 WAL 已 Commit 记录）。
func New(st storage.Storage, w wal.WAL) (TxnManager, error) {
	m := &txnManager{st: st, wal: w, applyPending: map[uint64]*pendingApply{}, applyWindow: defaultApplyWindow}
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
	var lastCommitted storage.LSN // 最后一条成功应用到存储的 StateCommit 记录 LSN（checkpoint 位点）
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
			lastCommitted = e.LSN
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
		m.appliedLSN = lastCommitted
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

// SetApplyGroupWindow 实现 txn.ApplyGroupObserver：设置提交应用聚合窗口。
// d<=0 关闭聚合（回退逐事务串行 st.Write，保持旧行为）；d>0 时仅在检测到
// 候补时才等待窗口收集合并。须在提交负载开始前调用（与 SetCommitWaitHook
// 同级配置窗口），运行期修改对已注册批次尽力而为。
func (m *txnManager) SetApplyGroupWindow(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applyWindow = d
}

// ApplyGroupStats 实现 txn.ApplyGroupObserver：返回累计 st.Write 次数与
// 累计应用事务数（观测合并度：txns/writes 即平均合并批次大小）。
func (m *txnManager) ApplyGroupStats() (writes, txns uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applyWrites, m.applyTxns
}

// ApplyBypassCount 返回累计直通应用事务数（无真实候补时零开销直通原
// st.Write 的次数）。C4 返工退化回归用：串行提交场景下该值应等于事务数，
// 证明低并发路径未进入注册/聚合/等待机制。
func (m *txnManager) ApplyBypassCount() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applyBypass
}

// SetGroupCommitWindow 设置 WAL 组提交定时刷盘窗口（v1.14.0 P1），透传底层 WAL。
// d<=0 关闭定时窗口（回退 v1.13.0 被动攒批行为）；调用方需持 m.mu 或串行访问。
func (m *txnManager) SetGroupCommitWindow(d time.Duration) {
	m.wal.SetGroupCommitWindow(d)
}

// SetActiveWindow 设置 WAL 组提交主动定时窗口开关（v1.17.0 D1.3），透传底层 WAL。
// 开启时 leader 无并发等待者也等待组提交窗口（收集稀疏到达 Append 合并 fsync）；
// 关闭回退 v1.16.0 C1 单等待者免窗口。
func (m *txnManager) SetActiveWindow(enabled bool) {
	m.wal.SetActiveWindow(enabled)
}

// ActiveWindow 返回 WAL 组提交主动定时窗口开关当前状态（v1.17.0 D1.3）。
func (m *txnManager) ActiveWindow() bool {
	return m.wal.ActiveWindow()
}

// FlushDeferredSync 统一刷盘延迟持久化的事务（v1.17.0 D1.2）：对
// deferredLSN 水位执行一次 WAL SyncUpTo，覆盖整批未 fsync 的提交记录。
// 幂等：无延迟事务或已覆盖到该 LSN 时立即返回。批尾必须在响应写回前调用，
// 保证客户端 ack 前全部事务已持久化。
func (m *txnManager) FlushDeferredSync() error {
	m.mu.Lock()
	lsn := m.deferredLSN
	m.mu.Unlock()
	if lsn == 0 {
		return nil
	}
	if err := m.wal.SyncUpTo(lsn); err != nil {
		return err
	}
	m.mu.Lock()
	m.deferredLSN = 0
	m.mu.Unlock()
	return nil
}

type txnImpl struct {
	id    uint64
	mgr   *txnManager
	batch *storage.WriteBatch
	ops   []opEntry // 事务内操作序列（按时间序，read-your-writes 判定依据）
	snap  storage.Snapshot
	state TxnState
	// v1.17.0 D1.2：本事务延迟持久化确认标记（事务级，非全局）。置位时
	// Commit 阶段 2 跳过 WAL SyncUpTo（deferredLSN 归并到管理器水位），
	// 由调用方批尾 FlushDeferredSync 统一刷盘。仅 autocommit 写 pipeline
	// 批内事务置位；其他事务保持逐事务 SyncUpTo（其 SyncUpTo 水位推进会
	// 顺带覆盖更早 defer 事务的 WAL 记录，持久化只提前不延后，ack 语义
	// 不受影响）。锁内读写（Commit 阶段 2 读取）。
	deferSync bool
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

// SetDeferSync 设置本事务延迟持久化确认标记（v1.17.0 D1.2，事务级）。
// 见 txn.Txn.SetDeferSync 注释；Commit 阶段 2 读取（锁内）。仅由 executor
// 在 autocommit 写 pipeline 批内置位，常规路径保持零回归。
func (t *txnImpl) SetDeferSync(enabled bool) {
	t.mgr.mu.Lock()
	t.deferSync = enabled
	t.mgr.mu.Unlock()
}

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
	// v1.17.0 D1.2 defer 模式（本事务置位）：跳过 SyncUpTo（记录
	// deferredLSN 水位，不推进 syncDone），WAL 持久化推迟到调用方批尾统一
	// FlushDeferredSync 覆盖——未 ack 事务在崩溃时允许丢失，存储侧由
	// disable_wal 保证无幽灵数据。其他事务照常 SyncUpTo（水位推进会顺带
	// 覆盖更早 defer 事务的 WAL，持久化只提前不延后）。
	if t.deferSync {
		m.mu.Lock()
		if lsn > m.deferredLSN {
			m.deferredLSN = lsn
		}
		m.mu.Unlock()
	} else {
		if err := m.wal.SyncUpTo(lsn); err != nil {
			// fsync 失败：该事务视为未提交成功。跳过自己的序号，避免后续事务永久等待。
			m.mu.Lock()
			m.applied++
			m.applyCond.Broadcast()
			m.mu.Unlock()
			return err
		}
		// 阶段 2.5：无锁记录 SyncUpTo 完成（C4 应用聚合的候补检测依据；仅观测/聚合用）。
		atomic.AddUint64(&m.syncDone, 1)
	}

	// 阶段 3：提交应用（C4 组提交，返工版）。
	// 语义不变：合并 WriteBatch 按 seq 顺序排列 = WAL 顺序，一次原子 st.Write
	// 应用整批，崩溃恢复重放结果与逐事务串行等价（「最后提交者胜」）。
	// 低并发性能返工：无真实候补（syncDone - applied - 1 <= 0，即没有事务
	// 完成 WAL SyncUpTo 却尚未应用）时，当前事务必为 leader 位（seq ==
	// applied+1）且无等待中的 follower，直通原 C3 逐事务 st.Write 路径——
	// 零注册、零等待、零广播，仅保留 applied 推进与观测计数，低并发零额外
	// 开销。两个条件缺一不可：seq 必须恰好是 leader 位，否则靠前 seq 空缺
	// 未应用时直通会破坏 WAL 应用顺序。
	m.mu.Lock()
	if seq == m.applied+1 && atomic.LoadUint64(&m.syncDone) <= m.applied+1 {
		applyErr := m.st.Write(t.batch)
		if applyErr != nil {
			m.mu.Unlock()
			return applyErr
		}
		m.notifyCommittedLocked()
		m.applied++
		m.appliedLSN = lsn // checkpoint 位点：仅成功应用到存储后推进
		m.applyCond.Broadcast() // C4 修复：bypass 应用推进 applied 后必须唤醒 register 的 follower，否则并发下 follower 永久等待
		m.applyWrites++
		m.applyTxns++
		m.applyBypass++
		if m.commitHook != nil {
			if herr := m.commitHook(lsn, seq, t.batch); herr != nil {
				m.hookErr = herr
			}
		}
		t.finish(TxnCommitted)
		m.mu.Unlock()
		// T33 同步/半同步复制（M20）：锁外、Commit 返回前执行复制 ACK 等待。
		if m.commitWaitHook != nil {
			if herr := m.commitWaitHook(lsn, seq); herr != nil {
				return herr
			}
		}
		return nil
	}

	// 聚合路径：检测到真实候补（syncDone > applied+1）才启用注册+组提交。
	pa := &pendingApply{seq: seq, lsn: lsn, batch: t.batch}
	m.applyPending[seq] = pa
	// 等待自己成为 leader（m.applied+1 == seq，轮到自己应用）或已被合并
	// 跳过（m.applied >= seq）。两种情况下 m.applied < seq 为假或
	// m.applied+1 == seq 为真，循环退出。
	for m.applied < seq && m.applied+1 != seq {
		m.applyCond.Wait()
	}
	if pa.errFilled {
		// follower：批次已被 leader 合并应用（或整批失败跳过），读取结果。
		err := pa.err
		m.mu.Unlock()
		if err != nil {
			return err
		}
		t.finish(TxnCommitted)
		if m.commitWaitHook != nil {
			if herr := m.commitWaitHook(lsn, seq); herr != nil {
				return herr
			}
		}
		return nil
	}
	// 自己是 leader（m.applied+1 == seq）：pending 仅自己且候补 >=2
	// （syncDone - (applied+1) > 1，即至少两个已完成 SyncUpTo 的事务排队）
	// 时才等待聚合窗口收集注册者扩大合并批次；候补 ==1 免窗口直接应用
	// （C1「单等待者免窗口」精神，杜绝 follower 等待引入的延迟）。
	if m.applyWindow > 0 && len(m.applyPending) == 1 &&
		atomic.LoadUint64(&m.syncDone) > m.applied+2 {
		m.mu.Unlock()
		time.Sleep(m.applyWindow)
		m.mu.Lock()
		// 窗口期间仅 follower 会注册/等待，不会推进 applied；防御性确认未被合并。
		if pa.errFilled {
			err := pa.err
			m.mu.Unlock()
			if err != nil {
				return err
			}
			t.finish(TxnCommitted)
			if m.commitWaitHook != nil {
				if herr := m.commitWaitHook(lsn, seq); herr != nil {
					return herr
				}
			}
			return nil
		}
	}
	// 快速路径：候补已检测但尚未注册（批大小仍为 1）时直接应用原始
	// batch——单事务 batch 内 Puts/Deletes 顺序由 WAL 记录固定，等价性不变。
	if _, hasNext := m.applyPending[seq+1]; !hasNext {
		applyErr := m.st.Write(t.batch)
		pa.err = applyErr
		pa.errFilled = true
		delete(m.applyPending, seq) // 本批不合并，立即摘除本条目防 map 泄漏
		m.notifyCommittedLocked()
		m.applied++
		if applyErr == nil {
			m.appliedLSN = lsn // checkpoint 位点：仅成功应用到存储后推进
		}
		m.applyWrites++
		m.applyTxns++
		m.applyCond.Broadcast()
		if applyErr != nil {
			m.mu.Unlock()
			return applyErr
		}
		if m.commitHook != nil {
			if herr := m.commitHook(lsn, seq, t.batch); herr != nil {
				m.hookErr = herr
			}
		}
		t.finish(TxnCommitted)
		m.mu.Unlock()
		if m.commitWaitHook != nil {
			if herr := m.commitWaitHook(lsn, seq); herr != nil {
				return herr
			}
		}
		return nil
	}
	// 聚合路径：收集从 seq 起连续已注册的待应用事务，合并为一次原子写。
	// 等价性约束：storage.WriteBatch 的 Puts/Deletes 分组应用模型无法保持
	// 跨记录的「先 Put 后 Delete / 先 Delete 后 Put」顺序；检测到同 key
	// 跨记录类型冲突时在本条记录处截断批次，冲突记录留给下一 leader 单独
	// 应用（C1 组提交聚簇下无冲突批仍整体合并，冲突场景退化但语义严格等价）。
	merged := []*pendingApply{}
	mb := &storage.WriteBatch{}
	putKeys := map[string]struct{}{}
	delKeys := map[string]struct{}{}
	first := true
	s := seq
	for {
		p, ok := m.applyPending[s]
		if !ok {
			break
		}
		if !first && (len(delKeys) > 0 || len(p.batch.Deletes) > 0) {
			// 仅当批内含 Delete（可能被后续 Put 反转）或新记录含 Delete
			// （可能反转已收集 Put）时才做冲突检测；纯 Put 批同 key 多次
			// 写入顺序安全（后写覆盖），无需检测——省去低并发下的固定开销。
			bad := false
			for _, kv := range p.batch.Puts {
				if _, inDel := delKeys[string(kv.Key)]; inDel {
					bad = true
					break
				}
			}
			if !bad {
				for _, k := range p.batch.Deletes {
					if _, inPut := putKeys[string(k)]; inPut {
						bad = true
						break
					}
				}
			}
			if bad {
				break // 冲突：截断本批，冲突点留给下一 leader
			}
		}
		merged = append(merged, p)
		mb.Puts = append(mb.Puts, p.batch.Puts...)
		mb.Deletes = append(mb.Deletes, p.batch.Deletes...)
		for _, kv := range p.batch.Puts {
			putKeys[string(kv.Key)] = struct{}{}
		}
		for _, k := range p.batch.Deletes {
			delKeys[string(k)] = struct{}{}
		}
		delete(m.applyPending, s)
		first = false
		s++
	}
	applyErr := m.st.Write(mb)
	n := uint64(len(merged))
	for _, p := range merged {
		p.err = applyErr
		p.errFilled = true
	}
	m.notifyCommittedLocked()
	m.applied += n
	if applyErr == nil && n > 0 {
		m.appliedLSN = merged[n-1].lsn // checkpoint 位点：整批成功应用后推进到最后一条记录
	}
	m.applyWrites++
	m.applyTxns += n
	m.applyCond.Broadcast()
	if applyErr != nil {
		// 应用失败：整批跳过该序号段让后续事务继续（记录已落 WAL，崩溃恢复会补齐）。
		m.mu.Unlock()
		return applyErr
	}
	if m.commitHook != nil {
		for _, p := range merged {
			if herr := m.commitHook(p.lsn, p.seq, p.batch); herr != nil {
				m.hookErr = herr
				break
			}
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

