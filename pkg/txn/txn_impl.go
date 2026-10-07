package txn

import (
	"bytes"
	"sort"
	"sync"

	"github.com/openxdb/openxdb/pkg/storage"
	"github.com/openxdb/openxdb/pkg/wal"
)

type txnManager struct {
	mu     sync.Mutex
	st     storage.Storage
	wal    wal.WAL
	nextID uint64
}

// New 创建事务管理器并执行启动恢复（重放 WAL 已 Commit 记录）。
func New(st storage.Storage, w wal.WAL) (TxnManager, error) {
	m := &txnManager{st: st, wal: w}
	if err := m.Recover(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *txnManager) Begin() (Txn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	snap, err := m.st.Snapshot()
	if err != nil {
		return nil, err
	}
	return &txnImpl{
		id:    m.nextID,
		mgr:   m,
		batch: &storage.WriteBatch{},
		snap:  snap,
		state: TxnActive,
	}, nil
}

// Recover 重放 WAL 中所有 StateCommit 记录到存储。
// 提交顺序为 WAL 先行：崩溃于「WAL 已落盘、存储未应用」时由本方法补齐；重复应用幂等。
func (m *txnManager) Recover() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var applyErr error
	err := m.wal.Replay(func(e *wal.WalEntry) {
		if e.State == wal.StateCommit && applyErr == nil {
			applyErr = m.st.Write(e.Batch)
		}
	})
	if err != nil {
		return err
	}
	return applyErr
}

func (m *txnManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.wal.Close()
}

type txnImpl struct {
	id    uint64
	mgr   *txnManager
	batch *storage.WriteBatch
	ops   []opEntry // 事务内操作序列（按时间序，read-your-writes 判定依据）
	snap  storage.Snapshot
	state TxnState
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

// Scan 合并快照数据与事务内未提交写（read-your-writes），结果按 Key 升序。
func (t *txnImpl) Scan(r storage.KeyRange, limit int) ([]storage.KVPair, error) {
	if t.state != TxnActive {
		return nil, ErrTxnClosed
	}
	base, err := t.snap.Scan(r, 0)
	if err != nil {
		return nil, err
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
	return out, nil
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
	t.batch.Puts = append(t.batch.Puts, storage.KVPair{Key: key, Value: value})
	t.ops = append(t.ops, opEntry{key: key, value: value})
	return nil
}

func (t *txnImpl) Delete(key []byte) error {
	if t.state != TxnActive {
		return ErrTxnClosed
	}
	t.batch.Deletes = append(t.batch.Deletes, key)
	t.ops = append(t.ops, opEntry{isDelete: true, key: key})
	return nil
}

// Commit：WAL 先行落盘（StateCommit）→ 存储原子应用 → 释放快照。
// 提交串行化（全局锁）：单机 MVP 语义为「最后提交者胜」，等价于串行调度。
func (t *txnImpl) Commit() error {
	if t.state != TxnActive {
		return ErrTxnClosed
	}
	m := t.mgr
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(t.batch.Puts) == 0 && len(t.batch.Deletes) == 0 {
		t.finish(TxnCommitted)
		return nil
	}
	if err := m.wal.Append(&wal.WalEntry{
		TxnID: t.id,
		Batch: t.batch,
		State: wal.StateCommit,
	}); err != nil {
		return err
	}
	if err := m.st.Write(t.batch); err != nil {
		return err
	}
	t.finish(TxnCommitted)
	return nil
}

// Rollback：丢弃缓冲并释放快照。未写存储，恢复时不会被重放。
// 可选写一条 StateRollback 记录留审计（失败不阻断回滚语义）。
func (t *txnImpl) Rollback() error {
	if t.state != TxnActive {
		return ErrTxnClosed
	}
	m := t.mgr
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.wal.Append(&wal.WalEntry{
		TxnID: t.id,
		Batch: t.batch,
		State: wal.StateRollback,
	})
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
