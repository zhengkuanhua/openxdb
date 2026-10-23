// P2-13：Serializable 提交冲突检测（协调者内）。
//
// 语义：Serializable 显式事务在 BEGIN 时以注册序号（beginSeq）固定读视图
// （单机快照 / 分布式 curBeginTS 首读取号，均不晚于首读），COMMIT 时做
// rw-antidependency 检测：
//   - 本事务读集 ∩ 活跃事务写集 → 冲突（并发未提交写，宁可误杀不放过）；
//   - 本事务读集 ∩ 已提交写集（提交序号 > 本事务 beginSeq）→ 冲突
//     （本事务读到旧值，后提交者覆盖，串行序被打破）。
//
// 冲突返回 storage.ErrConflict 语义。写写冲突已由行锁 / 2PC 行锁前置挡住，
// 此处仅对活跃事务写集做双保险。仅显式事务（BEGIN..COMMIT）注册；
// autocommit 单语句在引擎锁内执行并提交，无并发窗口。
package sql

import (
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// serialRegistry Serializable 事务注册表与提交序号源。
type serialRegistry struct {
	mu        sync.Mutex
	seq       uint64 // 已提交计数（含回滚后未消费），提交写集按此定序
	active    map[uint64]*serialTx
	committed []*serialCommit
}

// serialTx 一个已注册的 Serializable 显式事务。
type serialTx struct {
	id       uint64
	beginSeq uint64 // 注册时的 seq：所有更晚提交都不得写本事务读集
	readSet  map[string]bool
	writeSet map[string]bool
}

// serialCommit 一个已提交 Serializable 事务的写集（窗口内保留）。
type serialCommit struct {
	commitSeq uint64
	writeSet  map[string]bool
}

// serialCommitWindow 保留的已提交写集窗口大小：窗口外更早提交
// 序号 ≤ 所有活跃事务 beginSeq，其写集无需再参与 antidependency 检测
// （活跃事务快照均晚于这些提交）。
const serialCommitWindow = 128

func newSerialRegistry() *serialRegistry {
	return &serialRegistry{active: map[uint64]*serialTx{}}
}

// register 注册 Serializable 显式事务（beginTx 调用，锁内快照已固定）。
func (r *serialRegistry) register(id uint64, readSet, writeSet map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active[id] = &serialTx{id: id, beginSeq: r.seq, readSet: readSet, writeSet: writeSet}
}

// unregister 注销（回滚 / 会话复位）。
func (r *serialRegistry) unregister(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.active, id)
}

// checkAndCommit 提交前冲突检测；通过则登记已提交写集并推进序号。
// extraWrite 为行锁未覆盖的附加写 key（如 2PC 远端写 ops 的 key），
// 一并纳入已提交写集，避免后续事务读到旧值后误通过。
// extraRead 为 txn 层读集未覆盖的附加读 key（如 2PC 远端读返回的行 key，
// 不走本地 txn.Get/Scan），一并纳入本事务读集参与 antidependency 检测。
func (r *serialRegistry) checkAndCommit(id uint64, readSet, writeSet map[string]bool, extraWrite, extraRead map[string]bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.active[id]
	if !ok {
		// 未注册（非 Serializable 路径误入）直接放行。
		return nil
	}
	defer delete(r.active, id)
	// 0) 合并附加读集（分布式远端读的行 key）
	for k := range extraRead {
		readSet[k] = true
	}
	// 1) rw-antidependency：本事务读集 vs 活跃写集 / 窗口内已提交写集
	for k := range readSet {
		for _, x := range r.active {
			if x.id != id && x.writeSet[k] {
				return r.conflict()
			}
		}
		for _, c := range r.committed {
			if c.commitSeq > st.beginSeq && c.writeSet[k] {
				return r.conflict()
			}
		}
	}
	// 2) 写写双保险：活跃事务写集交集（行锁漏网兜底）
	for k := range writeSet {
		for _, x := range r.active {
			if x.id != id && x.writeSet[k] {
				return r.conflict()
			}
		}
	}
	// 3) 登记提交
	r.seq++
	ws := make(map[string]bool, len(writeSet)+len(extraWrite))
	for k := range writeSet {
		ws[k] = true
	}
	for k := range extraWrite {
		ws[k] = true
	}
	r.committed = append(r.committed, &serialCommit{commitSeq: r.seq, writeSet: ws})
	if len(r.committed) > serialCommitWindow {
		r.committed = r.committed[len(r.committed)-serialCommitWindow:]
	}
	return nil
}

func (r *serialRegistry) conflict() error {
	return &SQLError{Msg: "serialization conflict: " + storage.ErrConflict.Error()}
}
