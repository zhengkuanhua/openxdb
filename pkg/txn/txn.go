// Package txn 提供单机事务层（M1 T3，开发手册 §3.4）。
// 语义：快照隔离读 + 提交串行化（WAL 先行 + 存储原子写 + 崩溃恢复重放）。
package txn

import (
	"errors"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// TxnState 事务生命周期状态。
type TxnState uint8

const (
	TxnActive     TxnState = 1
	TxnCommitted  TxnState = 2
	TxnRolledBack TxnState = 3
)

// Txn 单事务句柄：写操作先入内存缓冲，Commit 时经 WAL + 存储原子提交。
type Txn interface {
	// ID 返回事务 ID（从 1 开始递增）。
	ID() uint64
	// Get 读取：优先自己未提交的写（read-your-writes），否则走 BEGIN 时刻快照（隔离读）。
	Get(key []byte) ([]byte, error)
	// MGet 批量读取：优先自己未提交的写（read-your-writes），其余走快照批量读。
	// 返回命中键（string(key)）→ 值；未命中键不出现在结果中。与逐键 Get 语义等价。
	MGet(keys [][]byte) (map[string][]byte, error)
	// Scan 范围扫描 [Start, End)：快照数据 + 合并事务内未提交写（read-your-writes）。
	// limit<=0 表示不限制；结果按 Key 升序。
	Scan(r storage.KeyRange, limit int) ([]storage.KVPair, error)
	// Put 写入事务缓冲。
	Put(key, value []byte) error
	// Delete 删除（写入事务缓冲）。
	Delete(key []byte) error
	// Commit 提交：WAL 写 Commit 记录 → 存储原子应用 → 释放快照。
	Commit() error
	// Rollback 回滚：丢弃缓冲、释放快照；已落 WAL 的记录不会被重放。
	Rollback() error
	// SetDeferSync 设置本事务延迟持久化确认标记（v1.17.0 D1.2，事务级）。
	// 置位后 Commit 阶段 2 跳过 WAL SyncUpTo（持久化推迟到调用方批尾
	// FlushDeferredSync 统一覆盖）。仅 autocommit 写 pipeline 批内事务由
	// executor 置位；显式事务与常规 autocommit 路径保持默认（逐事务
	// SyncUpTo，零回归）。Commit 前调用生效。
	SetDeferSync(enabled bool)
}

// TxnManager 事务管理器。
type TxnManager interface {
	// Begin 开启新事务（获取一致性快照）。
	Begin() (Txn, error)
	// Recover 启动恢复：按 LSN 顺序重放 WAL 中已 Commit 记录到存储（幂等，可重复调用）。
	Recover() error
	// SetCommitHook 注册提交后回调（M2 复制挂接点）：每次事务提交成功应用存储后，
	// 在提交串行化锁内按提交顺序调用 hook(lsn, seq, batch)。hook 返回的错误仅
	// 记录在 HookErr，不影响事务提交成功语义（复制推送为异步）。
	SetCommitHook(hook func(lsn storage.LSN, seq uint64, batch *storage.WriteBatch) error)
	// SetCommitWaitHook 注册提交后置确认回调（T33 同步/半同步复制挂接点）：
	// 在事务提交串行化锁外、Commit 返回前调用 hook(lsn, seq)，可安全阻塞等待
	// 副本 ACK。hook 返回的错误直接回传 Commit 调用者（此时事务已本地提交，
	// 属"提交不确定"窗口）。async 模式下可注册但不阻塞（立即返回 nil）。
	SetCommitWaitHook(hook func(lsn storage.LSN, seq uint64) error)
	// HookErr 返回最近一次提交后回调的错误（无错误返回 nil）。
	HookErr() error
	// SetGroupCommitWindow 设置 WAL 组提交定时刷盘窗口（v1.14.0 P1）：
	// 透传至底层 WAL；d<=0 关闭定时窗口（回退 v1.13.0 被动攒批行为）。
	SetGroupCommitWindow(d time.Duration)
	// FlushDeferredSync 统一刷盘延迟持久化的提交（v1.17.0 D1.2）：对
	// deferredLSN 水位执行一次 WAL SyncUpTo。幂等；批尾响应写回前调用。
	FlushDeferredSync() error
	Close() error
}

// ApplyGroupObserver 提交应用组提交（C4）观测/控制可选接口。
// 不属于 TxnManager 基础接口语义，通过类型断言使用（如 m.(txn.ApplyGroupObserver)）。
type ApplyGroupObserver interface {
	// SetApplyGroupWindow 设置提交应用聚合窗口（v1.16.0 C4）：
	// d<=0 关闭聚合（回退逐事务串行 st.Write，保持旧行为）；
	// d>0 时仅当检测到已完成 WAL SyncUpTo 的候补事务时才等待窗口收集合并，
	// 单事务场景候补为 0，不引入额外延迟。
	SetApplyGroupWindow(d time.Duration)
	// ApplyGroupStats 返回累计 st.Write 次数与累计应用事务数（观测合并度，测试辅助）。
	ApplyGroupStats() (writes, txns uint64)
}

// CommitSeqProvider 全局提交序列观测可选接口（v1.16.0 C6）。
// 不属于 TxnManager 基础接口语义，通过类型断言使用（如 m.(txn.CommitSeqProvider)）。
// 供 executor 的 autocommit 只读共享快照窗口检测「窗口期间是否发生了对外可见
// 的提交」：任何产生存储可见写的成功提交（含 2PC 参与方）递增序列，窗口复用
// 前比对序列，不一致即关闭窗口重新取快照，保持 autocommit 每条语句独立快照
// 语义（写后读正确性）。
type CommitSeqProvider interface {
	// CommitSeq 返回自启动以来产生可见写的成功提交次数（单调递增，线程安全）。
	CommitSeq() uint64
}

// AppliedLSNProvider 已应用位点观测可选接口（v1.16.0 WAL 自动 checkpoint）。
// 不属于 TxnManager 基础接口语义，通过类型断言使用（如 m.(txn.AppliedLSNProvider)）。
// 供 DB 层 checkpoint 判定「已安全应用到存储的最大 WAL LSN」：只可截断
// <= AppliedLSN() 且 <= WAL.LastSyncedLSN() 的记录，保证截断后崩溃恢复
// 既不丢已提交数据，也不因截断尚未落盘/未应用的记录而破坏恢复一致性。
type AppliedLSNProvider interface {
	// AppliedLSN 返回已成功应用到存储的最大提交记录 LSN（0 = 尚无）。
	// 单调递增，线程安全；Recover 重放结束后推进到最后重放的提交记录。
	AppliedLSN() storage.LSN
}

// 标准错误。
var (
	ErrTxnClosed = errors.New("txn: transaction already finished")
)
