package sql

// B1 启动期存储就绪栅栏（D4.5 Phase8 / NEW-2 v2 修复）
//
// 缺陷现象（Phase7 复测 NEW-2 v2）：persisted.json 含 ha_quorum_enabled=on
// 时，节点重启必然进程级崩溃（0xc0000005，无 Go panic 栈、无 crash.log）。
// 原生栈：b1EnsureAsync.func1 → EnsureRegionReplicas → holdsRegion →
// regionHoldsLocalTarget → regionHoldsLocal → tx.Scan → rocks.Snapshot →
// C.xdb_snapshot_create(0x0)。
//
// 根因：持久化配置在 db.Open 内应用（LoadPersistedConfig）即触发
// b1EnsureAsync 异步补齐；而进程启动存在"试开-即关-重开"路径
// （cmd/openxdb 读取 TLS 持久化意图：db.Open → Close → db.Open），异步
// goroutine 在 db.Close 释放 C 层句柄（rocks.h = nil）之后仍继续副本扫描
// 与快照创建，C 层收到空句柄 → 原生访问违例。
//
// 修复（候选 A：启动期栅栏 + 就绪事件驱动）：
//  1. db.Open 装配 SQL 引擎后立即 MarkStorageStarting（栅栏关闭）；
//  2. db.Open 完成持久化配置加载、返回前 MarkStorageReady（栅栏打开，并把
//     启动期挂起的补齐请求以事件驱动方式补发一次，保证开关真实生效）；
//  3. db.Close 第一时间 MarkStorageClosed（栅栏关闭且不复位，禁止任何后续
//     快照与副本扫描）。
//
// 零回归：直接用 sql.New 构造的引擎（单测/嵌入式）栅栏默认放行，行为与
// v1.17.0 一致，只有经 db.Open 装配的实例才走启动期门控。

import "sync"

// b1StorageGate 副本补齐（B1）存储就绪栅栏状态。
type b1StorageGate struct {
	mu sync.Mutex
	// ready 存储句柄已就绪（db.Open 完成装配后置位）。
	ready bool
	// closed 存储已关闭（db.Close 置位，永久生效、不复位）。
	closed bool
	// deferred 就绪前被挂起的补齐请求数（观测口径）。
	deferred uint64
	// fired 就绪事件补发的补齐次数（观测口径）。
	fired uint64
}

// newB1StorageGate 创建栅栏：默认放行（面向直接 sql.New 的调用方零回归）。
func newB1StorageGate() *b1StorageGate {
	return &b1StorageGate{ready: true}
}

// MarkStorageStarting 标记进入启动期：存储句柄尚未就绪，期间禁止任何副本
// 补齐触发的快照与扫描（请求挂起，待 MarkStorageReady 事件补发）。
func (e *Executor) MarkStorageStarting() {
	g := e.b1Gate
	if g == nil {
		return
	}
	g.mu.Lock()
	g.ready = false
	g.closed = false
	g.deferred = 0
	g.fired = 0
	g.mu.Unlock()
}

// MarkStorageReady 标记存储句柄就绪（db.Open 完成持久化配置加载后调用）：
// 打开栅栏；若启动期有被挂起的补齐请求，则在此以事件驱动方式补发一次，
// 确保持久化 ha_quorum_enabled=on / ha_replica_enabled=on 的补齐动作不会
// 因栅栏而永久丢失（开关真实生效）。
func (e *Executor) MarkStorageReady() {
	g := e.b1Gate
	if g == nil {
		return
	}
	g.mu.Lock()
	g.ready = true
	fire := g.deferred > 0 && !g.closed
	if fire {
		g.fired++
		g.deferred = 0
	}
	g.mu.Unlock()
	if fire {
		e.b1EnsureGo()
	}
}

// MarkStorageClosed 标记存储已关闭（db.Close 第一时间调用）：此后禁止任何
// 副本补齐与快照创建，避免句柄释放后仍被异步 goroutine 访问。
func (e *Executor) MarkStorageClosed() {
	g := e.b1Gate
	if g == nil {
		return
	}
	g.mu.Lock()
	g.closed = true
	g.ready = false
	g.deferred = 0
	g.mu.Unlock()
}

// b1EnsureAllowed 判定当前是否允许启动一次副本补齐：
//   - 已关闭 → 拒绝（无副作用）；
//   - 启动期未就绪 → 登记挂起请求并拒绝（待就绪事件补发）；
//   - 就绪 → 放行。
func (e *Executor) b1EnsureAllowed() bool {
	g := e.b1Gate
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	if !g.ready {
		g.deferred++
		return false
	}
	return true
}

// b1StorageClosed 存储是否已关闭（长循环内的逐轮中断判定）。
func (e *Executor) b1StorageClosed() bool {
	g := e.b1Gate
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

// Engine 层转发：db 装配路径以 *Engine 持有执行器，需在引擎入口暴露栅栏
// 生命周期钩子（db.Open / db.Close 调用）。
func (en *Engine) MarkStorageStarting() {
	if en != nil && en.ex != nil {
		en.ex.MarkStorageStarting()
	}
}

// MarkStorageReady 见 Executor.MarkStorageReady。
func (en *Engine) MarkStorageReady() {
	if en != nil && en.ex != nil {
		en.ex.MarkStorageReady()
	}
}

// MarkStorageClosed 见 Executor.MarkStorageClosed。
func (en *Engine) MarkStorageClosed() {
	if en != nil && en.ex != nil {
		en.ex.MarkStorageClosed()
	}
}

// B1StorageGateState 见 Executor.B1StorageGateState。
func (en *Engine) B1StorageGateState() (ready, closed bool, deferred, fired uint64) {
	if en == nil || en.ex == nil {
		return true, false, 0, 0
	}
	return en.ex.B1StorageGateState()
}

// B1StorageGateState 返回栅栏观测状态（就绪 / 已关闭 / 挂起数 / 补发数），
// 供运维观测与回归测试断言。
func (e *Executor) B1StorageGateState() (ready, closed bool, deferred, fired uint64) {
	g := e.b1Gate
	if g == nil {
		return true, false, 0, 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ready, g.closed, g.deferred, g.fired
}
