// Package tso 提供全局时间戳（Timestamp Oracle）发号组件（M8 T17）。
//
// 设计要点：
//   - 64 位时间戳布局：高 40 位物理毫秒 + 低 24 位逻辑序号（同毫秒内单调递增）；
//   - 严格单调：时钟回拨 / 同一毫秒内连续发号时，自动续在最后一个已发号之后，
//     不依赖系统时钟前进（last+1 兜底），保证任何观察者都拿到严格递增序列；
//   - 批量发放：GetBatch 一次分配 n 个连续号（低 24 位序号连续），
//     客户端可缓存整批逐次消费，把"每事务一次 RTT"摊薄为"每批一次 RTT"，
//     降低协调开销；单机 / 未配置远程源时直接用本地实例退化发号（零回归）。
//
// Source 抽象：远程 TSO 客户端（见 pkg/cluster）与本地实例都实现 Source，
// 上层（SQL 引擎）只依赖接口，单机退化与中心化部署切换无需改业务代码。
package tso

import (
	"sync"
	"time"
)

// Source 全局时间戳来源：返回一个严格递增的时间戳。
// 实现：*TSO（本地发号，含批量缓存）与 cluster 包中的远程客户端。
type Source interface {
	// Get 返回一个严格递增的全局时间戳（错误仅在远程源不可达时返回）。
	Get() (uint64, error)
	// GetBatch 一次分配 n 个连续时间戳，返回 [start, start+n)。
	// 本地发号器不产生错误；远程客户端在源不可达时返回 (0, 0)。
	GetBatch(n uint64) (uint64, uint64)
	// Reset 丢弃未消费的缓存批次（事务边界调用）：下次 Get 从权威序列
	// 当前位置重新补充/拉取，保证新事务时间戳不滞后于已提交事务。
	Reset()
}

const (
	// logicalBits 逻辑序号位数：低 24 位，物理毫秒高 40 位。
	logicalBits = 24
	// logicalMask 逻辑序号掩码。
	logicalMask = (1 << logicalBits) - 1
	// defaultBatch 单次补充批次大小（Get 单发路径的缓存批）。
	defaultBatch = 64
	// maxInt64 溢出保护：物理毫秒推进一格所需的最小进位步长。
	maxLogical = uint64(1<<logicalBits) - 1
)

// TSO 本地发号器：互斥锁保护单调序列，支持单发与批量发放。
// 线程安全，可被多个执行器共享。
type TSO struct {
	mu        sync.Mutex
	last      uint64 // 最后发放的时间戳（全局单调上界）
	base      uint64 // 当前缓存批次起点（含）
	count     uint64 // 当前缓存批次剩余可发数量
	batchSize uint64 // 单发路径每次补充的批次大小
}

// New 创建本地发号器（batchSize 默认 64）。
func New() *TSO {
	return &TSO{batchSize: defaultBatch}
}

// NewWithBatch 创建本地发号器并指定单发路径的批次大小（测试用）。
func NewWithBatch(batchSize uint64) *TSO {
	if batchSize == 0 {
		batchSize = 1
	}
	return &TSO{batchSize: batchSize}
}

// nowTS 将当前物理毫秒编码为时间戳基数（低 24 位归零）。
func nowTS() uint64 {
	return uint64(time.Now().UnixMilli()) << logicalBits
}

// allocLocked 在锁内分配 n 个连续号 [start, start+n)，推进 last 保证严格单调。
func (t *TSO) allocLocked(n uint64) (uint64, uint64) {
	if n == 0 {
		n = 1
	}
	start := nowTS()
	if start <= t.last { // 同毫秒或时钟回拨：续在最后一个已发号之后
		start = t.last + 1
	}
	end := start + n - 1
	// 跨物理毫秒边界：end 的高 40 位物理毫秒部分自然推进一格（end 落在下一
	// 毫秒、低 24 位仍连续，布局合法且区间完整无洞），无需改写 start。
	// n <= 2^24 时 end 最多跨一格；极端超大批量（n >> 2^24）end 可跨多毫秒，
	// 同样为合法编码（高 40 位毫秒 + 低 24 位逻辑）。
	t.last = end
	return start, n
}

// getLocked 单发：缓存批次耗尽时补充一批，再取批次内下一个号。
func (t *TSO) getLocked() uint64 {
	if t.count == 0 {
		s, _ := t.allocLocked(t.batchSize)
		t.base, t.count = s, t.batchSize
	}
	v := t.base
	t.base++
	t.count--
	return v
}

// Get 返回一个严格递增的时间戳（本地发号，无网络错误）。
func (t *TSO) Get() (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.getLocked(), nil
}

// GetBatch 一次分配 n 个连续时间戳，返回 [start, start+n)。
// n<=0 按 1 处理；调用方可缓存整批逐次消费。分配的批同时对齐单发路径的
// 补充批大小（batchSize=n）：后续 Get 从该批之后补充等量新批，保证批量
// 与单发序列严格衔接。
func (t *TSO) GetBatch(n uint64) (uint64, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// 丢弃未消费的缓存批次：新批与 last 严格衔接，批次边界不产生洞。
	t.count = 0
	if n == 0 {
		n = 1
	}
	t.batchSize = n
	return t.allocLocked(n)
}

// Reset 丢弃未消费的缓存批次（事务边界调用）：下次 Get 从权威序列当前
// 位置重新补充/拉取，避免批量缓存使新事务时间戳滞后于已提交事务。
func (t *TSO) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.count = 0
}

// Batch 返回缓存批次剩余数量（测试观察用）。
func (t *TSO) Batch() (base, count uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.base, t.count
}
