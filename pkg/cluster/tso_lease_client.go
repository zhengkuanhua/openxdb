package cluster

// v2.1.0 H1（NEW-5）：TSO 租约模式下的客户端策略。
//
// 租约模式下"谁能发号"由服务端多数派授予的租约决定，客户端不能自封主：
// 当组内回执"他主租约未到期（retry_after_ms）"时，客户端应按该时长等待
// 租约到期后再重新拉票，而不是在同一个租约窗口内反复抢主。
//
// 本文件只影响"抢主失败后的等待与重试"，不改动号段消费路径：
// 低水位预取、maxSeen 单调护栏、acceptLocked 回退拒绝全部保持原样。

import (
	"fmt"
	"time"
)

// leaseRetryAfter 从选主回执中取出建议等待时长（无则 0）。
// 只在服务端处于租约模式且拒绝理由是"他主持有租约"时非零。
func leaseRetryAfter(resp TsoElectResp) time.Duration {
	if resp.RetryAfterMs <= 0 {
		return 0
	}
	return time.Duration(resp.RetryAfterMs) * time.Millisecond
}

// SetLeaseMode 打开客户端的租约等待策略（与服务端 EnableTSOLease 配套）。
// ttl <= 0 时取 DefaultTSOLeaseTTL。默认关闭，未调用本方法时行为不变。
func (c *HATSOClient) SetLeaseMode(ttl time.Duration) {
	if c == nil {
		return
	}
	if ttl <= 0 {
		ttl = DefaultTSOLeaseTTL
	}
	c.mu.Lock()
	c.leaseMode = true
	c.leaseTTL = ttl
	c.mu.Unlock()
}

// LeaseMode 返回客户端是否处于租约模式（测试/运维观测用）。
func (c *HATSOClient) LeaseMode() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaseMode
}

// LeaseWaitCount 返回因等待他主租约到期而重试的次数（观测用）。
func (c *HATSOClient) LeaseWaitCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaseWaits
}

// leaseWaitBudgetLocked 计算一次取号允许等待租约过期的总预算：
// 一个 TTL（等旧主租约自然过期）+ 半个 TTL（新主拉票与拉批余量），
// 上限 tsoLeaseWaitMax，避免写路径无限期挂住。
func (c *HATSOClient) leaseWaitBudgetLocked() time.Duration {
	ttl := c.leaseTTL
	if ttl <= 0 {
		ttl = DefaultTSOLeaseTTL
	}
	budget := ttl + ttl/2
	if budget > tsoLeaseWaitMax {
		budget = tsoLeaseWaitMax
	}
	return budget
}

// electLeaseLocked 租约模式选主：在预算内"拉票 → 按回执等待 → 再拉票"。
//
// 与 electOnceLocked 的差别只有等待策略：
//   - 回执带 retry_after_ms（他主租约未到期）→ 睡到该时刻再拉票；
//   - 回执不带等待时长（对端不可达 / 无多数派 / 更高纪元）→ 立即返回错误，
//     交给上层按既有失败语义处理，不在写路径上空转。
//
// 调用方需持有 c.mu（本方法内会忙等，但等待期间写路径本就不可能取得号，
// 且租约窗口内没有其它节点能发号，因此不引入额外的并发风险）。
func (c *HATSOClient) electLeaseLocked(want uint64) error {
	budget := c.leaseWaitBudgetLocked()
	deadline := time.Now().Add(budget)
	var lastErr error
	for {
		wait, err := c.electOnceLocked(want)
		if err == nil {
			return nil
		}
		lastErr = err
		if wait <= 0 {
			return lastErr
		}
		remain := time.Until(deadline)
		if remain <= 0 {
			return fmt.Errorf("%w（等待旧主租约过期超时 %s）", lastErr, budget)
		}
		if wait > remain {
			wait = remain
		}
		if wait > tsoLeaseWaitMax {
			wait = tsoLeaseWaitMax
		}
		c.leaseWaits++
		time.Sleep(wait)
	}
}
