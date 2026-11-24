package cluster

// v2.1.0 H1（NEW-5）选举活性回归：真机停掉旧主后剩余多数派永久选不出主。
//
// 现场：3 节点组停掉原 TSO 主后，客户端持续报
//   "cluster: tso elect node-c rejected: lease quorum not reached (1/2) (epoch=4)"
// 60 秒零写入，重启原主亦不恢复（内存态里的纪元/投票残留使组内互拒）。
//
// 三个叠加缺陷（旧实现）：
//   1. runElect 拉票失败时只清 seenLeader/seenUntil，不清 votedEpoch/votedFor，
//      且不推进本地 epoch —— 本节点把"在该纪元投过票"永久记住；
//   2. vote() 对"同纪元已投给别的候选"返回拒绝并让候选等满一个 TTL，与 1 叠加后
//      两个候选在同一纪元互相永久拒票，grants 恒为 1/2（多数派门槛 2）；
//   3. electOnceLocked / electLeaseLocked 失败后不推进本地纪元，每轮重试复用
//      同一纪元，把上述互斥态永久重演。
//
// 本文件用例在旧实现下会失败（断言处标注），修复后通过；全部使用假网络 +
// 假时钟，不依赖真实网络与真实时钟。

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// denyEdge 切断单向链路（leaseNet.isolate 只能整块分区，这里需要精确方向控制，
// 以复现"切换瞬间某两个节点短暂互不可达"的时序）。
func (n *leaseNet) denyEdge(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.denied[from] == nil {
		n.denied[from] = make(map[string]bool, len(n.ids))
	}
	n.denied[from][to] = true
}

// allowEdge 恢复单向链路。
func (n *leaseNet) allowEdge(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.denied[from] != nil {
		delete(n.denied[from], to)
	}
}

// newLeaseClient 构造挂在假网络上的 HA 客户端（self 为本节点）。
// 三个 hook 把"选主 / 探测 / 拉批"全部指向内存状态机，时序完全可控。
func newLeaseClient(n *leaseNet, self string, ttl time.Duration) *HATSOClient {
	c := &HATSOClient{
		group:     append([]string(nil), n.ids...),
		batch:     16,
		selfID:    self,
		leaseMode: true,
		leaseTTL:  ttl,
	}
	c.electHook = func(id string, reqEpoch, observed uint64) (TsoElectResp, error) {
		if id != self && !n.reachable(self, id) {
			return TsoElectResp{}, errors.New("partition: unreachable")
		}
		st, ok := n.nodes[id]
		if !ok || st == nil {
			return TsoElectResp{}, errors.New("partition: unknown node")
		}
		return st.runElect(reqEpoch, observed), nil
	}
	c.probeHook = func(id string) bool { return n.reachable(self, id) }
	c.fetchHook = func(id string, want uint64) (uint64, uint64, error) {
		st, ok := n.nodes[id]
		if !ok || st == nil {
			return 0, 0, errors.New("partition: unknown node")
		}
		start, cnt, served, epoch := st.serve(want)
		if !served {
			return 0, 0, fmt.Errorf("cluster: tso error: not leader (epoch=%d)", epoch)
		}
		return start, cnt, nil
	}
	return c
}

// TestTSOLeaseElectNoLivelockAfterSameEpochCollision 真机死锁回归。
//
// 现场回放：a（旧主）不可达；b、c 都曾在同一纪元 5 把票投给自己但未当选
// （旧实现失败分支不回滚自投，现场内存态即如此）；客户端本地纪元停在 4。
// 旧实现：candidate 依次自投于纪元 5，互相被判"same epoch already voted"，
// grants 恒为 1/2，回执 epoch 恒为 4 → 客户端下一轮仍请求 5 → 永久失败。
// 修复后：失败残留被回滚 + 纪元推进，b 在纪元 6 拿到多数派并立即恢复发号。
func TestTSOLeaseElectNoLivelockAfterSameEpochCollision(t *testing.T) {
	const ttl = 10 * time.Second
	n := newLeaseNet(ttl, "a", "b", "c")
	// 旧主 a 掉线：两侧互不可达。
	n.isolate([]string{"a"}, []string{"b", "c"})
	// 复现失败竞选的残留：b、c 都在纪元 5 自投过（st.epoch 已到 4）。
	for _, id := range []string{"b", "c"} {
		st := n.st(id)
		st.mu.Lock()
		st.epoch, st.votedEpoch, st.votedFor = 4, 5, id
		st.leader, st.leaseUntil = false, time.Time{}
		st.mu.Unlock()
	}

	c := newLeaseClient(n, "b", ttl)
	c.mu.Lock()
	c.epoch = 4
	c.mu.Unlock()

	c.mu.Lock()
	err := c.electLeaseLocked(16)
	leader, gotEpoch := c.leader, c.epoch
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("1 个节点不可达时应能在 TTL 内选出新主，得到错误: %v（旧实现在此永久 1/2 死锁）", err)
	}
	if leader != "b" && leader != "c" {
		t.Fatalf("新主必须是存活节点，得到 %q", leader)
	}
	if gotEpoch <= 5 {
		t.Fatalf("纪元必须越过残留投票的纪元 5，得到 %d", gotEpoch)
	}

	// 恢复发号：新主立即能发号，且号段越过旧水位（单调不减）。
	newLeader := n.st(leader)
	start, count := issue(t, newLeader, 8)
	if count != 8 {
		t.Fatalf("新主发号数量 = %d, want 8", count)
	}
	if start == 0 {
		t.Fatal("新主发号起点不得为 0")
	}

	// 无双主：另一个存活节点此时既不能自封主，也不能发号。
	other := "b"
	if leader == "b" {
		other = "c"
	}
	if r := n.st(other).runElect(0, 0); r.Granted {
		t.Fatalf("%s 不得在 %s 持有有效租约期间当选（双主）", other, leader)
	}
	if _, _, served, _ := n.st(other).serve(8); served {
		t.Fatalf("%s 不得在新主租约窗口内发号（双主）", other)
	}
}

// TestTSOLeaseFailedElectionHealsNextCandidate 失败竞选必须回滚自投并推进纪元。
//
// 旧实现：b 拉票失败（组内仅 1 票）后仍把"纪元 1 投给自己"留在本地，
// 于是 c 在同一纪元请求时被 b 拒票，c 与 b 互相拒票，多数派门槛 2 永远差一票。
// 修复后：失败分支回滚 votedEpoch/votedFor 并把 st.epoch 推进到该轮纪元，
// c 的下一轮竞选自然换用更高纪元，一次即当选。
func TestTSOLeaseFailedElectionHealsNextCandidate(t *testing.T) {
	const ttl = 10 * time.Second
	n := newLeaseNet(ttl, "a", "b", "c")
	n.isolate([]string{"a"}, []string{"b", "c"})
	// 切换瞬间的抖动：b 到 c 短暂不可达 → b 本轮只能拿到自己 1 票。
	n.denyEdge("b", "c")

	b, c := n.st("b"), n.st("c")
	bad := b.runElect(1, 0)
	if bad.Granted {
		t.Fatal("b 只能拿到自己 1 票（1/2），不应当选")
	}
	// 断言 1（旧实现失败点）：失败后本地纪元必须推进到本轮纪元，
	// 否则下一轮仍复刻同一纪元。
	if bad.Epoch != 1 {
		t.Fatalf("拉票失败后本地纪元应推进到本轮纪元 1，得到 %d（旧实现保持 0）", bad.Epoch)
	}
	b.mu.Lock()
	residualEpoch, residualFor, bEpoch := b.votedEpoch, b.votedFor, b.epoch
	b.mu.Unlock()
	// 断言 2（旧实现失败点）：失败后不得残留"纪元 1 投给自己"。
	if residualEpoch == 1 && residualFor == "b" {
		t.Fatalf("失败竞选的残留投票未回滚：votedEpoch=%d votedFor=%q（旧实现即此状态，导致永久互拒）",
			residualEpoch, residualFor)
	}
	if bEpoch != 1 {
		t.Fatalf("b.epoch = %d, want 1", bEpoch)
	}

	// 链路恢复（a 仍不可达）：同一客户端纪元重试，必须换用更高纪元并当选。
	n.allowEdge("b", "c")
	retry := b.runElect(1, 0)
	if !retry.Granted {
		t.Fatalf("同一纪元重试未推进纪元导致仍只有 1/2: %v（旧实现永久失败）", retry.Err)
	}
	if retry.Epoch <= 1 {
		t.Fatalf("重试的当选纪元应越过失败纪元 1，得到 %d（旧实现复刻纪元 1）", retry.Epoch)
	}
	issue(t, b, 4)

	// 无双主：b 持有有效租约期间 c 不得当选。
	if r := c.runElect(0, 0); r.Granted {
		t.Fatal("b 持有有效租约期间 c 不得当选（双主）")
	}
	if _, _, served, _ := c.serve(4); served {
		t.Fatal("c 不得在 b 的租约窗口内发号（双主）")
	}
}

// TestTSOLeaseRetryAdvancesEpochUntilSuccess 重试必须持续推进纪元直至成功。
//
// 旧实现：客户端每轮重试都复用同一 req（c.epoch 不动），候选每轮都算出同一 ne
// 且把"在该纪元投过票"留在本地，失败无限循环（真机 60 秒 0 写入）。
// 修复后：候选侧失败推进 st.epoch，客户端失败推进 c.epoch，重试在有限轮内收敛。
func TestTSOLeaseRetryAdvancesEpochUntilSuccess(t *testing.T) {
	const ttl = 10 * time.Second
	// 5 节点组、d/e 掉线：多数派仍是 3 票，探测可达（不会因"整体不可达"提前返回），
	// 失败只来自拉票不足 —— 正是需要验证的路径。
	n := newLeaseNet(ttl, "a", "b", "c", "d", "e")
	n.isolate([]string{"d", "e"}, []string{"a", "b", "c"})
	c := newLeaseClient(n, "a", ttl)
	// 心跳可达但选主/投票 RPC 超时（真机切换期常见）：探测仍报三节点存活，
	// 但 a、b、c 之间互相拿不到选票 → 每个候选最多 1/3，本轮必然拉票失败。
	pairs := [][2]string{{"a", "b"}, {"a", "c"}, {"b", "c"}}
	c.probeHook = func(id string) bool { return id != "d" && id != "e" }
	for _, p := range pairs {
		n.denyEdge(p[0], p[1])
		n.denyEdge(p[1], p[0])
	}

	c.mu.Lock()
	if err := c.electLeaseLocked(8); err == nil {
		c.mu.Unlock()
		t.Fatal("抖动期间最多 2/3 票，不应选主成功")
	}
	stuck := c.epoch
	c.mu.Unlock()
	// 断言（旧实现失败点）：重试失败后本地纪元必须前移，否则下一轮重演同一纪元。
	if stuck == 0 {
		t.Fatalf("拉票失败后本地纪元应前移，得到 %d（旧实现恒为 0，重试永远无效）", stuck)
	}

	// 抖动恢复：重试应当选并恢复发号。
	for _, p := range pairs {
		n.allowEdge(p[0], p[1])
		n.allowEdge(p[1], p[0])
	}
	c.mu.Lock()
	err := c.electLeaseLocked(8)
	leader, gotEpoch := c.leader, c.epoch
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("抖动恢复后重试应当选，得到错误: %v（旧实现复用陈旧纪元，永久失败）", err)
	}
	if gotEpoch <= stuck {
		t.Fatalf("当选纪元 %d 必须高于失败时的纪元 %d", gotEpoch, stuck)
	}
	switch leader {
	case "a", "b", "c":
	default:
		t.Fatalf("新主必须是存活节点，得到 %q", leader)
	}
	start, count := issue(t, n.st(leader), 8)
	if count != 8 || start == 0 {
		t.Fatalf("恢复发号失败：start=%d count=%d", start, count)
	}
}

// TestTSOLeaseTakeoverAfterLeaderLeaseExpiry 旧主失联、租约到期后由存活节点接任，
// 首批发号严格越过旧主已发号水位（号段单调不减、不重号）。
func TestTSOLeaseTakeoverAfterLeaderLeaseExpiry(t *testing.T) {
	const ttl = 10 * time.Second
	n := newLeaseNet(ttl, "a", "b", "c")
	a, b := n.st("a"), n.st("b")

	if r := a.runElect(1, 0); !r.Granted {
		t.Fatalf("a 应能当选: %v", r.Err)
	}
	oldStart, oldCount := issue(t, a, 8)
	maxSeen := oldStart + oldCount - 1

	// 旧主 a 失联；时间推进越过租约 TTL（假时钟，精确验证过期语义）。
	n.isolate([]string{"a"}, []string{"b", "c"})
	n.clk.Advance(ttl + time.Second)
	if _, _, served, _ := a.serve(8); served {
		t.Fatal("租约过期后旧主必须拒绝发号（防双主）")
	}

	r := b.runElect(1, maxSeen)
	if !r.Granted {
		t.Fatalf("租约到期后 b 应在 TTL 内接任，得到: %v", r.Err)
	}
	start, count := issue(t, b, 8)
	if start <= maxSeen {
		t.Fatalf("新主首批发号起点 %d 必须 > 旧主最大号 %d（不重号）", start, maxSeen)
	}
	if count != 8 {
		t.Fatalf("count = %d, want 8", count)
	}
}
