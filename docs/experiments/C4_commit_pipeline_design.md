---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_03943be9d1dc11f18460525400393706
    ReservedCode1: LX/QN1HzL1lCkw7cKIpjT4MGKKcJl8C4J7jTPOgp5O9qBrBGCg2Y8QE14I408GqX/LEjykVm4yiz9MdyQPSOCW/lFll1Excu/STS5wVT3036hkAh/ndziYxkf30X2kFpyoQoAOYUaEX6IhmligyIyzmTkAhtssSA1QvcEHo9f3r4r4xIxcuX/2SCnnw=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_03943be9d1dc11f18460525400393706
    ReservedCode2: LX/QN1HzL1lCkw7cKIpjT4MGKKcJl8C4J7jTPOgp5O9qBrBGCg2Y8QE14I408GqX/LEjykVm4yiz9MdyQPSOCW/lFll1Excu/STS5wVT3036hkAh/ndziYxkf30X2kFpyoQoAOYUaEX6IhmligyIyzmTkAhtssSA1QvcEHo9f3r4r4xIxcuX/2SCnnw=
---

# C4 提交应用流水线化/组提交 —— 设计方案（Design）

> 版本：v1.16.0 C4
> 日期：2026-10-27
> 目标：在 C1 WAL 组提交之上，消除 COMMIT 阶段 `st.Write` 全局单写者逐事务串行瓶颈，保持崩溃恢复等价。
> 基线：C3 411.25 TPS（30s/2conns，sql_bench -oltp -ro-parallel on）。

## 1. 核心思路

现状阶段 3：每个提交事务在 `m.mu` 临界区内 `st.Write(t.batch)` 一次，RocksDB 全局单写者逐事务串行，N 次提交 = N 次 st.Write（每次都有 batch 编码 / cgo / RocksDB writer 串行固定开销）。

C4 方案：**阶段 3 改为「注册 → 组提交应用」**——并发提交事务在阶段 2 组提交 fsync 返回后几乎同时到达阶段 3，由一个应用 leader 把**按 seq 连续就绪**的多个事务合并为一个 `WriteBatch`，一次 `st.Write` 应用整批，然后统一推进 `applied`。

### 1.1 关键机制（三合一）

1. **自然聚合（零等待）**：由于 C1 SyncUpTo 的组提交使同批事务同时返回、同时竞争 `m.mu`，先抢到锁的后续事务会先注册 `applyPending[seq]` 并等待；最后成为 leader（`applied+1 == seq`）的事务扫描 `applyPending` 即可合并**已注册**的连续就绪事务——不需要任何额外等待。
2. **候选检测 + 聚合窗口（仅高并发时启用）**：leader 检测到「已有事务完成 SyncUpTo 但尚未注册」（`syncDone - applied - 1 > 0`）时，等待一个短聚合窗口（默认 200µs，可配置）让它们注册，扩大合并批次。窗口仅在检测到候补时等待；**单事务场景候补数为 0，零额外延迟**（与 C1「单等待者免窗口」精神一致）。
3. **按 seq 有序合并**：合并 WriteBatch 的 Puts/Deletes 按 seq 递增顺序 append（= WAL 顺序），一次原子 `st.Write`。崩溃恢复等价由「WAL 顺序不变 + 合并顺序 = WAL 顺序 + 重放逐条幂等」三重保证。

### 1.2 数据结构（txnManager 新增，均为私有）

```go
type pendingApply struct {
    seq       uint64             // 提交序号
    lsn       storage.LSN        // WAL LSN（hook 用）
    batch     *storage.WriteBatch
    err       error              // leader 合并应用后填写
    errFilled bool               // err 是否已填写（区分 leader/follower）
}

// txnManager 新增字段
applyPending map[uint64]*pendingApply // seq -> 已就绪待应用事务（锁内读写）
syncDone     uint64                   // 完成 WAL SyncUpTo 的累计事务数（atomic，锁外写）
applyWindow  time.Duration            // 应用聚合窗口；<=0 关闭（回退逐事务串行 st.Write）
applyWrites  uint64                   // 累计 st.Write 次数（观测）
applyTxns    uint64                   // 累计应用事务数（观测）
```

### 1.3 Commit 阶段 3 新流程

```
阶段 2 成功 → atomic.AddUint64(&m.syncDone, 1)   // 无锁记录 SyncUpTo 完成
阶段 3:
  lock
  pa := &pendingApply{seq, lsn, t.batch}; m.applyPending[seq] = pa
  for m.applied < seq { m.applyCond.Wait() }       // 等到自己被应用或跳过
  if pa.errFilled {                                 // follower：已被合并
      unlock; return pa.err（nil 则 finish + waitHook + return nil）
  }
  // 自己是 leader（applied+1 == seq）
  if 窗口开启 && applyPending 无其他条目 && syncDone > applied+1 {
      unlock; sleep(applyWindow); lock              // 聚合窗口收集候补
      // 防御：窗口后若 applied+1 != seq（理论不可能），重入等待循环
  }
  merged := 从 seq 起连续扫描 applyPending 收集
  err := m.st.Write(mergedBatch)                     // 一次原子写整批
  for p in merged: p.err = err; p.errFilled = true   // follower 结果可见
  m.applied += n; m.applyWrites++; m.applyTxns += n; m.applyCond.Broadcast()
  if err != nil { unlock; return err }               // 整批失败：全部跳过，恢复补齐
  for p in merged: commitHook(p.lsn, p.seq, p.batch) // 按 seq 顺序（锁内）
  finish(TxnCommitted); unlock
  commitWaitHook(lsn, seq)                            // 各自锁外 ACK
```

### 1.4 失败路径与语义保持

| 场景 | 现状 | C4 合并模式 | 崩溃恢复 |
|---|---|---|---|
| 阶段 2 fsync 失败 | `applied++` 跳过自己，广播，返回 err | 同现状（不进入阶段 3，不注册） | WAL 未落盘，恢复不补齐（不变） |
| 阶段 3 st.Write 失败 | `applied++` 跳过自己，广播，返回 err | 整批 `applied += n` 跳过，全部返回 err | 记录已落 WAL，恢复补齐（不变） |
| commitHook 错误 | 记录 hookErr，不阻断 | 按 seq 依次调用，错误记录 hookErr（不阻断后续） | 不影响 |
| commitWaitHook 错误 | 锁外返回 err（本地已提交） | 各自锁外返回 err（本地已提交） | 不变 |

### 1.5 可见性说明（必须披露）

合并应用使同一批次事务的存储写入**同时可见**（同一次 RocksDB Write 原子写入）。相比纯串行版「T1 已应用、T2 未应用」的瞬时窗口，合并版中 T2 的数据可能在 T2 的 Commit 返回前被新快照读到——这是业界 group commit 的标准行为（MySQL Group Commit 同理）。**最终状态与崩溃恢复等价不受影响**：WAL 顺序不变、重放逐条幂等、合并顺序 = WAL 顺序。此差异以扩展测试（随机并发负载 + 模拟中断重放逐字节对比）验证最终一致性。

## 2. 崩溃恢复等价论证

- **WAL 顺序**：阶段 1 Append 仍在 `m.mu` 锁内按序进行，`pendingBuf` 写入顺序 = 锁获取顺序 = 提交顺序，与 C1 完全一致，**未改动**。
- **重放语义**：`Recover` 逻辑不改，仍按 LSN 物理顺序逐条重放 StateCommit 到存储，跳过 `m:regions`/`m:tables` 路由元数据；幂等。
- **合并顺序 = WAL 顺序**：合并 WriteBatch 按 seq 递增收集（seq 分配顺序 = Append 顺序），Puts/Deletes append 顺序即 WAL 记录顺序。RocksDB WriteBatch 内操作按 append 顺序获得递增 sequence，最终键值状态与逐条应用**逐字节一致**（同一键多次写入取最后值，顺序一致则结果一致）。
- **验证手段**：TestC4CrashRecoveryEquivalence —— 固定随机种子并发负载，分别在「聚合开 / 聚合关（窗口=0 纯串行）」两种模式下执行，对比最终存储全量状态逐 key 一致；再用同一 WAL 重放到新存储，对比与运行期状态一致。

## 3. 收益预估

| 指标 | 预估 |
|---|---|
| st.Write 次数 | 高并发下 N 次 → ~N/批大小 次（批大小受并发度与窗口影响） |
| 单事务延迟 | 无并发/低并发：0 额外延迟（候补=0 不等待窗口） |
| TPS | 目标 ≥ C3 基线 411 TPS 并有提升；主要来自 RocksDB 写放大与 cgo 往返摊薄 |
| 内存 | applyPending 注册即处理（leader 合并后删除），无无序增长 |

## 4. 风险与对策

| 风险 | 对策 |
|---|---|
| 聚合窗口引入延迟 | 仅在检测到候补时等待；默认 200µs 可调，单事务零等待 |
| 可见性瞬时差异 | 披露 + 崩溃恢复等价测试兜底（最终状态一致） |
| follower 醒来误判 leader | `errFilled` 标志区分；等待条件改为 `m.applied < seq` |
| 合并整批失败语义变化 | 与现状「跳过 + 恢复补齐」一致；测试验证 |
| 死锁 | leader 窗口等待时不持锁；follower 在 cond Wait 不持锁；注册短暂持锁 |
| -race | syncDone 用 atomic；applyPending 仅锁内读写 |

## 4.1 实测结论（2026-10-27，最终收敛）

- **聚合窗口默认改为 0**：2 conns OLTP 下主动等待窗口（20-200µs）经内部探针实测引入固定延迟与时序抖动（window=0 → 1884 TPS，20µs → 1048 TPS），改为依赖 C1 组提交天然聚簇；SetApplyGroupWindow 保留可调。
- **低并发快速路径**：无后续注册者（批大小为 1）时直接应用原始 batch，复用 C3 逐事务语义，零聚合分配开销（单事务 batch 内 Puts/Deletes 顺序由 WAL 记录固定，等价性不变）。
- **冲突检测剪枝**：仅批内含 Delete 或新记录含 Delete 时做同 key 跨记录类型冲突检测；纯 Put 批同 key 多次写入顺序安全（后写覆盖），无需检测。
- **基准对照**（同环境同日，tests/bench/sql_bench -oltp -duration 30 -oltp-rows 1000 -oltp-ratio 8:2 -conns 2）：
  - C3 基线原始代码：381.7 / 447.6 TPS（均值 414.7）
  - C4 最终代码：398.4 / 359.5 / 479.0 / 382.7 TPS（均值 404.9，与基线持平，噪声内）
  - 历史 C3 基线单次：411.25 TPS
  - 结论：低并发 2 conns 与 C3 持平（无退化）；高并发下聚合生效（TestC4ConcurrentCommitOrder：200 txns → 7-9 次 st.Write，批均 22-28 事务），st.Write 调用次数结构性减少。

## 5. 测试计划

1. `TestC4ConcurrentCommitOrder`：并发 N 提交，seq 连续单调、applied 无跳跃、数据全部一致、合并观测（applyWrites < N 说明有合并）。
2. `TestC4WALReplayEquivalence`：并发负载后，用同一 WAL 重放到新存储，对比全量状态一致。
3. `TestC4CrashRecoveryEquivalence`：固定随机种子，聚合开/关两模式负载，最终状态逐 key 一致；再重放对比。
4. `TestC4SingleTxnNoExtraDelay`：单事务提交成功、不触发窗口等待（语义正确性冒烟）。

## 6. 可选观测接口（不破坏基础接口）

```go
// txn.go 新增可选接口（类型断言使用，不属于 TxnManager 基础接口语义）
type ApplyGroupObserver interface {
    SetApplyGroupWindow(d time.Duration) // <=0 关闭聚合（回退逐事务串行）
    ApplyGroupStats() (writes, txns uint64) // 观测：累计 st.Write 次数 / 累计应用事务数
}
```
*（内容由AI生成，仅供参考）*
