# D6.2 批量点查合并（Batch Point-Get）实验报告

- 版本：v1.18.0（分布式纵深）D6 读路径优化第 2 项
- 提交：`2cd89b6 v1.18.0 D6.2: batch point-get in routed join / distributed subquery`
- 复算环境：Windows 11 / go1.27.1 windows/amd64，单机模式（`Executor.mgr == nil`，走 `mgetKVCluster` 本地键分支）

## 1. 背景与选点依据

`docs/experiments/storage_engine_selection.md` §5–§6 的瓶颈口径：SQL 系统路径 240 QPS 量级下
`runtime.cgocall` flat 占比 63.75%；块缓存从默认值调到 256MB 曾试过，收益 -1.6%（负收益，已回退）。
因此 D6 收口方向定为**削减点查往返次数**，而非调缓存。

翻查读路径后确认两处 N+1 热点（驱动行/驱动值逐个点查）：

| 文件 | 函数 | 原形态 |
| --- | --- | --- |
| `pkg/sql/distjoin.go` | `routedJoin` | 每左行一次 `getKVCluster` → N 次点查 |
| `pkg/sql/distributed_subquery.go` | `evalInSubqueryRouted` | 每个不同左值一次 `getKVCluster` → N 次点查 |

已有能力复用：`pkg/sql/multinode.go:127 mgetKVCluster`（本地键单次 `tx.MGet`，远端键逐条转发），
`order_index_fastpath.go` 已用它批量回表；`pkg/txn/txn_impl.go:348` 的 `MGet` 与 `Get` 一样记录
Serializable 读集（含 phantom），语义可等价替换。

## 2. 改动形态

两处均改为三段式：

1. 收集：按分区键路由每个驱动行/值 → `locateRow` → `rowKeyAt` 编码带 region 前缀的行键；
2. 批量读：一次 `mgetKVCluster(keys)`（本地键单次 `tx.MGet`）；
3. 归并：按原序回放，`routedJoin` 未命中时 INNER 丢弃 / LEFT `leftJoinNullRow` 补空，
   `evalInSubqueryRouted` 未命中跳过，命中再走 `matchWhere` 过滤。

语义等价性要点：

- 路由与编码错误仍在任何读取之前返回（`errJoinUnroutable` / `errSubqueryUnroutable` 提前返回）；
- 输出顺序仍由驱动侧原序决定，不因批量读的打乱而变化；
- `mgetKVCluster` 与逐条 `getKVCluster` 的未命中语义一致（未命中键不出现在结果 map 中）。

## 3. 验证用例（`pkg/sql/v1180_d6_batch_pointget_internal_test.go`）

计数事务桩 `d62Txn` 统计 `Get` / `MGet` 调用次数与键数，右表偶数主键存在、奇数缺失。

| 用例 | 断言 | 结果 |
| --- | --- | --- |
| `TestD62RoutedJoinBatchPointGet` | 12 行驱动、INNER：`Get=0`、`MGet=1`、键数=12、命中 6 行且原序 2/4/6… | PASS |
| `TestD62RoutedJoinLeftSemantics` | 6 行驱动、LEFT：不丢驱动行，奇数行右列补空串、偶数行命中 | PASS |
| `TestD62InSubqueryBatchPointGet` | 左值重复两次：去重后 `MGet=1`、键数=10、命中 5 个偶数且原序 | PASS |

## 4. 基准对比

`-benchtime 20x -count=3`，`d62RoundTrip=5µs` 注入每次点查往返（模拟 cgo 调用成本），
驱动行数 N=64：

| 基准 | ns/op（3 次） | get/op | mget/op |
| --- | --- | --- | --- |
| `BenchmarkD62RoutedJoinPerRow`（改前路径） | 33,508,245 / 33,463,800 / 32,064,930 | 64.00 | 0 |
| `BenchmarkD62RoutedJoinBatch`（改后） | 474,270 / 521,880 / 560,180 | 0 | 1.000 |

- 点查往返次数：**N 次 → 1 次**（N=64 时 64:1）；
- 墙钟耗时：约 **33.3ms → 0.52ms，约 64 倍**。比值与往返次数成正比，说明该路径耗时由往返次数
  主导而非本地 CPU；
- 绝对耗时受 Windows 定时器调度粒度影响（`time.Sleep(5µs)` 实测落在数百微秒），因此以
  `get/op`、`mget/op` 计数指标作为平台无关的对照口径。

复算命令：

```
go test ./pkg/sql/ -run 'TestD62' -count=1 -v
go test ./pkg/sql/ -run '^$' -bench 'BenchmarkD62RoutedJoin' -benchtime 20x -count=3
```

## 5. 回归面

- `go build ./...` RC=0；`go vet ./pkg/sql/` 无告警；
- `go test ./pkg/sql/ -count=1` 单包全量 PASS（459.8s），含既有 JOIN / 子查询 / 分片用例；
- 全仓 `go test ./...` 仍有 3 处失败（`cmd/openxdb` doctor 两例、`pkg/sql`
  `TestBackupScheduleRetainRotation`、`pkg/db` `TestD44RestartRefreshesAuthoritativeRoutes`），
  三者单独 `-run` 均 PASS，且改动前即存在 → 判定为既有 flaky，单列为 D6 收尾小项处理，不计入本项回归。

## 6. 结论

D6 验收「批量点查合并落地 + 基准对比 + 无回归」中，批量点查合并与基准对比已完成；
接续项为：JOIN/IN 端到端场景基准（真实引擎 + cgo 计数）、flaky 三例修复。
