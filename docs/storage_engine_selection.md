---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_260c0c82d5bd11f180d5525400248c00
    ReservedCode1: D5oo7GvzwVVtHTDEVKOu2CR+l5BRHfrsTtJs+w+7W4uEKebTjKs3nEI+IUZPxvGoa3uXsMuoqaQA+9GX+6FHt5gyXUfEsvFpIhageqNWQjQfgpvYZvpccsOpMxOMBdfeNh2XHp0eW9qDrjhkcUwYDh1REd3SDqOmwXZl0P5yC1EcPxWuwyncySRPj2E=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_260c0c82d5bd11f180d5525400248c00
    ReservedCode2: D5oo7GvzwVVtHTDEVKOu2CR+l5BRHfrsTtJs+w+7W4uEKebTjKs3nEI+IUZPxvGoa3uXsMuoqaQA+9GX+6FHt5gyXUfEsvFpIhageqNWQjQfgpvYZvpccsOpMxOMBdfeNh2XHp0eW9qDrjhkcUwYDh1REd3SDqOmwXZl0P5yC1EcPxWuwyncySRPj2E=
---



# 存储引擎选型罗盘（Storage Engine Selection Compass）

- 日期：2026-11-01
- 状态：汇总既有决策与后续演进口径（ADR-004 已接受，本文为罗盘性总纲）
- 适用范围：OpenXDB 单机存储引擎选型回顾、新成员上手、对外答疑口径
- 上游依据：[ADR-004 存储引擎选型](W4_adr_engine_selection.md)、[首发博文《为什么 OpenXDB 选 RocksDB》](blog_why_openxdb_rocksdb.md)、[E1 存储引擎基准实验](experiments/E1_rocksdb_bench.md)、[D2.3 并发伸缩验证](experiments/D2_3_concurrency_scaling.md)

## 1. 选型背景与决策标准

OpenXDB 的路线是"从零写一个单机关系型数据库内核，再慢慢长成分布式数据库"。M0-lite 阶段选定单机存储引擎，是地基级决策：**选错后面全得返工**。决策发生于 2026-10-07，以 ADR-004 形式固化并已接受。

决策标准与权重（引用 ADR-004，`docs/W4_adr_engine_selection.md`）：

| 标准 | 权重 | 说明 |
| --- | --- | --- |
| 持久化与崩溃恢复 | 高 | 数据库内核的第一性需求，WAL 之外的最终落盘保障 |
| 成熟度与稳定性 | 高 | 商用招牌必须可对外演示，不能频繁丢数据 |
| 维护成本 | 中 | 单开发者项目，构建链路越简单越好 |
| 吞吐表现 | 中 | 实测数据支撑，但不单独决定选型 |
| 演进式开源路线 | 中 | 从单机长出来的分布式数据库：LSM/分区/SST 复用潜力 |
| Go 主体 | 低 | 教学价值：核心逻辑仍以 Go 编写，cgo 仅限存储桥 |

核心方法论（引用博文 `docs/blog_why_openxdb_rocksdb.md`）：**基准数据要真实跑，但选型决策要看第一性需求**——吞吐不是全部，数据库的地基不是跑分，是数据安全。

## 2. 候选路线对比

| 维度 | RocksDB（cgo 静态链接） | InnoDB（MySQL 内嵌） | 自研内存 B+Tree（纯 Go） | 纯 Go LSM（Pebble 类） |
| --- | --- | --- | --- | --- |
| 嵌入方式 | 独立引擎库，C++ API + 静态库，可 cgo 链接 | MySQL Server 层附属组件，**不可独立链接** | 纯 Go，进程内结构 | 纯 Go 第三方库 |
| 持久化/崩溃恢复 | SST + MANIFEST + WAL，自带恢复与压缩 | 依赖 MySQL Server 的 redo/undo 体系 | **无**，需自研落盘与恢复 | 有（LSM 自带） |
| 成熟度 | 生产级（Facebook/Meta 大规模生产） | 生产级但绑死 MySQL | 自研，正确性风险高（曾修复 merge/borrowLeft 缺陷） | 社区成熟（CockroachDB 生产验证） |
| 事务原语 | Snapshot / WriteBatch / Column Family / TransactionDB | 依赖 MySQL 事务层 | 无，需自研 | 有快照与 WriteBatch 类原语 |
| 吞吐（E1 实测，100 万 key，ADR 口径） | 顺序写 961,538 TPS / 随机写 186,498 TPS / 随机读 71,003 QPS（P99 60us） | 本项目未实测（未进候选） | 顺序写 1,043,841 TPS / 随机写 406,339 TPS / 随机读 613,497 QPS（P99 0us） | 本项目未实测（未进候选） |
| 演进衔接（M2 复制 / M3 分片） | LSM/SST 文件级复制、分区友好 | 页结构分布式化成本高 | 无落盘形态，无从衔接 | 可衔接，但削弱从零实现价值 |
| 维护成本 | 中（cgo 桥 + 工具链） | 高（耦合 MySQL 大体系） | 低但正确性风险高 | 低（第三方库） |
| 本项目结论 | **采纳（主路径）** | 不采纳（结构性不兼容） | 保留为对照/内存实验引擎 | 不采纳（教学价值与路径依赖） |

E1 实测口径说明：RocksDB 数据来自 E1a（`tests/bench/rocksdb/e1_bench.cpp`，未开 fsync，单线程、批量 1000）；B+Tree 数据来自 ADR-004 引用的 E1b 口径（纯内存、同批量口径）；E1b 复测（2026-10-09）绝对数值略有差异（顺序写 1,379,310 / 随机写 459,137 / 随机读 608,643），但相对关系与结论不变，详见 [E1 实验记录](experiments/E1_rocksdb_bench.md)。

## 3. 为什么不选 InnoDB：结构性理由

InnoDB 在选型时**根本未进候选名单**（ADR-004 候选仅为 RocksDB 与自研 B+Tree）。排除它不是跑分问题，而是结构性不兼容：

1. **可嵌入性（最根本）**：InnoDB 是 MySQL Server 层的附属组件，随 MySQL 源码树分发、依赖 Server 层的 buffer pool 管理、redo/undo 事务体系与存储接口；不存在可独立链接的公开库产物。RocksDB 则是独立引擎库，提供稳定的 C++ API 与静态库，OpenXDB 通过一层薄 cgo 桥即可静态链接接入。
2. **集成成本**：接入 InnoDB 等于把 MySQL 内核半嵌入 OpenXDB，需要适配其 redo/undo、半同步、数据字典等 Server 交互；而 RocksDB 仅需封装 Put/Get/Scan/WriteBatch/Snapshot 等最小读写接口，构建链路与 API 边界清晰。
3. **项目定位冲突**：OpenXDB 的定位是"从零实现"的教学/实验数据库内核，SQL、事务、存储抽象、Key 编码全部自研；用 InnoDB 意味着事务与存储语义被 MySQL 体系接管，违背从零实现的第一性原则。
4. **演进路线衔接**：目标架构是"从单机长出来的分布式数据库"。RocksDB 的 LSM/SST 天然支持文件级复制（M2 复制可基于 SST 文件同步）与分区/分片友好布局（M3 分片）；InnoDB 的页式结构（16KB 页 + B+Tree 聚簇）在分布式化时需要额外抽象与跨页协调，成本高且与既有生态不兼容。
5. **对标案例**：
   - **TiKV / NebulaGraph 用 RocksDB**：分布式系统内嵌独立 LSM 引擎，在 RocksDB 之上实现分布式事务（TiKV 的 MVCC + 事务层），证明"独立 LSM + 自研事务/SQL 层"是成熟路线，与 OpenXDB 同构。
   - **Aurora 用 InnoDB**：其本质是"MySQL 云化"——保留 MySQL 兼容性与 InnoDB 语义、把存储下沉到分布式日志存储，服务的是 MySQL 存量生态。OpenXDB 不追求 MySQL Server 兼容（仅协议/语法兼容），路线不同，不构成 InnoDB 候选依据。

## 4. RocksDB 作为事务型存储引擎的适用性辨析

RocksDB 常被误认为"只适合 KV 缓存"，但其事务原语足以支撑关系型内核的事务层：

- **Snapshot**：读快照，为 MVCC / 快照隔离提供基础，OpenXDB 读路径（含只读并行批）以 Snapshot 隔离实现 read-your-writes。
- **WriteBatch**：原子批量写，天然对应语句/事务内的多行写入聚合。
- **Column Family**：逻辑分区与多租户布局，为 M3 分片与索引/表空间隔离预留。
- **TransactionDB**：悲观/乐观事务 API，提供锁与冲突检测，可承载显式事务 BEGIN/COMMIT 语义。

**业界佐证**：

- **TiKV** 在 RocksDB 上实现了完整的分布式事务（MVCC 版本、两阶段提交、冲突检测），是"RocksDB 之上盖事务层"的最大规模生产验证。
- **CockroachDB** 采用 Pebble（纯 Go LSM，同族思想）支撑其 SQL 与分布式事务，证明 LSM 系引擎作为事务型数据库地基的通用性。

**LSM 点查延迟与读放大取舍**：

- LSM 点查需查询 memtable + 多层 SST（读放大），相对 B+Tree 聚簇结构延迟更高；E1a 实测 100 万 key 点查 P99 60us，处于可接受区间。
- 缓解手段：布隆过滤器（拒绝大多数不命中 SST）、块缓存（热数据命中）、层级合并（降低层数）。OpenXDB 读路径以 Snapshot + MGet 批量点查为主，将上述手段作为后续优化方向（见 §6）。

## 5. 240 QPS 饱和口径解读（重要答疑口径）

对外讨论"D2 复测 16 连接 read 约 240 QPS"时，必须区分两个完全不同的读数：

| 读数 | 来源 | 数值 | 含义 |
| --- | --- | --- | --- |
| **系统路径 SQL 吞吐** | D2.3 OLTP 等价模型（read 17.12 TPS × 14 ops/事务） | ≈ **239.7 QPS** | SQL 完整路径（协议 → 解析 → 执行 → Snapshot → RocksDB 读）的**系统级饱和读数** |
| **引擎裸读上限** | E1a 引擎级基准（1,000,000 点查） | **71,003 QPS** | RocksDB 引擎自身单线程随机读吞吐，与系统读数相差约 **300 倍** |

**240 QPS ≠ RocksDB 读不动**。该数字是"OLTP 等价模型下 SQL 完整路径"的系统读数，瓶颈不在引擎而在系统路径：

1. **cgocall 63.75%**：16 连接 read CPU profile 显示 `runtime.cgocall` 占 flat 63.75%（RocksDB cgo 读主导），单次点查的 cgo 往返固定开销是主要成本。
2. **单点 Get 固定开销**：逐条 MGet 的 cgo 桥调用、Go 侧调度与对象编解码，构成每条操作的固定成本。
3. **调度争用**：`runtime.lock2 / semasleep` 显示 Go 运行时调度争用；写/提交路径的全局串行点（txnManager.mu 提交队列、WAL）进一步压制多连接收益。

**"16 连接不扩展"的正确解读**：8/16 连接 read 均饱和于约 240 QPS（D2.3：8 连接 238.70 / 16 连接 239.63），说明系统路径已逼近 RocksDB 迭代器吞吐上限（本机单线程读吞吐墙），连接翻倍吞吐不增属于"路径已饱和"而非"引擎不可扩展"；32 连接回落（~154 QPS）才真正指向全局提交/调度串行。**结论：优化应针对系统路径（Go 侧批处理、减少 cgo 往返），而非更换存储引擎**。

## 6. 后续读路径优化方向

基于 §5 的瓶颈分解，读路径优化优先级如下（均不改变引擎选型）：

| 方向 | 思路 | 预期收益 | 状态 |
| --- | --- | --- | --- |
| 块缓存调优 | block cache 容量/分片、access hint、逐出策略 | 提升热数据命中、摊薄 cgo 读开销 | 已实验 256MB（-1.6%，负收益）已回退；需结合读模型再调 |
| 批量点查合并 | multi-get 聚合、复用同一 Snapshot 的 MGet 批化 | 直接削减 cgo 往返次数（profile 最大热点） | 待实施 |
| Snapshot 批量 Scan | 范围扫描批化、迭代器复用、减少逐条 Get | 削减单点固定开销，利好 4 range ops/事务 | 待实施 |
| 写路径组提交 | WAL/Txn 组提交（B2），缓解全局提交串行 | 提升 16/32 连接 rw 与多连接收益 | 已立项（B2_group_commit.md） |
| cgo 桥批量接口 | 在 C++ 桥层提供批量 Get/Scan 接口 | 减少 Go↔C++ 边界往返 | 待评估 |

## 7. 结论

- **选型已定**：RocksDB（cgo 静态链接）为存储主路径；自研 B+Tree 保留为对照/内存实验引擎（ADR-004 已接受）。
- **InnoDB 结构性排除**：不可嵌入、集成成本高、违背从零实现定位、页式结构与分布式演进不匹配。
- **RocksDB 事务能力够用**：Snapshot/WriteBatch/TransactionDB + TiKV/CockroachDB 类生产佐证，事务层由 OpenXDB Go 侧自研叠加。
- **240 QPS 是系统口径**：SQL 完整路径饱和（read TPS × 14 ops），非引擎裸读上限（E1 71,003 QPS）；瓶颈在 cgocall 与调度，后续按 §6 优化系统路径。

*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
