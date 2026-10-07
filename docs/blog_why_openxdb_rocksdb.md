---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_2f1207bcc22711f18019525400248c00
    ReservedCode1: pSYXr32pd0Ezd3pRfvbjWeK9edbqhw2SqP8Zk6cJX80Me9X7dKalZ3OeSsDtTpfvNTn+SaUbmEbho7XqJ6qM8GM1kMCKfnsUVTy/v1Y96rqlsoBAgxLIEnXaeuODJwis7mDJzSoQJr0A57wm68FmrOBqr+ZS10IAAQN3e155cpnEaI5JFJaOoOjgckE=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_2f1207bcc22711f18019525400248c00
    ReservedCode2: pSYXr32pd0Ezd3pRfvbjWeK9edbqhw2SqP8Zk6cJX80Me9X7dKalZ3OeSsDtTpfvNTn+SaUbmEbho7XqJ6qM8GM1kMCKfnsUVTy/v1Y96rqlsoBAgxLIEnXaeuODJwis7mDJzSoQJr0A57wm68FmrOBqr+ZS10IAAQN3e155cpnEaI5JFJaOoOjgckE=
---

# 为什么 OpenXDB 选 RocksDB：一次存储引擎选型的真实实验

> OpenXDB 首发博文 | 2026-10-07 | [github.com/zhengkuanhua/openxdb](https://github.com/zhengkuanhua/openxdb)

## 从零写数据库，第一关就是存储引擎

OpenXDB 的目标是从零写一个单机关系型数据库内核，再慢慢长成分布式数据库。存储引擎是地基，选错后面全得返工。在动手前，我们做了一次真实基准实验（E1），把两条候选路线摆在桌面上：

- **RocksDB**：生产级 LSM 存储引擎，通过一层薄薄的 cgo 桥接入 Go。
- **自研内存 B+Tree**：纯 Go 手写，作为"完全掌控"的对照。

## 实验：100 万 key，同口径对决

两条路跑同一套基准：顺序写、随机写、随机读（批量 1000、单线程、记录 P99 延迟）。

| 场景 | RocksDB | B+Tree（内存） | 倍率 |
| --- | --- | --- | --- |
| 顺序写 | 961,538 TPS | 1,043,841 TPS | 1.09x |
| 随机写 | 186,498 TPS | 406,339 TPS | 2.18x |
| 随机读 | 71,003 QPS（P99 60us） | 613,497 QPS（P99 0us） | **8.64x** |

内存 B+Tree 完胜，随机读快了将近 9 倍。数据这么好看，为什么还是选了 RocksDB？

## 吞吐不是全部：快 9 倍的那台引擎，不落盘

答案藏在实验条件里——**B+Tree 是纯内存结构**。它的"快"来自不写磁盘：没有 fsync、没有 SST、没有 MANIFEST。数据库的第一性需求是什么？是**数据不丢**。一个不落盘的引擎，进程一崩数据全没，吞吐再高也没法当数据库用。

RocksDB 的价值恰恰在看不见的地方：

- **SST + MANIFEST**：数据落盘、元数据可恢复，崩溃后自动找回一致性；
- **成熟稳定**：生产级引擎，压缩、缓存、合并策略久经考验；
- **演进衔接**：LSM 的 SST/分区能力，天然适配"从单机长出来的分布式数据库"——M2 复制、M3 分片可以在存储层之上叠加，不用推翻重写。

而自研 B+Tree 的正确性风险，我们也真实踩到了：调试过程中修复了两个破坏树不变量的缺陷（内部节点 merge 漏插分隔键、borrowLeft 取错分隔键）。这正是教学实验的价值——但作为对外招牌，不能拿它当生产地基。

## 选型结论：全都要

最终方案不是二选一：

1. **存储主路径走 RocksDB**（cgo 静态链接），把持久化、恢复、压缩这些"脏活"交给生产级引擎；
2. **自研 B+Tree 保留**，作为对照参考引擎、内存引擎实验与数据结构教学素材；
3. Go 侧保持自研：存储抽象、Key 编码、WAL、事务、SQL 引擎全部从零实现，cgo 桥只封装最小读写接口。

## 留给你的话

如果你的项目也是"从零写数据库"，请记住这次实验的教训：**基准数据要真实跑，但选型决策要看第一性需求**。B+Tree 快 9 倍是事实，可它不落盘也是事实。数据库的地基不是跑分，是数据安全。

OpenXDB 的完整实验记录、实现文档与源码都在 GitHub，欢迎 star、提 issue，一起把它从单机长成分布式。
*（内容由AI生成，仅供参考）*
