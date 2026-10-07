---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_0c6e4dbbc1f611f18019525400248c00
    ReservedCode1: jsTB9xKCWRny6qYMWHcQUKNZl6XrW6iPd6L3BmFDSuXpm2Vc47SApgoeXTEF3Fm8vszvE5w//S7hZHegRZxCKNjos4cItirDYr1JncZ3Va4T5elFCW2JboLBR3jtmirj8l4jt84SlRZkJ7SGxmkKSqMCzJUvoDlgKxV7m5hmPLPQG7KwvBrc7GKCu9Y=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_0c6e4dbbc1f611f18019525400248c00
    ReservedCode2: jsTB9xKCWRny6qYMWHcQUKNZl6XrW6iPd6L3BmFDSuXpm2Vc47SApgoeXTEF3Fm8vszvE5w//S7hZHegRZxCKNjos4cItirDYr1JncZ3Va4T5elFCW2JboLBR3jtmirj8l4jt84SlRZkJ7SGxmkKSqMCzJUvoDlgKxV7m5hmPLPQG7KwvBrc7GKCu9Y=
---

# E1 存储引擎基准实验记录（M0-lite）

- 实验编号：E1a（RocksDB 写读基准初版）+ E1b（B+Tree 对照）
- 日期：2026-10-07
- 状态：E1a / E1b 数据均已产出，引擎定夺见 W4 ADR

## 环境

| 项 | 值 |
| --- | --- |
| OS | Windows 11 (Build 22631) |
| 编译器 | MinGW-w64 GCC 16.2.0（winlibs x86_64-posix-seh-ucrt，静态链接） |
| RocksDB | v11.8.1 源码编译（Release / PORTABLE=1 / 无第三方压缩库） |
| 基准程序 | `tests/bench/e1_bench.cpp`（C++20） |

## 方法

- 数据规模：1,000,000 个 key（16 字节十六进制 key + 固定 value）
- 单线程；顺序写/随机写按 1000 条一批组提交（RocksDB WriteBatch）
- 随机读记录逐条延迟，取 P99
- 本次未开启 fsync（纯引擎吞吐）；WAL 场景的同步写由后续 fsync 版补充

## 结果

| 场景 | 数量 | 耗时 | 吞吐 | P99 延迟 |
| --- | --- | --- | --- | --- |
| 顺序写 | 1,000,000 | 1,040 ms | 961,538 TPS | - |
| 随机写 | 1,000,000 | 5,362 ms | 186,498 TPS | - |
| 随机读 | 1,000,000 | 14,084 ms | 71,003 QPS | 60 us |

- 数据目录：`tests/bench/e1_data_1m/`（91 MB）
- 小规模验证（200k）：顺序写 108 万 TPS / 随机写 22 万 TPS / 随机读 18 万 QPS / P99 20us

## 初步结论

1. RocksDB 顺序写吞吐接近百万 TPS，随机写约 18.6 万 TPS，均达单机存储引擎预期水平。
2. 随机读 P99 60 us，冷/热数据混合下点查延迟可控。
3. 待 B+Tree 对照（E1b）产出后，结合维护成本、持久化能力与文档既定标准（演进式开源、Go 主体）做引擎定夺。

## E1b：B+Tree 对照基准（内存实现）

### 环境

| 项 | 值 |
| --- | --- |
| 运行时 | Go 1.27.1（windows/amd64） |
| 实现 | `pkg/storage/btree`（纯 Go B+Tree，变体 B，order=16，KV 适配 Storage 接口） |
| 基准程序 | `tests/bench/e1_btree_bench.go` |

### 方法

- 与 E1a 同口径：1,000,000 个 key；顺序写/随机写按 1000 条一批提交（WriteBatch 聚合）；随机读记录逐条延迟取 P99。
- 随机序列：Go 实现线性同余生成器（种子 42 / 7），与 C++ mt19937_64 数值序列不完全一致，分布同为均匀随机，口径可比。
- 纯内存结构，无 fsync / 持久化开销，天然代表"无落盘成本"的引擎上限。

### 结果

| 场景 | 数量 | 耗时 | 吞吐 | P99 延迟 |
| --- | --- | --- | --- | --- |
| 顺序写 | 1,000,000 | 958 ms | 1,043,841 TPS | - |
| 随机写 | 1,000,000 | 2,461 ms | 406,339 TPS | - |
| 随机读 | 1,000,000 | 1,630 ms | 613,497 QPS | 0 us |

### E1a vs E1b 对照

| 场景 | RocksDB（E1a） | B+Tree（E1b） | 倍率 |
| --- | --- | --- | --- |
| 顺序写 | 961,538 TPS | 1,043,841 TPS | 1.09x |
| 随机写 | 186,498 TPS | 406,339 TPS | 2.18x |
| 随机读 | 71,003 QPS（P99 60 us） | 613,497 QPS（P99 0 us） | 8.64x |

### 初步结论

1. 内存 B+Tree 在三项指标上全面优于未开 fsync 的 RocksDB：随机读提升近 9 倍（P99 降至亚微秒），随机写约 2.2 倍，顺序写约 1.1 倍。
2. 但 B+Tree 为纯内存结构：无持久化、无崩溃恢复、无 WAL 复用；RocksDB 自带 SST 落盘、MANIFEST 恢复与成熟稳定性。
3. 吞吐不能单独决定引擎选型，需综合持久化/恢复/维护成本，结论见 W4 ADR《为什么 OpenXDB 选 RocksDB》。

## 下一步

- W4：输出 ADR 决策记录 + 首发博文《为什么 OpenXDB 选 RocksDB》
- B2：写路径组提交优化（当前 WAL/Txn 每记录 fsync）
*（内容由AI生成，仅供参考）*
