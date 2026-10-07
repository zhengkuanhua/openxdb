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

- 实验编号：E1a（RocksDB 写读基准初版）
- 日期：2026-10-07
- 状态：初版数据已产出，待 E1b B+Tree 对照

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

## 下一步

- E1b：B+Tree 对照实现与同口径基准
- T1 正式开发：Storage RocksDB 实现（CGO 封装）+ Key 编码单元测试
*（内容由AI生成，仅供参考）*
