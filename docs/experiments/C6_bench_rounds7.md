---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_b342e90dd27711f18460525400393706
    ReservedCode1: OqdYbAgSTE70hrtY39j5VmGVwI6h6GYHK4nG5a9p+6nfHYf8svZfWcW/EUrCCIDd2f9XJSd643y/z8jDUGXvlq6itm8QJjwWaYrdV8h7Rpei6ronjWCxKascDgzXEArbB+B6tjz06hkygzqlOMmXaBNsvyAHCh593KtyEqzmT33+31dzkCFTSLpDKtY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_b342e90dd27711f18460525400393706
    ReservedCode2: OqdYbAgSTE70hrtY39j5VmGVwI6h6GYHK4nG5a9p+6nfHYf8svZfWcW/EUrCCIDd2f9XJSd643y/z8jDUGXvlq6itm8QJjwWaYrdV8h7Rpei6ronjWCxKascDgzXEArbB+B6tjz06hkygzqlOMmXaBNsvyAHCh593KtyEqzmT33+31dzkCFTSLpDKtY=
---

# C6 单连接 pipeline / 快照复用收益基准（v1.16.0 P3）

- 表：c6_t(id INT, name TEXT, score INT, PRIMARY KEY(id))，共 100000 行
- 环境：Windows + Go 1.27.1 + CGO + RocksDB
- 每场景轮数：7（取中位数）；每轮每语句重复 100 条

## 场景 1：snapshot_share（单 Session 连续只读语句，平均每条耗时）

| 语句 | 说明 | off(us/条) | on(us/条) | 收益 |
|------|------|----------:|----------:|-----:|
| count-all | 全表计数（COUNT 快速路径） | 174005.6 | 174528.9 | -0.3% |
| range-all | 全表范围扫描（物化） | 582002.8 | 577522.8 | 0.8% |
| point-var | 点查（PK 索引，每条不同 key） | 46.2 | 42.1 | 8.9% |
| sum-all | 全表 SUM（物化聚合） | 570600.0 | 576997.7 | -1.1% |

## 场景 2：pipeline_flush（REPL 行协议一次性批量发送 100 条 SET，总耗时；w=底层 Write 次数）

| 开关 | 总耗时(ms) | 底层 Write 次数 |
|------|----------:|----------:|
| off（逐条 flush） | 51.99 | 100 |
| on（延迟合并 flush） | 1.40 | 2 |
| 收益 | 97.3% | - |
*（内容由AI生成，仅供参考）*
