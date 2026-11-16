---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_b2723afbd27711f196c5525400248c00
    ReservedCode1: gOSJmK7Xdi3rzPhZlLF4Fd12PYUZrOhKcOw6c8ylhX9MAT6oeu/wA5k+aMmfMLWgZS0M6yG3PCkeMr8MywL5/Jo1gyvmJXPV9NxsGy2ZNdzpmF3qjBGpv3YyKnXXBu1iOpFeX5bn56+fCAOjSUeaXN/XSsdKgH+tuVh6sIRHHRL7PjdmQnYRdxNnJhE=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_b2723afbd27711f196c5525400248c00
    ReservedCode2: gOSJmK7Xdi3rzPhZlLF4Fd12PYUZrOhKcOw6c8ylhX9MAT6oeu/wA5k+aMmfMLWgZS0M6yG3PCkeMr8MywL5/Jo1gyvmJXPV9NxsGy2ZNdzpmF3qjBGpv3YyKnXXBu1iOpFeX5bn56+fCAOjSUeaXN/XSsdKgH+tuVh6sIRHHRL7PjdmQnYRdxNnJhE=
---

# C6 单连接 pipeline / 快照复用收益基准（v1.16.0 P3）

- 表：c6_t(id INT, name TEXT, score INT, PRIMARY KEY(id))，共 100000 行
- 环境：Windows + Go 1.27.1 + CGO + RocksDB
- 每场景轮数：5（取中位数）；每轮每语句重复 100 条

## 场景 1：snapshot_share（单 Session 连续只读语句，平均每条耗时）

| 语句 | 说明 | off(us/条) | on(us/条) | 收益 |
|------|------|----------:|----------:|-----:|
| count-all | 全表计数（COUNT 快速路径） | 97808.0 | 94643.9 | 3.2% |
| range-all | 全表范围扫描（物化） | 410439.1 | 438694.9 | -6.9% |
| point-var | 点查（PK 索引，每条不同 key） | 26.5 | 26.8 | -1.1% |
| sum-all | 全表 SUM（物化聚合） | 447880.4 | 437051.6 | 2.4% |

## 场景 2：pipeline_flush（REPL 行协议一次性批量发送 100 条 SET，总耗时；w=底层 Write 次数）

| 开关 | 总耗时(ms) | 底层 Write 次数 |
|------|----------:|----------:|
| off（逐条 flush） | 53.50 | 100 |
| on（延迟合并 flush） | 0.90 | 2 |
| 收益 | 98.3% | - |
*（内容由AI生成，仅供参考）*
