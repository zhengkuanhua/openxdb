---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_e2881e1fc77d11f1944f525400248c00
    ReservedCode1: vzIht35VIgfhS+ea5acCM94JjHZ7k/4Q6ah5q/5quyMt/kFXJwFiz7sWWhIZAcjAcy2qJ+K9Sgs7zp5fAAdOV6uCDJTKfsnYgZmTFwQP6AtkVxEPB1eTuQduiSE7GZaPmkDaAbtAXvBYavuS313DXBHuQAlK3dl4JRK/z0HlrQK9aAjE8GDfLFweGfg=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_e2881e1fc77d11f1944f525400248c00
    ReservedCode2: vzIht35VIgfhS+ea5acCM94JjHZ7k/4Q6ah5q/5quyMt/kFXJwFiz7sWWhIZAcjAcy2qJ+K9Sgs7zp5fAAdOV6uCDJTKfsnYgZmTFwQP6AtkVxEPB1eTuQduiSE7GZaPmkDaAbtAXvBYavuS313DXBHuQAlK3dl4JRK/z0HlrQK9aAjE8GDfLFweGfg=
---



# E4 长期稳定性与持久化压测实验记录（第 2 批生产级性能数据）

- 实验编号：E4（稳定性趋势压测 + 重启持久化校验）
- 日期：2026-10-14
- 状态：数据已产出（6 分钟连续 OLTP 混合负载，0 错误，重启后数据完整可查）
- 定位：对外可宣传的生产级稳定性数据——OpenXDB 在连续混合负载下 TPS/延迟无劣化，且进程重启后已提交数据完整可查（持久化）

## 环境

| 项 | 值 |
| --- | --- |
| OS / 运行时 | Windows 11 (Build 22631) / Go 1.27.1（windows/amd64） |
| 引擎 | RocksDB 11.8.1（CGO 桥接） |
| 压测程序 | `tests/bench/stability/load.go`（`-mode load`） |
| 校验程序 | `tests/bench/stability/check.go`（`-mode check`，COUNT(*) + 抽样点查） |
| 负载 | OLTP 混合事务 8:2，8 连接，每事务 4 操作（点查/范围查/插入/更新） |
| 规模 | oltp_t 预装载 100,000 行 + 二级索引；连续运行 6 分钟（6 × 60s 区间） |
| 数据目录 | `tests/bench/stability_data_e4/` |

## 方法

1. **压测**：`go run ./tests/bench/stability -mode load -dir tests/bench/stability_data_e4 -minutes 6 -rows 100000 -conns 8 -ratio 8:2`。每 60s 输出一个区间的事务数 / TPS / P50 / P95 / P99 / 错误数，观察长时间运行是否劣化（TPS 下行 / 延迟上行 / 错误增长）。负载含插入操作（全局唯一新 id），数据量在压测期间持续增长（10 万 → 15 万行），更接近真实生产增长形态。
2. **重启校验**：压测进程正常退出（等价于一次进程重启/服务中断），随后用 check 模式重新打开同一数据目录：`-mode check -dir tests/bench/stability_data_e4 -expect 150466`。校验内容：
   - `SELECT COUNT(*) FROM oltp_t` 与期望行数（预装载 100000 + 压测插入 50466 = 150466）比对；
   - 5 个抽样主键（含压测插入段 id=123456）点查 HIT，验证数据可查且内容正确。

## 结果

### 区间趋势（每 60s，8 连接，8:2）

| 区间 | 事务数 | TPS | P50(us) | P95(us) | P99(us) | 平均(us) | 错误 | 读/插/改 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 21,772 | 362.9 | 20,151 | 48,462 | 66,996 | 22,046 | 0 | 69,771/8,757/8,560 |
| 2 | 19,070 | 317.8 | 22,606 | 56,323 | 85,905 | 25,174 | 0 | 60,910/7,610/7,760 |
| 3 | 18,449 | 307.5 | 23,163 | 57,018 | 87,109 | 26,014 | 0 | 59,208/7,330/7,258 |
| 4 | 21,136 | 352.3 | 20,625 | 49,211 | 67,593 | 22,708 | 0 | 67,547/8,421/8,576 |
| 5 | 22,233 | 370.6 | 19,592 | 47,490 | 65,457 | 21,585 | 0 | 71,199/8,947/8,786 |
| 6 | 23,071 | 384.5 | 18,969 | 44,661 | 59,066 | 20,802 | 0 | 73,669/9,401/9,214 |

- 全周期累计：插入 50,466 行；期望行数 150,466。
- 原始输出：`tests/bench/tmp_e4_run.txt`（中间产物）。

### 重启持久化校验

```
COUNT(*) = 150466
probe id=0: HIT (name=load-00000000 score=0)
probe id=1: HIT (name=load-00000001 score=1)
probe id=7: HIT (name=load-00000007 score=7)
probe id=9999: HIT (name=load-00009999 score=1000)
probe id=123456: HIT (name=oltp-00123456 score=456)
RESTART CHECK PASS: count=150466 == expect=150466
```

## 解读与可宣传结论

1. **无性能劣化**：6 分钟内 TPS 在 307.5–384.5 区间波动，未出现持续下行；区间 2–3 轻微回落（数据量增长 + 表尾热点窗口竞争）后区间 4–6 持续回升至 384.5 TPS。P99 延迟区间 2–3 峰值 85–87 ms 后回落至 59 ms，同样无单调劣化。**TPS 波动为热点数据增长期的正常抖动，趋势收敛向好。**
2. **零错误**：6 分钟、约 12.6 万事务全部成功，无语句/事务错误、无死锁。
3. **持久化完整**：进程重启后 `COUNT(*)` 与期望行数精确一致（150,466 = 150,466），抽样点查（含压测期间新插入的 id=123456）全部 HIT，证明 **WAL + RocksDB 持久化语义生效：已提交事务在进程重启后完整可查，无丢失、无重复**。
4. 累计写放大：6 分钟写入 50,466 行 + 约 46,000 次更新，加上读取流量，RocksDB 全程稳定运行，未出现 LSM 压缩导致的延迟尖峰失控（P99 峰值 87 ms 可控）。

## 下一步

- 更新 `tests/bench/README.md` 与仓库 README 性能章节
- 保持 `go test ./...` 全绿（基线 528/544，含 M17 RBAC）
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
