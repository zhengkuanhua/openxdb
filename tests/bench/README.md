---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_e51b644dc77d11f1b5a6525400393706
    ReservedCode1: cqf7XbB4w2Sixm5GuAvVNvE5mdQKPI9eZe1iPvza6VY6At7kjf8vG7vWEu7Gq7uGuWA38crnizJw8boTBzz9YYOzlymuE7d7M5myxnlnWg6hhj+0xlSIdu5FianBmHpxd8JBhs2RGqrIVh+aUXM9+XXxt06a6t9J5b2801pos8bTxj2++sB+8S9JFlw=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_e51b644dc77d11f1b5a6525400393706
    ReservedCode2: cqf7XbB4w2Sixm5GuAvVNvE5mdQKPI9eZe1iPvza6VY6At7kjf8vG7vWEu7Gq7uGuWA38crnizJw8boTBzz9YYOzlymuE7d7M5myxnlnWg6hhj+0xlSIdu5FianBmHpxd8JBhs2RGqrIVh+aUXM9+XXxt06a6t9J5b2801pos8bTxj2++sB+8S9JFlw=
---



# openxdb tests/bench 性能基准

本目录存放 openxdb 的可重复运行性能基准，按层级分为：**SQL 层基准**（`sql_bench/`）与
**引擎层基准**（`e1_btree_bench.go`、`rocksdb/e1_bench.cpp`、`rocksdb/e2_fsync_bench.cpp`）。
SQL 层基准串入引擎层对照场景，用于展示 SQL 层（解析/事务/索引）在引擎之上的叠加开销。
第 2 批生产级实验文档见 `docs/experiments/E2_rocksdb_fsync_bench.md`（fsync 同步写）、
`docs/experiments/E3_sql_oltp_bench.md`（SQL OLTP 混合负载）、`docs/experiments/E4_stability.md`（稳定性与持久化）。

## 前置环境（Windows，必须执行）

RocksDB CGO 编译/运行依赖环境变量（`CGO_CFLAGS`/`CGO_LDFLAGS`），每次打开新终端后先执行：

```powershell
.\scripts\build.ps1 -SkipAllBuild
```

脚本会探测 `C:\rocksdb-src` / `C:\rocksdb-build` 或
`%USERPROFILE%\AppData\Local\OpenXDBTools\rocksdb-*` 路径并注入环境变量。

## SQL 层基准（tests/bench/sql_bench）

独立 main 程序，覆盖 SQL 层典型读写场景，数据量可配置，输出 QPS/TPS 与
平均/P50/P95/P99 延迟。

### 运行

```powershell
# 仓库根目录执行
go run ./tests/bench/sql_bench -n 100000 -dir sql_bench_data -out tests/bench/docs/sql_bench_result.md
```

参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-n` | 100000 | 数据量（行数/操作数），默认 10 万行，可调小快速冒烟 |
| `-dir` | sql_bench_data | 基准数据目录（自动 `db.Init`/`db.Open`；换新目录可得干净数据） |
| `-out` | tests/bench/docs/sql_bench_result.md | 结果 Markdown 输出路径 |
| `-conns` | 1 | 并发连接数（每连接独立 SQL 会话，跨连接事务并行提交） |
| `-oltp` | false | 启用 G.OLTP 混合事务场景 |
| `-orows` | 100000 | G 场景 oltp_t 预装载行数 |
| `-orat` | 8:2 | G 场景读写比（`8:2` 或 `5:5`） |
| `-duration` | 60 | G 场景压测时长（秒） |

### 场景

| 场景 | 内容 | 口径 |
| --- | --- | --- |
| A | 单点写入：逐条 `INSERT` autocommit | QPS / 逐条延迟 |
| B | 单点查询：随机主键 `SELECT * ... WHERE id = k` | QPS / 逐条延迟 |
| C1 | 范围扫描（无索引）：`WHERE id >= a AND id < a+100`，主键范围回退全表扫描，固定 3 次控时 | 每次延迟 |
| C2 | 范围扫描（二级索引）：先 `CREATE INDEX idx_bench_id`，再跑同一查询（尾部窗口控行数），固定 100 次 | QPS / 逐条延迟 |
| D | 混合事务：`BEGIN` + 10 INSERT + 5 SELECT + `COMMIT`，事务数 = n/10 | TPS / 事务延迟 |
| E1 | 引擎层对照：同一 RocksDB 批量写（batch=1000，KV 前缀 `e:`） | TPS |
| E2 | 引擎层对照：同一 RocksDB 随机点查（绕过 SQL 层） | QPS |
| G | OLTP 混合事务（`-oltp`）：独立表 `oltp_t` 预装载 + 二级索引，8 连接 `BEGIN`+4 操作+`COMMIT`，读写比 8:2 / 5:5 | TPS / 事务延迟 |

结果文件：`tests/bench/docs/sql_bench_result.md`（含场景汇总表、场景说明、SQL vs 引擎开销解读）。
G 场景结果：`tests/bench/docs/sql_bench_e3_oltp.md`（8:2）与 `sql_bench_e3_oltp_55.md`（5:5），
正式实验记录见 `docs/experiments/E3_sql_oltp_bench.md`（含 6.2 TPS 根因诊断与调优）。

### 解读要点

- 绝对数值与机器相关（本机 CGO/RocksDB 环境），相对开销（SQL vs 引擎）更具可比性。
- C1 如实呈现已知现状：SQL 层主键不在二级索引集合（`TableMeta.Indexes`），主键范围条件
  回退全表扫描+逐行过滤，成本随数据量线性放大；C2 展示建二级索引后的索引范围读对照。
- C1/C2 在 D 之后运行（表行数 = 2n），窗口基于 `2n` 计算。

## 引擎层基准

- `e1_btree_bench.go`：btree 存储批量写/点查微基准（独立 main）。
- `rocksdb/e1_bench.cpp`：RocksDB C++ 微基准（无 fsync，需单独编译，见文件头注释）。
- `rocksdb/e2_fsync_bench.cpp`：RocksDB fsync 同步写基准（`WriteOptions.sync=true`，含组提交 batch=1/10/100/1000 对照），
  编译命令与取数结果见 `docs/experiments/E2_rocksdb_fsync_bench.md`。

## 稳定性压测（tests/bench/stability）

第 2 批 E4 实验程序（`load.go` + `check.go`，同一 package main）：

```powershell
# 压测：6 分钟 OLTP 混合负载（8:2，8 连接），每 60s 输出 TPS/P50/P95/P99 趋势
go run ./tests/bench/stability -mode load -dir tests/bench/stability_data_e4 -minutes 6 -rows 100000 -conns 8 -ratio 8:2
# 重启校验：进程退出后重新打开数据目录，COUNT(*) 与抽样点查验证持久化完整
go run ./tests/bench/stability -mode check -dir tests/bench/stability_data_e4 -expect 150466
```

参数：`-mode`（load/check）、`-minutes`（分钟，默认 6）、`-rows`（预装载行数）、
`-conns`（并发连接数）、`-ratio`（8:2 / 5:5）；check 模式复用 `-dir` / `-expect`。
正式实验记录见 `docs/experiments/E4_stability.md`。

## 数据目录说明

`e1_data_1m/`、`e1_data_200k/` 为引擎层基准产生的 RocksDB 数据目录（只读样例，勿手动修改）；
`sql_bench_data/` 由 SQL 层基准自动生成（默认在仓库根目录，可用后删除重建）；
`rocksdb/e2_fsync_run1m/`、`rocksdb/e2_fsync_group_b{1,10,100,1000}/` 为 E2 fsync 基准数据目录；
`stability_data_e4/` 为 E4 稳定性压测数据目录（重启校验用，可保留复验）。
*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
