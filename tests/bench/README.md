# openxdb tests/bench 性能基准

本目录存放 openxdb 的可重复运行性能基准，按层级分为：**SQL 层基准**（`sql_bench/`）与
**引擎层基准**（`e1_btree_bench.go`、`rocksdb/e1_bench.cpp`）。SQL 层基准串入引擎层对照场景，
用于展示 SQL 层（解析/事务/索引）在引擎之上的叠加开销。

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

结果文件：`tests/bench/docs/sql_bench_result.md`（含场景汇总表、场景说明、SQL vs 引擎开销解读）。

### 解读要点

- 绝对数值与机器相关（本机 CGO/RocksDB 环境），相对开销（SQL vs 引擎）更具可比性。
- C1 如实呈现已知现状：SQL 层主键不在二级索引集合（`TableMeta.Indexes`），主键范围条件
  回退全表扫描+逐行过滤，成本随数据量线性放大；C2 展示建二级索引后的索引范围读对照。
- C1/C2 在 D 之后运行（表行数 = 2n），窗口基于 `2n` 计算。

## 引擎层基准

- `e1_btree_bench.go`：btree 存储批量写/点查微基准（独立 main）。
- `rocksdb/e1_bench.cpp`：RocksDB C++ 微基准（需单独编译，见文件头注释）。

## 数据目录说明

`e1_data_1m/`、`e1_data_200k/` 为引擎层基准产生的 RocksDB 数据目录（只读样例，勿手动修改）；
`sql_bench_data/` 由 SQL 层基准自动生成（默认在仓库根目录，可用后删除重建）。
