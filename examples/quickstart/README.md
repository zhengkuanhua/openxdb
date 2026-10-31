---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_bbff5b1ed40c11f1a0da525400393706
    ReservedCode1: 6kb4UxX7ta4vyS1p/jQKt8cfOB0xPBuwvRbCEqg2see+DM3vjrd48MhLin7c4e7tOxqHEa0Zv/3ZgdAfaHfvJoz5JW/kI92y6mPgaNfjBTCPJok+5g0z9LMKxe0itdmxdf9DHJ3+7ux/Ym+0Jdb4YofOGadBndNP3N21kNg82yOFh6MeNm1z2OzkysY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_bbff5b1ed40c11f1a0da525400393706
    ReservedCode2: 6kb4UxX7ta4vyS1p/jQKt8cfOB0xPBuwvRbCEqg2see+DM3vjrd48MhLin7c4e7tOxqHEa0Zv/3ZgdAfaHfvJoz5JW/kI92y6mPgaNfjBTCPJok+5g0z9LMKxe0itdmxdf9DHJ3+7ux/Ym+0Jdb4YofOGadBndNP3N21kNg82yOFh6MeNm1z2OzkysY=
---

# OpenXDB Quickstart 端到端演示（examples/quickstart）

> 适用版本：v1.17.0。演示一条完整链路：**init → start → 建表/插入/二级索引/查询 → EXPLAIN → SHOW SLOWQUERIES SUMMARY → doctor/stats**。
> 仅依赖仓库内 `openxdb` 二进制与 REPL stdin 管道，**不引入任何第三方依赖**。

## 一键运行（Windows PowerShell）

```powershell
# 仓库根目录下执行；自动探测 bin\openxdb.exe（未编译可先跑 scripts\build.ps1）
powershell -ExecutionPolicy Bypass -File examples\quickstart\quickstart.ps1

# 指定二进制 / 数据目录 / 端口
powershell -ExecutionPolicy Bypass -File examples\quickstart\quickstart.ps1 `
    -OpenXDB .\bin\openxdb.exe -DataDir .\quickstart-data -Port 7788
```

脚本依次执行：

| 步骤 | 命令 | 演示内容 |
|------|------|----------|
| 1 | `openxdb init --data-dir ./quickstart-data` | 初始化数据目录 |
| 2 | `openxdb repl --data-dir ./quickstart-data`（stdin 管道） | 建表 / 插入 / 二级索引 / 查询 / EXPLAIN / SHOW SLOWQUERIES SUMMARY |
| 3 | `openxdb start --data-dir ... --port 7788`（后台） | 启动 TCP 服务（默认端口 7788） |
| 4 | `openxdb doctor --data-dir ... --addr 127.0.0.1:7788` | 在线健康检查（数据目录/WAL/RocksDB/元数据/binlog/端口等） |
| 5 | `openxdb stats --addr 127.0.0.1:7788` | 实时指标（连接/表/region/慢查询/binlog LSN） |
| 6 | 停止后台服务 | 收尾清理 |

> 说明：REPL 直接持有数据目录锁，因此 SQL 演示（步骤 2）安排在服务启动（步骤 3）之前串行执行；`doctor` 支持本地（`--data-dir`）与在线（`--addr`）两种检查模式，脚本演示在线模式。

## 手动分步（Windows / Linux 通用）

先构建二进制（任选其一）：

```sh
# Linux/macOS
./scripts/build.sh          # 产物 bin/openxdb

# Windows
powershell -ExecutionPolicy Bypass -File scripts\build.ps1   # 产物 bin\openxdb.exe
```

### 1. 初始化

```sh
openxdb init --data-dir ./data
```

### 2. 建表 / 插入 / 二级索引 / 查询（REPL）

```sh
openxdb repl --data-dir ./data
```

```sql
CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id));
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35);
CREATE INDEX idx_age ON users (age);
SELECT name FROM users WHERE age >= 25 AND age < 35 ORDER BY age;
-- 预期输出：alice(30)、bob(25) 两行（按 age 升序）
```

也可以直接用管道批量执行（脚本即采用此方式）：

```sh
cat demo.sql | openxdb repl --data-dir ./data
```

### 3. EXPLAIN（v1.17.0 节点级耗时/行数估算）

```sql
EXPLAIN SELECT name FROM users WHERE id = 1;
-- access: point lookup on PK (id) rows=1 est_ms=0.51
-- projection: name rows=1 est_ms=0.02

EXPLAIN SELECT name FROM users WHERE age >= 25 ORDER BY name;
-- access: index scan on idx_age(age) rows=3 est_ms=0.53
-- filter: age rows=2 est_ms=0.05
-- sort: name rows=2 est_ms=0.05
-- projection: name rows=2 est_ms=0.04
```

每个算子行末尾追加 `rows=N`（行数估算，优先走 RangeCounter 真实计数）与 `est_ms=X.XX`（耗时估算），输出向后兼容。

### 4. SHOW SLOWQUERIES SUMMARY TOP N（阈值区间桶汇总）

```sql
-- 阈值热配置（毫秒），随后执行的慢查询进入统计
SET slow_query_threshold = 1;

-- 制造几条慢查询（300 行全表/范围扫描）
SELECT id FROM big WHERE id > 0;
SELECT id FROM big WHERE id > 100;
SELECT id FROM big WHERE id > 250;

-- 汇总报表：阈值/记录数/平均/最大 + 区间桶 + TOP N 明细（默认 TOP 10）
SHOW SLOWQUERIES SUMMARY TOP 5;

-- 明细列表
SHOW SLOWQUERIES;
```

`SHOW SLOWQUERIES SUMMARY` 输出 `metric | value` 两列，包含 `summary.threshold_ms` / `summary.records` / `summary.avg_ms` / `summary.max_ms`、`bucket.*`（阈值区间桶汇总）与 `top.N`（按耗时降序的 TOP N 明细）。

### 5. 启动服务 + doctor / stats（在线运维）

```sh
# 启动 TCP 服务（默认端口 7788；可选 --mysql-port 3306 启用 MySQL wire 兼容监听）
openxdb start --data-dir ./data --port 7788

# 在线健康检查（退出码 0=pass / 1=warn / 2=error）
openxdb doctor --data-dir ./data --addr 127.0.0.1:7788

# 实时指标（连接数/表数/region 分布/慢查询数/binlog LSN）
openxdb stats --addr 127.0.0.1:7788 [--interval 5] [--json]
```

### 6. 收尾

```sh
# 优雅停机（服务端 SHUTDOWN GRACEFUL，或 Ctrl-C / SIGTERM）
# 脚本演示环境直接停止后台进程即可；数据目录可随时删除重来
```

## 更多参考

- README「快速开始」「Demo」「Quick start」：仓库根 [README.md](../../README.md)
- 发布说明：[RELEASE_NOTES.md](../../RELEASE_NOTES.md)
- 变更记录：[CHANGELOG.md](../../CHANGELOG.md)
- 基准实验：[docs/experiments/E5_standard_bench_alignment.md](../../docs/experiments/E5_standard_bench_alignment.md)
*（内容由AI生成，仅供参考）*
