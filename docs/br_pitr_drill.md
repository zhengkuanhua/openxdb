# BR / PITR 自动化演练（br_pitr_drill）

## 概述

针对 OpenXDB 已实现的备份恢复能力，提供一键可复现的端到端自动化演练：

- **BR（全量备份与恢复）**：`BACKUP TO '<path>'` / `RESTORE FROM '<path>'`，含灾难恢复闭环；
- **PITR（时间点恢复）**：`RESTORE FROM '<path>' TO LSN <n>`，基于 binlog 精确回放到备份点之后的任意提交位点。

演练包含完整的数据校验断言（行数、首末行、LSN 边界内外泄漏检查），任何一步失败即终止并返回失败点描述。相关实现见 `pkg/sql/backup.go`，设计说明见 `docs/T18_backup_restore.md`。

## 位置

| 内容 | 路径 |
| --- | --- |
| 演练主程序（独立 main） | `tests/bench/br_pitr_drill/main.go` |
| 端到端测试（复用主流程） | `tests/bench/br_pitr_drill/drill_test.go` |
| 演练输出报告 | `-out` 指定的 Markdown 文件（默认 `br_pitr_drill_report.md`） |

风格沿用 `tests/bench/sql_bench` 的独立 main 程序约定：flag 参数、`db.Init` + `db.Open`、`e.Execute` 执行 SQL、Markdown 报告输出。

## 一键运行

```powershell
# 先注入 RocksDB CGO 环境（本仓库 Windows 构建前提）
.\scripts\build.ps1 -SkipAllBuild

# 方式一：端到端测试（推荐，临时数据目录，默认数据量）
go test ./tests/bench/br_pitr_drill/ -count=1 -v

# 方式二：手动运行演练（自选数据量与输出路径）
go run ./tests/bench/br_pitr_drill -dir "D:\tmp\brdrill-data" -backup "D:\tmp\brdrill.bak" -rows 1000 -pitr-rows 10 -out "D:\tmp\brdrill_report.md"
```

## 参数说明（方式二）

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `-dir` | `brdrill-data` | 演练数据目录（RocksDB + WAL + binlog） |
| `-backup` | `brdrill.bak` | 备份文件输出路径 |
| `-rows` | `500` | 阶段 A 写入的基线行数 |
| `-pitr-rows` | `8` | 阶段 C/D 备份点后写入行数 |
| `-out` | `br_pitr_drill_report.md` | 演练报告输出路径 |

## 演练闭环（A → D）

### A. 准备
建表 `drill_users`，写入 `-rows` 行基线数据（id 自 1 递增）。

### B. 全量备份（BR 起点）
`BACKUP TO '<backup>'`，校验备份结果行数与基线一致、`backup_lsn > 0`。

### C. PITR 精确回放
1. 备份点后逐条写入 `-pitr-rows` 行，记录每条提交后的 binlog LSN；
2. 选取中间与末尾若干 LSN 作为检查点，逐个执行 `RESTORE FROM '<backup>' TO LSN <n>`；
3. 每个检查点校验：行数 = 基线 + 已回放条数、末行存在、超出 LSN 的行不存在（无泄漏）；
4. `TO LSN = backup_lsn`（备份点本身）时，结果应恰为基线快照。

### D. BR 灾难恢复闭环
1. 模拟灾难：备份点后写入数据并 `DROP TABLE`（误删/损坏场景）；
2. `RESTORE FROM '<backup>'` 全量恢复；
3. 校验行数等于基线、id=1 与 id=rows 均存在。

## 校验断言清单

| 阶段 | 断言 |
| --- | --- |
| B | 备份返回 1 行；rows 与基线一致；backup_lsn > 0 |
| C | 每个 TO LSN 检查点行数精确；末行在、越界行无泄漏 |
| C | TO LSN = backup_lsn 时行数恰为基线，无备份点后数据 |
| D | 恢复后行数 = 基线；首行/末行存在 |

## 输出报告

演练完成后输出 Markdown 报告（默认 `br_pitr_drill_report.md`），包含：

- 演练时间、参数与通过/失败结论；
- 各阶段（A/B/C/D）执行步骤与校验结果；
- 备份文件信息（tables / rows / backup_lsn）；
- PITR 检查点 LSN 与校验明细。

## 约束与说明

- 演练使用真实 RocksDB、WAL 与 binlog 链路（无 mock），可验证 `pkg/sql/backup.go` 与 server 命令入口的完整行为；
- `drill_test.go` 以 `t.TempDir()` 创建临时数据目录、较小数据量（50/6）快速回归，不污染仓库工作区；
- 仓库构建/测试前提：Windows + RocksDB CGO 环境，先执行 `scripts/build.ps1`（或 `-SkipAllBuild` 仅注入环境）。
