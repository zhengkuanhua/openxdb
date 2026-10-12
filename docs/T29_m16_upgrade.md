---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_83e7d151c60711f197b3525400248c00
    ReservedCode1: ATGQ4rGwMGweeR+DSkDdZZeYKvuPylNYJNYQxqJHqyB8hwScGx2LzNeH2S7Azen4C1AtcaFhAAEICxWwABYifQjZSdjKtESk0yu7jMlkKkcEDMz7harvA4C3Gb5aLbYTso0kGJAK1fRwWz5NGPuhYY4dgFAXSWy0lmWI0CJr5nqHg+1fczzntvzK6Fc=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_83e7d151c60711f197b3525400248c00
    ReservedCode2: ATGQ4rGwMGweeR+DSkDdZZeYKvuPylNYJNYQxqJHqyB8hwScGx2LzNeH2S7Azen4C1AtcaFhAAEICxWwABYifQjZSdjKtESk0yu7jMlkKkcEDMz7harvA4C3Gb5aLbYTso0kGJAK1fRwWz5NGPuhYY4dgFAXSWy0lmWI0CJr5nqHg+1fczzntvzK6Fc=
---

# T29 M16 运维增强簇（3/3）：灰度升级辅助

> 版本：v5.0-P16-M16, v0.20.0-alpha
> 日期：2026-10-12
> 范围：新增 `pkg/db/upgrade.go`（数据格式版本 marker 与兼容判定）、`cmd/openxdb/version.go`（version 子命令构建信息 + 数据格式版本）、`cmd/openxdb/preflight.go`（preflight 升级前健康检查，复用 `db.Open` / M16-1 持久化配置 / M16-2 优雅停机）、`scripts/rolling_upgrade.ps1`（滚动升级编排）、`cmd/openxdb/main.go`（接入 version/preflight 子命令与 usage）、测试 `pkg/db/upgrade_test.go`、`cmd/openxdb/preflight_test.go`、`cmd/openxdb/version_test.go`、`docs/T29_m16_upgrade.md`（本文档）。
> 闭环：CGO 环境下 `go build ./...` + `go test ./... -count=1` 全绿（含 M16-3 新增用例），未 push
> 提交：`M16: upgrade assist`（本地，未 push）

## 背景
M16 运维增强簇按 Roadmap 依次落地三个子功能：在线配置热更新（1/3，60cd6ce）、优雅停机（2/3，2f05c83）、灰度升级辅助（3/3，本次）。灰度升级辅助在 2f05c83 之上增量开发，目标：为新版本发布提供"升级前健康检查 → 版本信息判定 → 滚动升级编排 → 升级后数据完整性验证"的完整辅助链，同时给出回滚路径（备份 + 旧版本 reopen）。核心原则：**只读检查 + 复用既有能力（`db.Open` 装配、M16-1 持久化配置解析、M16-2 优雅停机），不重复造轮子、不改动已提交代码**。

## 设计

### 数据格式版本（pkg/db/upgrade.go）
- 常量：`DataFormatVersion = 1`（当前二进制数据格式版本）、`DataFormatVersionFile = "data_format_version"`（数据目录 marker 文件名）；
- `ReadDataFormatVersion(dir)`：读取目录 marker 数值；**marker 缺失（旧版本初始化目录）视为版本 1**，向后兼容；
- `CheckDataFormatCompat(dir, binaryFormat)`：目录格式版本 <= 二进制版本 → 兼容（升级路径）；目录格式版本 > 二进制版本 → 返回 `ErrDataFormatNewer`（需先升级二进制，防误开未来数据）；
- 当前版本 `db.Init` 不主动写 marker（保持旧目录兼容），版本升级工具/脚本可在升级完成后落 marker 便于后续判定。

### openxdb preflight 升级前健康检查（cmd/openxdb/preflight.go）
对指定数据目录做**只读**健康检查，输出人类可读清单（`--json` 输出 JSON 报告）。检查项：

| # | 检查项 | 说明 | FAIL 示例 |
|---|---|---|---|
| 1 | `data_dir` | 数据目录存在、`openxdb.conf` 可读且 `engine` 匹配 | 目录未初始化 / engine 不匹配 |
| 2 | `persisted_config` | `persisted.json`（M16-1）JSON 可解析；不存在视为 PASS | 配置文件损坏 |
| 3 | `open_storage` | 复用 `db.Open` 试开：RocksDB + WAL + `txn.Recover` + `LoadPersistedConfig` 全部走通（WAL/存储一致性即事务恢复） | 存储损坏 / WAL 无法恢复 |
| 4 | `version_compat` | 目录数据格式版本 <= 当前二进制版本；`--target-version` 输出目标版本提示 | 目录为未来版本（v999） |

- 退出码：`0`=PASS（全部通过）；`2`=FAIL（任一检查失败）；`1`=WARN（预留）；
- 用法：`openxdb preflight --data-dir <dir> [--target-version <ver>] [--json]`。

### openxdb version 版本信息（cmd/openxdb/version.go）
输出发行版本、构建信息与数据文件格式版本（升级判定依据）：

```
OpenXDB v0.1.0-alpha (M2 master-slave replication)
  build: v0.20.0-alpha
  built at: 2026-10-12T00:00:00Z
  commit: 2f05c83
  data format version: 1
```

- 构建注入规范（`-ldflags`，未注入显示 `dev` / 空）：
  `go build -ldflags "-X main.buildVersion=v0.20.0-alpha -X main.buildTime=2026-10-12T00:00:00Z -X main.buildCommit=<sha>"`;
- 首行保留旧格式（`OpenXDB <version> (M2 master-slave replication)`），向后兼容依赖该行的脚本/工具；
- `data format version` 与 preflight 的 `version_compat` 联动：目标二进制该值 >= 数据目录版本即可升级。

### 滚动升级辅助脚本（scripts/rolling_upgrade.ps1）
编排单节点滚动升级四步骤（多节点依次对每节点执行）：

```
备份(可选) → 步骤1 preflight(旧版或新版检查旧数据，全 PASS 才继续)
          → 步骤2 SHUTDOWN GRACEFUL(TCP, 复用 M16-2，等待端口关闭/存储 flush)
          → 步骤3 启动新版(openxdb start --data-dir --port)
          → 步骤4 升级后复检(PING 就绪 + preflight + doctor --addr 在线检查)
```

用法与参数见脚本 `-Help`（`-DataDir`/`-NewBin` 必填；`-OldBin` 可选指定旧版做升级前检查；`-BackupDir` 可选备份；`-SkipShutdown` 停机已由外部完成时跳过；`-NoStart` 仅检查模式）。退出码：0=成功 1=检查失败 2=停机失败 3=启动失败。

### 与 M16-1 / M16-2 衔接
- preflight `open_storage` 直接复用 `db.Open`（含 M16-1 `LoadPersistedConfig` 对持久化配置键/值合法性的统一校验），不重复实现配置解析；
- `persisted_config` 仅做 JSON 可解析预检，键/值合法性交 `db.Open`；
- 滚动脚本优雅停机通过 TCP 发送 `SHUTDOWN GRACEFUL`（M16-2 命令），停机语义、超时、强制路径数据完整性均由 M16-2 保证；
- `shutdown_timeout`（M16-2）可经 `SET [PERSIST]` 热更新（M16-1）后进入停机流程。

## 升级流程（推荐）
1. 备份数据目录（或使用脚本 `-BackupDir`）；
2. 旧版执行 `openxdb preflight --data-dir <dir>`，全部 PASS；
3. 确认版本兼容：`openxdb version` 的数据格式版本 >= 数据目录格式版本（preflight 已校验）；
4. `SHUTDOWN GRACEFUL`（或 Ctrl-C / SIGTERM）优雅停机，确认存储已关闭；
5. 启动新版二进制 `openxdb start --data-dir <dir> --port <port>`；
6. 升级后复检：`openxdb preflight --data-dir <dir>` + `openxdb doctor --data-dir <dir> --addr <host:port>`；抽样 `GET`/SQL 验证数据完整（测试 `TestPreflightUpgradeReopenDataIntact` 即该路径自动化覆盖）；
7. 确认无误后由运维按需落 `data_format_version` marker（v1 可省略）。

## 限制
- 仅保证**同族版本升级**（数据格式版本 <= 当前二进制版本）；未来格式版本目录会被 preflight 拒绝（`ErrDataFormatNewer`），不得强行打开；
- 不提供数据目录整体迁移/跨版本格式转换；格式版本升级属于未来独立工作；
- 滚动升级脚本为辅助编排，不含进程守护、故障自动回切；多节点场景需运维逐节点编排并配合负载摘除/恢复；
- preflight 为只读检查，不修改数据目录；检查后到停机之间的写入由业务侧自行收敛。

## 回滚说明
回滚 = **备份 + 旧版本 reopen**：
1. 停止新版（`SHUTDOWN GRACEFUL` / 信号停机）；
2. 用升级前备份恢复数据目录（`-BackupDir` 备份的直接还原）；
3. 用旧版二进制 `openxdb start --data-dir <dir>` 重新打开；
4. 执行旧版 `openxdb preflight` 验证可打开、数据完整；
5. 由于同族版本数据格式版本 <= 1 未变，旧版可正常 reopen（marker 缺失视为 v1，兼容）。

## 测试覆盖（M16-3）
| 测试 | 验证点 |
|---|---|
| `pkg/db`: TestReadDataFormatVersionDefaultMarker | 旧目录（无 marker）默认 v1 |
| `pkg/db`: TestReadDataFormatVersionMarker | marker 读取与非法内容报错 |
| `pkg/db`: TestCheckDataFormatCompat | 兼容通过 / 未来版本拒绝（ErrDataFormatNewer） |
| `cmd/openxdb`: TestPreflightPass | 已初始化目录全 PASS，退出码 0 |
| `cmd/openxdb`: TestPreflightUpgradeReopenDataIntact | 升级 reopen 数据完整 + 持久化配置保留 |
| `cmd/openxdb`: TestPreflightFailNotInitialized | 未初始化目录 FAIL（data_dir），退出码 2 |
| `cmd/openxdb`: TestPreflightFailCorruptPersistedConfig | 损坏 persisted.json FAIL，退出码 2 |
| `cmd/openxdb`: TestPreflightFailFutureDataFormat | 未来格式版本 FAIL（version_compat），退出码 2 |
| `cmd/openxdb`: TestPreflightJSONOutput | JSON 报告字段完整 |
| `cmd/openxdb`: TestPreflightTextOutput | 人类可读输出含 RESULT/PASS |
| `cmd/openxdb`: TestVersionTextContainsDataFormat / FirstLineCompat | version 含构建信息与数据格式版本、首行兼容 |

共 11 个 M16-3 用例；连同 M16-1/M16-2 存量用例全绿。
*（内容由AI生成，仅供参考）*
