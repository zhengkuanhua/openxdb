---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_21a7466fc5f011f19063525400393706
    ReservedCode1: BY9h0qVKCLR0xq91LqhDJxteTNkbD+H5J0Zd55W9r3BWjvFDFEAaLlN2rc3auc1gJCyxqLpwjjy/FtQ1P2LKq1WrnlomVsTzLQ04XguggqGXVgk94hZ8SILJjTdlbaqSthgJR4Pp5lA+mmjyLvNXi/35hRL5tvT1M2AaUHh1xAovCngI9//YWb8RvTY=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_21a7466fc5f011f19063525400393706
    ReservedCode2: BY9h0qVKCLR0xq91LqhDJxteTNkbD+H5J0Zd55W9r3BWjvFDFEAaLlN2rc3auc1gJCyxqLpwjjy/FtQ1P2LKq1WrnlomVsTzLQ04XguggqGXVgk94hZ8SILJjTdlbaqSthgJR4Pp5lA+mmjyLvNXi/35hRL5tvT1M2AaUHh1xAovCngI9//YWb8RvTY=
---



# T29 M16 运维增强簇（1/3）：在线配置热更新

> 版本：v5.0-P16-M16, v0.20.0-alpha
> 日期：2026-10-12
> 范围：`pkg/sql`（ast.go SetStmt 增加 `Persist` 字段并新增 `ShowConfigStmt`，parser.go parseSet 支持 `SET PERSIST` 前缀、parseShow 增加 CONFIG / PERSISTED CONFIG 分支，executor.go execSet 重构为配置注册表驱动并增加 M16-1 配置字段，新增 config.go 与 config_test.go）、`pkg/sql/sql.go`（Engine 透传 SetPersistedConfigPath / LoadPersistedConfig / PersistedSlowLogIntent）、`pkg/db/db.go`（新增 `PersistedConfigFile = "persisted.json"`，Open 装配并加载持久化配置）、`cmd/openxdb/main.go`（慢日志落盘开关尊重持久化 intent）、`docs/T29_m16_hot_config.md`（本文档）。
> 闭环：CGO 环境下 `go build ./...` + `go test -count=1 ./pkg/sql/...` 全绿（含 11 个 M16 配置用例），未 push
> 提交：`M16: hot config reload`（本地，未 push）

## 背景
M16 运维增强簇按 Roadmap 依次落地三个子功能：在线配置热更新（1/3，本次）、优雅停机（2/3）、灰度升级辅助（3/3）。在线配置热更新在远端 HEAD 6e0bbdb（M15 全簇）之上增量开发，目标：运行中的数据库支持通过 SQL 修改真实配置项并立即生效，其中 `SET PERSIST` 可持久化到配置文件，重启后仍生效，满足"不重启改配置"的运维诉求。

## 设计

### 语法（M16-1 新增）
| 语句 | 行为 |
|---|---|
| `SET PERSIST <key> = <value>` | 立即生效 + 持久化到配置文件（data 目录 `persisted.json`），重启后仍生效 |
| `SET <key> = <value>` | 仅当前会话/进程生效，不持久化；重启后由持久化配置或默认值接管 |
| `SHOW CONFIG` | 查看当前生效配置：`variable / value / source` 三列，source 为 `default` / `persisted` / `session` |
| `SHOW PERSISTED CONFIG` | 查看已持久化配置：仅列出已写入配置文件的键值对（`variable / value`） |

### 支持的配置项清单（真实存在，源码 grep 确认，禁止虚构）
| key | 说明 | 合法值 | 默认值 | 来源 |
|---|---|---|---|---|
| `query_cache` | 查询结果缓存开关（M9） | `on/off/1/0`（true/false 同义） | `off` | `pkg/sql/query_cache.go` |
| `slow_query_threshold` | 慢查询阈值毫秒（M13），`<=0` 关闭记录 | 整数（毫秒） | `100` | `pkg/sql/executor.go slowThreshold` |
| `slow_query_log` | 慢查询落盘开关（M13），重启生效依赖启动装配的日志路径 | `on/off/1/0` | `off` | `pkg/sql/slowlog.go` |

非法键报 `unknown session variable: <key>`；非法值/类型错误按配置项给出明确报错（如 `invalid value for slow_query_threshold: abc (expected integer ms)`），绝不静默忽略。`slow_query_log` 落盘依赖日志路径：路径未装配时置 `on` 保持关闭（安全默认），服务装配路径后按持久化 intent 恢复。

### 持久化与优先级
- **持久化文件**：`<data-dir>/persisted.json`（`db.PersistedConfigFile`），JSON 对象 `{ "<key>": "<value>", ... }`；不存在 = 首次启动空表；写入采用临时文件 + 改名原子写，避免半写损坏；
- **加载**：`db.Open` 装配路径并调用 `LoadPersistedConfig` 自动合并；逐项校验应用，任一非法键 / 非法值 / 损坏 JSON 整体失败并明确报错（`db: load persisted config: ...`），拒绝静默忽略；
- **优先级**：内存（`SET` / `SET PERSIST` 立即生效）> 持久化（重启合并）> 默认值；
- **服务装配**：`cmd/openxdb/main.go` 启动时通过 `PersistedSlowLogIntent` 读取持久化 `slow_query_log` intent（1=on / 0=off / -1=无记录），无记录时保持默认开启慢查询落盘。

### 与现有 SET 兼容（零回归）
`execSet` 重构为配置注册表（`configSpecs`）驱动：校验与应用逻辑完全复用 M9/M13 原有实现与报错文案，`SET query_cache` / `SET slow_query_threshold` / `SET slow_query_log` 原有行为不变；非法键仍报 `unknown session variable`。

### 实现结构
- `pkg/sql/config.go`：`configSpec` 注册表（key/desc/def/apply/read）、`persistConfigFile` 读写、`LoadPersistedConfig` / `persistConfigValue` / `execShowConfig`；
- `Executor` 新增：`cfgPath`（持久化文件路径）、`cfgMu`（配置互斥）、`persistedCfg` / `sessionCfg`（来源视图）；
- `ast.go`：`SetStmt.Persist bool`、`ShowConfigStmt{Persisted bool}`；`parser.go`：`SET PERSIST` 前缀解析、`SHOW CONFIG` / `SHOW PERSISTED CONFIG` 分支。

## 限制（文档化）
- 仅支持注册表内三个真实配置项，新增配置项需在 `configSpecs` 注册并提供 apply/read；
- `SET PERSIST` 依赖已装配持久化路径：未装配时明确报错 `persisted config not available`（`db.Open` 默认装配，测试中显式清空路径可复现）；
- 持久化配置仅记录原始值文本，不记录配置项版本；跨版本键语义变化由启动校验兜底报错；
- `SHOW CONFIG` 展示的 value 为当前生效值，source 反映其来源（default / persisted / session）。

## 测试（11 用例，全绿）
| 用例 | 覆盖 |
|---|---|
| `TestHotConfigShowDefaults` | SHOW CONFIG 默认值 + default 来源；SHOW PERSISTED CONFIG 初始为空 |
| `TestHotConfigSetSessionNotPersisted` | SET 生效、source=session、不落盘、重启回退默认值 |
| `TestHotConfigSetPersistEffectiveAndReload` | SET PERSIST 生效 + 落盘文件 + 重启仍生效（source=persisted） |
| `TestHotConfigSetPersistSlowLogIntent` | 持久化 slow_query_log intent（off→0）跨重启保持 |
| `TestHotConfigInvalidKey` | SET / SET PERSIST 非法键明确报错且不落盘 |
| `TestHotConfigInvalidValue` | 非法值/类型错误明确报错且不落盘 |
| `TestHotConfigShowPersistedOnly` | SHOW PERSISTED CONFIG 仅列出已持久化键；SHOW CONFIG 区分来源 |
| `TestHotConfigCompatExistingSet` | 兼容既有 SET slow_query_threshold（阈值口径 + 原报错） |
| `TestHotConfigPersistRequiresPath` | 未装配持久化路径时 SET PERSIST 明确报错 |
| `TestHotConfigCorruptFileRejected` | 损坏 JSON / 未知键 / 非法值：Open 明确报错拒绝启动 |
