---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_bc8cf807c86c11f18b7f525400393706
    ReservedCode1: fdaLlR/lKhV3WUBeLaNqJGqXYOloCAzb0lko+SeMCCA0xaZHwqXjHkUmmXEFvW8cjTNZzQ3KPQg8hG6vOYBSWGl5gIhQG/V+m0xmEbKcCBHs4nMm8dTgNMpJnFhxUvJj45C2blO46Epd6Cvbql+RlDyq7n/GdDyqfs3DvzJYuiVW7Fw7qBXWsI3f4AI=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_bc8cf807c86c11f18b7f525400393706
    ReservedCode2: fdaLlR/lKhV3WUBeLaNqJGqXYOloCAzb0lko+SeMCCA0xaZHwqXjHkUmmXEFvW8cjTNZzQ3KPQg8hG6vOYBSWGl5gIhQG/V+m0xmEbKcCBHs4nMm8dTgNMpJnFhxUvJj45C2blO46Epd6Cvbql+RlDyq7n/GdDyqfs3DvzJYuiVW7Fw7qBXWsI3f4AI=
---

# T37 商用档：监控告警（告警规则与阈值配置）

- 版本：v0.26.0-alpha（T37）
- 状态：已实现
- 关联：M13 可观测性（Prometheus 指标 / STATUS / SHOW STATS）、T20 运维工具链（doctor/stats CLI）、日志告警体系（pkg/logx 事件告警 + SHOW ALERTS）

## 1. 背景与目标

v1.0 商用档第二项：监控告警。目标是在已有可观测性基础上补齐**主动告警**能力：

1. 告警规则定义与存储：以指标阈值规则表达（阈值、持续时长、严重级别），支持热更新与重启持久化。
2. 告警评估引擎：定时评估指标快照，状态机 NORMAL/ALERT/RESOLVED，持续时长防抖。
3. 告警通知渠道：复用日志告警体系的文件 + webhook 出口，规则告警与事件告警同源展示（SHOW ALERTS）。
4. 告警管理语句：SHOW ALERT RULES / SHOW ALERT STATUS / SET alert_* 配置。
5. 与 Prometheus 指标同源：评估数据源为 pkg/metrics.Registry.Snapshot()，不另造指标口径。

## 2. 现状盘点（T37 前已有基础）

- pkg/metrics/metrics.go：Prometheus registry，Snapshot() 输出结构化快照（SQL/KV 总量与 QPS、活跃连接、慢查询累计、存储字节、region 分片、事务提交/回滚、本地/远端访问）。M13 已有。
- pkg/server/status.go：STATUS / SHOW STATS 聚合快照（读 metrics 注入源 + SQL 引擎）。
- pkg/logx/alerts.go：事件告警管理器（AlertConfig：Enabled/File/Webhook/Keep；Alert() 固定 error 级；内存环形缓冲 + JSON 行文件 + webhook 推送；DefaultAlerts 全局实例）。SHOW ALERTS / SHOW ALERT STATUS 已存在（pkg/sql/alert.go）。
- pkg/sql/config.go：M16-1 配置注册表（configSpecs）+ persisted.json 持久化（SET PERSIST / LoadPersistedConfig / persistOnly 启动级键 T36 tls_*）。已存在 alert_enabled/alert_file/alert_webhook/alert_keep 四个事件告警出口配置键。
- cmd/openxdb/main.go：start 装配 metrics registry（SetMetrics）、注入采集源、StartBackupScheduler 调度循环模式（500ms 节拍 + 配置唤醒）。

**缺口（T37 补齐）**：无"指标阈值规则"型告警：无法表达"活跃连接超过 N 持续 M 秒"这类规则；无规则存储与评估循环；无规则管理语句；通知渠道虽有但仅限事件告警（固定 error 级，无严重级别区分）。

## 3. 架构设计

### 3.1 分层模型

```
用户/SQL 客户端                    服务端
----------------                 ---------------------
SHOW ALERT RULES  ────────────►  Executor.execShowAlertRules
SHOW ALERT STATUS ────────────►  Executor.execShowAlertStatus（扩展）
SET alert_rules* / SET PERSIST ─► configSpecs（热更新/落盘）
                                 AlertRulesEngine（pkg/sql）
                                   │ 定时 Evaluate
                                   ▼
                              metrics.Registry.Snapshot()（M13 同源）
                                   │ 越界 + 持续时长
                                   ▼
                              logx.DefaultAlerts.Notify（文件 + webhook + SHOW ALERTS）
```

- 规则模型与评估引擎位于 pkg/sql（Executor 持有，与 backupScheduler 同模式），数据源为已注入的 metrics.Registry（e.m，nil 时评估跳过，保持未装配兼容）。
- 通知出口复用 logx.DefaultAlerts：规则告警进入与事件告警相同的环形缓冲 / 文件 / webhook，SHOW ALERTS 天然可见，无需新通道。

### 3.2 规则模型（AlertRule）

```json
{
  "id": "conns_high",
  "metric": "active_conns",
  "op": ">",
  "threshold": 1000,
  "duration_sec": 30,
  "severity": "critical",
  "enabled": true
}
```

| 字段 | 取值 | 说明 |
|---|---|---|
| id | 字符串 | 规则唯一标识（同一规则集内唯一，重复报错） |
| metric | 见下表 | 取自 metrics.Snapshot 的指标名 |
| op | > >= < <= | 比较运算符 |
| threshold | 非负浮点数 | 阈值 |
| duration_sec | 非负整数 | 越界需持续秒数；0 = 立即触发 |
| severity | info / warn / critical | 严重级别 |
| enabled | true/false | 规则开关 |

支持指标（与 metrics.Snapshot 字段一一对应，小写命名）：

| metric | 快照字段 | 说明 |
|---|---|---|
| sql_total | SQLTotal | SQL 查询累计数 |
| kv_total | KVTotal | KV 命令累计数 |
| sql_qps | SQLQPS | SQL QPS（当前秒窗） |
| kv_qps | KVQPS | KV QPS（当前秒窗） |
| active_conns | ActiveConns | 活跃 TCP 连接数 |
| slow_query_total | SlowQueryTotal | 慢查询累计数 |
| storage_bytes | StorageBytes | 存储总数据量字节 |
| txn_commit | TxnCommit | 事务提交累计数 |
| txn_rollback | TxnRollback | 事务回滚累计数 |
| local_total | LocalTotal | 本节点数据访问累计数 |
| remote_total | RemoteTotal | 远端转发数据访问累计数 |
| region_total | Regions.Total | region 总数 |
| region_local | Regions.Local | 本地 region 数 |
| region_remote | Regions.Remote | 远端 region 数 |

未知 metric / 非法 op / 负 threshold / 负 duration / 非法 severity / 重复 id 均返回明确报错，拒绝写入。

### 3.3 评估引擎与状态机

AlertRulesEngine（Executor.alertRules 字段，NewExecutor 装配）持规则集与每规则状态：

```
state: NORMAL | ALERT | RESOLVED
violatingSince: time.Time（NORMAL 下首次越界时刻；未越界为零值）
lastValue: float64（最近一次评估值，SHOW ALERT RULES 展示）
```

Evaluate(snapshot, now) 对每条 enabled 规则：

| 当前状态 | 本轮越界 | 行为 |
|---|---|---|
| NORMAL | 否 | 保持 NORMAL，violatingSince 清零 |
| NORMAL | 是 | 记录 violatingSince=now；now- violatingSince >= duration 时转 ALERT 并通知（level=severity, event=rule_triggered） |
| ALERT | 是 | 保持 ALERT（不重复通知） |
| ALERT | 否 | 转 RESOLVED 并通知（level=info, event=rule_resolved，一次性恢复通知） |
| RESOLVED | 否 | 保持 RESOLVED（不重复恢复通知） |
| RESOLVED | 是 | 回 NORMAL 并设 violatingSince=now（重新计时 duration，防抖） |

- 总开关 alert_rules_enabled=off 或 e.m==nil（未装配指标）时 Evaluate 直接跳过，不产生任何通知。
- 通知内容：`{"time","level","module":"alert-rules","event","msg"}`，msg 含规则 id/指标/阈值/当前值/严重级别/持续时长。

### 3.4 通知渠道

- 复用 logx.DefaultAlerts（文件 + webhook + 内存环形缓冲）。
- pkg/logx 新增 `Notify(level, module, event, msg)` 方法：带严重级别的告警写入（现有 `Alert()` 保持原语义，内部转发 Notify("error", ...)）。
- 规则触发通知 level=规则 severity；恢复通知 level=info。SHOW ALERTS 展示 time/level/module/event/msg。

### 3.5 管理语句与配置

新增/扩展 SQL 语句：

| 语句 | 行为 |
|---|---|
| SHOW ALERT RULES | 列出规则：id/metric/op/threshold/duration/severity/enabled/state/last_value |
| SHOW ALERT STATUS | 扩展行：alert_rules_enabled / alert_eval_interval / alert_rule_count（保持原 variable/value 两列） |
| SET alert_rules_enabled = on/off | 规则告警总开关（即时生效） |
| SET alert_eval_interval = N | 评估周期秒（1–3600，默认 10；即时生效） |
| SET alert_rules = '<json 数组>' | 全量替换规则集（即时生效；校验失败不生效） |
| SET PERSIST alert_rules* | 落盘 persisted.json，重启 LoadPersistedConfig 自动恢复 |

新增配置键（configSpecs 注册表，顺序排在现有 alert_* 之后）：

| 键 | 默认值 | 说明 |
|---|---|---|
| alert_rules_enabled | off | 规则告警总开关（on/off/1/0） |
| alert_eval_interval | 10 | 评估周期秒（正整数） |
| alert_rules | "" | 规则 JSON 数组文本（空 = 无规则） |

优先级与既有配置一致：内存 SET / SET PERSIST 立即生效 > 持久化（重启合并）> 默认值。SET PERSIST 写 JSON 原文本，重启 apply 时重新解析校验。

### 3.6 评估循环装配

- 模式复制 backupScheduler：固定 500ms 节拍 + 配置唤醒，Engine.StartAlertRules() 启动、StopAlertRules() 停止（幂等）。
- cmd/openxdb/main.go start：metrics 装配后调用 d.SQL.StartAlertRules()；关闭流程 StopAlertRules()（与 StopBackupScheduler 相邻）。
- 评估周期由 alert_eval_interval 控制：节拍轮询检查距上次评估 >= interval 时执行 Evaluate。
- 未配置规则 / 总开关 off / 未装配指标 时循环空转，开销可忽略。

## 4. 测试方案

新增 pkg/sql/alert_rules_test.go（不依赖外部服务）：

1. 规则 JSON 校验：合法规则通过；未知 metric / 非法 op / 负阈值 / 负 duration / 非法 severity / 重复 id / 损坏 JSON 均报错。
2. 状态机触发：duration=0 立即 ALERT；duration>0 时先 PENDING 后 ALERT（注入假时钟推进）；通知内容含级别/指标/当前值。
3. 状态机恢复：ALERT→RESOLVED 发 info 恢复通知；RESOLVED 后保持（不重复）；RESOLVED 后再越界重新计时 duration。
4. 总开关与未装配指标：alert_rules_enabled=off 不通知；e.m=nil 不 panic。
5. 配置热更新与持久化：SET alert_rules / alert_rules_enabled / alert_eval_interval 即时生效；SET PERSIST 落盘 + 重启（重新 Open）恢复（复用 config_test 的 initEngineDir/openEngineDir 模式）。
6. SHOW 语句集成：SHOW ALERT RULES 返回规则行与状态；SHOW ALERT STATUS 含新增行。
7. logx.Notify：带级别写入环形缓冲与 SHOW ALERTS 展示（pkg/logx/alerts_test.go 扩展或 alert_rules 测试内验证）。

回归约束：go test ./... -count=1 -p 1 全绿（并行端口竞争串行验证）。

## 5. 兼容性

- 现有 SHOW ALERTS / SHOW ALERT STATUS / alert_enabled/alert_file/alert_webhook/alert_keep 语义零变化（alert.go 原逻辑保留，仅扩展 SHOW ALERT STATUS 行）。
- logx.AlertManager.Alert() 签名不变（内部转发 Notify）。
- 指标与 CLI（stats/doctor/metrics 端点）不动；未配置规则时行为与 T37 前一致。
- 新增键全部可 SET PERSIST，旧 persisted.json 文件不受影响（新键缺省即默认值）。

## 6. 交付清单

- 设计文档：docs/T37_commercial_alerting.md
- 代码：pkg/sql/alert_rules.go（规则模型/引擎/调度/Executor 方法/execShowAlertRules）、pkg/sql/ast.go、pkg/sql/parser.go（SHOW ALERT RULES）、pkg/sql/executor.go（字段/初始化/分发）、pkg/sql/alert.go（SHOW ALERT STATUS 扩展）、pkg/sql/config.go（3 个配置键）、pkg/sql/sql.go（Engine 装配）、pkg/logx/alerts.go（Notify）、cmd/openxdb/main.go（启动/停止装配）
- 测试：pkg/sql/alert_rules_test.go
- README：Status 行 + Roadmap 勾选 T37 + 版本 v0.26.0-alpha
*（内容由AI生成，仅供参考）*
