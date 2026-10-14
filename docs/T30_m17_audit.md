---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_1c9cfc20c6f311f197b3525400248c00
    ReservedCode1: JQaByRObjko0FoP7rJnZ6spi4gS/MLTcPxQF9YMBFPlNNwenuMf2ik7u4SKvsFXbNG8hcdaeu8qPiFnc4pOg5Nv5JyuqdFUT3Rt7g2RikAMzkOx8qda1IZVs8bVbhaYpIkGrW7quzr6jsUkw70imt2P9wI683zyM+kjtl+pvPPr+xPW3cW+QrKjcLks=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_1c9cfc20c6f311f197b3525400248c00
    ReservedCode2: JQaByRObjko0FoP7rJnZ6spi4gS/MLTcPxQF9YMBFPlNNwenuMf2ik7u4SKvsFXbNG8hcdaeu8qPiFnc4pOg5Nv5JyuqdFUT3Rt7g2RikAMzkOx8qda1IZVs8bVbhaYpIkGrW7quzr6jsUkw70imt2P9wI683zyM+kjtl+pvPPr+xPW3cW+QrKjcLks=
---

# T30 M17-1：SQL 审计日志（Audit Log）

## 概述

M17 安全增强里程碑的第一个子功能：为 OpenXDB 引入**可配置的 SQL 审计日志**，
覆盖连接登录/登出、DML、DDL、权限变更等安全相关事件，支持：

- 事件级分类（CONNECT / DISCONNECT / AUTH_FAILED / DML / DDL / PRIVILEGE）
- 可配置开关（`SET audit_log on/off`）与落盘路径（`audit_log_file`）
- 可配置慢审计阈值（`audit_log_threshold`，标记慢的 DML/DDL）
- 内存环形缓冲 + JSON 落盘双通道，`SHOW AUDITLOG [LIMIT n]` 查询
- 持久化支持（`SET PERSIST` 写入 persisted.json，重启恢复 intent）

## 设计

### 事件分类与记录结构

```
AuditEvent: CONNECT | DISCONNECT | AUTH_FAILED | DML | DDL | PRIVILEGE
AuditRecord:
  id          int64    # 单调递增序号（内存缓冲内）
  time        string   # 本地时间 2006-01-02T15:04:05.000
  user        string   # 发起用户（root / 已鉴权用户 / guest 空串）
  event       string   # 事件分类
  object      string   # 目标对象（表名 / 库名 / 用户 / 角色）
  detail      string   # 原始 SQL 文本（连接事件为 "AUTH" / "connection closed"）
  success     bool     # 是否成功
  err         string   # 失败原因（成功为空）
  duration_ms int64    # 执行耗时（DML/DDL；连接事件 0）
  slow        bool     # 是否超过审计慢阈值（仅 DML/DDL）
```

### 触发点（统一埋点，避免裸 switch 漏挂）

| 事件 | 触发位置 | 说明 |
|------|----------|------|
| CONNECT / AUTH_FAILED | `pkg/server` doAuth | AUTH 握手成功/失败，user 为尝试登录名 |
| DISCONNECT | `pkg/server` serveConn 退出 | 已鉴权会话（user != ""）记录登出 |
| DML / DDL / PRIVILEGE | `pkg/sql` executor 统一执行路径 | ExecuteTextAsSession 权限拒绝分支 + execTimed 末尾 |

关键原则（吸取 T13/M13 慢查询踩坑）：**审计挂载在统一执行入口**
`ExecuteTextAsSession` / `execTimed`，而不是分散在 exec* 各分支，保证
新语句天然被覆盖。execTimed 增加 stmt 参数用于事件分类。

- DML：INSERT / UPDATE / DELETE（object = 表名）
- DDL：CREATE/DROP/ALTER TABLE、INDEX、VIEW、MATERIALIZED VIEW、
  TRIGGER、PROCEDURE、IMPORT/EXPORT、BACKUP/RESTORE、集群 DDL
  （object = 目标对象名，提取逻辑见 audit.go `auditEventOf`）
- PRIVILEGE：CREATE/DROP/ALTER USER、GRANT/REVOKE、CREATE/DROP ROLE、
  GRANT/REVOKE ROLE（object = 目标用户/角色/表）
- SELECT / 事务控制等只读语句**不审计**（避免噪声，文档声明）

### 开关、阈值与持久化

沿用 M16-1 配置注册表（configSpecs）扩展三个键：

| 键 | 默认 | 语义 |
|----|------|------|
| `audit_log` | on | 审计总开关（on/off/1/0） |
| `audit_log_file` | （启动装配） | 落盘路径，空 = 不落盘（内存仍记录） |
| `audit_log_threshold` | 0 | DML/DDL 慢审计阈值 ms；>0 时标记 slow 字段，0 不标记 |

优先级：内存 > 持久化 > 默认；`SET PERSIST` 落盘 persisted.json，
`PersistedAuditLogIntent` 供服务启动装配路径时恢复开关。

### 落盘格式

审计事件低频，采用 JSON 一行一条（`encoding/json` 天然处理引号/换行转义）：

```json
{"id":1,"time":"2026-10-13T10:00:00.000","user":"alice","event":"DML","object":"users","detail":"INSERT INTO users VALUES (1,'a',30)","success":true,"duration_ms":2,"slow":false}
```

写失败静默忽略（审计不阻塞 SQL 执行），返回错误供测试断言。

### 内存缓冲与查询

- `Executor.auditRecords` 环形缓冲，`auditMax` 默认 1000 条，满了丢弃最旧
- `SHOW AUDITLOG [LIMIT <n>]`：返回最近 n 条（倒序展示），列
  `id, time, user, event, object, success, duration_ms, slow, error, detail`
- 审计记录为管理员功能：`SHOW AUDITLOG` 归入 admin 语句（仅 root）

## 测试要点

- 开关关闭时 DML 不产生审计记录；开启后 DML/DDL/权限变更均记录
- 权限拒绝路径（guest 无授权 DML）记录 success=false 的 DML 事件
- SHOW AUDITLOG 返回最近记录且按 LIMIT 截断
- 落盘 JSON 可反序列化回 AuditRecord（Round-trip）
- SET audit_log / SET audit_log_threshold 热更新即时生效
- server 层：AUTH 成功记录 CONNECT、失败记录 AUTH_FAILED、连接关闭记录 DISCONNECT

## 兼容性

- 未装配审计（默认路径为空）时零开销：auditEnabled 判定在最前，普通语句
  仅一次 bool 判断；落盘仅在事件产生后追加写
- 配置键新增不破坏既有 SET / SET PERSIST / SHOW CONFIG 行为
- 既有 slow query / query cache 行为零回归
*（内容由AI生成，仅供参考）*
