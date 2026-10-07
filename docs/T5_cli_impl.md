---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_b8081490c1fd11f18019525400248c00
    ReservedCode1: XTIhRphaNerTIC7FEiE2mAJkIz23c1qxu6wNjfd0NvkfWd7RFt8iKhrr+uMdNe5G3pd8PmjrIpAqI5byYpKJorKXjCKn/58cnQaxqcGSBIeatuTSoidQE8v75l9adDwTOX+/3z4r2qTbq4xNkWPgm05EIEzSO25tpooK7z7hXY+l1jTEUb84/zE6J48=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_b8081490c1fd11f18019525400248c00
    ReservedCode2: XTIhRphaNerTIC7FEiE2mAJkIz23c1qxu6wNjfd0NvkfWd7RFt8iKhrr+uMdNe5G3pd8PmjrIpAqI5byYpKJorKXjCKn/58cnQaxqcGSBIeatuTSoidQE8v75l9adDwTOX+/3z4r2qTbq4xNkWPgm05EIEzSO25tpooK7z7hXY+l1jTEUb84/zE6J48=
---

# T5 协议层 + 数据目录 + CLI 实现记录

- 状态：完成（2026-10-07）
- 里程碑：M1（v2.0-FP）
- 依赖：T1 存储层（rocksdb）/ T2 WAL / T3 事务层
- 全仓回归：storage / rocksdb / wal / txn / db / server 全绿

## 1. 交付模块

| 模块 | 路径 | 说明 |
| --- | --- | --- |
| 数据目录 | `pkg/db` | `Init` / `Open` / `Close`，完成 存储+WAL+事务层 装配与崩溃恢复 |
| 协议层 | `pkg/server` | 行文本协议（PING/GET/SET/DEL/BEGIN/COMMIT/ROLLBACK/QUIT），REPL + TCP |
| CLI | `cmd/openxdb` | `version` / `init` / `repl` / `start` 四子命令 |

## 2. 数据目录布局

```
<data-dir>/
  openxdb.conf   # 引擎、创建时间（Init 写入）
  data/          # RocksDB 数据目录
  wal.log        # WAL（首次 Open 自动创建）
```

- `db.Init(dir)`：创建目录结构，重复 Init 返回 `ErrAlreadyInitialized`。
- `db.Open(dir)`：校验配置 → 打开 RocksDB（createIfMissing=true，首次空目录自动初始化引擎文件）→ 打开 WAL → 装配事务管理器 → **执行崩溃恢复（WAL 重放）**。未 Init 目录返回 `ErrNotInitialized`。
- `db.Close()`：按 事务 → WAL → 存储 逆序关闭。

## 3. 协议规范（逐行文本，大小写不敏感）

| 命令 | 语义 | 响应 |
| --- | --- | --- |
| `PING` | 存活探测 | `PONG` |
| `HELP` | 命令列表 | 命令名列表 |
| `GET <key>` | 读取 | 值文本 / `ERR not found` |
| `SET <key> <value>` | 写入（value 允许空格） | `OK` |
| `DEL <key>` | 删除 | `OK` |
| `BEGIN` | 开启显式事务（嵌套拒绝） | `OK` |
| `COMMIT` | 提交显式事务（无事务时报错） | `OK` |
| `ROLLBACK` | 回滚显式事务（无事务时报错） | `OK` |
| `QUIT` | 结束当前 REPL / 连接 | `BYE` |

事务语义：
- 显式事务内 GET 遵循 read-your-writes + 快照隔离（BEGIN 时刻快照）。
- 无显式事务时，SET/DEL 自动以**隐式事务**立即提交（写即持久化）；GET 以只读事务拿一致性快照（不写 WAL）。
- 命令处理由 Server 内部互斥锁串行化，多 TCP 连接安全。
- 容忍 Windows 控制台 BOM（`\ufeff` 前缀）。

## 4. CLI 用法

```
openxdb version
openxdb init --data-dir <dir>
openxdb repl --data-dir <dir>
openxdb start --data-dir <dir> [--port <n>]   # 默认 7788
```

## 5. 测试

- `pkg/db`：Init 结构 / Open 未初始化报错 / 数据跨重开持久 / 崩溃恢复重放（4 用例）。
- `pkg/server`：SET/GET、含空格 value、GET 缺失、DEL、显式事务提交、显式事务回滚、嵌套 BEGIN、无事务 COMMIT/ROLLBACK、语法错误、PING/HELP、TCP 往返（11 用例）。
- 全仓：`go test ./...` 全部通过。

## 6. 冒烟记录

- REPL：`SET greeting hello-openxdb` → OK；`GET` → hello-openxdb；显式事务 COMMIT 后可见、ROLLBACK 后不可见。
- TCP（127.0.0.1:17788）：PING→PONG，SET→OK，GET→值，QUIT→BYE。

## 7. 踩坑与决策

1. **RocksDB 空目录初始化**：`db.Init` 只创建空 `data/`，RocksDB 以 `create_if_missing=false` 打开会报 CURRENT 不存在；统一用 `createIfMissing=true`（已存在引擎文件时无害）。
2. **Windows 管道 BOM**：PowerShell 管道向 stdin 写入会带 `\ufeff`，首条命令解析失败；协议层 TrimPrefix 容忍。
3. **覆盖写文件**：write_file 对已存在文件自动改名，覆盖 main.go 时需先删旧再 Move-Item。

## 8. 下一步建议

- T6 聚合排序索引（二级索引 + 范围扫描排序）
- E1b B+Tree 对照实现（评估 RocksDB vs B+Tree 单机表现）
- GitHub 上线：README（架构 + 构建方式 + 行协议文档）+ 首个 release
*（内容由AI生成，仅供参考）*
