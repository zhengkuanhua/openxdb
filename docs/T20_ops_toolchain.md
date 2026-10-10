---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_bae9a31dc48811f19063525400393706
    ReservedCode1: /bCC+14QfcCwPZWtTvhJCuAwIX//7mcsAaVUYCt1ASgIMJCe3m+6OSRpEkmjC6cunKNHz+8u1OGZ2gP5u3htUHcGCGmzme6hDVcn0A3X0eWaJl33EBB2AWxvtyTQWx+brjgRxwWBariDuMQsEucKoalt46nyMARFCxcScP2g5v7yeb6xJiYplQylsYg=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_bae9a31dc48811f19063525400393706
    ReservedCode2: /bCC+14QfcCwPZWtTvhJCuAwIX//7mcsAaVUYCt1ASgIMJCe3m+6OSRpEkmjC6cunKNHz+8u1OGZ2gP5u3htUHcGCGmzme6hDVcn0A3X0eWaJl33EBB2AWxvtyTQWx+brjgRxwWBariDuMQsEucKoalt46nyMARFCxcScP2g5v7yeb6xJiYplQylsYg=
---

# T20 运维工具链（doctor 巡检 + stats 监控 + 一键部署）

> 功能簇 OT（Ops Toolchain），版本 v5.0-P9-OT / v0.11.0-alpha。
> 本文档覆盖：巡检项与退出码、监控指标定义、部署脚本用法、测试清单。

## 1. 概览

T20 为 OpenXDB 提供生产可用的运维能力，共三件套：

| 组件 | 位置 | 职责 |
|------|------|------|
| 巡检 `openxdb doctor` | `cmd/openxdb/doctor.go` | 对数据目录 / 本地服务做健康巡检，输出人类可读报告或 JSON，退出码表达健康等级 |
| 监控 `openxdb stats` | `cmd/openxdb/stats.go` | 对本地 / 远端服务拉取关键运行指标，输出对齐表格或 JSON，支持二次采样计算 binlog 增长 |
| 一键部署 | `scripts/deploy.ps1` / `scripts/deploy.sh` | 参数化完成 init + start + 健康自检 + 部署摘要 |

配套内核改动：

- `pkg/server`：新增活跃 TCP 会话计数（`conns` 原子计数 + `ActiveConns()`），供监控指标使用；
- `pkg/sql`：新增运维语句 `SHOW STATS`（`ShowStatsStmt` / `execShowStats`），输出连接数、表数、region 数、慢查询数、binlog 位点五类指标；`Engine.SetStatsSources` 透传统计源注入；
- `pkg/replication`：新增只读 `PeekLastLSN(path)`（不创建文件），供 binlog 位点巡检；
- `pkg/db`：新增 `BinlogLSN()`（未启用复制时读 binlog 文件，不存在返回 0）；
- `cmd/openxdb`：`start` 装配 `SHOW STATS` 统计源（`SetStatsSources(s.ActiveConns, d.BinlogLSN)`）。

## 2. 巡检 `openxdb doctor`

### 2.1 用法

```
openxdb doctor --data-dir <dir> [--addr <host:port>] [--json]
```

- `--data-dir`：必填，被巡检的数据目录；
- `--addr`：可选，在线检查地址（端口连通 + 协议 PING + 慢查询可用性）；
- `--json`：可选，输出 JSON 报告。

### 2.2 巡检项

| # | 巡检项 | 检查内容 | 失败/警告判据 |
|---|--------|----------|----------------|
| 1 | `data_dir` | 目录存在性、`openxdb.conf` 可读且含 `engine = rocksdb` | 目录缺失/不可访问、配置缺失或引擎不符 → error |
| 2 | `wal` | WAL 文件存在性、大小、可打开 | 缺失/打不开 → error；空文件 → warn（空库正常） |
| 3 | `rocksdb` | 以 `createIfMissing=false` 打开 `data/` 引擎目录 | 打开失败（数据损坏/未初始化）→ error |
| 4 | `metadata` | `m:tables` 元数据可读（`ErrNotFound` 视为空库正常） | 读取异常 → error |
| 5 | `region_routes` | `m:regions` 可读且 JSON 自洽（region_id 合法、node 字符串） | 解析失败/记录非法 → error；`ErrNotFound` 视为空库正常 |
| 6 | `binlog` | binlog 文件存在性、只读 `PeekLastLSN` 成功、输出当前位点 | 未启用复制（文件不存在）→ warn；读取失败 → error |
| 7 | `port` | 在线检查：TCP 连接 + `PING`→`PONG` 协议响应 | 未传 `--addr` → warn；连不上/协议异常 → error |
| 8 | `slowqueries` | 在线检查：`SHOW SLOWQUERIES` 可执行并返回行数 | 未传 `--addr` → warn；返回 `ERR` → error |

### 2.3 退出码语义

| 退出码 | 语义 |
|--------|------|
| 0 | 全部通过（无警告无错误） |
| 1 | 有警告（如未启用复制、未提供在线地址） |
| 2 | 有错误（目录缺失 / 引擎打不开 / 端口不通 / 元数据异常等） |

### 2.4 输出示例

文本输出（人类可读）：

```
OpenXDB doctor: data dir D:\data
  [PASS] data_dir - 目录与配置文件正常
  [PASS] wal - WAL 存在且可打开
  [PASS] rocksdb - RocksDB 打开成功
  [PASS] metadata - m:tables 可读
  [PASS] region_routes - m:regions 可读且路由自洽
  [PASS] binlog - binlog 可读，当前位点 LSN=42
  [WARN] port - 未指定 --addr，跳过在线端口检查
  [WARN] slowqueries - 未指定 --addr，跳过在线慢查询检查
result: WARNINGS (exit code 1)
```

JSON 输出（`--json`）：

```json
{
  "data_dir": "D:\\data",
  "checks": [
    {"name": "data_dir", "status": "ok", "message": "..."},
    ...
  ],
  "exit_code": 1,
  "generated_at": "2026-10-10T..."
}
```

## 3. 监控 `openxdb stats`

### 3.1 用法

```
openxdb stats [--addr <host:port>] [--interval <sec>] [--json]
```

- `--addr`：服务地址，默认 `127.0.0.1:7788`（支持远端）；
- `--interval <sec>`：二次采样间隔（秒），用于计算 binlog 位点增长；
- `--json`：输出 JSON 指标。

### 3.2 指标定义

| 指标 | 定义 | 来源 |
|------|------|------|
| `connection_count` | 当前活跃 TCP 连接数（含采样连接自身） | `SHOW STATS`（server 会话计数 `ActiveConns`） |
| `table_count` | 当前表数量 | `SHOW TABLES` 行数 |
| `region_count` | 当前 region 总数 | `SHOW REGION ROUTES` 行数（跳过表头/分隔线） |
| `route_distribution` | region 按归属节点分布（`(local)` = 本节点） | `SHOW REGION ROUTES` node 列聚合 |
| `slow_query_count` | 慢查询记录条数 | `SHOW SLOWQUERIES` 行数 |
| `binlog_lsn` | binlog 当前位点 | `SHOW STATS`（db `BinlogLSN`） |
| `binlog_growth` | 两次采样间 binlog 增长量 | 二次采样（`--interval`）差分 |

`SHOW STATS` 文本表格式（metric | value 两列）：

```
connection_count | 1
table_count      | 2
region_count     | 2
slow_query_count | 0
binlog_lsn       | 0
```

### 3.3 输出示例

```
OpenXDB stats: 127.0.0.1:7788 (collected at 2026-10-10T10:00:00+08:00)
metric             value
------------------ ------------
connections        1
tables             2
regions            2
slow_queries       0
binlog_lsn         42
route distribution:
  (local)          2
```

失败（连接失败 / 协议不支持 / 解析错误）退出码 2，成功退出码 0。

## 4. 一键部署脚本

### 4.1 `scripts/deploy.ps1`（Windows PowerShell）

```
.\deploy.ps1 -DataDir <dir> [-Port <n>] [-Bin <path>] [-ReplicaPort <n>] [-ClusterAddr <addr>] [-NoStart] [-Help]
```

| 参数 | 说明 |
|------|------|
| `-DataDir` | 数据目录（必填） |
| `-Port` | 服务监听端口，默认 7788 |
| `-Bin` | openxdb 可执行文件路径（默认 PATH 中的 `openxdb`） |
| `-ReplicaPort` | 主节点复制端口（可选，启用 M2 复制） |
| `-ClusterAddr` | 集群节点链路地址（可选，启用 M4 节点链路） |
| `-NoStart` | 仅初始化与检查，不启动服务 |

执行流程：`openxdb init` → `Start-Process openxdb start` → TCP 就绪探测（40 × 500ms）→ `openxdb doctor --data-dir <dir> --addr 127.0.0.1:<port>` → 输出部署摘要（PID / 端口 / 数据目录 / 日志路径）。

### 4.2 `scripts/deploy.sh`（Linux / macOS）

```
./deploy.sh -DataDir <dir> [-Port <n>] [-Bin <path>] [-ReplicaPort <n>] [-ClusterAddr <addr>] [-NoStart] [-Help]
```

参数语义与 PowerShell 版本一致；流程：`openxdb init` → `nohup openxdb start &` → `/dev/tcp` 就绪探测 → `openxdb doctor` → 部署摘要。

### 4.3 部署摘要

```
=== OpenXDB 部署摘要 ===
PID:          12345
端口:         7788
数据目录:     D:\openxdb-data
stdout 日志:  D:\openxdb-data\deploy.stdout.log
stderr 日志:  D:\openxdb-data\deploy.stderr.log
状态:          运行中 (doctor 通过)
```

## 5. 测试清单

| 级别 | 内容 | 位置 |
|------|------|------|
| 单元 | doctor 正常目录无 error、损坏 WAL/引擎 → 退出码 2、目录缺失 → 2、JSON 可解析 | `cmd/openxdb/doctor_test.go` |
| 单元 | doctor 行数解析（文本表 `(N rows)`） | `cmd/openxdb/doctor_test.go` |
| 单元 | stats 本地服务采集（连接数/表数/region 数/慢查询数/binlog 位点）、建表后指标增长、连接拒绝报错 | `cmd/openxdb/stats_test.go` |
| 单元 | deploy 脚本语法级验证：PowerShell `[scriptblock]::Create` 解析通过、`bash -n` 通过（bash 存在时）、脚本存在且非空 | `cmd/openxdb/deploy_test.go` |
| 冒烟 | 本机端到端：`openxdb init` + `openxdb start` + `openxdb doctor --addr` 通过 + 停服（验收环节以真实二进制执行） | — |
| 回归 | 全仓 `go build ./...`、`go test ./... -count=1` 全绿 | 验收 |

## 6. 验收环境（本机 CGO 工具链）

```
PATH:            C:\Users\27756\AppData\Local\OpenXDBTools\mingw64\bin
CGO_CFLAGS:      -IC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-11.8.1\include
CGO_CXXFLAGS:    -IC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-11.8.1\include
CGO_LDFLAGS:     -LC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-build -lrocksdb -lshlwapi -lrpcrt4 -lws2_32 -static
```

注意：`shell_executor` 跑 go 命令会吞输出，须用 `python_executor` 的 `subprocess.run(capture_output=True)` 拿完整结果；pytest 使用 `py -m pytest`。

## 7. 边界语义

- **只读优先**：doctor 不修改数据目录（引擎以 `createIfMissing=false` 打开、binlog 用只读 PeekLastLSN）；
- **空库正常**：`m:tables` / `m:regions` 的 `ErrNotFound`、空 WAL 均为警告而非错误；
- **退出码是协议**：脚本（deploy）与 CI 均按 0/1/2 决策，`>=2` 视为失败；
- **在线检查可选**：未传 `--addr` 时 doctor 不阻塞，以警告提示。
*（内容由AI生成，仅供参考）*
