# OpenXDB 真实用户验证指南（T38）

> 适用版本：v0.27.0-alpha（2026-10-15）
> 环境：Windows 11 + Go + RocksDB（CGO）单机
> 目的：以真实用户动作（TCP 协议连接、真实运维命令）端到端验证 OpenXDB 商用能力面，暴露文档与兼容盲区。

## 1. 前置准备

```powershell
# CGO 编译环境（RocksDB 11.8.1）
$env:CGO_CFLAGS   = '-ID:\AI项目\OpenXDB开源数据库开发\OpenXDBTools\rocksdb-11.8.1\include'
$env:CGO_CXXFLAGS = '-ID:\AI项目\OpenXDB开源数据库开发\OpenXDBTools\rocksdb-11.8.1\include'
$env:CGO_LDFLAGS  = '-LD:\AI项目\OpenXDB开源数据库开发\OpenXDBTools\rocksdb-build -lrocksdb -lm'
```

> 注意：严禁添加 `-lstdc++` / `-lz` 等链接项，否则破坏既有编译约束。

## 2. 快速运行（一键）

```powershell
powershell -ExecutionPolicy Bypass -File validation\t38\run_validation.ps1
```

脚本自动完成 9 个阶段，结果输出到 `validation/t38/results/`：

| 阶段 | 内容 | 结果文件 |
| --- | --- | --- |
| 1 | 主节点 init/start（7788 + binlog 7789 + metrics 7790） | step01~02_*.txt |
| 2 | 执行 4 个 SQL 脚本（业务库/事务/查询分析/运维生态） | step03_sql_*.txt |
| 3 | TLS 实例（gen-cert + 7799 纯 TLS 监听） | step04~06_*.txt |
| 4 | Python 驱动端到端验证（validate_real_user.py） | step07_validate_real_user.txt |
| 5 | CLI 工具链：doctor / status / stats / metrics / preflight | step08_cli_*.txt |
| 6 | 复制从节点（replica --master 7789）数据同步 | step09~11_*.txt |
| 7 | mysqldump 兼容导入（T32） | step12~14_*.txt |
| 8 | CDC 订阅（binlog_subscriber 消费 30 条事件） | step15_cdc_subscriber.txt |
| 9 | 汇总 | SUMMARY.txt / run.log |

退出码：0 = 全部 PASS；非 0 = 存在 FAIL 标记。

## 3. 分步运行（手动）

### 3.1 建库与业务闭环

```powershell
bin\openxdb.exe init --data-dir <work>\master
bin\openxdb.exe start --data-dir <work>\master --port 7788 --replica-port 7789 --metrics-port 7790
```

执行 SQL 脚本（可用 `validate_real_user.py` 中的 exec_file 逻辑，或任意驱动逐条执行）：

```powershell
python validation\t38\validate_real_user.py --port 7788 --tls-port 7799 --tls-ca <work>\cert.pem --backup <work>\t38-backup.bak
```

### 3.2 验证场景解读

- **业务闭环**：新订单 → 视图可见 → 触发器审计 → 物化视图刷新 → 存储过程下单。
- **事务 ACID**：commit 持久 / rollback 回滚 / 双连接并发互不干扰。
- **约束拦截**：唯一键、外键、主键冲突均被拒绝（协议层 ERR → OperationalError）。
- **查询分析**：CTE / RANK 窗口 / 子查询 / GROUP BY / REPLACE upsert / EXPLAIN / 参数化 PREPARE。
- **RBAC**：授权表可读、未授权表被拒。
- **TLS**：自签名证书下 Python 驱动 verify-ca 连接成功；明文连接被服务器拒绝。
- **备份恢复**：BACKUP TO → 写增量 → RESTORE FROM 回滚增量；PITR：RESTORE TO LSN。
- **告警**：配置 qps>1 规则后 SHOW ALERT RULES / SHOW ALERT STATUS 可见。
- **性能**：1000 行批量写入计时（INFO 记录 rows/s）。

### 3.3 运维与生态命令

```powershell
bin\openxdb.exe doctor --data-dir <work>\master --addr 127.0.0.1:7788
bin\openxdb.exe status --addr 127.0.0.1:7788
bin\openxdb.exe stats --addr 127.0.0.1:7788
bin\openxdb.exe metrics --addr 127.0.0.1:7790
bin\openxdb.exe preflight --data-dir <work>\master

# 复制从节点（拉取主节点 binlog）
bin\openxdb.exe init --data-dir <work>\slave
bin\openxdb.exe replica --data-dir <work>\slave --master 127.0.0.1:7789

# mysqldump 兼容导入（T32）
bin\openxdb.exe import --data-dir <work>\sample_import --dump validation\t38\t38_mysqldump_sample.sql

# CDC 订阅（T27）
go build -o <work>\binlog_subscriber.exe examples\binlog_subscriber.go
<work>\binlog_subscriber.exe --binlog <work>\master\binlog.log --offset <work>\sub.offset --limit 30
```

## 4. 结果解读

- `step07_validate_real_user.txt` 中每行 `[PASS]/[FAIL]` + `SUMMARY passed=.. failed=..`；任一 FAIL 表示对应能力未达标。
- `step08_cli_*.txt` 应包含 doctor 全项检查（binlog/checksum）、status 版本与角色、stats 实时指标、metrics 的 Prometheus 文本、preflight 升级预检。
- `step10_replica_stdout.txt` 显示从节点连接主节点复制端口；`step11_doctor_slave.txt` 对从节点 binlog 检查通过即同步成功。
- `step13_import_mysqldump.txt` 导入成功后可启动该目录实例执行 `SELECT COUNT(*) FROM legacy_customers` 应为 3。
- `step15_cdc_subscriber.txt` 应包含 binlog 事件（INSERT/UPDATE 等），位点写入 `sub.offset`。

## 5. 已知约定

- 从节点（replica）不暴露 SQL 监听端口（与 T11 设计一致），一致性通过 binlog 落盘与 doctor 检查确认。
- 备份恢复为在线事务内恢复，无需重启；PITR 回放以 binlog LSN 为锚。
- TLS 实例使用独立空库（TLS 验证只关注加密握手与明文拒绝，不承载业务数据）。
