# D6 备库常驻读服务（replica resident read）专项验证

版本：v1.17.0 D6（F1-F4）
日期：2026-10-31
范围：`openxdb replica --mysql-port` 常驻读服务、只读写拒绝、复制应用与 SQL 读并发、崩溃恢复一致性。

## 1. 功能实现

| 项 | commit | 说明 |
|---|---|---|
| F1 replica sql listener | `2988ba2` | replica 子命令支持 `--mysql-port` 常驻 MySQL wire 监听（复用主端装配路径），支持 `--auth`、`--tls-cert/--tls-key` |
| F2 read-only enforcement | `39df3a2` | 备库写语句按 `isWriteStmt` 白名单统一拒绝（`read-only replica: write statement rejected`），仅 replica 模式开放 |
| F3 apply/read concurrency | `5f7a6d7` | 复制应用与 SQL 读并发；读快照创建前经 `ReadBarrier` 短临界区以 `AppliedLSN` 为界对齐，存储原子批次 + 快照隔离保证无半批次可见 |
| F4 validation + doc | 本文 | 专项验证脚本与文档 |

## 2. 并发方案取舍

- **采用方案（非全互斥）**：`Follower.ApplyBinlog` 保持 `f.mu` 串行写存储；读路径在执行前调用 `Follower.ReadBarrier()`（`f.mu` Lock/Unlock 短临界区），确保读快照创建时已应用批次到达 `AppliedLSN` 边界。存储层（RocksDB/btree）批次写入原子 + 读快照隔离，因此读侧永远看不到半批次；`ReadBarrier` 只等当前批次完成，不阻塞后续复制，无锁序环，不会死锁。
- **回退方案（未采用）**：读/复制全互斥（读持全局锁直到查询结束）。缺点：长查询会阻塞复制应用，滞后窗口放大；实现简单但吞吐损失大。并发方案经 `go test -race` 与专项回归验证通过，故未回退。
- **边界**：读可见性以查询开始时 `AppliedLSN` 为界，随后到达的复制批次不影响当前查询；单连接内后续查询会重新经过 `ReadBarrier` 看到新边界。

## 3. 验证结果（6 项验收）

环境：Windows 11，openxdb.exe / oxsql.exe 构建自 F4 前 HEAD。主端 `start --port 17788 --replica-port 17789 --mysql-port 13306`；备库 `replica --data-dir ... --master 127.0.0.1:17789 --mysql-port 13307`。

### 3.1 常驻查询（①）
备库 `--mysql-port 13307` 常驻监听，oxsql 多次连接查询均可达：

- 主端建表 `t(id INT PRIMARY KEY, v VARCHAR(64))` 并写入 10 行后，备库 `SELECT COUNT(*)` = 10；
- 备库可重复查询（3.3/3.4 多次执行均成功），监听不随单次查询退出。

### 3.2 写语句拒绝（②）
备库执行 `INSERT INTO t VALUES (2000,'x')` 返回：

```
oxsql: read-only replica: write statement rejected
```

拒绝后备库 `SELECT COUNT(*)` 未增加，数据不受污染。

### 3.3 滞后窗口读断言（③）
主端连续写入 id=11..40（30 轮），每轮写后立即并行读主/备 `COUNT(*)`：

- 全程 **lagOk=True，worstGap=0**：备库计数从不超前主端；
- 最终备库收敛至 40 行；
- 循环 30 轮约 5 秒完成（本机 oxsql 进程启动开销）。

### 3.4 复制顺序性与崩溃恢复（④）
- 主端写 id=41..60 期间强杀备库进程（`Stop-Process -Force`）；
- 重启备库（同 `--data-dir`），30 秒内追平：`COUNT=60`，`SELECT v WHERE id=60` = `m60`（顺序一致）；
- 再次强杀并重启，仍追平至 60 行且 `id=60` 内容一致；
- 结论：崩溃后基于复制位点续传，顺序与内容一致性保持。

### 3.5 全仓构建与测试（⑤）
- `go build ./...` 通过；
- `go test ./...` 全绿（含 F2/F3 新增单测：`TestD6ReplicaReadOnly`、`TestFollowerReadBarrierConcurrent`，均通过 `-race`）。

### 3.6 主端读写无回归（⑥）
主端 `INSERT` / `SELECT` / `UPDATE` 均正常（`Query OK`、读取返回预期值），备库拒绝写不影响主端。

## 4. 复现方法

```powershell
# 1) 构建二进制
go build -o bin/openxdb.exe ./cmd/openxdb
go build -o bin/oxsql.exe ./cmd/oxsql

# 2) 运行专项验证（自动 init 主备、起服务、执行 ①-⑥ 断言）
powershell -NoProfile -ExecutionPolicy Bypass -File docs/experiments/run_d6_replica_validate.ps1
```

脚本输出逐项 `[PASS]/[FAIL]` 与汇总行；崩溃恢复段内含二次重启断言。
