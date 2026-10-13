# 备份调度与远程备份（BR 方向②）

本文档说明 OpenXDB 的备份调度与远程备份能力：周期自动 BACKUP、保留份数、
状态查询，以及 HTTP 远程备份（push/pull）与恢复路径。

## 1. 备份调度

### 1.1 配置项（SET PERSIST 热更新，默认全部关闭）

| 配置键 | 默认 | 说明 |
|---|---|---|
| `backup_interval` | `0` | 调度周期（秒）。正整数开启周期自动 BACKUP；`0` 关闭 |
| `backup_keep` | `0` | 保留份数。`N>0` 仅保留最近 N 份调度备份；`0` 不清理 |
| `backup_dir` | 空 | 调度备份输出目录；空=未配置（不执行调度备份） |
| `backup_remote_url` | 空 | 远程 HTTP PUT 上传目标；空=不上传 |

配置经现有 SET PERSIST 体系即时生效并落盘，重启后保留；也可在
`cmd start` 启动时通过命令行装配：

```
openxdb start --data-dir <dir> \
  --backup-interval 3600 \
  --backup-keep 7 \
  --backup-dir <dir>/backups \
  --backup-remote-url http://127.0.0.1:8090/backups
```

启动未配置调度时默认目录为 `<data>/backups`（`--backup-dir` 缺省值）。

### 1.2 状态查看

```
SHOW BACKUP STATUS;
```

返回调度配置（周期/保留份数/目录/远程目标）与最近执行状态
（下次执行时间/最近结果/最近错误）。

### 1.3 行为

- 调度器在每个周期触发一次引擎级 `BACKUP`，产物写入 `backup_dir`。
- 每个周期触发前清理旧备份：`backup_keep > 0` 时仅保留最近 N 份。
- 备份完成后若配置了 `backup_remote_url`，自动将备份文件 HTTP PUT 上传至远程。
- `db.Close` 会停止调度循环，避免关闭过程中误触发 BACKUP。

## 2. 远程备份（HTTP）

远程备份基于现有网络栈实现为 HTTP 传输，至少支持两类目标：
- 支持 HTTP PUT/GET 的自建端点（如 WebDAV 或自定义备份服务）。

### 2.1 push：上传本地备份到远程

```
openxdb backup push --file <local-backup-file> --url http://127.0.0.1:8090/backups/<name>
```

将本地备份文件上传至远程目标（HTTP PUT，`--file` 指定本地文件，
`--url` 为完整远程目标 URL）。

### 2.2 pull：从远程下载备份到本地

```
openxdb backup pull --url http://127.0.0.1:8090/backups/<name> --out <local-path>
```

从远程目标下载备份到本地（HTTP GET，`--url` 为完整远程文件 URL，
`--out` 为本地保存路径；先写 `.tmp` 再 rename，避免半文件被误用）。

## 3. 恢复路径

远程备份的恢复路径：

1. `openxdb backup pull <remote-file> ...` 将远程备份下载到本地目录；
2. 使用现有恢复命令还原：

```
RESTORE FROM '<local-downloaded-backup>';
```

或按仓库现有 RESTORE/PITR 流程从下载的备份文件恢复。调度产生的本地备份
（`backup_dir` 下）也可直接 `RESTORE FROM '<backup_dir>/<file>'` 恢复。

## 4. 与既有能力的关系

- 复用现有 `BACKUP`/`RESTORE` 语句与引擎快照导出/物理恢复路径，调度器仅负责
  周期性触发与保留清理。
- `SHOW BACKUP STATUS` 与 `SHOW CONFIG` 同为只读状态查询，语法风格一致
  （BACKUP/STATUS 按文本匹配兼容关键字表）。
- 远程传输仅使用标准库 HTTP client，不引入外部依赖。
