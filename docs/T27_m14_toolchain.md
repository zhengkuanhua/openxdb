---
# T27 M14 功能簇（3/3）：工具链收尾与 README 勾选

> 版本：v5.0-P16-M14, v0.18.0-alpha
> 日期：2026-10-11
> 范围：`cmd/openxdb`（新增 import.go、main.go 注册 import 子命令与 usage、新增 m14_toolchain_test.go 端到端测试）、`examples`（新增 binlog_subscriber.go 示例客户端）、`README.md`（Status 行同步 M14/v0.18.0-alpha/T1–T27 complete，Roadmap 新增 M14 行并勾选 T27）
> 闭环：`go vet ./cmd/openxdb ./examples` + `go test ./cmd/openxdb -run 'TestImportCLISmoke|TestBinlogSubscriberBuild' -v` 全绿，不 push
> 提交：`M14: toolchain + README`（本地，未 push）

## 背景

M14 生态接入簇（子功能一 mysqldump 兼容导入、子功能二 binlog 对外订阅 CDC）已在 `pkg/import`、`pkg/sql`、`pkg/cdc` 层完成。子功能三把这两项能力沉淀为对外可用的**工具链**并收尾 README：

1. **CLI 入口**：`openxdb import` 子命令复用 `pkg/sql` 的 `IMPORT MYSQLDUMP` 能力，让用户无需进入 REPL 即可一键导入 mysqldump 文件；
2. **示例客户端**：`examples/binlog_subscriber.go` 演示 `pkg/cdc.Subscriber` + `pkg/cdc.FileOffsetStore` 的典型订阅用法（续读/位点持久化），供外部数据管道参考；
3. **端到端测试**：CLI 冒烟（真实 CGO 存储上导入 + 落库验证）+ 示例可编译验证；
4. **README 收尾**：Status 行同步 M14/v0.18.0-alpha/T1–T27 complete，Roadmap 新增 M14 行并链接两份 T27 文档勾选。

## 设计

### `openxdb import` 子命令（`cmd/openxdb/import.go`）

沿用现有子命令风格（`flagValue` / `needDataDir` / `db.Open` / `d.SQL.Execute`），把 dump 路径拼为 `IMPORT MYSQLDUMP` 语句交给已实现的执行器：

```
openxdb import --data-dir <dir> --dump <file> [--batch <n>] [--ignore-errors]
```

- `--dump <file>`：mysqldump 文件（必填，启动前做存在性/非目录校验）；
- `--batch <n>`：透传 `BATCH n`（每批行数提交一次事务，默认单原子事务）；
- `--ignore-errors`：透传 `IGNORE ERRORS`（坏行/建表/执行错误跳过继续）；
- 输出与执行器结果一致的统计行：`imported <file> -> tables=.. inserted=.. skipped=.. errors=.. batches=..`，供脚本解析；
- 复用 `needDataDir` 缺失提示与 `db.Open` 的错误处理，退出码约定与其它子命令一致（错误打印 `openxdb: ...` 到 stderr 并 exit 1）。

### binlog 订阅示例客户端（`examples/binlog_subscriber.go`）

以独立 `main` 包发布，展示 `pkg/cdc` 的完整消费循环：

```
go run ./examples/binlog_subscriber.go --binlog <binlog.log> --offset <offset-file> [--limit <n>] [--follow]
```

- `cdc.NewSubscriber(binlog)` 打开只读订阅会话（文件不存在原样报错，不创建）；
- `cdc.NewFileOffsetStore(offset)` + `store.Load()` 读取上次消费位点（首次为 0）；
- `sub.Fetch(from, limit)` 拉取事件，打印 `lsn/commit/schema/kind/table/index/pk`；
- 每次拉取后 `store.Save(next)` 保存 `nextLSN`，重启后从该位点续读（LSN 单调递增，无重复无遗漏）；
- `--follow` 进入持续轮询（500ms 间隔），Ctrl-C 时位点已保存、正常退出；
- 注释给出与主节点 `start --replica-port` 联动的典型用法。

### 端到端测试（`cmd/openxdb/m14_toolchain_test.go`）

- `TestImportCLISmoke`：`db.Init` 真实 CGO 数据目录 → 生成 mysqldump 文件 → 调用 `cmdImport` 灌入（原子路径 + `--batch 1 --ignore-errors` 路径 + 缺参报错路径）→ `db.Open` 后用 `SELECT COUNT(*)` 验证落库行数（含重复主键跳过语义）；
- `TestBinlogSubscriberBuild`：`go build ./examples/...` 验证示例客户端随 CGO 工具链可编译。

### README 收尾

- **Status 行**：`M14 ecosystem access cluster: mysqldump-compatible import + binlog subscribe CDC + toolchain closing & README (v5.0-P16-M14), v0.18.0-alpha.`，T1–T26 → **T1–T27 complete**，追加 T27 描述；
- **Roadmap**：在 T26 之后新增 `[x] M14 ecosystem access cluster` 行（简述 import/CDC/toolchain 三部分）并链接 `docs/T27_m14_mysqldump_import.md` 与 `docs/T27_m14_binlog_sub.md`，随后新增 `[x] T27 M14 ecosystem access cluster` 勾选行（样式与 M13 一致）。

## 测试清单

| 命令 | 期望 |
|---|---|
| `go vet ./cmd/openxdb ./examples` | 无输出（通过） |
| `go test ./cmd/openxdb -run 'TestImportCLISmoke|TestBinlogSubscriberBuild' -v` | 2 个用例 PASS |
| `openxdb import --data-dir <dir> --dump <dump.sql>` | 输出 `imported ... -> tables=N ...` 且落库行数与 dump 一致 |

> 说明：`go test` 需本机 CGO 工具链（PATH 前置 mingw64、CGO_CPPFLAGS/CGO_CXXFLAGS 指向 rocksdb include、CGO_LDFLAGS 指向 rocksdb-build 且**严禁 `-lstdc++`**）。

## 闭环

- 代码：`cmd/openxdb/import.go`、`cmd/openxdb/main.go`（分发 + usage）、`examples/binlog_subscriber.go`；
- 测试：`cmd/openxdb/m14_toolchain_test.go`；
- 文档：本文档；README Status / Roadmap 已同步 M14 并勾选 T27（两份 T27 文档链接已就位）。
