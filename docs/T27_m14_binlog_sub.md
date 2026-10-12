---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_15003266c57711f19063525400393706
    ReservedCode1: Kb+HbeFxovG4QHef3mpMepV9YhHQPafWj0MC0/VjWGUccszBFKNxPvJ7iDHcsX1F6olTwkSBc1A3HHvsfKgjc34f4f8JCg66VV5TzM01+enev3H8f4llKNLp6TXmsjQsAleavAKRIWU7o5GNhTaL8zKfsfcurOUnS7IExv3t2/w+fcXwUdkvHqXSZlU=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_15003266c57711f19063525400393706
    ReservedCode2: Kb+HbeFxovG4QHef3mpMepV9YhHQPafWj0MC0/VjWGUccszBFKNxPvJ7iDHcsX1F6olTwkSBc1A3HHvsfKgjc34f4f8JCg66VV5TzM01+enev3H8f4llKNLp6TXmsjQsAleavAKRIWU7o5GNhTaL8zKfsfcurOUnS7IExv3t2/w+fcXwUdkvHqXSZlU=
---

---
# T27 M14 生态接入簇（2/3）：binlog 对外订阅（CDC）

> 版本：v5.0-P13-M14, v0.14.0-alpha
> 日期：2026-10-11
> 范围：`pkg/replication`（binlog.go 尾部容忍解码 + 新增 binlog_reader.go / binlog_reader_test.go）、`pkg/cdc`（新增 cdc.go / offset.go / cdc_test.go，含 OffsetStore 重启恢复用例）、`pkg/sql`（ast.go / parser.go / executor.go SHOW BINLOG EVENTS 语法与执行器 + m14_binlog_sub.go / m14_binlog_sub_test.go）
> 闭环：`go build ./...` 无输出；`go test ./pkg/... -count=1` 全绿，不 push（README 勾选由 M14 整簇收尾提交统一更新）
> 提交：`M14: binlog subscribe CDC`（本地，未 push）

## 背景

M14 生态接入簇在子功能一 mysqldump 兼容导入（94a7cbc，一次性全量灌入）基础上，新增**binlog 对外订阅（CDC）**：把主节点已有 binlog 暴露为可订阅的增量变更流，供外部数据管道 / 增量同步消费。与导入形成**迁移闭环**——先用 mysqldump 导入存量，再按 LSN 增量订阅后续变更，实现"全量导入 + 增量同步"的完整迁移方案。

## 设计

### 事件模型（`pkg/cdc` 新增）

| 概念 | 说明 |
|---|---|
| `Event` | 一条表/行级变更：`LSN`（binlog 位点，续读/幂等基准）、`CommitLSN`（源 WAL 提交点）、`SchemaVer`、`Kind`（PUT/DELETE）、`TableID`、`PK`、`IsIndex`（二级索引键，跟随行变更，消费方通常忽略）、`RawKey`/`RawValue` |
| `Decode(entry)` | 把一条 binlog 记录解码为事件列表；物理键 `r{region}` 前缀剥除后按行键 `s{tableID}{pk}` / 索引键 `i{tableID}{indexID}{idxVal}p{pk}` 解析；元数据键 `m:...` 与无法识别的键不产出事件，顺序保持 binlog 内 put/delete 顺序 |
| `ChangeKind` | `KindPut`（写入/更新，binlog 不区分 insert/update）/ `KindDelete` |

### 订阅会话与并发读保护（`pkg/replication/binlog_reader.go` 新增 + `pkg/cdc/cdc.go`）

- **只读拉取器 `BinlogReader`**：与 `BinlogStore` 分离——只读打开（不创建不写入）、持有独立文件句柄与自身锁、尾部不完整记录（正在追加的半条）按正常结束容忍（新增 `decodeBinRecordOpt` 尾部容忍解码 + `ErrStopFetch` 扫描中止哨兵）；不持有 binlog 写路径锁，与主写 `Append` / 复制推送**并发共存**（binlog 为 append-only 落盘文件，落盘内容对并发读稳定可见）。
- **订阅会话 `Subscriber`**：`NewSubscriber(path)` 打开只读会话；`Fetch(from, limit)` 从 `from`（含）增量读取至多 `limit` 条事件（`limit<=0` 一次读完当前文件），返回事件列表与**下一消费位点 `nextLSN`**——达到 limit 时 `nextLSN = 最后事件 LSN + 1`，读到文件尾时 `nextLSN = 文件尾位点 + 1`（无新事件时保持不倒退）；`Close` 后再次 `Fetch` 返回 `ErrClosed`。单会话 `Fetch` 加锁，支持顺序拉取。

### Offset 管理（`pkg/cdc/offset.go` 新增）

- `OffsetStore` 接口：`Load()` 返回已保存消费位点（不存在为 0）、`Save(lsn)` 原子保存下一次 `Fetch` 的 from。
- `FileOffsetStore`：单文件十进制文本 + 换行，临时文件 + 重命名原子写；适合单订阅者，多订阅者可各指 offset 文件。客户端每次 `Fetch` 后 `Save(nextLSN)`，重启后 `Load` 并从该位点续读（LSN 单调递增，重连续读不重复不遗漏）。

### SQL 入口（`pkg/sql` SHOW BINLOG EVENTS）

```
SHOW BINLOG EVENTS [FROM <lsn>] [LIMIT <n>]
```

- 输出列：`lsn / commit_lsn / schema_ver / kind / table / pk / row`。
- `kind`：PUT 或 DELETE；`table`：按表 ID 查表目录还原表名（未知表 ID 显示 `#table-<id>`，索引键显示 `(index)`）；`pk`：主键字节文本；`row`：PUT 行值按表列解码为 `col=val, ...` 摘要（元数据缺失时显示原始 JSON 字节）。
- 执行器 `execShowBinlogEvents`：binlog 未启用（`binlogPath` 为空）时报 `binlog not enabled`；只读巡检，不改变主写路径；与复制开启状态无关，复制开启时同样可查（并发共存验证见测试）。

### 与 M14-1 迁移闭环

导入（IMPORT MYSQLDUMP）落地存量 → `SHOW BINLOG EVENTS` / `Subscriber.Fetch` 从 LSN 0 或任意位点增量订阅后续变更 → `FileOffsetStore` 持久化位点，进程重启后续读。外部管道可按 `Event.TableID` / `PK` 还原目标行。

## 测试

| 覆盖点 | 用例（`pkg/replication` / `pkg/cdc` / `pkg/sql`） |
|---|---|
| 多表变更 | `TestM14ShowBinlogEvents`：t1/t2 多行 PUT + DELETE，表名按表目录还原、行值列解码 |
| LSN 续读 | `SHOW BINLOG EVENTS FROM <lsn>` 全量等价、越过末尾为空；`Fetch` from/limit 位点推进 |
| offset 重启恢复 | `FileOffsetStore.Save/Load` 原子写与重启续读（cdc 测试） |
| 与复制路径共存 | `StartReplication` 开启时 SHOW 读取 + 继续写入立即可见（SQL 端到端）；BinlogReader 尾部半条容忍扫描 |
| 事件解码 | PUT/DELETE、行键/索引键、元数据键跳过、region 前缀剥离（cdc 测试） |

闭环验证：`go build ./...` 无输出；`go test ./pkg/... -count=1` 全绿（15 包）。

## 边界与后续

- binlog 为单文件 append-only，未做日志轮转；超大 binlog 下 `Fetch` 为顺序扫描（LSN 定位 + 逐条解码），后续可按需增加索引位点或分片。
- 订阅为拉取模式 + 客户端持久化位点；未提供推送 / 断线重连自动恢复（由客户端按 nextLSN 重试）。
- 二级索引键事件（`IsIndex`）默认跟随行变更输出，消费方按需忽略。
*（内容由AI生成，仅供参考）*
