# T7: M2/M3 Interface Reservations

> 状态：已完成（接口预留，不实现）
> 对应：开发手册 §8（M2 复制）/ §9（M3 分片）；README Roadmap「M2/M3 interface reservations」
> 提交：见 git log（T7_m2m3_reservations）

## 背景

M1 目标是**单机数据库闭环**（存储 / WAL / SQL / 事务 / 索引 / CLI）。
为避免 M2（复制）、M3（分片）引入的复杂度（TSO、PD、Raft、分布式事务等）
污染 M1 设计，本任务仅在代码中**预留演进接口**，不实现任何跨节点能力。

## 落地内容

### pkg/replication（M2 复制接口预留）

| 类型 | 说明 |
|------|------|
| `ReplicaRole` | Master / Slave 角色枚举 |
| `BinlogEntry` | 复制日志条目：`LSN` + `CommitLSN` + `Batch` + `SchemaVer`，从 WAL 演进 |
| `Replica` | 复制节点描述（角色 + binlog 文件） |
| `Replicator` | 复制器接口：`Append` / `Pull` / `Ack` / `Close` |

约定：
- 复制日志与 WAL LSN 对齐（`BinlogEntry.LSN` 即 WAL LSN）；
- 半同步语义：至少 1 个 Slave ack 后才返回提交成功（RPO≈0）；
- `ErrNotImplemented`：M1 不应实例化本包类型。

### pkg/sharding（M3 分片接口预留）

| 类型 | 说明 |
|------|------|
| `RegionInfo` | 分片元信息：`RegionID` + `[StartKey, EndKey)` 区间 |
| `Router` | 路由接口：`Locate(tableID, key)` / `Refresh(metaVersion)` |
| `EncodeRegionKey` | Key 外层 region 前缀编码预留（M1 不调用） |

约定：
- M1 Key 编码**无 region 前缀**，M3 启用时由 `EncodeRegionKey` 在外层追加；
- 集中 meta 服务（单点 or etcd）M3 时再设计，M1/M2 不做。

## 演进路径

```
M1 单机闭环（当前）
  └─ M2 复制：BinlogEntry 从 WAL 演进；Replicator 实现 Append/Pull/Ack
       └─ M3 分片：EncodeRegionKey 启用；Router 对接集中 meta；在线分裂/合并
```

## 验证

- `go build ./pkg/replication ./pkg/sharding` 通过；
- 新包不引入 rocksdb / 其它 M1 外依赖，CI 全量编译校验；
- M1 运行路径零改动（预留包仅编译，不实例化）。
