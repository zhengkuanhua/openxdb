---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_3cacfe6ec36c11f18019525400248c00
    ReservedCode1: omwahCh9jkGNFc5fNRtl2gk3TI9Vt6aTgc8mNE/xEnpPOLAG5RmKP6o8ogRdh2CjPhnZKjs5vwh0jUVqxpZggZy5HJlNFU8GA+P07A2adk86phaDte9syTpCyASE6tPEzkXkp6AoOsh7RTHhDYUlmD7D2xzASBArPEBkLdCRPfJmI+WkWUyUb8RBIt4=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_3cacfe6ec36c11f18019525400248c00
    ReservedCode2: omwahCh9jkGNFc5fNRtl2gk3TI9Vt6aTgc8mNE/xEnpPOLAG5RmKP6o8ogRdh2CjPhnZKjs5vwh0jUVqxpZggZy5HJlNFU8GA+P07A2adk86phaDte9syTpCyASE6tPEzkXkp6AoOsh7RTHhDYUlmD7D2xzASBArPEBkLdCRPfJmI+WkWUyUb8RBIt4=
---

# T12 — M3 分片落地（Sharding）

> 状态：已完成（M3，v3.0-P3-M3）。
> 范围：单机内核上的逻辑分片第一步落地——Region 元数据、Router 路由、跨 Region 查询与 DML/DDL 全路径注入。
> 目标：**分片不破坏既有单机 SQL/事务语义**（默认单 region 整表形态零回归），并提供可测试的分片管理入口。

## 1. 分片架构

M3 采用「逻辑分片」形态：存储层键空间保持单一 RocksDB 实例，SQL 层通过 **region 前缀**把物理键按表键空间切分为多个逻辑分区。每个分区称为一个 Region，由 `RegionInfo` 描述；`Router` 负责键 → region 的路由；所有物理键访问路径（插入/点查/范围/全表/索引/UPDATE/DELETE/建删索引/建删表/迁移）统一走带 region 前缀的键编码，跨 region 扫描时按 region 展开后合并。

```
┌──────────────────────────────────────────────────────────┐
│ SQL 层 (pkg/sql)                                          │
│  Executor ──router──► Router (pkg/sharding)               │
│    │  rowKeyAt / indexKeyAt / rowRanges / indexRanges     │
│    │  scanMerged（跨 region 合并排序）                      │
│    ▼                                                      │
│  物理键 = EncodeRegionKey(regionID, innerKey)             │
│    = 'r' + regionID(8B 大端) + innerKey                    │
│    ▼                                                      │
│ Txn / Storage (RocksDB 单实例)                            │
└──────────────────────────────────────────────────────────┘
```

- 分片元数据（region 列表 + ID 水位）以全局键 `m:regions`（**不带** region 前缀）与表目录 `m:tables` 同层落盘，经同一事务/WAL 通道原子持久化。
- `pkg/db` 在 `Open` 时读取 `m:regions` 并注入 `Engine.LoadSharding`，进程内 Router 与落盘状态保持一致。
- 单机模式下所有 Region `Local=true`（归属本节点）；`State` 目前恒为 `ACTIVE`，`LEAVING` 为迁移中预留。

## 2. 键空间划分与 RegionInfo 元数据

### 2.1 物理键编码

```
物理键 = 'r' + regionID(8B 大端) + innerKey
innerKey = M1 内层键：
  行键   s{tableID:8B}{pk}
  索引键 i{tableID:8B}{indexID:8B}{idxVal}p{pk}
```

- 前缀长度 `RegionKeyPrefixLen = 9`（1 字节 'r' + 8 字节 regionID），`DecodeRegionKey` 可无损剥离。
- **同一 region 内键序与 innerKey 字节序完全一致**（region 前缀固定，不改变相对顺序）；跨 region 以 regionID 前缀隔离，regionID 单调分配，物理上按 region 聚集。
- 区域边界定义在 **innerKey 层级**（`StartKey`/`EndKey` 落盘为完整内层行键，如 `s{tid}{boundary}`），拼上 region 前缀即为物理区间。

### 2.2 RegionInfo 字段

| 字段 | 类型 | 语义 |
|---|---|---|
| `RegionID` | `storage.ID`(uint64) | Region 唯一 ID；显式 region 由全局 `seq` 水位递增分配 |
| `TableID` | `storage.ID` | 所属表 ID |
| `StartKey` | `[]byte` | 区间下界（**含**），内层行键；`nil` = 表起始（-inf） |
| `EndKey` | `[]byte` | 区间上界（**不含**），内层行键；`nil` = 表结束（+inf） |
| `State` | `RegionState` | `ACTIVE`（当前）/ `LEAVING`（迁移预留） |
| `Local` | `bool` | 归属本节点（单机恒 `true`） |
| `Explicit` | `bool` | 是否显式分片；隐式单 region 为 `false` |

### 2.3 落盘格式

`m:regions` 载荷为 JSON：

```json
{
  "seq": 3,
  "regions": [
    {"region_id": 1, "table_id": 1, "start_key": null, "end_key": "s\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0004", "state": "ACTIVE", "local": true, "explicit": true},
    {"region_id": 2, "table_id": 1, "start_key": "s\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0004", "end_key": "s\u0000\u0000\u0000\u0000\u0000\u0000\u0000\b", "state": "ACTIVE", "local": true, "explicit": true},
    {"region_id": 3, "table_id": 1, "start_key": "s\u0000\u0000\u0000\u0000\u0000\u0000\u0000\b", "end_key": null, "state": "ACTIVE", "local": true, "explicit": true}
  ]
}
```

- `seq` 为显式 region ID 分配水位，`SplitTable` 推进，`ReplaceRegions` 保证单调。
- 表目录（`m:tables`）与分片目录（`m:regions`）分离，DROP TABLE 时通过 `Router.RemoveTable` 同步清理。

### 2.4 SPLIT 划分规则

`Engine.SplitTable(name, boundaries)` 按主键字节边界把表键空间切分为 `len(boundaries)+1` 个显式 region：

- `boundaries` 必须**严格升序**，为不含首尾的主键字节边界（INT 主键即 8B 大端值）。
- 边界语义：每个 region 区间 `[StartKey, EndKey)`——**StartKey 含、EndKey 排他**；首个 region `StartKey=nil`（表起始），末个 region `EndKey=nil`（表结束）→ 区间连续、无空洞、无重叠。
- 划分后每个 region 覆盖一整段连续 innerKey 空间，物理键 `r{rid}{start} ~ r{rid}{end}` 同样连续。
- `SplitTable` 在**单事务**内完成：构造新 region 列表 → 存量数据（行 + 全部索引键）按新边界重路由迁移（写新键、删旧键）→ 替换 Router 视图 → 落盘 `m:regions` → 提交。中途失败整体回滚，无半迁移状态。
- 迁移用**临时 Router** 定位新归属，不污染主路由；已在新 region 的键自动跳过（`bytes.Equal(newKey, kv.Key)`），幂等安全。

## 3. Router 路由与边界语义

### 3.1 隐式单 region（零回归保障）

表未显式分片时，`Router.RegionsOf` 返回隐式整表单 region：

- `RegionID = tableID | 1<<63`（高位置 1，与显式 region 低位自增 ID 隔离，永不冲突）；
- `StartKey=nil`、`EndKey=nil`，覆盖整表键空间；
- `Explicit=false`。

因此**所有表、所有访问路径始终带 region 前缀**，未启用分片的 SQL 行为与 M1 完全一致——只是物理键多了一截固定前缀，索引/事务/复制不受影响（零回归）。

### 3.2 Locate 定位

`Router.Locate(tableID, innerKey)`（innerKey 为完整内层键，如 `s{tid}{pk}`）：

1. 无显式 region → 返回隐式 region ID；
2. 有显式 region → 按 `StartKey` 升序二分（`sort.Search` 找第一个 `StartKey > key` 的 region），候选为前一 region；
3. 若 `key < 最小 StartKey`（i==0）或 `key >= 候选 region 的 EndKey`（显式划分出现空洞时触发）→ 返回 `ErrRegionNotFound`。

边界语义汇总：

| 场景 | 结果 |
|---|---|
| `key` 位于某 region 的 `[StartKey, EndKey)` | 返回该 region |
| `key` 小于首个 region 的 StartKey | `ErrRegionNotFound`（正常划分不会出现） |
| `key` 大于等于某 region EndKey 且无后续 region 承接 | `ErrRegionNotFound`（划分空洞） |
| 表未分片 | 隐式整表单 region，恒命中 |

### 3.3 并发安全

Router 内部 `sync.RWMutex` 保护 region 表与 seq 水位；`SetRegions`（Open 加载）、`ReplaceRegions`（SplitTable）、`RemoveTable`（DropTable）为写操作，`RegionsOf`/`Locate`/`Dump` 为读操作。

## 4. 跨 Region 查询执行路径

### 4.1 区间展开

| 辅助函数 | 语义 |
|---|---|
| `rowRanges(meta)` | 表全部行物理区间：每 region 展开为 `[r{rid}s{tid}(StartKey), r{rid}s{tid+1}(EndKey))`；`StartKey`/`EndKey` 落盘已是内层行键，**直接拼接 region 前缀，禁止二次编码**（历史踩坑：双重编码会扫描空集） |
| `indexRanges(meta, idxID, startInner, endInner)` | 给定索引扫描的内层完整边界 `[startInner, endInner)` 按 regions 展开；对每个 region 均扫描 `[r{rid}{startInner}, r{rid}{endInner})` |
| `indexTableRanges(meta, idxID)` | 整索引区间 `[i{tid}{idxID}, i{tid}{idxID+1})` 按 regions 展开（建索引回填 / 删索引 / 迁移用） |
| `scanMerged(tx, ranges)` | 对多个物理区间分别 `tx.Scan`，结果按 **innerKey 升序稳定合并**（region 前缀不参与比较） |

### 4.2 各路径注入（全部物理键访问点）

| 操作 | 路径 |
|---|---|
| `INSERT` | 主键唯一性检查与写入用 `locateRow` + `rowKeyAt`（点路由）；索引键用 `indexKeyAt` 同 region 前缀 |
| `SELECT` 点查（PK 等值） | `locateRow` + `rowKeyAt` 单点 Get |
| `SELECT` 全表 / 范围 | `rowRanges` 展开 + `scanMerged` 合并，行级过滤后统一排序/聚合/投影 |
| `SELECT` 索引路径 | `indexRanges` 展开扫描索引键 → 回表 `rowKeyAt`（索引键与行键同 region 前缀，回表键路由一致） |
| `UPDATE` / `DELETE` | `rowRanges` 展开 + `scanMerged` 跨 region 扫描；按 `ridOf(kv.Key)` 在原 region 写回/删除，索引键维护同样按原 region 前缀（键不跨 region 迁移） |
| `CREATE INDEX` 回填 | `rowRanges` 展开扫全表，`indexKeyAt` 按行所在 region 写索引键 |
| `DROP INDEX` | `indexRanges`（单索引区间）展开删除 |
| `DROP TABLE` | 行区间 + 索引整表区间按 regions 展开删除 |
| `SplitTable` 迁移 | `remapTable` 用临时 Router 对行 + 全部索引键重路由 |

### 4.3 索引一致性

二级索引键（T6）与行键**归属同一 region**（写入时按行的 PK 路由），因此：

- 索引区间展开覆盖的每个 region 内，索引键与对应行键前缀一致 → 回表无需跨 region；
- 跨 region 索引扫描时按 region 分片扫描、合并排序，结果序与单 region 索引序一致（`ordered` 优化在合并后仍成立，因为合并按 innerKey 序）；
- UPDATE 改索引列时先删旧索引键（按原 region）、再写新索引键（按原 region 前缀；索引键前缀只依赖 tid/idxID/idxVal/pk 与 rid，rid 不因 UPDATE 改变）。

## 5. 与 M2 binlog 复制的关系

- M2 binlog 记录的是**写入的物理键**（已含 region 前缀）+ 值/删除标记；从端按位点幂等重放，因此**复制对分片键编码透明**——分片表与未分片表的复制路径完全一致。
- 分片元数据 `m:regions`（不带 region 前缀的全局键）通过同一事务/WAL/binlog 通道落盘与重放，从端重放后 Router 状态与主端一致。
- 取舍：binlog 不感知 region 语义（不记录"哪个 region 被修改"），只重放物理键写入。单机逻辑分片下该取舍成立；未来多节点物理分片需扩展 binlog 携带 region 迁移事件（State=LEAVING 等），属后续 M4 范围。

## 6. 默认单 region 兼容说明

- 未调用 `SplitTable` 的表 = 隐式整表单 region（`RegionID` 高位置 1 的合成 ID），所有存取路径照常带前缀。
- 新建表、DROP TABLE、索引、事务、复制在分片引入前后行为完全一致（已由全仓既有测试 + 分片回归用例验证）。
- `ListRegions` 对未分片表返回 1 个 `Explicit=false` 的隐式 region，便于协议层/管理端统一展示。

## 7. 分片管理入口

SQL 层（`Engine`）暴露三个管理方法，供 CLI / 协议层 / 测试使用：

```go
// 按主键边界切分表为 len(boundaries)+1 个显式 region（事务内迁移）
func (e *Engine) SplitTable(name string, boundaries [][]byte) error
// 查看表分片列表（显式划分或隐式单 region）
func (e *Engine) ListRegions(name string) ([]sharding.RegionInfo, error)
// 按主键字节定位所属 region（未命中返回 sharding.ErrRegionNotFound）
func (e *Engine) LocateRegion(name string, pk []byte) (storage.ID, error)
```

`pkg/db` 打开数据目录时自动加载落盘分片元数据（`Engine.LoadSharding`）。

## 8. 设计取舍

1. **逻辑分片先行**：M3 在单机内核上实现 region 前缀 + 路由 + 跨 region 查询，物理存储仍单实例；换取最小侵入、可完整测试的闭环，为多节点物理分片留接口。
2. **regionID 作为物理前缀而非表 ID**：保证同一 region 内键序连续且整 region 物理聚集（便于未来按 region 搬迁/扫描）；隐式 region 用高位 ID 避免与显式 ID 冲突。
3. **边界落在 innerKey 层级**：`StartKey`/`EndKey` 存内层行键（含 `s{tid}` 前缀），拼前缀即为物理区间，避免两层编码歧义（历史踩坑：对已完整内层键再编码导致双重前缀、扫描空集）。
4. **SplitTable 事务内迁移**：迁移 + 元数据落盘同事务，失败整体回滚；迁移用临时 Router，幂等（已在新 region 的键跳过）。
5. **索引键与行键同 region**：回表零跨区、索引扫描可按 region 分片，语义最简。
6. **binlog 不感知 region**：复制透明于分片键编码；region 迁移事件留给后续里程碑。
7. **行级过滤不做 region 级裁剪**：单机合并扫描成本可忽略；未来可下推谓词到 region 层优化。

## 9. 测试覆盖

`pkg/sql/sharding_test.go`（12 个用例）+ `pkg/sharding/sharding_test.go`：

| 用例 | 覆盖点 |
|---|---|
| `TestShardingRouter*`（pkg/sharding） | 隐式单 region、Locate 边界含/排他、二分定位、空洞/未命中报错、排序 |
| `TestSplitTableListRegions` | 划分后 region 列表、边界字段、ID 水位 |
| `TestSplitTableFullSelectConsistency` | 3 region 建表后 `SELECT *` 全量正确（跨 region 合并） |
| `TestSplitTablePointAndRange` | 跨 region 点查 / 范围查、边界点归属 |
| `TestSplitTableLocateRegion` | 管理入口定位正确性 |
| `TestSplitTableWithIndex` | 分片 + 二级索引：建索引回填、索引查询、UPDATE 索引维护 |
| `TestSplitTableDMLRegression` | 跨 region UPDATE/DELETE（含 `WHERE id IN (...)`）、边界重插、重复主键、计数 |
| `TestSplitTableDropTable` | 分片表 DROP 后物理键与元数据清理 |
| `TestSplitTableBoundaryValidation` | 非法边界（非升序）拒绝 |
| `TestSplitTableEmptyTable` | 空表划分与迁移 |
| `TestSplitTableOrderingMerge` | 跨 region 合并后的排序正确性（ORDER BY） |

## 10. 验收结果

```sh
go build ./...
go test ./... -count=1
```

本机真实工具链（mingw64 + rocksdb-11.8.1 静态库）全仓构建通过、全仓测试全绿（含 12 个分片用例与既有全部回归）。
*（内容由AI生成，仅供参考）*
