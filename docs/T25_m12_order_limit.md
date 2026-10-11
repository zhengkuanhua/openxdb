---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_885ee792c53a11f197b3525400248c00
    ReservedCode1: G8z+dwssFSvgCuERNBt7VgirAU8l9fpekDqTKy9v33h0oDV6reQEBtH0my6g+SRyVALVS34iMrtkUHiYJSHXp/ZfFqoP4PY4KCz4gsdLZAnUoq7bzTg9LLZvmxtmfVsC3Mst0RhaIQKapC97E/Bht/u44xt/8dy4YzIJxe83oV9Fhkx8P60AWquoH2A=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_885ee792c53a11f197b3525400248c00
    ReservedCode2: G8z+dwssFSvgCuERNBt7VgirAU8l9fpekDqTKy9v33h0oDV6reQEBtH0my6g+SRyVALVS34iMrtkUHiYJSHXp/ZfFqoP4PY4KCz4gsdLZAnUoq7bzTg9LLZvmxtmfVsC3Mst0RhaIQKapC97E/Bht/u44xt/8dy4YzIJxe83oV9Fhkx8P60AWquoH2A=
---

# T25 M12 功能簇（2/3）：跨节点 ORDER BY / LIMIT 分页

> 版本：v5.0-P13-M12, v0.14.0-alpha
> 日期：2026-10-11
> 范围：`pkg/sql`（orderlimit.go / executor.go + 测试 `m12_orderlimit_test.go`）、`pkg/db`（跨节点集成测试 `db_m12_orderlimit_test.go`）
> 闭环：`go test ./pkg/sharding/ ./pkg/sql/ ./pkg/server/ -count=1` 全绿，不 push（README 勾选由 M12 整簇收尾提交统一更新）
> 提交：`M12: distributed order by/limit`（本地，未 push）

## 背景
OpenXDB 已具备单机 SQL 排序分页（`finishSelect`/`finishRel` 对单 region 全量结果排序 + LIMIT）与分布式扫描合并（`scanMergedCluster` 跨 region / 跨节点按内层物理键序合并）。但跨分片 SELECT 带 `ORDER BY` 时，数据分散在多个 region（可归属不同节点）：若直接全量拉回 coordinator 排序，跨节点传输随表增长放大；若不拉全量，又须保证 LIMIT/OFFSET 结果与单机语义一致。本子功能补齐**分布式 SQL 路由与归并层**：各分片本地排序后按需取窗口行（LIMIT 场景下推 top-N，OFFSET 场景返回 top-(offset+limit) 足量行），coordinator 归并排序后应用 LIMIT/OFFSET；无 ORDER BY 仅 LIMIT/OFFSET 时各分片按键序取足量再截断；单分区 / 无远端节点时退化为既有本地路径，行为与单机一致。

## 设计
### 执行模型
```
跨分片 SELECT 带 ORDER BY（无 WHERE）：
  每个 region（分片）本地排序 → 取 top-(offset+limit) 窗口行
    ├─ 本地 region：scanMergedVisible 扫描 → decode → sort.SliceStable(compareVal) → 截窗
    └─ 远端 region：cluster.ScanSortedRemote(携带 SortSpec{ColIndex,Kind,Desc,Scale,Limit})
                   由归属节点在 server.scanRange 本地排序 + 截窗后返回窗口行
  coordinator：统一 decode → sort.SliceStable(compareVal) 归并（与单机同序）
    → finishSelect(ordered=true) 应用 OFFSET/LIMIT 与投影
无 ORDER BY 仅 LIMIT/OFFSET（无 WHERE）：
  每个 region 按键序（内层物理键）取足量 offset+limit 行
    ├─ 本地：scanMergedVisible → 取前 n 行
    └─ 远端：cluster.ScanRemote(limit=n) 取前 n 行
  coordinator：按内层物理键序合并（innerLess，与 scanMergedCluster 同序）→ 截断 offset+limit
    → finishSelect(ordered=false) 应用 OFFSET/LIMIT
```

### 路由判定
- `canDistributedOrder`：集群启用（mgr != nil 且有远端节点）+ ORDER BY 裸列（无别名 Ref）+ 无聚合/窗口/限定列 + 有分页（LIMIT/OFFSET > 0）+ **无 WHERE** + 表 rowRanges 覆盖的 region 中存在**远端归属**（`hasRemoteRegion`），否则退化为既有全量路径；
- `canDistributedTop`：集群启用 + 无 ORDER BY + 有分页 + 无 WHERE + 存在远端 region；
- **无 WHERE 是窗口截断正确性前提**：窗口截断（每分片只取 offset+limit 行）必须发生在 WHERE 过滤之后，带 WHERE 时无法安全下推，走既有全量扫描 + coordinator 过滤排序（正确性优先）。

### 排序一致性
- 分片端按列类型比较（cluster.SortSpec 的 Kind），coordinator 端用 `compareVal`：INT/DECIMAL 均为数值比较（DECIMAL 同为 scale=4 定点整数），TEXT/DATE/BLOB 按字符串/字节序，两端等价；
- coordinator 使用稳定排序，与单机 `finishSelect` 排序序一致，保证分片/单机结果 DeepEqual 一致。

### 单机侧补齐（OFFSET 语义）
- `finishSelect` / `finishRel`：排序后先应用 OFFSET 截断，再应用 LIMIT（此前仅处理 LIMIT）；
- `EXPLAIN SELECT ... OFFSET n`：输出 `offset` 行。

### 代码落点
- `pkg/sql/orderlimit.go`（新增）：`canDistributedOrder` / `canDistributedTop` / `hasRemoteRegion` / `execDistributedOrder` / `scanMergedClusterTop` / `innerLess`；
- `pkg/sql/executor.go`：`execSelect` 在无 JOIN 路径 `rows==nil` 时先判分布式排序分页捷径；`execSelectRel` 单物理表 + 无 JOIN/子查询/GROUP BY 时同样接入；`finishSelect`/`finishRel`/`explainSelect` 增加 OFFSET 处理；
- `pkg/cluster`：`QueryReq.Sort *SortSpec` + `Client.ScanSortedRemote` + `Manager.ScanSortedRemote`（前序背景已有，本子功能复用）。

## 测试
### pkg/sql/m12_orderlimit_test.go（单引擎：退化路径 + 单机语义一致）
| 测试 | 覆盖 |
|---|---|
| `TestM12OrderLimitShardedAsc` | 拆 3 region 后 ORDER BY 升序 + LIMIT（退化本地路径）结果正确 |
| `TestM12OrderLimitShardedDesc` | 降序 top-N |
| `TestM12OrderLimitOffset` | LIMIT+OFFSET 先排序再跳偏移；偏移超出返回空集 |
| `TestM12OrderLimitNoOrderBy` | 无 ORDER BY 仅 LIMIT/OFFSET：内层物理键序截断 |
| `TestM12OrderLimitTypeSort` | TEXT 字典序（u1<u10<u2）、DECIMAL、DATE 列排序 |
| `TestM12OrderLimitWhereFallback` | 带 WHERE 走全量兜底，排序分页结果正确 |
| `TestM12OrderLimitSinglePartitionDegrade` | 未分片表 ORDER BY/LIMIT 与单机一致 |
| `TestM12OrderLimitExplainOffset` | EXPLAIN 输出 offset 行 |
| `TestM12OrderLimitAliasFallback` | 别名投影不误走分布式捷径，结果正确 |

### pkg/db/db_m12_orderlimit_test.go（双节点真分布式）
| 测试 | 覆盖 |
|---|---|
| `TestM12OrderLimitCrossNodeTopN` | 中间 region 指派给 B（远端）后 ORDER BY 升序 LIMIT 3/5/10，各分片窗口下推 + 归并正确 |
| `TestM12OrderLimitCrossNodeDesc` | 跨节点降序 top-N |
| `TestM12OrderLimitCrossNodeOffset` | ORDER BY + OFFSET（窗口取足量，协调端跳偏移）；含 DESC + OFFSET、偏移逼近行数 |
| `TestM12OrderLimitCrossNodeNoOrderBy` | 无 ORDER BY 仅 LIMIT/OFFSET 跨节点取足量截断 |
| `TestM12OrderLimitCrossNodeText` | 跨节点 TEXT 列排序下推一致性 |

## 取舍说明
- **LIMIT 下推 top-N、OFFSET 取足量**：OFFSET 场景每分片必须返回 top-(offset+limit) 而非仅 top-limit，否则合并后偏移错误；本实现统一窗口 fetchN = offset+limit，单次 RPC 即可满足；
- **无 ORDER BY 语义**：结果序 = 内层物理键序（region 键序合并），与单机全表扫描一致；文档明确此语义——无 ORDER BY 时不承诺用户可感知的全局顺序，仅保证取足量截断后结果正确且与单机一致；
- **无 WHERE 才下推**：下推窗口与 WHERE 过滤存在顺序冲突，宁可全量兜底也不冒险；后续可在分片端下推过滤后再截窗优化；
- **退化路径零回归**：单分区 / 未分片 / 无远端节点时 canDistributed* 恒假，行为与 M1-M8 既有本地执行完全一致；引擎层测试显式覆盖；
- **两端排序等价**：分片端（server.scanRange + SortSpec）与 coordinator（compareVal）比较规则同构，避免跨类型排序偏差；未用浮动字节序技巧，正确性优先。

## README 勾选

由 M12 整簇收尾提交统一执行：
- Status 行更新为 M12 分布式 SQL 纵深簇；
- T 表格追加 `T25 M12 distributed SQL cluster: distributed join + cross-node ORDER BY/paging + distributed subquery`；
- 文档列表追加 `T25_m12_order_limit.md`（随本提交加入）。

*（内容由AI生成，仅供参考）*
*（内容由AI生成，仅供参考）*
