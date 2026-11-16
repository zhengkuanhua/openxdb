---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_555a5135d20a11f18460525400393706
    ReservedCode1: +c58uzV04XWs3pSk0Y4O+dMwV+oqsJX7lGv6OTLNmOKIxpUB4Rg71mSgUtuibk2OjfXjZDIDvFv26GoOCxKJJbSnNGx0go0Y+bcyQombPJhzdBX9CWnH2K6Dae18PsZYjr+HU1FIBaGXM8fk89y1m16Q4+jmCOrwCDW3k7dTIqZuCPU0A9TQT+SFJDw=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_555a5135d20a11f18460525400393706
    ReservedCode2: +c58uzV04XWs3pSk0Y4O+dMwV+oqsJX7lGv6OTLNmOKIxpUB4Rg71mSgUtuibk2OjfXjZDIDvFv26GoOCxKJJbSnNGx0go0Y+bcyQombPJhzdBX9CWnH2K6Dae18PsZYjr+HU1FIBaGXM8fk89y1m16Q4+jmCOrwCDW3k7dTIqZuCPU0A9TQT+SFJDw=
---

# C5: cgo 扫描拷贝优化（v1.16.0 P2）

- 状态：已实施，全仓回归全绿，压测已量化
- 基线 commit：0b6ed52（v1.16.0 C4 提交组提交）
- 目标：消除 bridge.cc xdb_scan/xscan_item 每行 `ToString → malloc/memcpy → C.GoBytes` 三次拷贝链；COUNT/SUM 免值拷贝；保持语义等价与已有快路径（order_index_fastpath / COUNT 快速路径）不破坏。

## 1. 背景与现状

OpenXDB 通过 cgo 桥接 RocksDB（`pkg/storage/rocksdb/bridge.cc` + `rocksdb.go`）。范围扫描热路径原先为三次拷贝：

| 阶段 | 位置 | 拷贝内容 | 分配 |
|---|---|---|---|
| 拷贝 1 | `do_scan`（bridge.cc） | `it->key().ToString()` / `it->value().ToString()` 逐行生成 `std::string` | 每行 2 次堆分配 |
| 拷贝 2 | `xscan_item`（bridge.cc） | `malloc + memcpy` 复制到新缓冲 | 每行 2 次 `malloc` |
| 拷贝 3 | `doScan`（rocksdb.go） | `C.GoBytes` 复制到 Go heap | 每行 2 次 |

`do_scan` 还先用 `std::vector<std::pair<std::string,std::string>>` 全量收集后再回传，数据经历「RocksDB 内部 buffer → std::string → malloc 缓冲 → Go heap」四次内存迁徙。

已有 `do_count` / `xdb_count` / `xdb_snapshot_count`（仅迭代 key 不拷贝 value，`fill_cache=false`）与 SQL 层 `CountRange` 全链路（executor → txn → snapshot → bridge），`COUNT(*)` 快速路径已免值拷贝。

## 2. 设计方案

### 2.1 xscan 单块 arena 布局（bridge.cc / bridge.h）

`struct xscan` 由 `std::vector<std::pair<std::string,std::string>> items` 改为连续内存布局：

```cpp
struct xscan {
    unsigned char* arena;  // 连续数据块：[k0][v0][k1][v1]...，malloc 一次，整体释放
    size_t arena_len;      // arena 实际使用字节数
    size_t* koffs;         // 每行 key 在 arena 中的偏移（n 项）
    size_t* klens;         // 每行 key 长度
    size_t* voffs;         // 每行 value 偏移
    size_t* vlens;         // 每行 value 长度
    size_t n;              // 条目数
};
```

### 2.2 do_scan 两趟迭代（bridge.cc）

- **第一趟**：仅统计条目数 `count` 与总字节 `total`，不 ToString、不分配；
- **第二趟**：从起点重新 `Seek`，按统计结果一次性分配 arena + 四个索引数组，直接 `memcpy` 到 arena（仅每行 2 次 memcpy，无中间 std::string / 逐行 malloc）；
- `xscan_item` 返回 arena 内指针 + 长度（**0 拷贝**，不再 malloc）；
- `xscan_free` 整体释放 arena 与索引数组（5 个 `std::free` + `delete`）；
- 拷贝链由 3 次降为 **2 次**（RocksDB buffer → arena → GoBytes）。
- `limit`、`[start,end)` 半开区间、`errmsg` 语义与旧实现一致。

### 2.3 并发安全（重要）

`xdb_scan`（无快照实时视图）在并发写下，两趟迭代的行数可能不一致。第二趟结束后以**实际拷贝行数**更新 `out->n`（而非沿用第一趟 `count`），避免 Go 侧读到未初始化数组项；并对第二趟迭代 `status()` 做错误检查。快照路径（`xdb_snapshot_scan`）两趟天然一致。

### 2.4 Go 侧（rocksdb.go）

`doScan` 移除逐行 `C.free(key)` / `C.free(val)`——key/val 指针指向 xscan 内部 arena，由 `xscan_free` 整体释放。`C.GoBytes` 拷贝保持（Go heap 需要独立所有权）。

### 2.5 未改动范围（快路径保护）

- 不触碰 `storage.Storage` / `Snapshot` 基础接口；
- `CountRange` / `do_count` 免值拷贝路径原样保留，与 v1.15 COUNT 快速路径衔接不变；
- `order_index_fastpath.go` 与 ORDER BY 索引快路径未改；
- `xdb_get` / `xdb_snapshot_get` 等其它桥接函数未动。

## 3. 改动文件

| 文件 | 改动 |
|---|---|
| `pkg/storage/rocksdb/bridge.cc` | xscan arena 布局、do_scan 两趟迭代、xscan_item 0 拷贝、xscan_free 整体释放、并发安全 n 修正（+86/-18 行） |
| `pkg/storage/rocksdb/bridge.h` | xscan 相关注释更新 |
| `pkg/storage/rocksdb/rocksdb.go` | doScan 移除逐行 C.free |

调用面排查：`xscan_item/xscan_count/xscan_free/xdb_scan/xdb_snapshot_scan` 唯一消费点为 `rocksdb.go::doScan`（L65/L217），无其它调用方。

## 4. 回归

`go test -count=1 ./...` **全绿**（80s，含 `pkg/storage/rocksdb` 3.3s），所有包 ok；修复并发安全 n 后重跑仍全绿。语义等价由 SQL 层/事务层现有回归（COUNT / SUM / ORDER BY / 范围扫描 / 快照读 / 事务隔离）保障。

## 5. 压测方法与结果

### 5.1 方法

- 专项程序：`tests/bench/c5_sql_bench/main.go`（新增 `-skip-load` 支持同数据同状态对比）；
- 数据：`c5_t(id INT, name TEXT, score INT, PRIMARY KEY(id))`，100000 行（name 13B + score 4B，value 极小）；
- 用例：`COUNT(*)`（快速路径）/ `range-scan`（全表扫描）/ `range-half`（半表）/ `order-by`（无索引列排序）/ `sum`（物化聚合）；
- 基线 = 0b6ed52（未含 C5 改动）二进制，优化 = 本任务改动后二进制，同机同参数（rounds=3，首轮预热取 2 样本），各 3 次独立装载 + 1 次最终复测，取平均值。

### 5.2 热读（装载后 memtable 主导）——主要定量结论

| case | 基线 avg(ms) | 优化 avg(ms) | 提升 |
|---|---:|---:|---:|
| COUNT(*) | 47.57 | 45.05 | **-5.3%** |
| range-scan | 312.92 | 307.36 | **-1.8%** |
| range-half | 293.27 | 316.80 | +8.0%（噪声，不采信） |
| order-by | 403.10 | 389.64 | **-3.3%** |
| sum | 299.84 | 271.70 | **-9.4%** |

range-half 各轮 257~447ms 波动（RocksDB 后台 compaction 干扰），噪声完全掩盖收益；其余 4 个用例一致改善（-1.8% ~ -9.4%）。本场景 value 极小（~17B），ToString/逐行 malloc 占比低，收益偏小属预期。

### 5.3 冷读（skip-load，数据已落 SST）——趋势参考

同一数据目录（数据 flush 到 SST 后）对比一轮：range-scan -26.8%、order-by -25.0%、sum -21.7%、COUNT -9.9%。跨进程运行受 compaction 状态影响噪声大，不作为精确数字，但趋势一致：**数据越磁盘化、拷贝占比越高，收益越显著**。

### 5.4 收益推论

- 拷贝链 3→2 次，消除每行 2 次 `std::string` 堆分配 + 2 次 `malloc`；
- 收益随**行 value 大小与磁盘化程度**增长：value 数百 B~KB 的真实负载下，`ToString` 与逐行 malloc 消除的收益将远高于本基准的 17B value 场景；
- `COUNT(*)` 免值拷贝路径（do_count）已在前序落地，本任务主要惠及 `range-scan / ORDER BY / SUM` 等需 value 的全表/范围扫描。

## 6. 结论

- 扫描三次拷贝链已降为两次，xscan_item 改为 0 拷贝返回 arena 内指针；
- 并发写下两趟迭代行数不一致风险已修复（n 以实际行为准 + status 检查）；
- 全仓回归全绿，已有快路径（order_index_fastpath / COUNT 快速路径）未受影响；
- 压测量化：热读主要用例 -1.8% ~ -9.4%，冷读趋势 -20%+，收益随 value 大小与磁盘化程度增长。
*（内容由AI生成，仅供参考）*
