---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_5dc2c976c1f911f197eb525400393706
    ReservedCode1: aVUbCGOVawCIebMJxWcsNhKOVWwrlWBhDlaah6EMWVlFoWo95kkkeApBcrItSdNXe+ZE6HOnYmAYSBtayTq7jKTtE3y7bPIwjfqvYmk1Galkqn/95qZV7RI2MJBmahgnNsesg+1t6PhtpDdJPL2iKIZvUxyHlaC7Xxb3TXFl1LvtaGzOcyRjNcZ6Azw=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_5dc2c976c1f911f197eb525400393706
    ReservedCode2: aVUbCGOVawCIebMJxWcsNhKOVWwrlWBhDlaah6EMWVlFoWo95kkkeApBcrItSdNXe+ZE6HOnYmAYSBtayTq7jKTtE3y7bPIwjfqvYmk1Galkqn/95qZV7RI2MJBmahgnNsesg+1t6PhtpDdJPL2iKIZvUxyHlaC7Xxb3TXFl1LvtaGzOcyRjNcZ6Azw=
---

# T2 WAL（Write-Ahead Log）实现文档

- 状态：已完成
- 关联手册：M1 开发手册 §3.3（T2 WAL）
- 代码：`pkg/wal/{wal.go, wal_impl.go, wal_test.go}`
- 依赖：`pkg/storage`（WriteBatch / KVPair / LSN）

## 1. 目标与验收

| 验收项 | 结果 |
|---|---|
| Append 顺序写 + fsync，返回即持久化（D 保证） | ✅ |
| Replay 崩溃恢复重放，损坏检测（CRC） | ✅ |
| Truncate checkpoint 截断 | ✅ |
| 重启后 LastLSN 正确续接 | ✅ |
| 单测 4/4 通过，全仓回归不破坏 T1 | ✅ |

## 2. 文件格式

```
文件头：magic "OPENXDBWAL"(10B) + version uint16(2B) = 12B
记录：  crc(4B) | len(4B) | lsn(8B) | txn(8B) | state(1B) | nputs(4B) | ndeletes(4B) | body
body：  puts: nputs × [klen(4B) vlen(4B) key val]
        deletes: ndeletes × [klen(4B) key]
crc：   crc32(IEEE)，覆盖 len 字段到记录结尾；全文件 Big Endian
```

- state 取值：1=Prepare，2=Commit，3=Rollback（供事务层重放判定）。
- len 字段上限 1GB（maxRecordBody），解码时校验 nputs/ndeletes 与 body 长度自洽，防畸形记录。

## 3. 接口

```go
type WAL interface {
    Append(entry *WalEntry) error   // entry.LSN==0 时自动分配 nextLSN
    Replay(apply func(*WalEntry)) error
    Truncate(lsn storage.LSN) error // 保留 lsn 及之后（checkpoint 后调用）
    LastLSN() storage.LSN
    Close() error
}
```

- `Open(path)`：文件不存在则建头；已存在则校验头 + Replay 扫描末尾定 nextLSN，再 seek 到文件尾。
- Append 加互斥锁顺序写 + `f.Sync()`，当前为每记录 fsync 的可靠模式；组提交留作 B2 写路径优化项（见 §5）。
- Truncate 写临时文件 → 关旧句柄 → rename 替换；Windows 下目标存在时先删旧文件再 rename（直接覆盖会被拒）。

## 4. 测试

```
=== RUN   TestWALAppendReplay        PASS（3 条记录含 Put/Delete，重放逐条校验）
=== RUN   TestWALPersistAcrossReopen PASS（Close 后重开数据仍在，续写 LSN 自动 = 6）
=== RUN   TestWALTruncate            PASS（Truncate(3) 后仅剩 LSN 3/4/5，重开仍生效）
=== RUN   TestWALCorruptionDetected  PASS（翻转记录字节后 Open/Replay 报 ErrCorrupt）
```

全仓回归：`go test ./...` → storage / rocksdb / wal 全部通过。

## 5. 已知限制与下一步

1. **组提交未实现**：当前每 Append 一次 fsync，事务并发提交时 TPS 会受 fsync 延迟限制；后续 B2 写路径基准若未达标，加入批量缓冲 + 定时 flush（组提交窗口）。
2. **与事务层集成**：T3 TxnManager 将调用 WAL：BEGIN 无日志、写操作先 Append Prepare、Commit 前 Append Commit、崩溃恢复时 Replay 将未 Commit 的 Prepare 回滚（Rollback 记录用于已显式回滚的标注）。
3. **Truncate 非原子**：Windows 场景为「写临时文件 → 删旧 → rename」，崩溃于中间态时会遗留 .tmp（Open 时忽略即可）；Linux 可换 renameat2 RENAME_EXCHANGE 做原子替换（暂不需要）。
4. 未做多 WAL 文件分段（WAL segment 切换与归档），M2 复制阶段再引入。
*（内容由AI生成，仅供参考）*
