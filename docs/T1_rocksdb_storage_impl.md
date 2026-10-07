---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_13c7c363c1f811f197eb525400393706
    ReservedCode1: /vwZghJOSo9Bns9weyN3zX+8bEmfbGYRAKDFN4BbG9R6bFz6q88LAlrKAIbsyPORQ9MH+lqmL6XxYe3ep7XsOUhq1qYWHbjyEcWPb8YvQavJbiEKKrZfq31aSxb6teoB9GmVdFOil/0NQlZWCdKH2ssQYmPK9DpE4Ur05wAYS9KnkwCJboqZquanWMg=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_13c7c363c1f811f197eb525400393706
    ReservedCode2: /vwZghJOSo9Bns9weyN3zX+8bEmfbGYRAKDFN4BbG9R6bFz6q88LAlrKAIbsyPORQ9MH+lqmL6XxYe3ep7XsOUhq1qYWHbjyEcWPb8YvQavJbiEKKrZfq31aSxb6teoB9GmVdFOil/0NQlZWCdKH2ssQYmPK9DpE4Ur05wAYS9KnkwCJboqZquanWMg=
---

# T1 RocksDB 存储实现（M1 存储层）

日期：2026-10-07
状态：完成，7/7 测试通过
前置：M0-lite 环境（Go1.27.1 / MinGW GCC16.2.0 / RocksDB 11.8.1 静态库）、E1 基准（见 E1_rocksdb_bench.md）

## 目标

按 v2.0-FP 开发手册 §3，实现 M1 的 Storage 接口 RocksDB 落地：
- Storage 接口（Get / Scan / Write 原子批次 / Snapshot / Close）全量实现
- Key 编码（EncodeRowKey / EncodeIndexKey，Big Endian）端到端验证
- 单测覆盖 CRUD、范围扫描、快照一致性、索引前缀扫描

## 实现结构

```
pkg/storage/             Storage 抽象 + Key 编码（M1 既有）
  storage.go             Storage / WriteBatch / KeyRange / Snapshot 接口
  keycodec.go            EncodeRowKey: t{tableID:8B} k{pk} v{lsn:8B}
                         EncodeIndexKey: t{tableID:8B} i{indexID:8B} k{idxVal} p{pk} v{lsn:8B}
  keycodec_test.go       编码结构/字节序/排序 4 用例
pkg/storage/rocksdb/     RocksDB 引擎实现
  bridge.h               C ABI 桥声明（Open/Get/Put/Delete/WriteBatch/Scan/Snapshot）
  bridge.cc              C++ 桥实现（RocksDB C++ API → C ABI，避免 cgo 直连 C++ 复杂类型）
  rocksdb.go             cgo 绑定 + Storage 接口实现
  rocksdb_test.go        引擎功能 3 用例
```

设计要点：
- 自研 C ABI 桥（非 grocksdb 第三方绑定）：静态链接可控、接口贴合项目手册、规避第三方版本漂移。
- 桥层返回码约定：0=成功、1=NotFound、-1=错误（errmsg 置位），错误串由 Go 侧负责释放。
- 扫描统一 [Start, End) 半开区间，End 空=无限；limit<=0=不限制。
- 快照基于 RocksDB Snapshot，满足备份/迁移复用场景。
- Go 侧所有 C 内存（val/errmsg/scan 项）均显式释放，无泄漏路径。

## 测试结果

```
ok  github.com/openxdb/openxdb/pkg/storage          0.647s
ok  github.com/openxdb/openxdb/pkg/storage/rocksdb  1.414s

=== RUN   TestEncodeRowKey                  PASS  编码结构/字节序
=== RUN   TestEncodeIndexKey               PASS  编码结构/字节序
=== RUN   TestIndexKeyOrderPreservesIdxVal PASS  索引值字典序
=== RUN   TestRowKeyOrder                  PASS  行键排序
=== RUN   TestStorageCRUD                  PASS  写/读/NotFound/更新/范围扫描/limit/批量删除+写
=== RUN   TestSnapshotConsistency          PASS  快照建立后写入不可见（一致性）
=== RUN   TestKeyEncodingEndToEnd          PASS  编码 Key 落库 + 索引前缀范围扫描命中
```

## 构建方式（Windows）

RocksDB 头文件/静态库路径通过 CGO 环境变量注入，代码不绑死绝对路径：

```powershell
$env:Path = 'C:\Users\27756\AppData\Local\OpenXDBTools\mingw64\bin;C:\Users\27756\AppData\Local\OpenXDBTools\go\bin;' + $env:Path
$env:CGO_ENABLED = '1'
$env:CGO_CFLAGS   = '-IC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-11.8.1\include'
$env:CGO_CXXFLAGS = '-IC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-11.8.1\include'
$env:CGO_LDFLAGS  = '-LC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-build -lrocksdb -lshlwapi -lrpcrt4 -lws2_32 -static-libgcc -static-libstdc++'
go test ./pkg/... -v
```

踩坑：cgo preamble 必须 `#include <stdlib.h>`，否则 `C.free` 报 "could not determine what C.free refers to"。

## 下一步候选

1. **T2 WAL**：Write-Ahead Log 接口实现（内存缓冲 + 文件 append + fsync，供 TxnManager 原子提交）。
2. **T3 事务层**：TxnManager 基于 WriteBatch + WAL 的单机事务（BEGIN/COMMIT/ROLLBACK、快照隔离）。
3. **E1b B+Tree 对照**：同一 Storage 接口下的 B+Tree 实现 + 基准，为 D1 引擎选型 ADR 提供对照数据（W4 里程碑）。
*（内容由AI生成，仅供参考）*
