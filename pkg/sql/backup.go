package sql

// M8(BR)：备份/恢复（BACKUP/RESTORE + PITR 基础），详见 docs/T18_backup_restore.md。
// T35：分布式备份一致性扩展（多节点协调、全局一致性标记），详见 docs/T35_m20_dist_backup_consistency.md。
//
// 语法：
//   BACKUP TO '<path>'                      — 基于一致快照生成完整逻辑备份文件
//   RESTORE FROM '<path>' [TO LSN <n>]      — 校验并恢复备份；可选回放 binlog 到指定 LSN
//
// 单机路径（mgr==nil 或未注册远端节点）：与 T18 完全一致，不产生 manifest。
// 集群路径（存在远端节点）：BACKUP 由协调者组织 PREPARE/DO 两阶段对齐全局 LSN，
//   额外生成 '<path>.manifest'（各节点备份文件清单 + 全局一致性标记）；
//   RESTORE 检测到 manifest 时按清单驱动各节点恢复到全局 LSN。
//
// 备份内容（单一文件，便于传输）：
//   - 全部表元数据（m:tables 原值：CREATE TABLE/CREATE INDEX 等价 DDL 记录）
//   - 全部数据行（物理键 s{tableID}{pk}，INSERT 等价记录；快照一致）
//   - region 路由与分片元数据（m:regions 原值：router/region 表）
//   - 备份点 LSN（备份时的 binlog 最大位点，TO LSN 回放的起点）
//   不备份 prepare 中间态（2PC 未决标记 m:2pc:* 天然不落快照行；仅已提交数据可见）。
//
// 文件格式（version 1）：
//   头部：magic(10B "OPENXDBBAK") + version(2B BE) + metaLen(4B BE)
//   meta：JSON(backupMeta) + metaCRC(4B BE, crc32(metaJSON))
//   数据段：klen(4B BE) vlen(4B BE) key value 循环；以 klen=0 vlen=0 结束
//   尾部：dataCRC(4B BE, crc32(数据段全部字节含结束标记))
//
// RESTORE 幂等语义：同名表已存在时"先 DROP 再建"——删除旧表的行/索引物理键后
// 按备份记录重建表结构与数据；RESTORE 为整体事务，任一步失败整体回滚。
// PITR：备份点 LSN 之后的 binlog 记录按 LSN 升序回放，直到指定 TO LSN（含）。

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

const (
	backupMagic     = "OPENXDBBAK"
	backupVersion   = 1
	backupHeaderLen = len(backupMagic) + 2 + 4 // magic + version + metaLen
	// backupRowPrefix 数据行物理键前缀（'s'，见 meta.go/rowKeyAt）。
	backupRowPrefix = 's'
	// backupIndexPrefix 二级索引物理键前缀（'i'，见 meta.go/EncodeIndexKey2）。
	// 备份/恢复将 's'（数据行）与 'i'（索引键）一并落盘/写回，恢复后索引立即可用。
	backupIndexPrefix = 'i'
	// backupEndMarker 数据段结束标记（klen=0 vlen=0）。
	backupEndMarker = 0
)

// backupMeta 备份文件元信息（JSON 段）。
// T35 新增字段（NodeID/BackupID/GlobalLSN/GlobalTSO）均为可选：单机备份与旧版
// reader 完全兼容（未知字段忽略），分布式备份据此完成节点归属与全局对齐。
type backupMeta struct {
	FormatVersion int    `json:"format_version"`
	CreatedAt     string `json:"created_at"`
	BackupLSN     uint64 `json:"backup_lsn"`     // 备份点 binlog 最大位点（PITR 回放起点）
	TablesRaw     []byte `json:"tables_raw"`     // m:tables 原值（"null" 表示空目录）
	HasRegions    bool   `json:"has_regions"`    // 是否含 region 路由元数据
	RegionsRaw    []byte `json:"regions_raw"`    // m:regions 原值

	NodeID    string `json:"node_id,omitempty"`     // 产生该备份的节点（单机为空）
	BackupID  string `json:"backup_id,omitempty"`   // 分布式备份批次 ID
	GlobalLSN uint64 `json:"global_lsn,omitempty"`  // 全局一致性点（0=单机）
	GlobalTSO uint64 `json:"global_tso,omitempty"`  // 全局一致性参考时间戳
}

// backupOutcome backupCore 的结果（供单机结果与分布式 DO 回执复用）。
type backupOutcome struct {
	BackupLSN uint64
	Tables    int
	Rows      int
}

// restoreOutcome restoreCore 的结果（供单机结果与分布式 DO 回执复用）。
type restoreOutcome struct {
	Tables int
	Rows   int
	LSN    uint64
}

// execBackup BACKUP TO '<path>'：单机走 backupCore；集群（存在远端节点）走
// distributedBackup（两阶段对齐全局 LSN，生成 manifest）。
func (e *Executor) execBackup(sess *Session, s *BackupStmt) (*Result, error) {
	// P2-12：BACKUP CONCURRENCY k 覆盖默认自动并发度（0/未指定 = 自动）。
	e.backupConcurrency = s.Concurrency
	if e.clusterEnabled() {
		return e.distributedBackup(s.Path)
	}
	out, err := e.backupCore(s.Path, "", "", 0)
	if err != nil {
		return nil, err
	}
	return &Result{
		Columns:      []string{"path", "tables", "rows", "lsn"},
		AffectedRows: out.Rows,
		Rows:         [][]Value{{StrVal(s.Path), IntVal(int64(out.Tables)), IntVal(int64(out.Rows)), IntVal(int64(out.BackupLSN))}},
	}, nil
}

// backupCore 单节点一致快照备份：生成 backupVersion 1 备份文件。
// 分布式场景由协调者传入 nodeID/backupID/globalLSN 写入 meta 全局字段；
// 单机场景传空/0，行为与 T18 完全一致。
func (e *Executor) backupCore(path, nodeID, backupID string, globalLSN uint64) (*backupOutcome, error) {
	if e.st == nil {
		return nil, &SQLError{Msg: "backup failed: storage not injected"}
	}
	snap, err := e.st.Snapshot()
	if err != nil {
		return nil, &SQLError{Msg: "backup failed: " + err.Error()}
	}
	defer snap.Release()

	// 元数据：表目录 + region 路由（同一快照，与并发写入隔离）。
	tablesRaw, err := snap.Get(metaKey)
	if err != nil && err != storage.ErrNotFound {
		return nil, &SQLError{Msg: "backup failed: read table catalog: " + err.Error()}
	}
	var regionsRaw []byte
	if raw, rerr := snap.Get(metaRegionsKey); rerr != nil {
		if rerr != storage.ErrNotFound {
			return nil, &SQLError{Msg: "backup failed: read region catalog: " + rerr.Error()}
		}
	} else {
		regionsRaw = raw
	}

	// 备份点 LSN：binlog 当前最大位点（TO LSN 回放的起点）。
	var backupLSN uint64
	if e.binlogPath != "" {
		if bs, berr := replication.OpenBinlog(e.binlogPath); berr == nil {
			backupLSN = uint64(bs.LastLSN())
			_ = bs.Close()
		}
	}
	var globalTSO uint64
	if e.ts != nil {
		if ts, terr := e.ts.Get(); terr == nil {
			globalTSO = ts
		}
	}
	meta := backupMeta{
		FormatVersion: backupVersion,
		CreatedAt:     timeNow(),
		BackupLSN:     backupLSN,
		TablesRaw:     tablesRaw,
		HasRegions:    len(regionsRaw) > 0,
		RegionsRaw:    regionsRaw,
		NodeID:        nodeID,
		BackupID:      backupID,
		GlobalLSN:     globalLSN,
		GlobalTSO:     globalTSO,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return nil, &SQLError{Msg: "backup failed: encode meta: " + err.Error()}
	}

	// 数据段：P2-12 分片级并行扫描（BACKUP CONCURRENCY 控制并发度），
	// 同一快照多 worker 并发扫描（表×region）物理区间；未显式指定时用
	// 默认自动并发度 min(GOMAXPROCS, 4)；串行路径与旧版完全一致
	// （单次全量 Scan，物理键序写入，字节级兼容）。
	bt, terr := loadTables(func(key []byte) ([]byte, error) { return snap.Get(key) })
	tables := 0
	if terr == nil {
		tables = len(bt)
	}
	workers := e.backupConcurrency
	if workers <= 0 {
		workers = 4
		if ncpu := runtime.GOMAXPROCS(0); ncpu > 0 && ncpu < 4 {
			workers = ncpu
		}
	}
	data, rows, serr := e.backupDataSegment(snap, bt, workers)
	if serr != nil {
		return nil, serr
	}
	dataCRC := crc32.ChecksumIEEE(data)

	// 写文件（原子替换：先写 .tmp 再 rename）。
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, &SQLError{Msg: "backup failed: create: " + err.Error()}
	}
	defer f.Close()
	header := make([]byte, backupHeaderLen)
	copy(header, backupMagic)
	binary.BigEndian.PutUint16(header[len(backupMagic):], backupVersion)
	binary.BigEndian.PutUint32(header[len(backupMagic)+2:], uint32(len(metaBytes)))
	if _, err := f.Write(header); err != nil {
		return nil, &SQLError{Msg: "backup failed: write header: " + err.Error()}
	}
	if _, err := f.Write(metaBytes); err != nil {
		return nil, &SQLError{Msg: "backup failed: write meta: " + err.Error()}
	}
	var mcrc [4]byte
	binary.BigEndian.PutUint32(mcrc[:], crc32.ChecksumIEEE(metaBytes))
	if _, err := f.Write(mcrc[:]); err != nil {
		return nil, &SQLError{Msg: "backup failed: write meta crc: " + err.Error()}
	}
	if _, err := f.Write(data); err != nil {
		return nil, &SQLError{Msg: "backup failed: write data: " + err.Error()}
	}
	var dcrc [4]byte
	binary.BigEndian.PutUint32(dcrc[:], dataCRC)
	if _, err := f.Write(dcrc[:]); err != nil {
		return nil, &SQLError{Msg: "backup failed: write data crc: " + err.Error()}
	}
	if err := f.Sync(); err != nil {
		return nil, &SQLError{Msg: "backup failed: sync: " + err.Error()}
	}
	if err := f.Close(); err != nil {
		return nil, &SQLError{Msg: "backup failed: close: " + err.Error()}
	}
	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err != nil {
			return nil, &SQLError{Msg: "backup failed: replace old backup: " + err.Error()}
		}
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return nil, &SQLError{Msg: "backup failed: finalize: " + err.Error()}
	}
	return &backupOutcome{BackupLSN: backupLSN, Tables: tables, Rows: rows}, nil
}

// execRestore RESTORE FROM '<path>' [TO LSN <n>]。
// 集群路径：'<path>.manifest' 存在时按清单驱动各节点恢复到全局 LSN；
// 否则单机路径（兼容旧备份文件与单机部署）。
func (e *Executor) execRestore(sess *Session, s *RestoreStmt) (*Result, error) {
	if manifestExists(s.Path) {
		return e.distributedRestore(s.Path)
	}
	out, err := e.restoreCore(s.Path, s.ToLSN, s.HasToLSN)
	if err != nil {
		return nil, err
	}
	return &Result{
		Columns:      []string{"tables", "rows", "lsn"},
		AffectedRows: out.Tables,
		Rows:         [][]Value{{IntVal(int64(out.Tables)), IntVal(int64(out.Rows)), IntVal(int64(out.LSN))}},
	}, nil
}

// restoreCore 单节点恢复：校验并恢复备份快照（整体事务、幂等），
// 可选 PITR 回放 binlog 到 toLSN（含）。toLSN 仅当 hasToLSN 时生效。
// 分布式场景由协调者传入全局 LSN（hasToLSN=true）完成跨节点对齐。
func (e *Executor) restoreCore(path string, toLSN uint64, hasToLSN bool) (*restoreOutcome, error) {
	if e.st == nil {
		return nil, &SQLError{Msg: "restore failed: storage not injected"}
	}
	meta, ops, err := readBackupFile(path)
	if err != nil {
		return nil, err
	}
	// 解析备份表目录（复用 loadTables 的元数据校验）。
	bakTabs, err := loadTables(func(key []byte) ([]byte, error) {
		if string(key) == string(metaKey) {
			return meta.TablesRaw, nil
		}
		return nil, storage.ErrNotFound
	})
	if err != nil {
		return nil, &SQLError{Msg: "restore failed: parse backup table catalog: " + err.Error()}
	}

	tx, err := e.tm.Begin()
	if err != nil {
		return nil, &SQLError{Msg: "restore failed: begin: " + err.Error()}
	}
	defer tx.Rollback()

	// 幂等：目标库同名表先删（行 + 索引物理键）。
	// 关键：备份包含的物理键不执行 Delete（由后续 Put 覆盖），
	// 避免同批 WriteBatch 中 Delete 后于 Put 应用导致同键行被删除。
	bakKeys := make(map[string]bool, len(ops))
	for _, op := range ops {
		bakKeys[string(op.Key)] = true
	}
	curTabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, &SQLError{Msg: "restore failed: read current tables: " + err.Error()}
	}
	for _, bt := range bakTabs {
		old := findTable(curTabs, bt.Name)
		if old == nil {
			continue
		}
		pairs, serr := e.scanMerged(tx, e.rowRanges(old))
		if serr != nil {
			return nil, &SQLError{Msg: "restore failed: scan rows of " + old.Name + ": " + serr.Error()}
		}
		for _, p := range pairs {
			if bakKeys[string(p.Key)] {
				continue
			}
			if derr := tx.Delete(p.Key); derr != nil {
				return nil, &SQLError{Msg: "restore failed: delete row of " + old.Name + ": " + derr.Error()}
			}
		}
		for _, idx := range old.Indexes {
			ip, ierr := e.scanMerged(tx, e.indexTableRanges(old, idx.ID))
			if ierr != nil {
				return nil, &SQLError{Msg: "restore failed: scan index of " + old.Name + ": " + ierr.Error()}
			}
			for _, p := range ip {
				if bakKeys[string(p.Key)] {
					continue
				}
				if derr := tx.Delete(p.Key); derr != nil {
					return nil, &SQLError{Msg: "restore failed: delete index of " + old.Name + ": " + derr.Error()}
				}
			}
		}
	}

	// 写回元数据与数据行（备份记录顺序：先结构后数据）。
	if err := tx.Put(metaKey, meta.TablesRaw); err != nil {
		return nil, &SQLError{Msg: "restore failed: write table catalog: " + err.Error()}
	}
	if meta.HasRegions {
		if err := tx.Put(metaRegionsKey, meta.RegionsRaw); err != nil {
			return nil, &SQLError{Msg: "restore failed: write region catalog: " + err.Error()}
		}
	} else {
		if err := tx.Delete(metaRegionsKey); err != nil {
			return nil, &SQLError{Msg: "restore failed: clear region catalog: " + err.Error()}
		}
	}
	for _, op := range ops {
		if err := tx.Put(op.Key, op.Value); err != nil {
			return nil, &SQLError{Msg: "restore failed: write row: " + err.Error()}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, &SQLError{Msg: "restore failed: commit: " + err.Error()}
	}

	restoredRows := 0
	for _, op := range ops {
		if len(op.Key) < sharding.RegionKeyPrefixLen {
			continue
		}
		_, inner := sharding.DecodeRegionKey(op.Key)
		if len(inner) > 0 && inner[0] == backupRowPrefix {
			restoredRows++
		}
	}

	// 内存路由重建。
	if meta.HasRegions {
		seq, regions, rerr := sharding.UnmarshalMeta(meta.RegionsRaw)
		if rerr != nil {
			return nil, &SQLError{Msg: "restore failed: corrupt region catalog in backup: " + rerr.Error()}
		}
		e.router.SetRegions(seq, regions)
	} else {
		e.router.SetRegions(0, nil)
	}

	// PITR：备份点之后的 binlog 回放到指定 TO LSN（含）。
	finalLSN := meta.BackupLSN
	if hasToLSN {
		if toLSN < meta.BackupLSN {
			return nil, &SQLError{Msg: fmt.Sprintf("restore failed: TO LSN %d earlier than backup LSN %d", toLSN, meta.BackupLSN)}
		}
		if toLSN > meta.BackupLSN {
			if e.binlogPath == "" {
				return nil, &SQLError{Msg: "restore failed: binlog not enabled, cannot replay to LSN"}
			}
			bs, berr := replication.OpenBinlog(e.binlogPath)
			if berr != nil {
				return nil, &SQLError{Msg: "restore failed: open binlog: " + berr.Error()}
			}
			defer bs.Close()
			errStop := errors.New("replay stop")
			rerr := bs.ReadFrom(storage.LSN(meta.BackupLSN+1), func(en *replication.BinlogEntry) error {
				if uint64(en.LSN) > toLSN {
					return errStop
				}
				if werr := e.st.Write(en.Batch); werr != nil {
					return werr
				}
				return nil
			})
			if rerr != nil && rerr != errStop {
				return nil, &SQLError{Msg: "restore failed: replay binlog: " + rerr.Error()}
			}
			finalLSN = toLSN
		}
	}

	return &restoreOutcome{Tables: len(bakTabs), Rows: restoredRows, LSN: finalLSN}, nil
}

// backupScanShard 备份数据段的一个并行扫描工作单元（表 × region）。
// 每个分片覆盖该 region 的行区间与全部索引区间，分片间物理键空间互不重叠，
// 合并后恰好覆盖全部 's'/'i' 键（与全量 Scan 等价）。
type backupScanShard struct {
	ranges []storage.KeyRange
}

// backupShardResult 单个分片扫描结果：字节块 + 行数；err 非空表示失败。
type backupShardResult struct {
	buf []byte
	rows int
	err error
}

// backupDataSegment 生成备份文件数据段字节（含结束标记）并统计行数。
// P2-12：
//   - 串行路径（workers<=1 或表目录解析失败）：单次全量 Scan，按物理键序
//     写入，与旧版字节级完全一致（零回归）；
//   - 并行路径（workers>1）：以（表×region）为分片，同一快照多 worker
//     并发 Scan 各自物理区间，各分片字节按固定顺序拼接，dataCRC 语义不变。
//     备份文件记录顺序与物理键序无关（RESTORE 按键值写回），语义等价。
// 快照共享并发安全：RocksDB 快照与 B+Tree 深拷贝均为只读。
func (e *Executor) backupDataSegment(snap storage.Snapshot, bt []*TableMeta, workers int) ([]byte, int, error) {
	if workers <= 1 || len(bt) == 0 {
		return e.backupDataSegmentSerial(snap)
	}
	return e.backupDataSegmentParallel(snap, bt, workers)
}

// backupDataSegmentSerial 旧版串行全量扫描路径（保持字节级兼容）。
func (e *Executor) backupDataSegmentSerial(snap storage.Snapshot) ([]byte, int, error) {
	pairs, err := snap.Scan(storage.KeyRange{}, 0)
	if err != nil {
		return nil, 0, &SQLError{Msg: "backup failed: scan snapshot: " + err.Error()}
	}
	var data bytes.Buffer
	rows := 0
	for _, kv := range pairs {
		if len(kv.Key) < sharding.RegionKeyPrefixLen {
			continue
		}
		_, inner := sharding.DecodeRegionKey(kv.Key)
		if len(inner) == 0 || (inner[0] != backupRowPrefix && inner[0] != backupIndexPrefix) {
			continue
		}
		if inner[0] == backupRowPrefix {
			rows++
		}
		writeBackupRecord(&data, kv.Key, kv.Value)
	}
	var end [8]byte // 结束标记 klen=0 vlen=0
	data.Write(end[:])
	return data.Bytes(), rows, nil
}

// backupDataSegmentParallel 分片级并行扫描路径（P2-12）。
// 分片 =（表, region）；每个分片依次扫描该 region 的行区间与各索引区间，
// 结果按分片编号顺序拼接（确定性输出，便于测试与 CRC 校验）。
func (e *Executor) backupDataSegmentParallel(snap storage.Snapshot, bt []*TableMeta, workers int) ([]byte, int, error) {
	var shards []backupScanShard
	for _, m := range bt {
		rs := e.regionsOf(m) // 显式 region 列表；无显式划分时返回隐式整表单 region
		for _, rg := range rs {
			// 行区间：[r{rid}s{tid}(StartKey), r{rid}s{tid+1}(EndKey))，
			// StartKey/EndKey 为内层行键边界（nil = 表起始/表结束），
			// 与 rowRanges 语义一致。
			var startInner []byte
			if rg.StartKey == nil {
				startInner = EncodeTableKey(m.ID, nil)
			} else {
				startInner = rg.StartKey
			}
			var endInner []byte
			if rg.EndKey == nil {
				endInner = EncodeTableKey(m.ID+1, nil)
			} else {
				endInner = rg.EndKey
			}
			rngs := []storage.KeyRange{{
				Start: sharding.EncodeRegionKey(rg.RegionID, startInner),
				End:   sharding.EncodeRegionKey(rg.RegionID, endInner),
			}}
			// 索引区间：该 region 上各索引键 [i{tid}{idxID}, i{tid}{idxID+1})。
			for _, idx := range m.Indexes {
				rngs = append(rngs, storage.KeyRange{
					Start: sharding.EncodeRegionKey(rg.RegionID, EncodeIndexKey2(m.ID, idx.ID, nil, nil)),
					End:   sharding.EncodeRegionKey(rg.RegionID, EncodeIndexKey2(m.ID, idx.ID+1, nil, nil)),
				})
			}
			shards = append(shards, backupScanShard{ranges: rngs})
		}
	}
	if len(shards) == 0 {
		// 空库：仅写结束标记，与串行空扫描结果一致。
		var end [8]byte
		return end[:], 0, nil
	}
	if workers > len(shards) {
		workers = len(shards)
	}

	results := make([]backupShardResult, len(shards))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i, sh := range shards {
		wg.Add(1)
		go func(i int, sh backupScanShard) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			var buf bytes.Buffer
			rows := 0
			for _, r := range sh.ranges {
				pairs, serr := snap.Scan(r, 0)
				if serr != nil {
					results[i] = backupShardResult{err: serr}
					return
				}
				for _, kv := range pairs {
					if len(kv.Key) < sharding.RegionKeyPrefixLen {
						continue
					}
					_, inner := sharding.DecodeRegionKey(kv.Key)
					if len(inner) == 0 || (inner[0] != backupRowPrefix && inner[0] != backupIndexPrefix) {
						continue
					}
					if inner[0] == backupRowPrefix {
						rows++
					}
					writeBackupRecord(&buf, kv.Key, kv.Value)
				}
			}
			results[i] = backupShardResult{buf: buf.Bytes(), rows: rows}
		}(i, sh)
	}
	wg.Wait()

	// 汇总：任一分片失败整体失败（与串行路径一致，不产生部分备份文件）。
	var data bytes.Buffer
	totalRows := 0
	for _, res := range results {
		if res.err != nil {
			return nil, 0, &SQLError{Msg: "backup failed: scan shard: " + res.err.Error()}
		}
		data.Write(res.buf)
		totalRows += res.rows
	}
	var end [8]byte // 结束标记 klen=0 vlen=0
	data.Write(end[:])
	return data.Bytes(), totalRows, nil
}

// writeBackupRecord 写一条备份记录：klen(4B BE) vlen(4B BE) key value。
func writeBackupRecord(buf *bytes.Buffer, key, value []byte) {
	var head [8]byte
	binary.BigEndian.PutUint32(head[0:4], uint32(len(key)))
	binary.BigEndian.PutUint32(head[4:8], uint32(len(value)))
	buf.Write(head[:])
	buf.Write(key)
	buf.Write(value)
}

// readBackupFile 读取并校验备份文件（magic/version/metaCRC/dataCRC），
// 返回元信息与数据行键值列表。
func readBackupFile(path string) (*backupMeta, []storage.KVPair, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, &SQLError{Msg: "restore failed: open backup: " + err.Error()}
	}
	defer f.Close()
	header := make([]byte, backupHeaderLen)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, nil, &SQLError{Msg: "restore failed: read header: " + err.Error()}
	}
	if string(header[:len(backupMagic)]) != backupMagic {
		return nil, nil, &SQLError{Msg: "restore failed: invalid backup file (bad magic)"}
	}
	if binary.BigEndian.Uint16(header[len(backupMagic):]) != backupVersion {
		return nil, nil, &SQLError{Msg: "restore failed: unsupported backup version"}
	}
	metaLen := binary.BigEndian.Uint32(header[len(backupMagic)+2:])
	if metaLen == 0 || metaLen > 1<<30 {
		return nil, nil, &SQLError{Msg: "restore failed: invalid meta length"}
	}
	metaBytes := make([]byte, metaLen)
	if _, err := io.ReadFull(f, metaBytes); err != nil {
		return nil, nil, &SQLError{Msg: "restore failed: read meta: " + err.Error()}
	}
	var mcrc [4]byte
	if _, err := io.ReadFull(f, mcrc[:]); err != nil {
		return nil, nil, &SQLError{Msg: "restore failed: read meta crc: " + err.Error()}
	}
	if crc32.ChecksumIEEE(metaBytes) != binary.BigEndian.Uint32(mcrc[:]) {
		return nil, nil, &SQLError{Msg: "restore failed: backup meta checksum mismatch (file corrupted)"}
	}
	var meta backupMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, nil, &SQLError{Msg: "restore failed: decode meta: " + err.Error()}
	}
	if meta.FormatVersion != backupVersion {
		return nil, nil, &SQLError{Msg: "restore failed: unsupported backup format version"}
	}

	// 数据段：累计全部字节（含结束标记）供尾部 CRC 校验。
	var data bytes.Buffer
	var ops []storage.KVPair
	for {
		var head [8]byte
		if _, err := io.ReadFull(f, head[:]); err != nil {
			return nil, nil, &SQLError{Msg: "restore failed: read record head: " + err.Error()}
		}
		data.Write(head[:])
		klen := binary.BigEndian.Uint32(head[0:4])
		vlen := binary.BigEndian.Uint32(head[4:8])
		if klen == backupEndMarker && vlen == backupEndMarker {
			break
		}
		if klen == 0 || klen > 1<<30 || vlen > 1<<30 {
			return nil, nil, &SQLError{Msg: "restore failed: invalid record length"}
		}
		k := make([]byte, klen)
		if _, err := io.ReadFull(f, k); err != nil {
			return nil, nil, &SQLError{Msg: "restore failed: read key: " + err.Error()}
		}
		data.Write(k)
		v := make([]byte, vlen)
		if _, err := io.ReadFull(f, v); err != nil {
			return nil, nil, &SQLError{Msg: "restore failed: read value: " + err.Error()}
		}
		data.Write(v)
		ops = append(ops, storage.KVPair{Key: k, Value: v})
	}
	var dcrc [4]byte
	if _, err := io.ReadFull(f, dcrc[:]); err != nil {
		return nil, nil, &SQLError{Msg: "restore failed: read data crc: " + err.Error()}
	}
	if crc32.ChecksumIEEE(data.Bytes()) != binary.BigEndian.Uint32(dcrc[:]) {
		return nil, nil, &SQLError{Msg: "restore failed: backup data checksum mismatch (file corrupted)"}
	}
	return &meta, ops, nil
}

// manifestExists 判断是否存在分布式备份 manifest（协调者备份路径旁）。
func manifestExists(path string) bool {
	_, err := os.Stat(path + ".manifest")
	return err == nil
}
