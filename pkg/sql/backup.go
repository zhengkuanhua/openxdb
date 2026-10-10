package sql

// M8(BR)：备份/恢复（BACKUP/RESTORE + PITR 基础），详见 docs/T18_backup_restore.md。
//
// 语法：
//   BACKUP TO '<path>'                      — 基于一致快照生成完整逻辑备份文件
//   RESTORE FROM '<path>' [TO LSN <n>]      — 校验并恢复备份；可选回放 binlog 到指定 LSN
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
type backupMeta struct {
	FormatVersion int    `json:"format_version"`
	CreatedAt     string `json:"created_at"`
	BackupLSN     uint64 `json:"backup_lsn"`     // 备份点 binlog 最大位点（PITR 回放起点）
	TablesRaw     []byte `json:"tables_raw"`     // m:tables 原值（"null" 表示空目录）
	HasRegions    bool   `json:"has_regions"`    // 是否含 region 路由元数据
	RegionsRaw    []byte `json:"regions_raw"`    // m:regions 原值
}

// execBackup BACKUP TO '<path>'：基于一致快照生成完整逻辑备份文件。
func (e *Executor) execBackup(s *BackupStmt) (*Result, error) {
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
	meta := backupMeta{
		FormatVersion: backupVersion,
		CreatedAt:     timeNow(),
		BackupLSN:     backupLSN,
		TablesRaw:     tablesRaw,
		HasRegions:    len(regionsRaw) > 0,
		RegionsRaw:    regionsRaw,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return nil, &SQLError{Msg: "backup failed: encode meta: " + err.Error()}
	}

	// 数据段：同一快照扫描全部用户表数据行（'s' 前缀）与索引键（'i' 前缀）。
	pairs, err := snap.Scan(storage.KeyRange{}, 0)
	if err != nil {
		return nil, &SQLError{Msg: "backup failed: scan snapshot: " + err.Error()}
	}
	// 数据段：同一快照扫描全部物理键，剥离 region 前缀后保留
	// 数据行（innerKey 's'）与索引键（innerKey 'i'）；元数据键（m:*）天然跳过。
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
		var head [8]byte
		binary.BigEndian.PutUint32(head[0:4], uint32(len(kv.Key)))
		binary.BigEndian.PutUint32(head[4:8], uint32(len(kv.Value)))
		data.Write(head[:])
		data.Write(kv.Key)
		data.Write(kv.Value)
	}
	tables := 0
	if len(tablesRaw) > 0 {
		if bt, terr := loadTables(func(key []byte) ([]byte, error) { return snap.Get(key) }); terr == nil {
			tables = len(bt)
		}
	}
	var end [8]byte // 结束标记 klen=0 vlen=0
	data.Write(end[:])
	dataCRC := crc32.ChecksumIEEE(data.Bytes())

	// 写文件（原子替换：先写 .tmp 再 rename）。
	tmpPath := s.Path + ".tmp"
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
	if _, err := f.Write(data.Bytes()); err != nil {
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
	if _, err := os.Stat(s.Path); err == nil {
		if err := os.Remove(s.Path); err != nil {
			return nil, &SQLError{Msg: "backup failed: replace old backup: " + err.Error()}
		}
	}
	if err := os.Rename(tmpPath, s.Path); err != nil {
		return nil, &SQLError{Msg: "backup failed: finalize: " + err.Error()}
	}
	return &Result{
		Columns:      []string{"path", "tables", "rows", "lsn"},
		AffectedRows: rows,
		Rows:         [][]Value{{StrVal(s.Path), IntVal(int64(tables)), IntVal(int64(rows)), IntVal(int64(backupLSN))}},
	}, nil
}

// execRestore RESTORE FROM '<path>' [TO LSN <n>]：校验并恢复备份快照。
// 幂等语义：目标库同名表先删（行+索引物理键）再按备份重建；整体事务，失败回滚。
// PITR：备份点之后的 binlog 记录回放到 TO LSN（含），物理写直接落到引擎级 Storage。
func (e *Executor) execRestore(s *RestoreStmt) (*Result, error) {
	if e.st == nil {
		return nil, &SQLError{Msg: "restore failed: storage not injected"}
	}
	meta, ops, err := readBackupFile(s.Path)
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
	if s.HasToLSN {
		if s.ToLSN < meta.BackupLSN {
			return nil, &SQLError{Msg: fmt.Sprintf("restore failed: TO LSN %d earlier than backup LSN %d", s.ToLSN, meta.BackupLSN)}
		}
		if s.ToLSN > meta.BackupLSN {
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
				if uint64(en.LSN) > s.ToLSN {
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
			finalLSN = s.ToLSN
		}
	}

	return &Result{
		Columns:      []string{"tables", "rows", "lsn"},
		AffectedRows: len(ops),
		Rows:         [][]Value{{IntVal(int64(len(bakTabs))), IntVal(int64(restoredRows)), IntVal(int64(finalLSN))}},
	}, nil
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
