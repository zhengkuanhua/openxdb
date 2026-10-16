// Package rocksdb 提供基于 RocksDB 引擎的 Storage 实现（M1 T1）。
//
// 依赖构建时注入：CGO_CFLAGS / CGO_CXXFLAGS 指向 RocksDB include，
// CGO_LDFLAGS 指向 librocksdb.a（Windows 需附加 shlwapi/rpcrt4/ws2_32）。
package rocksdb

/*
#cgo CXXFLAGS: -std=c++20
#cgo windows LDFLAGS: -static-libstdc++ -static-libgcc -lwinpthread -lshlwapi -lrpcrt4 -lws2_32
#cgo linux LDFLAGS: -lstdc++ -lpthread
#cgo darwin LDFLAGS: -lstdc++
#include <stdlib.h>
#include <string.h>
#include "bridge.h"
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

type rocks struct {
	h *C.xdb
}

// Open 打开（或创建）RocksDB 数据库目录。
func Open(dir string, createIfMissing bool) (storage.Storage, error) {
	cdir := C.CString(dir)
	defer C.free(unsafe.Pointer(cdir))
	var errmsg *C.char
	h := C.xdb_open(cdir, boolToInt(createIfMissing), &errmsg)
	if h == nil {
		err := fmt.Errorf("rocksdb: open %s: %s", dir, cString(errmsg))
		freeStr(errmsg)
		return nil, err
	}
	r := &rocks{h: h}
	runtime.SetFinalizer(r, (*rocks).Close)
	return r, nil
}

func (r *rocks) Get(key []byte) ([]byte, error) {
	var val *C.uchar
	var vlen C.size_t
	var errmsg *C.char
	ret := C.xdb_get(r.h, cBytes(key), C.size_t(len(key)), &val, &vlen, &errmsg)
	switch ret {
	case 1:
		return nil, storage.ErrNotFound
	case -1:
		err := fmt.Errorf("rocksdb: get: %s", cString(errmsg))
		freeStr(errmsg)
		return nil, err
	}
	defer C.free(unsafe.Pointer(val))
	return C.GoBytes(unsafe.Pointer(val), C.int(vlen)), nil
}

func (r *rocks) Scan(rng storage.KeyRange, limit int) ([]storage.KVPair, error) {
	var errmsg *C.char
	return doScan(C.xdb_scan(
		r.h,
		cBytesOrNil(rng.Start), C.size_t(len(rng.Start)),
		cBytesOrNil(rng.End), C.size_t(len(rng.End)),
		C.int(limit), &errmsg), rng, errmsg)
}

func (r *rocks) Write(batch *storage.WriteBatch) error {
	if batch == nil {
		return nil
	}
	var pairs []byte
	putSizes := make([]C.size_t, 0, len(batch.Puts)*2)
	for _, p := range batch.Puts {
		pairs = append(pairs, p.Key...)
		pairs = append(pairs, p.Value...)
		putSizes = append(putSizes, C.size_t(len(p.Key)), C.size_t(len(p.Value)))
	}
	var delkeys []byte
	delSizes := make([]C.size_t, 0, len(batch.Deletes))
	for _, k := range batch.Deletes {
		delkeys = append(delkeys, k...)
		delSizes = append(delSizes, C.size_t(len(k)))
	}
	var errmsg *C.char
	ret := C.xdb_write_batch(r.h,
		cBytesOrNil(pairs), cSizesOrNil(putSizes), C.int(len(batch.Puts)),
		cBytesOrNil(delkeys), cSizesOrNil(delSizes), C.int(len(batch.Deletes)),
		&errmsg)
	if ret != 0 {
		err := fmt.Errorf("rocksdb: write batch: %s", cString(errmsg))
		freeStr(errmsg)
		return err
	}
	return nil
}

func (r *rocks) Snapshot() (storage.Snapshot, error) {
	s := C.xdb_snapshot_create(r.h)
	if s == nil {
		return nil, fmt.Errorf("rocksdb: snapshot create failed")
	}
	return &snapshot{s: s}, nil
}

func (r *rocks) Close() error {
	if r.h == nil {
		return nil
	}
	C.xdb_close(r.h)
	r.h = nil
	return nil
}

// RocksStats RocksDB 内部统计快照（LSM/SST/MemTable/Compaction 等）。
// 由 xdb_stats_get 一次调用批量采集，供监控看板 / 管理台对接。
type RocksStats struct {
	NumLevels             int64    // LSM 层数（level 0 为内存 flush 目标层）
	NumFilesAtLevel       [8]int64 // 各层 SST 文件数（i < NumLevels）
	CurSizeAllMemTables   int64    // 当前 MemTable 总大小（字节）
	SizeAllMemTables      int64    // 含 immutable MemTable 的总大小（字节）
	CompactionPending     int64    // 待 compaction 任务数
	NumRunningCompactions int64    // 运行中 compaction 数
	NumRunningFlushes     int64    // 运行中 flush 数
	LiveSSTFilesSize      int64    // 活跃 SST 文件总字节
	NumLiveVersions       int64    // 当前 live versions 数
	EstimateNumKeys       int64    // 估算键数
}

// TotalSSTFiles 所有层 SST 文件总数。
func (s *RocksStats) TotalSSTFiles() int64 {
	var n int64
	for i := int64(0); i < s.NumLevels && i < 8; i++ {
		n += s.NumFilesAtLevel[i]
	}
	return n
}

// Stats 采集 RocksDB 内部统计（Storage 接口之外的可选能力，
// 通过类型断言使用：st.(*rocks).Stats()）。
func (r *rocks) Stats() (*RocksStats, error) {
	if r.h == nil {
		return nil, fmt.Errorf("rocksdb: stats: closed")
	}
	var cs C.xdb_stats
	var errmsg *C.char
	ret := C.xdb_stats_get(r.h, &cs, &errmsg)
	if ret != 0 {
		err := fmt.Errorf("rocksdb: stats: %s", cString(errmsg))
		freeStr(errmsg)
		return nil, err
	}
	s := &RocksStats{
		NumLevels:             int64(cs.num_levels),
		CurSizeAllMemTables:   int64(cs.cur_size_all_mem_tables),
		SizeAllMemTables:      int64(cs.size_all_mem_tables),
		CompactionPending:     int64(cs.compaction_pending),
		NumRunningCompactions: int64(cs.num_running_compactions),
		NumRunningFlushes:     int64(cs.num_running_flushes),
		LiveSSTFilesSize:      int64(cs.live_sst_files_size),
		NumLiveVersions:       int64(cs.num_live_versions),
		EstimateNumKeys:       int64(cs.estimate_num_keys),
	}
	for i := 0; i < 8; i++ {
		s.NumFilesAtLevel[i] = int64(cs.num_files_at_level[i])
	}
	return s, nil
}

type snapshot struct {
	s *C.xsnap
}

func (sn *snapshot) Get(key []byte) ([]byte, error) {
	var val *C.uchar
	var vlen C.size_t
	var errmsg *C.char
	ret := C.xdb_snapshot_get(sn.s, cBytes(key), C.size_t(len(key)), &val, &vlen, &errmsg)
	switch ret {
	case 1:
		return nil, storage.ErrNotFound
	case -1:
		err := fmt.Errorf("rocksdb: snapshot get: %s", cString(errmsg))
		freeStr(errmsg)
		return nil, err
	}
	defer C.free(unsafe.Pointer(val))
	return C.GoBytes(unsafe.Pointer(val), C.int(vlen)), nil
}

func (sn *snapshot) Scan(rng storage.KeyRange, limit int) ([]storage.KVPair, error) {
	var errmsg *C.char
	return doScan(C.xdb_snapshot_scan(
		sn.s,
		cBytesOrNil(rng.Start), C.size_t(len(rng.Start)),
		cBytesOrNil(rng.End), C.size_t(len(rng.End)),
		C.int(limit), &errmsg), rng, errmsg)
}

func (sn *snapshot) MGet(keys [][]byte) (map[string][]byte, error) {
	if sn.s == nil {
		return nil, fmt.Errorf("rocksdb: snapshot released")
	}
	n := len(keys)
	got := make(map[string][]byte, n)
	if n == 0 {
		return got, nil
	}
	// cgo 指针安全：Go 指针不得存入 C 内存，key 数据整体拷贝到 C 连续缓冲区。
	total := 0
	for _, k := range keys {
		total += len(k)
	}
	kbuf := C.malloc(C.size_t(total))
	offsPtr := C.malloc(C.size_t(n) * C.size_t(unsafe.Sizeof(C.size_t(0))))
	lenPtrs := C.malloc(C.size_t(n) * C.size_t(unsafe.Sizeof(C.size_t(0))))
	if kbuf == nil || offsPtr == nil || lenPtrs == nil {
		if kbuf != nil {
			C.free(kbuf)
		}
		if offsPtr != nil {
			C.free(offsPtr)
		}
		if lenPtrs != nil {
			C.free(lenPtrs)
		}
		return nil, fmt.Errorf("rocksdb: malloc failed for MGet")
	}
	defer C.free(kbuf)
	defer C.free(offsPtr)
	defer C.free(lenPtrs)
	offs := unsafe.Slice((*C.size_t)(offsPtr), n)
	lens := unsafe.Slice((*C.size_t)(lenPtrs), n)
	off := 0
	for i, k := range keys {
		if len(k) > 0 {
			C.memcpy(unsafe.Add(kbuf, uintptr(off)), unsafe.Pointer(cBytes(k)), C.size_t(len(k)))
		}
		offs[i] = C.size_t(off)
		lens[i] = C.size_t(len(k))
		off += len(k)
	}
	var errmsg *C.char
	items := C.xdb_snapshot_multi_get2(sn.s, (*C.uchar)(kbuf), (*C.size_t)(offsPtr), (*C.size_t)(lenPtrs), C.int(n), &errmsg)
	if items == nil {
		if errmsg != nil {
			err := fmt.Errorf("rocksdb: snapshot mget: %s", cString(errmsg))
			freeStr(errmsg)
			return nil, err
		}
		return nil, fmt.Errorf("rocksdb: snapshot mget: no items")
	}
	defer C.xmget_free(items, C.int(n))
	itemSize := unsafe.Sizeof(C.xmget_item{})
	base := unsafe.Pointer(items)
	for i := 0; i < n; i++ {
		it := (*C.xmget_item)(unsafe.Add(base, uintptr(i)*itemSize))
		if it.found == 0 {
			continue
		}
		got[string(keys[i])] = C.GoBytes(unsafe.Pointer(it.val), C.int(it.vlen))
	}
	return got, nil
}

func (sn *snapshot) Release() {
	if sn.s == nil {
		return
	}
	C.xdb_snapshot_release(sn.s)
	sn.s = nil
}

// StatsOf 从 storage.Storage 提取 RocksDB 内部统计；非 RocksDB 实现返回错误。
// 供管理台 / metrics 采集源装配使用：rocksdb.StatsOf(d.Storage)。
func StatsOf(st storage.Storage) (*RocksStats, error) {
	r, ok := st.(*rocks)
	if !ok {
		return nil, fmt.Errorf("rocksdb: stats: storage is not rocksdb-backed")
	}
	return r.Stats()
}

// ---- cgo 辅助 ----

func doScan(s *C.xscan, rng storage.KeyRange, errmsg *C.char) ([]storage.KVPair, error) {
	if s == nil {
		err := fmt.Errorf("rocksdb: scan: %s", cString(errmsg))
		freeStr(errmsg)
		return nil, err
	}
	defer C.xscan_free(s)
	n := int(C.xscan_count(s))
	out := make([]storage.KVPair, 0, n)
	for i := 0; i < n; i++ {
		var key, val *C.uchar
		var klen, vlen C.size_t
		if C.xscan_item(s, C.int(i), &key, &klen, &val, &vlen) != 0 {
			break
		}
		k := C.GoBytes(unsafe.Pointer(key), C.int(klen))
		v := C.GoBytes(unsafe.Pointer(val), C.int(vlen))
		C.free(unsafe.Pointer(key))
		C.free(unsafe.Pointer(val))
		out = append(out, storage.KVPair{Key: k, Value: v})
	}
	_ = rng
	return out, nil
}

func boolToInt(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

func cBytes(b []byte) *C.uchar {
	if len(b) == 0 {
		return nil
	}
	return (*C.uchar)(unsafe.Pointer(&b[0]))
}

func cBytesOrNil(b []byte) *C.uchar {
	return cBytes(b)
}

func cSizesOrNil(s []C.size_t) *C.size_t {
	if len(s) == 0 {
		return nil
	}
	return &s[0]
}

func cString(p *C.char) string {
	if p == nil {
		return "(unknown)"
	}
	return C.GoString(p)
}

func freeStr(p *C.char) {
	if p != nil {
		C.xdb_free_str(p)
	}
}
