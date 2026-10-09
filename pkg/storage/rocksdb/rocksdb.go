// Package rocksdb 提供基于 RocksDB 引擎的 Storage 实现（M1 T1）。
//
// 依赖构建时注入：CGO_CFLAGS / CGO_CXXFLAGS 指向 RocksDB include，
// CGO_LDFLAGS 指向 librocksdb.a（Windows 需附加 shlwapi/rpcrt4/ws2_32）。
package rocksdb

/*
#cgo CXXFLAGS: -std=c++20
#cgo LDFLAGS: -static-libstdc++ -static-libgcc -lwinpthread -lshlwapi -lrpcrt4 -lws2_32
#include <stdlib.h>
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
	return doScan(C.xdb_scan(
		r.h,
		cBytesOrNil(rng.Start), C.size_t(len(rng.Start)),
		cBytesOrNil(rng.End), C.size_t(len(rng.End)),
		C.int(limit), &cscanErr), rng)
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
	return doScan(C.xdb_snapshot_scan(
		sn.s,
		cBytesOrNil(rng.Start), C.size_t(len(rng.Start)),
		cBytesOrNil(rng.End), C.size_t(len(rng.End)),
		C.int(limit), &cscanErr), rng)
}

func (sn *snapshot) Release() {
	if sn.s == nil {
		return
	}
	C.xdb_snapshot_release(sn.s)
	sn.s = nil
}

// ---- cgo 辅助 ----

var cscanErr *C.char

func doScan(s *C.xscan, rng storage.KeyRange) ([]storage.KVPair, error) {
	if s == nil {
		err := fmt.Errorf("rocksdb: scan: %s", cString(cscanErr))
		freeStr(cscanErr)
		cscanErr = nil
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
