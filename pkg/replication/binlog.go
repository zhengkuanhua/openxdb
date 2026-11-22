package replication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// binlog 文件格式：
//  文件头：magic(10B) + version(2B) = 12B
//  每条记录：crc(4B) | len(4B) | lsn(8B) | commitlsn(8B) | schemaver(8B) | state(1B) | nputs(4B) | ndeletes(4B) | body
//  body = puts: nputs × [klen(4B) vlen(4B) key val]；deletes: ndeletes × [klen(4B) key]
//  crc 覆盖 len 字段到记录结尾；统一 Big Endian。
const (
	binMagic = "OPENXDBBIN"
	// binVersion binlog v1 记录格式（无 region 字段）。新文件按 binVersionRegion
	// 落盘，读取端同时接受 v1/v2 —— 这是 C4「旧 binlog 可被新 follower 读取」
	// 红线的实现基础（DEV_PLAN §4 红线 3）。
	binVersion    = 1
	binHeaderSize = len(binMagic) + 2
	binRecHead    = 4 + 4 + 8 + 8 + 8 + 1 + 4 + 4
	// binVersionRegion binlog v2 记录格式：记录头尾部追加 region(8B)。
	// 复制帧仍使用 v1 布局（从节点整批回放，region 维度无意义），
	// 因此 v2 只在文件落盘路径启用。
	binVersionRegion = 2
	// binRecHeadRegion v2 记录头长度（v1 + region 8B）。
	binRecHeadRegion = binRecHead + 8
	// binRegionOff v2 记录头中 region 字段偏移。
	binRegionOff = binRecHead
	binMaxBody   = 1 << 30

	// binStateCommit 记录状态（M2 只落已提交事务）。
	binStateCommit uint8 = 2
)

// BinlogStore 复制日志顺序存储（append-only，落盘）。
type BinlogStore interface {
	// Append 追加一条记录：LSN==0 时自动分配（从 1 开始单调递增），
	// CommitLSN==0 时置为 LSN。写入即 fsync（落盘保证）。
	Append(entry *BinlogEntry) error
	// ReadFrom 从文件头顺序读取，对 LSN>=from 的每条记录调用 apply；
	// apply 返回错误则终止并向上返回。
	ReadFrom(from storage.LSN, apply func(*BinlogEntry) error) error
	// LastLSN 返回已分配的最大位点（无记录时为 0）。
	LastLSN() storage.LSN
	Close() error
}

type binlogStore struct {
	mu      sync.Mutex
	f       *os.File
	path    string
	nextLSN storage.LSN
	closed  bool
	// ver 本文件实际的记录格式版本（1 或 2），由 checkHeader 从文件头读出。
	// v1 旧文件按 v1 解码，条目 RegionID 恒为 0（向下兼容）。
	ver uint16
}

// PeekLastLSN 只读地读取 binlog 文件的最大位点（不创建、不写入）。
// 用于运维巡检/监控读取位点；文件不存在时返回 os.ErrNotExist 原样错误。
func PeekLastLSN(path string) (storage.LSN, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if fi.Size() == 0 {
		return 0, nil
	}
	b := &binlogStore{f: f, path: path}
	if err := b.checkHeader(); err != nil {
		return 0, err
	}
	var last storage.LSN
	if err := b.replay(func(e *BinlogEntry) error { last = e.LSN; return nil }); err != nil {
		return 0, err
	}
	return last, nil
}

// OpenBinlog 打开（或创建）binlog 文件，扫描末尾确定 nextLSN。
func OpenBinlog(path string) (BinlogStore, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	b := &binlogStore{f: f, path: path}
	if fi.Size() == 0 {
		if err := b.writeHeader(); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	} else {
		if err := b.checkHeader(); err != nil {
			f.Close()
			return nil, err
		}
	}
	var last storage.LSN
	if err := b.replay(func(e *BinlogEntry) error { last = e.LSN; return nil }); err != nil {
		f.Close()
		return nil, err
	}
	b.nextLSN = last + 1
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return b, nil
}

// Append 顺序追加并 fsync 落盘。
func (b *binlogStore) Append(entry *BinlogEntry) error {
	if entry == nil || entry.Batch == nil {
		return errors.New("replication: nil entry or batch")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if entry.LSN == 0 {
		entry.LSN = b.nextLSN
	}
	if entry.CommitLSN == 0 {
		entry.CommitLSN = entry.LSN
	}
	if entry.LSN >= b.nextLSN {
		b.nextLSN = entry.LSN + 1
	}
	data, err := encodeBinRecordVer(entry, b.recordVer())
	if err != nil {
		return err
	}
	if _, err := b.f.Write(data); err != nil {
		return err
	}
	return b.f.Sync()
}

// ReadFrom 顺序读取 LSN>=from 的记录。
func (b *binlogStore) ReadFrom(from storage.LSN, apply func(*BinlogEntry) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	return b.replay(func(e *BinlogEntry) error {
		if e.LSN >= from {
			return apply(e)
		}
		return nil
	})
}

func (b *binlogStore) LastLSN() storage.LSN {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.nextLSN == 0 {
		return 0
	}
	return b.nextLSN - 1
}

func (b *binlogStore) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	if err := b.f.Sync(); err != nil {
		b.f.Close()
		return err
	}
	return b.f.Close()
}

func (b *binlogStore) writeHeader() error {
	h := make([]byte, binHeaderSize)
	copy(h, binMagic)
	// 新文件一律写 v2（携带 region 维度）；读取端同时接受 v1/v2。
	binary.BigEndian.PutUint16(h[len(binMagic):], binVersionRegion)
	b.ver = binVersionRegion
	_, err := b.f.Write(h)
	return err
}

func (b *binlogStore) checkHeader() error {
	if _, err := b.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	h := make([]byte, binHeaderSize)
	if _, err := io.ReadFull(b.f, h); err != nil {
		return ErrCorrupt
	}
	if string(h[:len(binMagic)]) != binMagic {
		return ErrCorrupt
	}
	ver, ok := binVerSupported(binary.BigEndian.Uint16(h[len(binMagic):]))
	if !ok {
		return ErrCorrupt
	}
	b.ver = ver
	return nil
}

// binVerSupported 校验并归一化 binlog 文件头版本：接受 v1（旧文件，无 region）
// 与 v2（C4 起新文件，带 region），其余视为损坏。
func binVerSupported(ver uint16) (uint16, bool) {
	if ver == binVersion || ver == binVersionRegion {
		return ver, true
	}
	return 0, false
}

func (b *binlogStore) recordVer() uint16 {
	if b.ver == 0 {
		return binVersionRegion
	}
	return b.ver
}

func (b *binlogStore) replay(apply func(*BinlogEntry) error) error {
	if _, err := b.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	h := make([]byte, binHeaderSize)
	if _, err := io.ReadFull(b.f, h); err != nil {
		return ErrCorrupt
	}
	if string(h[:len(binMagic)]) != binMagic {
		return ErrCorrupt
	}
	ver, ok := binVerSupported(binary.BigEndian.Uint16(h[len(binMagic):]))
	if !ok {
		return ErrCorrupt
	}
	b.ver = ver
	for {
		e, err := decodeBinRecordVerOpt(b.f, ver, false)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if apply != nil {
			if err := apply(e); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- 记录编解码（文件与复制帧共用） ----

// encodeBinRecord 以 v1 布局编码（复制帧与既有调用方使用，零改动）。
func encodeBinRecord(e *BinlogEntry) ([]byte, error) {
	return encodeBinRecordVer(e, binVersion)
}

// encodeBinRecordVer 按指定版本编码一条记录：
//   - v1：记录头 41B（无 region），复制帧沿用；
//   - v2：记录头 49B（尾部追加 region 8B），binlog v2 文件落盘沿用。
func encodeBinRecordVer(e *BinlogEntry, ver uint16) ([]byte, error) {
	if e.Batch == nil {
		return nil, errors.New("replication: nil batch")
	}
	headSize := binRecHead
	if ver == binVersionRegion {
		headSize = binRecHeadRegion
	}
	var body bytes.Buffer
	for _, p := range e.Batch.Puts {
		_ = binary.Write(&body, binary.BigEndian, uint32(len(p.Key)))
		_ = binary.Write(&body, binary.BigEndian, uint32(len(p.Value)))
		body.Write(p.Key)
		body.Write(p.Value)
	}
	for _, k := range e.Batch.Deletes {
		_ = binary.Write(&body, binary.BigEndian, uint32(len(k)))
		body.Write(k)
	}
	bodyLen := body.Len()
	if bodyLen > binMaxBody {
		return nil, errors.New("replication: record too large")
	}
	head := make([]byte, headSize)
	binary.BigEndian.PutUint32(head[4:8], uint32(bodyLen))
	binary.BigEndian.PutUint64(head[8:16], uint64(e.LSN))
	binary.BigEndian.PutUint64(head[16:24], uint64(e.CommitLSN))
	binary.BigEndian.PutUint64(head[24:32], e.SchemaVer)
	head[32] = binStateCommit
	binary.BigEndian.PutUint32(head[33:37], uint32(len(e.Batch.Puts)))
	binary.BigEndian.PutUint32(head[37:41], uint32(len(e.Batch.Deletes)))
	if ver == binVersionRegion {
		binary.BigEndian.PutUint64(head[binRegionOff:binRegionOff+8], uint64(e.RegionID))
	}
	payload := append(head[4:], body.Bytes()...)
	crc := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(head[0:4], crc)
	return append(head, body.Bytes()...), nil
}

// decodeBinRecord 解码一条完整记录（严格模式：半条视为损坏）。
func decodeBinRecord(r io.Reader) (*BinlogEntry, error) {
	return decodeBinRecordOpt(r, false)
}

// decodeBinRecordOpt 以 v1 布局解码一条记录（复制帧沿用，零改动）；
// tolerateTail=true 时把尾部不完整记录（io.ErrUnexpectedEOF，正在追加的半条）
// 视为正常 EOF，供只读拉取器使用。
func decodeBinRecordOpt(r io.Reader, tolerateTail bool) (*BinlogEntry, error) {
	return decodeBinRecordVerOpt(r, binVersion, tolerateTail)
}

// decodeBinRecordVerOpt 按指定版本解码一条记录（v1 无 region / v2 带 region）。
func decodeBinRecordVerOpt(r io.Reader, ver uint16, tolerateTail bool) (*BinlogEntry, error) {
	headSize := binRecHead
	if ver == binVersionRegion {
		headSize = binRecHeadRegion
	}
	head := make([]byte, headSize)
	if _, err := io.ReadFull(r, head); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		if tolerateTail && err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, ErrCorrupt
	}
	crcWant := binary.BigEndian.Uint32(head[0:4])
	bodyLen := binary.BigEndian.Uint32(head[4:8])
	if bodyLen > binMaxBody {
		return nil, ErrCorrupt
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		if tolerateTail && err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, ErrCorrupt
	}
	if crc32.ChecksumIEEE(append(head[4:], body...)) != crcWant {
		return nil, ErrCorrupt
	}
	e := &BinlogEntry{
		LSN:       storage.LSN(binary.BigEndian.Uint64(head[8:16])),
		CommitLSN: storage.LSN(binary.BigEndian.Uint64(head[16:24])),
		SchemaVer: binary.BigEndian.Uint64(head[24:32]),
	}
	if head[32] != binStateCommit {
		return nil, ErrCorrupt
	}
	nputs := binary.BigEndian.Uint32(head[33:37])
	ndeletes := binary.BigEndian.Uint32(head[37:41])
	if ver == binVersionRegion {
		e.RegionID = storage.ID(binary.BigEndian.Uint64(head[binRegionOff : binRegionOff+8]))
	}
	if uint64(nputs)*8+uint64(ndeletes)*4 > uint64(bodyLen) {
		return nil, ErrCorrupt
	}
	e.Batch = &storage.WriteBatch{}
	buf := bytes.NewReader(body)
	for i := uint32(0); i < nputs; i++ {
		var klen, vlen uint32
		if err := binary.Read(buf, binary.BigEndian, &klen); err != nil {
			return nil, ErrCorrupt
		}
		if err := binary.Read(buf, binary.BigEndian, &vlen); err != nil {
			return nil, ErrCorrupt
		}
		if uint64(klen)+uint64(vlen) > uint64(buf.Len()) {
			return nil, ErrCorrupt
		}
		k := make([]byte, klen)
		if _, err := io.ReadFull(buf, k); err != nil {
			return nil, ErrCorrupt
		}
		v := make([]byte, vlen)
		if _, err := io.ReadFull(buf, v); err != nil {
			return nil, ErrCorrupt
		}
		e.Batch.Puts = append(e.Batch.Puts, storage.KVPair{Key: k, Value: v})
	}
	for i := uint32(0); i < ndeletes; i++ {
		var klen uint32
		if err := binary.Read(buf, binary.BigEndian, &klen); err != nil {
			return nil, ErrCorrupt
		}
		if uint64(klen) > uint64(buf.Len()) {
			return nil, ErrCorrupt
		}
		k := make([]byte, klen)
		if _, err := io.ReadFull(buf, k); err != nil {
			return nil, ErrCorrupt
		}
		e.Batch.Deletes = append(e.Batch.Deletes, k)
	}
	return e, nil
}
