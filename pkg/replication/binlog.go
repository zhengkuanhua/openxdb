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
	binMagic      = "OPENXDBBIN"
	binVersion    = 1
	binHeaderSize = len(binMagic) + 2
	binRecHead    = 4 + 4 + 8 + 8 + 8 + 1 + 4 + 4
	binMaxBody    = 1 << 30

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
	data, err := encodeBinRecord(entry)
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
	binary.BigEndian.PutUint16(h[len(binMagic):], binVersion)
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
	if binary.BigEndian.Uint16(h[len(binMagic):]) != binVersion {
		return ErrCorrupt
	}
	return nil
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
	for {
		e, err := decodeBinRecord(b.f)
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

func encodeBinRecord(e *BinlogEntry) ([]byte, error) {
	if e.Batch == nil {
		return nil, errors.New("replication: nil batch")
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
	head := make([]byte, binRecHead)
	binary.BigEndian.PutUint32(head[4:8], uint32(bodyLen))
	binary.BigEndian.PutUint64(head[8:16], uint64(e.LSN))
	binary.BigEndian.PutUint64(head[16:24], uint64(e.CommitLSN))
	binary.BigEndian.PutUint64(head[24:32], e.SchemaVer)
	head[32] = binStateCommit
	binary.BigEndian.PutUint32(head[33:37], uint32(len(e.Batch.Puts)))
	binary.BigEndian.PutUint32(head[37:41], uint32(len(e.Batch.Deletes)))
	payload := append(head[4:], body.Bytes()...)
	crc := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(head[0:4], crc)
	return append(head, body.Bytes()...), nil
}

func decodeBinRecord(r io.Reader) (*BinlogEntry, error) {
	head := make([]byte, binRecHead)
	if _, err := io.ReadFull(r, head); err != nil {
		if err == io.EOF {
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
