package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

type walImpl struct {
	mu           sync.Mutex
	f            *os.File
	path         string
	nextLSN      storage.LSN
	lastSynced   storage.LSN // 已 fsync 落盘的最大 LSN（0 = 无已刷记录）
	syncOnAppend bool        // 默认 true：Append 每记录 fsync（组提交关闭）
	closed       bool

	// 组提交（leader 合并 fsync）状态。
	cond    *sync.Cond // 关联 mu
	waiters int        // 当前等待 SyncUpTo 的 goroutine 数（>0 表示有 leader 正在刷盘）
	syncErr error      // 上一轮 leader fsync 错误（广播给所有等待者）

	// 观测/测试辅助：累计 fsync 次数。
	fsyncCount uint64
	// 测试辅助：注入人工 fsync 延迟（模拟慢盘，放大组提交合并效果；0 = 不延迟）。
	fsyncDelay time.Duration
}

// Open 打开（或创建）WAL 文件，并扫描末尾确定 nextLSN。
func Open(path string) (WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &walImpl{f: f, path: path, syncOnAppend: true}
	w.cond = sync.NewCond(&w.mu)
	if fi.Size() == 0 {
		if err := w.writeHeader(); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	} else {
		if err := w.checkHeader(); err != nil {
			f.Close()
			return nil, err
		}
	}
	var last storage.LSN
	if err := w.Replay(func(e *WalEntry) { last = e.LSN }); err != nil {
		f.Close()
		return nil, err
	}
	w.nextLSN = last + 1
	// 已有记录来自上次持久化（Close 时已 fsync），视作已刷盘；新 Append 从 last+1 起由 SyncUpTo 覆盖。
	w.lastSynced = last
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// Append 顺序追加并 fsync（D 保证：返回 = 已持久化）。
func (w *walImpl) Append(entry *WalEntry) error {
	if entry == nil || entry.Batch == nil {
		return errors.New("wal: nil entry or batch")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if entry.LSN == 0 {
		entry.LSN = w.nextLSN
	}
	if entry.LSN >= w.nextLSN {
		w.nextLSN = entry.LSN + 1
	}
	data, err := encodeEntry(entry)
	if err != nil {
		return err
	}
	if _, err := w.f.Write(data); err != nil {
		return err
	}
	if w.syncOnAppend {
		if err := w.f.Sync(); err != nil {
			return err
		}
		w.fsyncCount++
		w.lastSynced = w.nextLSN - 1
	}
	return nil
}

// SetGroupCommit 开关组提交。默认关闭（Append 每记录 fsync）；
// 开启后 Append 仅写缓冲，由 SyncUpTo 兜底刷盘。
func (w *walImpl) SetGroupCommit(enabled bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.syncOnAppend = !enabled
}

// SyncUpTo 阻塞直至 lsn 及之前所有已追加记录 fsync 落盘。
// leader 机制：首个等待者成为 leader，一次 fsync 覆盖当前已写末尾后广播唤醒；
// 后续等待者发现 lastSynced 已推进则零额外 fsync 直接返回。无后台定时器。
// 契约：调用方必须先 Append 得到 lsn（事务层在锁内先 Append 再释放锁调本方法）。
func (w *walImpl) SyncUpTo(lsn storage.LSN) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	for lsn > w.lastSynced {
		if w.waiters > 0 {
			// follower：等待当前 leader 刷盘结果，被广播后重新检查。
			w.waiters++
			w.cond.Wait()
			w.waiters--
			if w.syncErr != nil {
				err := w.syncErr
				w.mu.Unlock()
				return err
			}
			continue
		}
		// leader：记录进入刷盘前的已写末尾，释放锁执行 fsync（期间 Append 仍可继续写入）。
		tail := w.lastLSNLocked()
		if lsn > tail {
			w.mu.Unlock()
			return fmt.Errorf("wal: SyncUpTo lsn=%d beyond written tail=%d", lsn, tail)
		}
		w.waiters = 1
		w.syncErr = nil
		w.mu.Unlock()
		if w.fsyncDelay > 0 {
			time.Sleep(w.fsyncDelay)
		}
		err := w.f.Sync()
		w.mu.Lock()
		w.fsyncCount++
		if err != nil {
			w.syncErr = err
			w.waiters = 0
			w.cond.Broadcast()
			w.mu.Unlock()
			return err
		}
		w.lastSynced = tail
		w.waiters = 0
		w.cond.Broadcast()
		// 循环条件 lsn > lastSynced 已为假（tail >= lsn），下一轮退出。
	}
	w.mu.Unlock()
	return nil
}

// FsyncCount 返回累计 fsync 次数（观测/测试辅助，不属于 WAL 接口语义）。
func (w *walImpl) FsyncCount() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fsyncCount
}

// SetFsyncDelay 注入人工 fsync 延迟（测试辅助，模拟慢盘放大组提交合并效果；不属于 WAL 接口语义）。
func (w *walImpl) SetFsyncDelay(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fsyncDelay = d
}

func (w *walImpl) lastLSNLocked() storage.LSN {
	if w.nextLSN == 0 {
		return 0
	}
	return w.nextLSN - 1
}

// Replay 顺序重放全部记录。
func (w *walImpl) Replay(apply func(*WalEntry)) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.replayLocked(apply)
}

// Truncate 保留 lsn 及之后的记录，重建文件（checkpoint 后调用）。
func (w *walImpl) Truncate(lsn storage.LSN) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	var keep []*WalEntry
	if err := w.replayLocked(func(e *WalEntry) {
		if e.LSN >= lsn {
			keep = append(keep, e)
		}
	}); err != nil {
		return err
	}
	tmp := w.path + ".tmp"
	nf, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	writeHeaderTo(nf)
	for _, e := range keep {
		data, err := encodeEntry(e)
		if err != nil {
			nf.Close()
			os.Remove(tmp)
			return err
		}
		if _, err := nf.Write(data); err != nil {
			nf.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := nf.Sync(); err != nil {
		nf.Close()
		os.Remove(tmp)
		return err
	}
	if err := nf.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	old := w.f
	old.Close()
	if err := os.Rename(tmp, w.path); err != nil {
		// Windows：目标已存在时 rename 直接覆盖会被拒，先删旧文件再重命名。
		if rmErr := os.Remove(w.path); rmErr == nil {
			err = os.Rename(tmp, w.path)
		}
		if err != nil {
			os.Remove(tmp)
			return err
		}
	}
	f, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	w.f = f
	w.nextLSN = lsn
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

func (w *walImpl) LastLSN() storage.LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.nextLSN == 0 {
		return 0
	}
	return w.nextLSN - 1
}

func (w *walImpl) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

func (w *walImpl) writeHeader() error { return writeHeaderTo(w.f) }

func (w *walImpl) checkHeader() error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	h := make([]byte, headerSize)
	if _, err := io.ReadFull(w.f, h); err != nil {
		return ErrCorrupt
	}
	if string(h[:len(fileMagic)]) != fileMagic {
		return ErrCorrupt
	}
	if binary.BigEndian.Uint16(h[len(fileMagic):]) != fileVersion {
		return ErrCorrupt
	}
	return nil
}

func (w *walImpl) replayLocked(apply func(*WalEntry)) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	h := make([]byte, headerSize)
	if _, err := io.ReadFull(w.f, h); err != nil {
		return ErrCorrupt
	}
	if string(h[:len(fileMagic)]) != fileMagic {
		return ErrCorrupt
	}
	for {
		e, err := decodeEntry(w.f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if apply != nil {
			apply(e)
		}
	}
	return nil
}

// ---- 序列化 ----

func writeHeaderTo(f *os.File) error {
	h := make([]byte, headerSize)
	copy(h, fileMagic)
	binary.BigEndian.PutUint16(h[len(fileMagic):], fileVersion)
	_, err := f.Write(h)
	return err
}

func encodeEntry(e *WalEntry) ([]byte, error) {
	if e.Batch == nil {
		return nil, errors.New("wal: nil batch")
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
	if bodyLen > maxRecordBody {
		return nil, errors.New("wal: record too large")
	}
	head := make([]byte, recHeadSize)
	binary.BigEndian.PutUint32(head[4:8], uint32(bodyLen))
	binary.BigEndian.PutUint64(head[8:16], uint64(e.LSN))
	binary.BigEndian.PutUint64(head[16:24], e.TxnID)
	head[24] = e.State
	binary.BigEndian.PutUint32(head[25:29], uint32(len(e.Batch.Puts)))
	binary.BigEndian.PutUint32(head[29:33], uint32(len(e.Batch.Deletes)))
	payload := append(head[4:], body.Bytes()...)
	crc := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(head[0:4], crc)
	return append(head, body.Bytes()...), nil
}

func decodeEntry(r io.Reader) (*WalEntry, error) {
	head := make([]byte, recHeadSize)
	if _, err := io.ReadFull(r, head); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, ErrCorrupt
	}
	crcWant := binary.BigEndian.Uint32(head[0:4])
	bodyLen := binary.BigEndian.Uint32(head[4:8])
	if bodyLen > maxRecordBody {
		return nil, ErrCorrupt
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, ErrCorrupt
	}
	if crc32.ChecksumIEEE(append(head[4:], body...)) != crcWant {
		return nil, ErrCorrupt
	}
	e := &WalEntry{
		LSN:   storage.LSN(binary.BigEndian.Uint64(head[8:16])),
		TxnID: binary.BigEndian.Uint64(head[16:24]),
		State: head[24],
	}
	nputs := binary.BigEndian.Uint32(head[25:29])
	ndeletes := binary.BigEndian.Uint32(head[29:33])
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
