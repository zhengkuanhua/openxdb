package replication

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ErrStopFetch 扫描中止哨兵：ScanFrom 的 apply 返回此错误表示达到拉取上限，
// 属于正常停止（ScanFrom 返回 nil 与当前位点），不视为失败。
var ErrStopFetch = errors.New("replication: stop fetch")

// BinlogReader binlog 只读拉取器（M14 CDC 订阅）。
//
// 与 BinlogStore 的区别：
//   - 只读打开（不创建、不写入），持有独立文件句柄与自身锁；
//   - 尾部不完整记录（正在追加的半条）视为正常结束，供拉取循环反复扫描；
//   - 不持有 binlogStore 内部锁，与主写路径 Append / 复制推送并发共存
//     （binlog 为 append-only 落盘文件，落盘内容对并发读稳定可见）。
type BinlogReader struct {
	mu sync.Mutex
	f  *os.File
}

// OpenBinlogRead 只读打开 binlog 文件并校验头。
// 文件不存在时返回 os.ErrNotExist 原样错误（不创建）。
func OpenBinlogRead(path string) (*BinlogReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &BinlogReader{f: f}
	if err := r.checkHeader(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

func (r *BinlogReader) checkHeader() error {
	if _, err := r.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	h := make([]byte, binHeaderSize)
	if _, err := io.ReadFull(r.f, h); err != nil {
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

// ScanFrom 顺序扫描 binlog，对 LSN>=from 的每条完整记录调用 apply。
// 返回最后一条完整记录的 LSN（无记录时返回 0）与错误：
//   - 尾部不完整记录（正在追加的半条）视为正常结束，不报错；
//   - apply 返回 errStopFetch 时中止并正常返回（供 limit 拉取使用）；
//   - 其它 apply 错误 / 真实损坏（crc 失败等）原样返回。
//
// 注意：返回的位点是"文件末尾完整记录位点"，即使该记录 LSN<from（无新事件）
// 也返回它；调用方需自行与 from 比较（见 pkg/cdc.Subscriber.Fetch）。
func (r *BinlogReader) ScanFrom(from storage.LSN, apply func(*BinlogEntry) error) (storage.LSN, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkHeader(); err != nil {
		return 0, err
	}
	var last storage.LSN
	for {
		e, err := decodeBinRecordOpt(r.f, true)
		if err == io.EOF {
			break
		}
		if err != nil {
			return last, err
		}
		last = e.LSN
		if e.LSN >= from && apply != nil {
			if err := apply(e); err != nil {
				if err == ErrStopFetch {
					return last, nil
				}
				return last, err
			}
		}
	}
	return last, nil
}

// LastLSN 返回当前 binlog 文件最后一条完整记录的 LSN（无记录返回 0）。
func (r *BinlogReader) LastLSN() (storage.LSN, error) {
	return r.ScanFrom(0, nil)
}

// Close 关闭只读句柄。
func (r *BinlogReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}
