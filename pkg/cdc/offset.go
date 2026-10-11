package cdc

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// OffsetStore 消费位点持久化接口。
// 客户端在每次 Fetch 后 Save(nextLSN)，重启后 Load 并从该位点续读。
type OffsetStore interface {
	// Load 返回已保存的消费位点；不存在时返回 (0, nil)。
	Load() (storage.LSN, error)
	// Save 原子保存消费位点（下一次 Fetch 的 from）。
	Save(lsn storage.LSN) error
}

// FileOffsetStore 单文件位点存储：十进制文本 + 换行，原子写（临时文件 + 重命名）。
// 适合单订阅者场景；多订阅者可各自指定不同 offset 文件。
type FileOffsetStore struct {
	path string
	mu   sync.Mutex
}

// NewFileOffsetStore 新建文件位点存储（不创建文件，Save 时落盘）。
func NewFileOffsetStore(path string) *FileOffsetStore {
	return &FileOffsetStore{path: path}
}

// Load 读取位点；文件不存在视为初始位点 0。
func (s *FileOffsetStore) Load() (storage.LSN, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cdc: corrupt offset file %s: %w", s.path, err)
	}
	return storage.LSN(n), nil
}

// Save 原子保存位点。
func (s *FileOffsetStore) Save(lsn storage.LSN) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(uint64(lsn), 10)+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
