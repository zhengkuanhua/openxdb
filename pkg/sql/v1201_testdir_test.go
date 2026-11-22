// v1.20.1 P0-2：测试临时目录清理超时保护（Windows delete-pending 修复）。
//
// 缺陷背景：Windows 上 rocksdb 关闭后，其 SST/WAL 的 mmap 映射与文件句柄可能短暂滞留，
// testing 框架在用例 Cleanup 阶段执行 os.RemoveAll 会阻塞在 NtSetInformationFile
// （DELETE_PENDING 等待），整个测试进程随之挂死，表现为 pkg/sql 全量随机超时
// （v1.20.0 期 422s 超时 goroutine 栈即停在
//  testing.(*common).makeTempDir.func1 -> os.RemoveAll -> Deleteat）。
//
// 处理策略：删除动作放进独立 goroutine 并限时，超时即放弃删除（目录留在 %TEMP%，
// 由系统清理），用例结果与覆盖率不受影响，全量测试不再被单目录清理拖死。
// 仅替换测试侧目录创建，不改变产品行为。
package sql

import (
	"os"
	"testing"
	"time"
)

// testDirCleanupTimeout 单个临时目录清理的最长等待时间。
const testDirCleanupTimeout = 5 * time.Second

// testDir 创建用例专用临时目录，等价于 tb.TempDir()，但清理带超时保护。
func testDir(tb testing.TB) string {
	tb.Helper()
	dir, err := os.MkdirTemp("", "openxdb-sqltest-")
	if err != nil {
		tb.Fatalf("MkdirTemp: %v", err)
	}
	tb.Cleanup(func() {
		done := make(chan struct{})
		go func() {
			_ = os.RemoveAll(dir)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(testDirCleanupTimeout):
			tb.Logf("testDir: 清理 %s 超时 %s（Windows delete-pending），已放弃删除", dir, testDirCleanupTimeout)
		}
	})
	return dir
}
