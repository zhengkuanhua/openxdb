// 监控指标：openxdb metrics（M13 metrics 导出）。
//
// 对本地或远端（--addr host:port，默认 127.0.0.1:7788）服务发起 METRICS 命令，
// 输出 Prometheus 文本格式指标（等价 HTTP GET http://<addr>/metrics 的内容）。
// 指标来源：pkg/metrics 注册表（SQL/KV/事务/节点计数 + 注入的连接数、
// 慢查询阈值、存储大小、region 分布采集源），详见 docs/T26_m13_metrics.md。
// 失败（连接/协议错误）退出码 2，成功退出码 0。
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// cmdMetrics 拉取并输出 Prometheus 文本格式指标。
func cmdMetrics(args []string) error {
	addr := "127.0.0.1:7788"
	if a, ok := flagValue(args, "--addr"); ok && a != "" {
		addr = a
	}
	out, err := onlineQuery(addr, "METRICS", 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "openxdb metrics: %v\n", err)
		os.Exit(2)
		return nil
	}
	fmt.Print(out)
	return nil
}

// 存储目录大小采样：目录递归求和，带 5 秒缓存降低 stat 开销（metrics 拉取
// 高频时避免每次全量遍历 RocksDB 数据目录）。
var (
	dirSizeMu   sync.Mutex
	dirSizeDir  string
	dirSizeVal  int64
	dirSizeTime time.Time
)

func dirSize(dir string) int64 {
	dirSizeMu.Lock()
	defer dirSizeMu.Unlock()
	if dirSizeDir == dir && time.Since(dirSizeTime) < 5*time.Second {
		return dirSizeVal
	}
	var total int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	dirSizeDir, dirSizeVal, dirSizeTime = dir, total, time.Now()
	return total
}
