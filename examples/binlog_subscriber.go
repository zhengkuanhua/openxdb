// Command binlog_subscriber 是 M14 生态接入 binlog 订阅示例客户端：
// 演示 pkg/cdc.Subscriber（拉取式增量读取）+ pkg/cdc.FileOffsetStore（位点持久化）
// 的典型用法——从上次消费位点续读 binlog 事件，打印表/行级变更并把
// nextLSN 保存回 offset 文件，重启后从该位点继续（无重复无遗漏）。
//
// 典型使用（配合主节点 start --replica-port）：
//
//	# 1. 启动主节点生成 binlog：
//	openxdb start --data-dir ./data --replica-port 7789
//	# 2. 消费 binlog（默认读当前文件全部，--follow 进入持续轮询）：
//	go run ./examples/binlog_subscriber.go \
//	    --binlog ./data/binlog.log --offset ./data/sub.offset --follow
//
// Ctrl-C 退出后位点已保存；再次运行从上次位点继续。
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cdc"
)

func main() {
	var (
		binlog = flag.String("binlog", "", "binlog 文件路径（主节点数据目录下 binlog.log）")
		offset = flag.String("offset", "", "消费位点文件路径（FileOffsetStore，不存在则从 0 开始）")
		limit  = flag.Int("limit", 0, "每次拉取事件数上限（<=0 表示读当前文件全部）")
		follow = flag.Bool("follow", false, "读完后继续轮询等待新事件（Ctrl-C 退出）")
	)
	flag.Parse()
	if *binlog == "" || *offset == "" {
		fmt.Fprintln(os.Stderr, "usage: binlog_subscriber --binlog <file> --offset <file> [--limit <n>] [--follow]")
		os.Exit(2)
	}

	sub, err := cdc.NewSubscriber(*binlog)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open binlog:", err)
		os.Exit(1)
	}
	defer sub.Close()
	store := cdc.NewFileOffsetStore(*offset)

	// 从持久化位点续读；首次运行 Load 返回 0。
	from, err := store.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "load offset:", err)
		os.Exit(1)
	}
	fmt.Printf("subscriber: binlog=%s offset=%s from=%d (limit=%d, follow=%v)\n",
		*binlog, *offset, from, *limit, *follow)

	// Ctrl-C 正常退出（位点已在每次拉取后保存）。
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	for {
		events, next, err := sub.Fetch(from, *limit)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fetch:", err)
			os.Exit(1)
		}
		for _, ev := range events {
			// 索引键变更通常跟随行变更，消费方按业务需要决定是否处理。
			fmt.Printf("lsn=%d commit=%d schema=%d kind=%s table=%d index=%v pk=%q\n",
				ev.LSN, ev.CommitLSN, ev.SchemaVer, ev.Kind, ev.TableID, ev.IsIndex, ev.PK)
		}
		if err := store.Save(next); err != nil {
			fmt.Fprintln(os.Stderr, "save offset:", err)
			os.Exit(1)
		}
		from = next
		if !*follow {
			break
		}
		// 无新事件时轮询等待主节点写入。
		select {
		case <-stop:
			fmt.Println("subscriber: stopped, offset saved")
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}
