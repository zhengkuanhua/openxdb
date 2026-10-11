// 监控状态查询：openxdb status（M13 子功能三）。
//
// 对本地或远端（--addr host:port，默认 127.0.0.1:7788）服务拉取聚合状态
// 快照：节点存活/角色、region 分布、存储用量、慢查询摘要、事务计数、
// 连接数与 QPS。默认输出人类可读表格；--json 输出 JSON（STATUS JSON）。
// 失败（连接/协议错误）退出码 2，成功退出码 0。
package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// cmdStatus 拉取并输出服务聚合状态快照。
func cmdStatus(args []string) error {
	addr := "127.0.0.1:7788"
	if a, ok := flagValue(args, "--addr"); ok && a != "" {
		addr = a
	}
	cmd := "STATUS"
	if flagBool(args, "--json") {
		cmd = "STATUS JSON"
	}
	out, err := onlineQuery(addr, cmd, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "openxdb status: %v\n", err)
		os.Exit(2)
		return nil
	}
	if strings.HasPrefix(out, "ERR") {
		fmt.Fprintf(os.Stderr, "openxdb status: 服务端拒绝状态查询: %s\n", out)
		os.Exit(2)
		return nil
	}
	fmt.Print(out)
	if !strings.HasSuffix(out, "\n") {
		fmt.Println()
	}
	return nil
}
