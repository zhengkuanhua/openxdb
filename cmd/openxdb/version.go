package main

// M16-3 灰度升级辅助：openxdb version 版本信息。
//
// 输出四类版本信息（升级判定依据）：
//   - 发行版本（const version，保留旧格式首行，向后兼容）；
//   - 构建版本 / 构建时间 / 构建 commit（-ldflags 注入，未注入显示 dev/空）；
//   - 数据文件格式版本（pkg/db.DataFormatVersion，升级兼容矩阵关键字段）。
//
// -ldflags 注入规范（构建时替换包级变量）：
//   go build -ldflags "-X main.buildVersion=v0.20.0-alpha \
//                      -X main.buildTime=2026-10-12T00:00:00Z \
//                      -X main.buildCommit=2f05c83"
// 未注入时显示 dev（开发构建），便于区分发布构建与本地构建。

import (
	"fmt"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// buildVersion / buildTime / buildCommit 为构建注入变量（-ldflags -X main.xxx）。
// 默认 dev / 空 / 空表示未经注入的本地构建；发布构建通过构建脚本注入。
var (
	buildVersion = "dev"
	buildTime    = ""
	buildCommit  = ""
)

// cmdVersion 打印版本信息。
func cmdVersion(args []string) error {
	fmt.Print(versionText())
	return nil
}

// versionText 返回完整版本文本（含构建信息与数据格式版本），供测试断言。
func versionText() string {
	return fmt.Sprintf("OpenXDB %s (M2 master-slave replication)\n"+
		"  build: %s\n"+
		"  built at: %s\n"+
		"  commit: %s\n"+
		"  data format version: %d\n",
		version, buildVersion, buildTime, buildCommit, db.DataFormatVersion)
}
