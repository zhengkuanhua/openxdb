package main

// M16-3 version 输出测试：构建信息 + 数据文件格式版本（升级判定依据）。

import (
	"fmt"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// TestVersionTextContainsDataFormat version 输出含发行版本、构建注入字段
// （-ldflags buildVersion/buildTime/buildCommit）与数据文件格式版本。
func TestVersionTextContainsDataFormat(t *testing.T) {
	out := versionText()
	for _, want := range []string{version, "build:", "built at:", "commit:", "data format version:"} {
		if !strings.Contains(out, want) {
			t.Fatalf("versionText missing %q:\n%s", want, out)
		}
	}
	wantFormat := fmt.Sprintf("data format version: %d", db.DataFormatVersion)
	if !strings.Contains(out, wantFormat) {
		t.Fatalf("versionText must contain current data format version %d:\n%s", db.DataFormatVersion, out)
	}
}

// TestVersionTextFirstLineCompat 首行保留旧格式（OpenXDB <version> ...），
// 向后兼容依赖该行的脚本/工具。
func TestVersionTextFirstLineCompat(t *testing.T) {
	out := versionText()
	first := strings.SplitN(strings.TrimSpace(out), "\n", 2)[0]
	if first != "OpenXDB "+version+" (M2 master-slave replication)" {
		t.Fatalf("first line changed: %q", first)
	}
}
