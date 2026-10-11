// M14 生态接入工具链端到端测试（子功能三：工具链收尾）。
//
//   - TestImportCLISmoke：openxdb import 子命令端到端冒烟——在真实 CGO 存储上
//     初始化数据目录 → 生成 mysqldump 文件 → 调用 cmdImport 灌入 → 打开同一
//     数据目录用 SQL 查询验证落库行数；覆盖原子导入与 --batch/--ignore-errors 参数路径。
//   - TestBinlogSubscriberBuild：examples/binlog_subscriber.go 示例客户端可编译
//     （CGO 工具链下 go build 通过），保证随仓库发布的 binlog 订阅示例可用。
package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

// writeDump 将 mysqldump 文本写入临时文件。
func writeDump(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "dump.sql")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write dump: %v", err)
	}
	return p
}

// mustCount 执行 SELECT COUNT(*) 并返回行数。
func mustCount(t *testing.T, d *db.DB, table string) int64 {
	t.Helper()
	res, err := d.SQL.Execute("SELECT COUNT(*) FROM " + table)
	if err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 || res.Rows[0][0].Kind != "INT" {
		t.Fatalf("count %s result: %+v", table, res.Rows)
	}
	return res.Rows[0][0].I
}

// TestImportCLISmoke：openxdb import 冒烟（原子导入 + 参数路径 + 落库验证）。
func TestImportCLISmoke(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("db.Init: %v", err)
	}

	// 冒烟 1：原子导入（无 BATCH，单事务），两表 + 多值 INSERT。
	p := writeDump(t, "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT);\n"+
		"INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c');\n"+
		"CREATE TABLE t2 (id INT PRIMARY KEY, amount DECIMAL(10,2));\n"+
		"INSERT INTO t2 VALUES (1,12.50),(2,0.99);\n")
	if err := cmdImport([]string{"--data-dir", dir, "--dump", p}); err != nil {
		t.Fatalf("cmdImport atomic: %v", err)
	}

	// 冒烟 2：--batch + --ignore-errors（文件内重复主键跳过，不中断）。
	q := writeDump(t, "CREATE TABLE t3 (id INT PRIMARY KEY, note TEXT);\n"+
		"INSERT INTO t3 VALUES (1,'x'),(1,'y'),(2,'z');\n")
	if err := cmdImport([]string{"--data-dir", dir, "--dump", q, "--batch", "1", "--ignore-errors"}); err != nil {
		t.Fatalf("cmdImport batch: %v", err)
	}

	// 冒烟 3：必填参数缺失报错。
	if err := cmdImport([]string{"--data-dir", dir}); err == nil {
		t.Fatalf("cmdImport without --dump should fail")
	}

	// 落库验证：t1=3、t2=2、t3=2（重复主键 '1' 跳过）。
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer d.Close()
	if got := mustCount(t, d, "t1"); got != 3 {
		t.Fatalf("t1 count = %d, want 3", got)
	}
	if got := mustCount(t, d, "t2"); got != 2 {
		t.Fatalf("t2 count = %d, want 2", got)
	}
	if got := mustCount(t, d, "t3"); got != 2 {
		t.Fatalf("t3 count = %d, want 2 (dup pk skipped)", got)
	}
}

// TestBinlogSubscriberBuild：examples/binlog_subscriber.go 可编译。
func TestBinlogSubscriberBuild(t *testing.T) {
	// 测试运行时工作目录为 cmd/openxdb，仓库根在 ../..。
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("abs root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "examples", "binlog_subscriber.go")); err != nil {
		t.Fatalf("examples/binlog_subscriber.go missing: %v", err)
	}
	cmd := exec.Command("go", "-C", root, "build", "./examples/...")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build ./examples/...: %v\n%s", err, string(out))
	}
}
