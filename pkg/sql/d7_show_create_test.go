package sql_test

import (
	"strings"
	"testing"
)

// D7.4：SHOW CREATE TABLE（排期 Phase G / G4）。
// 输出列与 MySQL 一致（Table | Create Table），DDL 以存储元数据为唯一事实源
// 重建：列定义/类型精度/自增/主键/二级索引/外键。

func TestD7ShowCreateTable(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t_d7c (id INT(11) AUTO_INCREMENT, name VARCHAR(50), price DECIMAL(12,2), PRIMARY KEY (id))")
	mustExec(t, e, "CREATE INDEX idx_d7c_name ON t_d7c (name)")

	res := mustRows(t, e, "SHOW CREATE TABLE t_d7c")
	if len(res) != 1 {
		t.Fatalf("SHOW CREATE TABLE: want 1 row, got %d", len(res))
	}
	got := res[0][1].String()
	for _, want := range []string{
		"CREATE TABLE `t_d7c` (",
		"`id` INT(11) AUTO_INCREMENT",
		"`name` TEXT(50)",
		"`price` DECIMAL(12,2)",
		"PRIMARY KEY (`id`)",
		"KEY `idx_d7c_name` (`name`)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("SHOW CREATE TABLE missing %q in:\n%s", want, got)
		}
	}
	// 复合主键 + 唯一索引 + 外键
	mustExec(t, e, "CREATE TABLE t_d7c_p (a INT, b TEXT, c INT, PRIMARY KEY (a, b))")
	mustExec(t, e, "CREATE UNIQUE INDEX uq_d7c_c ON t_d7c_p (c)")
	mustExec(t, e, "CREATE TABLE t_d7c_f (fid INT, ref_id INT, FOREIGN KEY (ref_id) REFERENCES t_d7c_p (a) ON DELETE CASCADE)")

	res2 := mustRows(t, e, "SHOW CREATE TABLE t_d7c_p")
	got2 := res2[0][1].String()
	for _, want := range []string{
		"PRIMARY KEY (`a`,`b`)",
		"UNIQUE KEY `uq_d7c_c` (`c`)",
	} {
		if !strings.Contains(got2, want) {
			t.Fatalf("SHOW CREATE TABLE missing %q in:\n%s", want, got2)
		}
	}
	res3 := mustRows(t, e, "SHOW CREATE TABLE t_d7c_f")
	got3 := res3[0][1].String()
	for _, want := range []string{
		"CONSTRAINT `fk_ref_id` FOREIGN KEY (`ref_id`) REFERENCES `t_d7c_p` (`a`)",
		"ON DELETE CASCADE",
	} {
		if !strings.Contains(got3, want) {
			t.Fatalf("SHOW CREATE TABLE missing %q in:\n%s", want, got3)
		}
	}
	// 不存在表报错
	mustErr(t, e, "SHOW CREATE TABLE nope", "not exists")
}
