package sql_test

import (
	"os"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// 冒烟：TPC-C/MySQL DDL 语法与复合主键元数据（BenchmarkSQL 接入硬前置）
func TestSmokeTpcCDDL(t *testing.T) {
	e := newEngine(t)

	// 1. TPC-C warehouse：DECIMAL 精度 + 单列主键
	mustExec(t, e, "CREATE TABLE warehouse (w_id INT NOT NULL, w_name VARCHAR(16), w_street_1 VARCHAR(32), w_street_2 VARCHAR(32), w_city VARCHAR(32), w_state TEXT, w_zip TEXT, w_tax DECIMAL(4,2), w_ytd DECIMAL(12,2), PRIMARY KEY (w_id))")
	mustExec(t, e, "INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES (1, 'WH-1', 'street1', 'street2', 'city', 'ST', 'zip', 0.12, 300000.00)")
	rows := mustRows(t, e, "SELECT w_id, w_name, w_tax FROM warehouse WHERE w_id = 1")
	if len(rows) != 1 || rows[0][0].I != 1 || rows[0][1].S != "WH-1" {
		t.Fatalf("warehouse point lookup failed: %+v", rows)
	}

	// 2. TPC-C district：复合主键 (d_w_id, d_id) + KEY 多列普通索引
	mustExec(t, e, "CREATE TABLE district (d_w_id INT NOT NULL, d_id INT NOT NULL, d_name VARCHAR(16), d_next_o_id INT, PRIMARY KEY (d_w_id, d_id), KEY idx_district_w (d_w_id, d_id))")
	mustExec(t, e, "INSERT INTO district (d_w_id, d_id, d_name, d_next_o_id) VALUES (1, 1, 'D-1', 3001)")

	// 3. TPC-C customer：复合主键 + UNIQUE KEY 带名 + KEY 多列
	mustExec(t, e, "CREATE TABLE customer (c_w_id INT NOT NULL, c_d_id INT NOT NULL, c_id INT NOT NULL, c_last VARCHAR(32), c_first VARCHAR(32), PRIMARY KEY (c_w_id, c_d_id, c_id), UNIQUE KEY idx_customer_uniq (c_w_id, c_d_id, c_id), KEY idx_customer_name (c_w_id, c_d_id, c_last, c_first))")
	mustExec(t, e, "INSERT INTO customer (c_w_id, c_d_id, c_id, c_last, c_first) VALUES (1, 1, 1, 'LAST1', 'FIRST1')")

	// 4. 行值 IN（TPC-C 未使用，但 MySQL 客户端/ORM 常见）
	mustExec(t, e, "CREATE TABLE tup (a INT, b INT, c INT, PRIMARY KEY (a, b))")
	mustExec(t, e, "INSERT INTO tup (a, b, c) VALUES (1, 2, 100), (3, 4, 200), (5, 6, 300)")
	rows = mustRows(t, e, "SELECT c FROM tup WHERE (a, b) IN ((1, 2), (5, 6)) ORDER BY c")
	if len(rows) != 2 || rows[0][0].I != 100 || rows[1][0].I != 300 {
		t.Fatalf("tuple IN failed: %+v", rows)
	}

	// 5. FOR UPDATE 语法吞掉
	mustExec(t, e, "SELECT c FROM tup WHERE a = 1 FOR UPDATE")
	mustExec(t, e, "SELECT c FROM tup WHERE a = 1 ORDER BY c FOR UPDATE")

	// 6. AUTO_INCREMENT 不带 NOT NULL + 列尾 PRIMARY KEY（仅验证建表语法与元数据，TPC-C 无自增列）
	mustExec(t, e, "CREATE TABLE ai1 (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	mustExec(t, e, "INSERT INTO ai1 (id, v) VALUES (1, 'x')")
	mustExec(t, e, "CREATE TABLE ai2 (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	mustExec(t, e, "INSERT INTO ai2 (id, v) VALUES (2, 'y')")

	// 7. CONSTRAINT 前缀
	mustExec(t, e, "CREATE TABLE cons (id INT, v TEXT, CONSTRAINT pk_cons PRIMARY KEY (id))")

	// 8. UNIQUE 无 KEY 无名字典式
	mustExec(t, e, "CREATE TABLE uq (id INT, v TEXT, UNIQUE (v))")
	mustExec(t, e, "INSERT INTO uq (id, v) VALUES (1, 'dup')")

	// 9. 复合主键表 DML 基础可用（元数据正确性）
	if _, err := e.Execute("SELECT COUNT(*) FROM district"); err != nil {
		t.Fatalf("district count: %v", err)
	}
}

// 冒烟：parser 层 AST 断言（多列 PK / 索引 / TUPLE_IN / FOR UPDATE）
func TestSmokeTpcCParserAST(t *testing.T) {
	st, err := sql.Parse("CREATE TABLE cc (a INT, b INT, c TEXT, PRIMARY KEY (a, b), KEY k_ab (a, b), UNIQUE KEY u_b (b))")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct, ok := st.(*sql.CreateTableStmt)
	if !ok {
		t.Fatalf("not CreateTableStmt: %T", st)
	}
	if len(ct.PKCols) != 2 || ct.PKCols[0] != "a" || ct.PKCols[1] != "b" {
		t.Fatalf("PKCols: %+v", ct.PKCols)
	}
	if len(ct.Indexes) != 2 {
		t.Fatalf("indexes: %+v", ct.Indexes)
	}
	if ct.Indexes[0].Unique || len(ct.Indexes[0].Cols) != 2 {
		t.Fatalf("idx0: %+v", ct.Indexes[0])
	}
	if !ct.Indexes[1].Unique || ct.Indexes[1].Cols[0] != "b" {
		t.Fatalf("idx1: %+v", ct.Indexes[1])
	}

	st2, err := sql.Parse("SELECT c FROM t WHERE (a, b) IN ((1, 2), (3, 4))")
	if err != nil {
		t.Fatalf("parse2: %v", err)
	}
	sel, ok := st2.(*sql.SelectStmt)
	if !ok {
		t.Fatalf("not SelectStmt: %T", st2)
	}
	if len(sel.Where.Conds) != 1 || sel.Where.Conds[0].Op != "TUPLE_IN" {
		t.Fatalf("tuple cond: %+v", sel.Where.Conds)
	}
	if len(sel.Where.Conds[0].TupleLeft) != 2 || len(sel.Where.Conds[0].TupleVals) != 2 {
		t.Fatalf("tuple fields: %+v", sel.Where.Conds[0])
	}

	st3, err := sql.Parse("SELECT * FROM t WHERE id = 1 FOR UPDATE")
	if err != nil {
		t.Fatalf("parse3: %v", err)
	}
	sel3, ok := st3.(*sql.SelectStmt)
	if !ok || !sel3.ForUpdate {
		t.Fatalf("for update: %+v", st3)
	}
}
// 冒烟：BenchmarkSQL 官方 MySQL TPC-C schema 资产逐条跑通
// （testdata/tpcc/tableCreates.sql：10 张 bmsql_* 表，含 ENGINE=innodb 表选项）
func TestSmokeTpcCSchemaAsset(t *testing.T) {
	e := newEngine(t)
	raw, err := os.ReadFile("testdata/tpcc/tableCreates.sql")
	if err != nil {
		t.Fatalf("read schema asset: %v", err)
	}
	var n int
	for _, stmt := range strings.Split(string(raw), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		mustExec(t, e, stmt)
		n++
	}
	if n != 10 {
		t.Fatalf("expected 10 CREATE TABLE, got %d", n)
	}
	// 1. 10 张表全部存在
	tables := map[string]bool{}
	rows := mustRows(t, e, "SELECT TABLE_NAME FROM information_schema.tables")
	for _, r := range rows {
		if len(r) > 0 {
			tables[r[0].S] = true
		}
	}
	for _, want := range []string{"bmsql_config", "bmsql_warehouse", "bmsql_district", "bmsql_customer",
		"bmsql_history", "bmsql_new_order", "bmsql_oorder", "bmsql_order_line", "bmsql_item", "bmsql_stock"} {
		if !tables[want] {
			t.Fatalf("table %s missing after schema load", want)
		}
	}
	// 2. 复合主键元数据：bmsql_customer PRIMARY (c_w_id, c_d_id, c_id)
	rows = mustRows(t, e, "SELECT COLUMN_NAME FROM information_schema.statistics WHERE TABLE_NAME = 'bmsql_customer' AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX")
	if len(rows) != 3 || rows[0][0].S != "c_w_id" || rows[1][0].S != "c_d_id" || rows[2][0].S != "c_id" {
		t.Fatalf("bmsql_customer pk: %+v", rows)
	}
	// 3. bmsql_order_line 复合主键 4 列
	rows = mustRows(t, e, "SELECT COLUMN_NAME FROM information_schema.statistics WHERE TABLE_NAME = 'bmsql_order_line' AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX")
	if len(rows) != 4 || rows[0][0].S != "ol_w_id" || rows[3][0].S != "ol_number" {
		t.Fatalf("bmsql_order_line pk: %+v", rows)
	}
	// 4. 关键表 DML 冒烟：warehouse / customer 复合主键 / history（AUTO_INCREMENT 列）
	mustExec(t, e, "INSERT INTO bmsql_warehouse (w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, w_state, w_zip) VALUES (1, 300000.00, 0.1000, 'WH-1', 's1', 's2', 'city', 'ST', '123456789')")
	mustExec(t, e, "INSERT INTO bmsql_customer (c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_middle, c_data) VALUES (1, 1, 1, 0.1000, 'GC', 'LAST1', 'FIRST1', 50000.00, -10.00, 10.00, 1, 0, 's1', 's2', 'city', 'ST', '123456789', '1234567890123456', '2026-10-16 10:00:00', 'OE', 'data')")
	mustExec(t, e, "INSERT INTO bmsql_history (hist_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (1, 1, 1, 1, 1, 1, '2026-10-16 10:00:00', 10.00, 'hist')")
	rows = mustRows(t, e, "SELECT w_name FROM bmsql_warehouse WHERE w_id = 1")
	if len(rows) != 1 || rows[0][0].S != "WH-1" {
		t.Fatalf("warehouse lookup: %+v", rows)
	}
	rows = mustRows(t, e, "SELECT c_last FROM bmsql_customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1")
	if len(rows) != 1 || rows[0][0].S != "LAST1" {
		t.Fatalf("customer lookup: %+v", rows)
	}
}
