package mysqldump

import (
	"strings"
	"testing"
)

// parse 解析字符串并返回 Dump（严格模式）。
func parse(t *testing.T, s string) *Dump {
	t.Helper()
	d, err := Parse(strings.NewReader(s), Options{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return d
}

// TestM14BacktickIdentifiers 反引号标识符（表名/列名含特殊字符）。
func TestM14BacktickIdentifiers(t *testing.T) {
	d := parse(t, "CREATE TABLE `order detail` (`id` INT PRIMARY KEY, `select` VARCHAR(10));")
	if len(d.Tables) != 1 {
		t.Fatalf("tables: want 1, got %d", len(d.Tables))
	}
	tb := d.Tables[0]
	if tb.Name != "order detail" {
		t.Fatalf("name: want 'order detail', got %q", tb.Name)
	}
	if len(tb.Columns) != 2 || tb.Columns[0].Name != "id" || tb.Columns[1].Name != "select" {
		t.Fatalf("columns: %+v", tb.Columns)
	}
	if !tb.Columns[0].Primary {
		t.Fatal("id should be primary")
	}
}

// TestM14TypeMapping MySQL 类型到 OpenXDB 类型映射。
func TestM14TypeMapping(t *testing.T) {
	d := parse(t, `CREATE TABLE t (
  a TINYINT, b BIGINT, c DECIMAL(10,2), d FLOAT, e DATE,
  f VARCHAR(255), g TEXT, h ENUM('x','y'), i DATETIME, j TIMESTAMP,
  k BLOB, l MEDIUMBLOB, m BOOLEAN, n JSON
);`)
	tb := d.Tables[0]
	want := []string{"INT", "INT", "DECIMAL", "DECIMAL", "DATE", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "BLOB", "BLOB", "INT", "TEXT"}
	if len(tb.Columns) != len(want) {
		t.Fatalf("column count: want %d, got %d", len(want), len(tb.Columns))
	}
	for i, w := range want {
		if tb.Columns[i].Type != w {
			t.Errorf("col %s: want %s, got %s", tb.Columns[i].Name, w, tb.Columns[i].Type)
		}
	}
}

// TestM14IgnoredClauses CREATE TABLE 中的可忽略子句。
func TestM14IgnoredClauses(t *testing.T) {
	d := parse(t, `CREATE TABLE user (
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(64) DEFAULT 'guest' COMMENT 'display name',
  email VARCHAR(128) NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  UNIQUE KEY uq_email (email),
  KEY idx_name (name),
  CONSTRAINT fk FOREIGN KEY (email) REFERENCES other(email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)
	tb := d.Tables[0]
	if len(tb.Columns) != 3 {
		t.Fatalf("columns: want 3, got %d (%+v)", len(tb.Columns), tb.Columns)
	}
	if !tb.Columns[0].Primary {
		t.Fatal("id should be primary via table-level PK")
	}
}

// TestM14InlinePrimaryKey 行内 PRIMARY KEY 标记。
func TestM14InlinePrimaryKey(t *testing.T) {
	d := parse(t, "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v TEXT);")
	tb := d.Tables[0]
	if !tb.Columns[0].Primary {
		t.Fatal("inline primary key not detected")
	}
}

// TestM14MultiValueInsert 多值 INSERT 与普通值。
func TestM14MultiValueInsert(t *testing.T) {
	d := parse(t, "CREATE TABLE t (id INT, name TEXT); INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');")
	tb := d.Tables[0]
	if len(tb.Rows) != 3 {
		t.Fatalf("rows: want 3, got %d", len(tb.Rows))
	}
	if tb.Rows[0][0].Raw != "1" || tb.Rows[0][1].Raw != "alice" {
		t.Fatalf("row0: %+v", tb.Rows[0])
	}
}

// TestM14ExplicitColumnReorder INSERT 显式列名并按表定义重排。
func TestM14ExplicitColumnReorder(t *testing.T) {
	d := parse(t, `CREATE TABLE t (id INT, name TEXT, age INT);
INSERT INTO t (age, name, id) VALUES (30, 'x', 7);`)
	tb := d.Tables[0]
	if len(tb.Rows) != 1 {
		t.Fatalf("rows: want 1, got %d", len(tb.Rows))
	}
	row := tb.Rows[0]
	if row[0].Raw != "7" || row[1].Raw != "x" || row[2].Raw != "30" {
		t.Fatalf("reordered row: %+v", row)
	}
}

// TestM14StringEscapes 字符串转义还原。
func TestM14StringEscapes(t *testing.T) {
	d := parse(t, `CREATE TABLE t (id INT, s TEXT);
INSERT INTO t VALUES (1, 'it\'s'), (2, 'line1\nline2'), (3, 'back\\slash');`)
	tb := d.Tables[0]
	if tb.Rows[0][1].Raw != "it's" {
		t.Errorf("escape quote: %q", tb.Rows[0][1].Raw)
	}
	if tb.Rows[1][1].Raw != "line1\nline2" {
		t.Errorf("escape newline: %q", tb.Rows[1][1].Raw)
	}
	if tb.Rows[2][1].Raw != `back\slash` {
		t.Errorf("escape backslash: %q", tb.Rows[2][1].Raw)
	}
}

// TestM14SpecialValues NULL / hex / TRUE / FALSE。
func TestM14SpecialValues(t *testing.T) {
	d := parse(t, `CREATE TABLE t (id INT, blob BLOB, flag BOOLEAN);
INSERT INTO t VALUES (1, 0xDEADBEEF, TRUE), (2, NULL, FALSE);`)
	tb := d.Tables[0]
	if tb.Rows[0][1].Raw != "DEADBEEF" {
		t.Errorf("hex: %q", tb.Rows[0][1].Raw)
	}
	if tb.Rows[0][2].Raw != "1" {
		t.Errorf("TRUE: %q", tb.Rows[0][2].Raw)
	}
	if !tb.Rows[1][1].Null {
		t.Error("NULL not marked")
	}
	if tb.Rows[1][2].Raw != "0" {
		t.Errorf("FALSE: %q", tb.Rows[1][2].Raw)
	}
}

// TestM14AuxiliarySkipped SET/LOCK 等辅助语句跳过。
func TestM14AuxiliarySkipped(t *testing.T) {
	d := parse(t, `SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;
LOCK TABLES t WRITE;
CREATE TABLE t (id INT);
INSERT INTO t VALUES (1);
UNLOCK TABLES;`)
	if len(d.Tables) != 1 {
		t.Fatalf("tables: want 1, got %d", len(d.Tables))
	}
	if d.Skipped != 3 {
		t.Fatalf("skipped: want 3, got %d", d.Skipped)
	}
	if d.Statements != 5 {
		t.Fatalf("statements: want 5, got %d", d.Statements)
	}
}

// TestM14DropTable DROP TABLE IF EXISTS 记录。
func TestM14DropTable(t *testing.T) {
	d := parse(t, "DROP TABLE IF EXISTS `old_t`;\nCREATE TABLE old_t (id INT);")
	if len(d.Drops) != 1 || d.Drops[0] != "old_t" {
		t.Fatalf("drops: %+v", d.Drops)
	}
	if len(d.Tables) != 1 {
		t.Fatalf("tables after drop: %+v", d.Tables)
	}
}

// TestM14RealWorldDump 真实 mysqldump 风格样本（含版本注释、表注释、多语句）。
func TestM14RealWorldDump(t *testing.T) {
	sample := `-- MySQL dump 10.13  Distrib 8.0.36, for Linux (x86_64)
--
-- Host: localhost    Database: shop
-- ------------------------------------------------------
-- Server version	8.0.36

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table customers
--

DROP TABLE IF EXISTS customers;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE customers (
  id int NOT NULL AUTO_INCREMENT,
  name varchar(100) NOT NULL,
  email varchar(255) DEFAULT NULL,
  balance decimal(10,2) DEFAULT '0.00',
  created_at datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY email (email)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table customers
--

LOCK TABLES customers WRITE;
/*!40000 ALTER TABLE customers DISABLE KEYS */;
INSERT INTO customers VALUES (1,'Alice','alice@example.com',100.50,'2024-01-01 10:00:00'),(2,'Bob','bob@example.com',0.00,'2024-01-02 09:30:00');
INSERT INTO customers (id,name,email,balance,created_at) VALUES (3,'Carol',NULL,25.00,'2024-02-01 08:00:00');
/*!40000 ALTER TABLE customers ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table orders
--

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  order_id bigint NOT NULL,
  customer_id int DEFAULT NULL,
  note text,
  PRIMARY KEY (order_id),
  KEY customer_id (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO orders VALUES (1001,1,'first order'),(1002,2,NULL);
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
`
	d := parse(t, sample)
	if len(d.Tables) != 2 {
		t.Fatalf("tables: want 2, got %d", len(d.Tables))
	}
	if len(d.Drops) != 2 {
		t.Fatalf("drops: want 2, got %d", len(d.Drops))
	}
	// customers：3 列 -> 实际 5 列（id/name/email/balance/created_at）
	c := d.Tables[0]
	if c.Name != "customers" || len(c.Columns) != 5 {
		t.Fatalf("customers: %+v", c)
	}
	if c.Columns[0].Type != "INT" || c.Columns[1].Type != "TEXT" || c.Columns[3].Type != "DECIMAL" || c.Columns[4].Type != "TEXT" {
		t.Fatalf("customers types: %+v", c.Columns)
	}
	if !c.Columns[0].Primary {
		t.Fatal("customers.id should be primary")
	}
	if len(c.Rows) != 3 {
		t.Fatalf("customers rows: want 3, got %d", len(c.Rows))
	}
	if c.Rows[0][3].Raw != "100.50" {
		t.Errorf("balance: %q", c.Rows[0][3].Raw)
	}
	if c.Rows[2][2].Null != true {
		t.Error("row2 email should be NULL")
	}
	// orders
	o := d.Tables[1]
	if o.Name != "orders" || len(o.Columns) != 3 {
		t.Fatalf("orders: %+v", o)
	}
	if o.Columns[0].Type != "INT" || o.Columns[2].Type != "TEXT" {
		t.Fatalf("orders types: %+v", o.Columns)
	}
	if len(o.Rows) != 2 {
		t.Fatalf("orders rows: want 2, got %d", len(o.Rows))
	}
	if !o.Rows[1][2].Null {
		t.Error("orders row2 note should be NULL")
	}
}

// TestM14IgnoreErrors 容错模式：坏语句/坏行跳过。
func TestM14IgnoreErrors(t *testing.T) {
	src := `CREATE TABLE t (id INT, name TEXT);
INSERT INTO t VALUES (1, 'ok'), (bad_value, 'bad'), (2, 'ok2');
INSERT INTO t VALUES (NOW(), 'fn');
SET NAMES utf8;`
	d, err := Parse(strings.NewReader(src), Options{IgnoreErrors: true})
	if err != nil {
		t.Fatalf("ignore parse: %v", err)
	}
	if len(d.Tables) != 1 || len(d.Tables[0].Rows) != 2 {
		t.Fatalf("rows after ignore: %+v", d.Tables)
	}
	if d.BadRows < 1 {
		t.Fatalf("bad rows: want >=1, got %d", d.BadRows)
	}
	// NOW() 语句整体跳过 + SET 语句跳过
	if d.Skipped != 2 {
		t.Fatalf("skipped: want 2, got %d", d.Skipped)
	}
}

// TestM14StrictError 严格模式：首个错误即返回。
func TestM14StrictError(t *testing.T) {
	_, err := Parse(strings.NewReader("CREATE TABLE t (id INT); INSERT INTO t VALUES (NOW());"), Options{})
	if err == nil {
		t.Fatal("strict mode should fail")
	}
}

// TestM14UndefinedTableInsert INSERT 引用未定义表。
func TestM14UndefinedTableInsert(t *testing.T) {
	_, err := Parse(strings.NewReader("INSERT INTO ghost VALUES (1);"), Options{})
	if err == nil {
		t.Fatal("expected undefined table error")
	}
}

// TestM14UnsupportedType 不支持的列类型报错（严格模式）。
func TestM14UnsupportedType(t *testing.T) {
	_, err := Parse(strings.NewReader("CREATE TABLE t (v SOMETYPE);"), Options{})
	if err == nil {
		t.Fatal("expected unsupported type error")
	}
}

// TestM14EscapedBacktick 反引号内双反引号转义。
func TestM14EscapedBacktick(t *testing.T) {
	d := parse(t, "CREATE TABLE t (`a``b` INT);")
	if len(d.Tables[0].Columns) != 1 || d.Tables[0].Columns[0].Name != "a`b" {
		t.Fatalf("escaped backtick: %+v", d.Tables[0].Columns)
	}
}


// TestM14XHexLiteral MySQL X'' and 0x hex BLOB literals.
func TestM14XHexLiteral(t *testing.T) {
	d := parse(t, "CREATE TABLE t (id INT, b BLOB); INSERT INTO t VALUES (1, X'48656C6C6F'), (2, 0x776f726c64);")
	tb := d.Tables[0]
	if len(tb.Rows) != 2 {
		t.Fatalf("rows: want 2, got %d", len(tb.Rows))
	}
	if tb.Rows[0][1].Raw != "48656C6C6F" {
		t.Errorf("X-hex: %q", tb.Rows[0][1].Raw)
	}
	if tb.Rows[1][1].Raw != "776F726C64" {
		t.Errorf("0x hex uppercase: %q", tb.Rows[1][1].Raw)
	}
}

// TestM14TypeMappingExtended 扩展类型映射：SERIAL/浮点别名/空间类型/UNSIGNED 修饰符/NCHAR。
func TestM14TypeMappingExtended(t *testing.T) {
	d := parse(t, `CREATE TABLE t (
  a SERIAL, b FLOAT4, c FLOAT8, d DOUBLE PRECISION, e INT UNSIGNED,
  f BIGINT UNSIGNED ZEROFILL, g POINT, h LINESTRING, i POLYGON,
  j MULTIPOINT, k MULTILINESTRING, l MULTIPOLYGON, m GEOMETRYCOLLECTION,
  n GEOMETRY, o NCHAR(10), p NVARCHAR(20)
);`)
	tb := d.Tables[0]
	want := []string{"INT", "DECIMAL", "DECIMAL", "DECIMAL", "INT", "INT",
		"TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT"}
	if len(tb.Columns) != len(want) {
		t.Fatalf("column count: want %d, got %d", len(want), len(tb.Columns))
	}
	for i, w := range want {
		if tb.Columns[i].Type != w {
			t.Errorf("col %s: want %s, got %s", tb.Columns[i].Name, w, tb.Columns[i].Type)
		}
	}
}

// TestM14BitLiteral 位字面量 b''/B'' 转十进制数字。
func TestM14BitLiteral(t *testing.T) {
	d := parse(t, "CREATE TABLE t (id INT, bits BIT(8)); INSERT INTO t VALUES (1, b'1010'), (2, B'11111111'), (3, b'');")
	tb := d.Tables[0]
	if len(tb.Rows) != 3 {
		t.Fatalf("rows: want 3, got %d", len(tb.Rows))
	}
	if tb.Rows[0][1].Raw != "10" {
		t.Errorf("b'1010': %q", tb.Rows[0][1].Raw)
	}
	if tb.Rows[1][1].Raw != "255" {
		t.Errorf("B'11111111': %q", tb.Rows[1][1].Raw)
	}
	if tb.Rows[2][1].Raw != "0" {
		t.Errorf("empty bit literal: %q", tb.Rows[2][1].Raw)
	}
}

// TestM14IntroducerLiteral 字符集引导符字面量（_binary/_utf8mb4 等）。
func TestM14IntroducerLiteral(t *testing.T) {
	d := parse(t, "CREATE TABLE t (id INT, s TEXT, b BLOB); INSERT INTO t VALUES (1, _utf8mb4'你好', _binary'raw-bytes');")
	tb := d.Tables[0]
	if len(tb.Rows) != 1 {
		t.Fatalf("rows: want 1, got %d", len(tb.Rows))
	}
	if tb.Rows[0][1].Raw != "你好" {
		t.Errorf("_utf8mb4 string: %q", tb.Rows[0][1].Raw)
	}
	if tb.Rows[0][2].Raw != "raw-bytes" {
		t.Errorf("_binary string: %q", tb.Rows[0][2].Raw)
	}
}

// TestM14ScientificNumber 科学计数法数字字面量。
func TestM14ScientificNumber(t *testing.T) {
	d := parse(t, "CREATE TABLE t (id INT, f DOUBLE); INSERT INTO t VALUES (1, 1e5), (2, 1.5E-3), (3, 2E+06), (4, -1.2e-2);")
	tb := d.Tables[0]
	if len(tb.Rows) != 4 {
		t.Fatalf("rows: want 4, got %d", len(tb.Rows))
	}
	want := []string{"1e5", "1.5E-3", "2E+06", "-1.2e-2"}
	for i, w := range want {
		if tb.Rows[i][1].Raw != w {
			t.Errorf("row %d scientific: want %q, got %q", i, w, tb.Rows[i][1].Raw)
		}
	}
}
