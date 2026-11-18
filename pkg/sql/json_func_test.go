package sql_test

import (
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// v1.19.0 D6：SQL JSON 函数族测试。
// 覆盖 JSON_VALID / JSON_EXTRACT / JSON_UNQUOTE / JSON_TYPE / JSON_LENGTH /
// JSON_OBJECT / JSON_ARRAY / JSON_SET 的正常路径、嵌套调用、列引用、非法输入，
// 以及与 WHERE / UPDATE / 聚合 / CASE 的查询组合。

// jsonSingle 执行单值查询并返回唯一结果值。
func jsonSingle(t *testing.T, e *sql.Engine, stmt string) sql.Value {
	t.Helper()
	rows := mustRows(t, e, stmt)
	if len(rows) != 1 {
		t.Fatalf("Execute(%q): want 1 row, got %d", stmt, len(rows))
	}
	if len(rows[0]) != 1 {
		t.Fatalf("Execute(%q): want 1 column, got %d", stmt, len(rows[0]))
	}
	return rows[0][0]
}

// jsonWantText 断言单值查询返回期望 TEXT。
func jsonWantText(t *testing.T, e *sql.Engine, stmt, want string) {
	t.Helper()
	v := jsonSingle(t, e, stmt)
	if v.Kind != "TEXT" {
		t.Fatalf("Execute(%q): want TEXT, got kind=%s val=%q", stmt, v.Kind, v.String())
	}
	if v.S != want {
		t.Fatalf("Execute(%q): want %q, got %q", stmt, want, v.S)
	}
}

// jsonWantInt 断言单值查询返回期望 INT。
func jsonWantInt(t *testing.T, e *sql.Engine, stmt string, want int64) {
	t.Helper()
	v := jsonSingle(t, e, stmt)
	if v.Kind != "INT" {
		t.Fatalf("Execute(%q): want INT, got kind=%s val=%q", stmt, v.Kind, v.String())
	}
	if v.I != want {
		t.Fatalf("Execute(%q): want %d, got %d", stmt, want, v.I)
	}
}

// jsonWantNull 断言单值查询返回 NULL。
func jsonWantNull(t *testing.T, e *sql.Engine, stmt string) {
	t.Helper()
	v := jsonSingle(t, e, stmt)
	if v.Kind != "NULL" {
		t.Fatalf("Execute(%q): want NULL, got kind=%s val=%q", stmt, v.Kind, v.String())
	}
}

// setupDocs 建立带 JSON 文本列的表。
func setupDocs(t *testing.T, e *sql.Engine) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE docs (id INT, doc TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO docs (id, doc) VALUES (1, '{\"name\": \"alice\", \"age\": 30}'), (2, '{\"name\": \"bob\", \"age\": 25}'), (3, 'not json')")
}

// TestJSONValid JSON_VALID：合法 1 / 非法 0 / NULL 传播 / 无参报错。
func TestJSONValid(t *testing.T) {
	e := newEngine(t)
	jsonWantInt(t, e, "SELECT JSON_VALID('{\"a\": 1}')", 1)
	jsonWantInt(t, e, "SELECT JSON_VALID('[1, 2, 3]')", 1)
	jsonWantInt(t, e, "SELECT JSON_VALID('123')", 1)
	jsonWantInt(t, e, "SELECT JSON_VALID('null')", 1)
	jsonWantInt(t, e, "SELECT JSON_VALID('{bad}')", 0)
	jsonWantInt(t, e, "SELECT JSON_VALID('{\"a\": 1} tail')", 0)
	jsonWantInt(t, e, "SELECT JSON_VALID('')", 0)
	jsonWantNull(t, e, "SELECT JSON_VALID(NULL)")
	mustErr(t, e, "SELECT JSON_VALID()", "wrong number of arguments")
	mustErr(t, e, "SELECT JSON_VALID('{}', '[]')", "wrong number of arguments")
}

// TestJSONExtract JSON_EXTRACT：对象键 / 数组下标 / 组合路径 / 引号键名 / 缺失路径 / 多路径。
func TestJSONExtract(t *testing.T) {
	e := newEngine(t)
	jsonWantText(t, e, "SELECT JSON_EXTRACT('{\"a\": 1, \"b\": \"x\"}', '$.a')", "1")
	jsonWantText(t, e, "SELECT JSON_EXTRACT('{\"a\": {\"b\": [10, 20]}}', '$.a.b[1]')", "20")
	jsonWantText(t, e, "SELECT JSON_EXTRACT('[1, {\"k\": \"v\"}]', '$[1].k')", `"v"`)
	jsonWantText(t, e, "SELECT JSON_EXTRACT('{\"a b\": 1}', '$.\"a b\"')", "1")
	jsonWantText(t, e, "SELECT JSON_EXTRACT('{\"a\": 1}', '$')", `{"a": 1}`)
	jsonWantText(t, e, "SELECT JSON_EXTRACT('{\"a\": 1.50}', '$.a')", "1.50")
	jsonWantNull(t, e, "SELECT JSON_EXTRACT('{\"a\": 1}', '$.missing')")
	jsonWantNull(t, e, "SELECT JSON_EXTRACT('[1]', '$[5]')")
	jsonWantNull(t, e, "SELECT JSON_EXTRACT(NULL, '$.a')")
	jsonWantNull(t, e, "SELECT JSON_EXTRACT('{\"a\": 1}', NULL)")
	// 多路径：按路径顺序组成数组，缺失路径以 null 占位。
	jsonWantText(t, e, "SELECT JSON_EXTRACT('{\"a\": 1, \"b\": \"x\"}', '$.a', '$.b')", `[1, "x"]`)
	jsonWantText(t, e, "SELECT JSON_EXTRACT('{\"a\": 1}', '$.a', '$.zz')", `[1, null]`)
}

// TestJSONUnquote JSON_UNQUOTE：去引号反转义 / 非 JSON 文本原样返回 / NULL 传播。
func TestJSONUnquote(t *testing.T) {
	e := newEngine(t)
	jsonWantText(t, e, `SELECT JSON_UNQUOTE('"abc"')`, "abc")
	jsonWantText(t, e, `SELECT JSON_UNQUOTE('"a b"')`, "a b")
	jsonWantText(t, e, "SELECT JSON_UNQUOTE('plain text')", "plain text")
	jsonWantText(t, e, "SELECT JSON_UNQUOTE('[1, 2]')", "[1, 2]")
	jsonWantNull(t, e, "SELECT JSON_UNQUOTE(NULL)")
	mustErr(t, e, "SELECT JSON_UNQUOTE()", "wrong number of arguments")
}

// TestJSONType JSON_TYPE：OBJECT/ARRAY/STRING/INTEGER/DOUBLE/BOOLEAN/NULL。
func TestJSONType(t *testing.T) {
	e := newEngine(t)
	jsonWantText(t, e, "SELECT JSON_TYPE('{\"a\": 1}')", "OBJECT")
	jsonWantText(t, e, "SELECT JSON_TYPE('[1, 2]')", "ARRAY")
	jsonWantText(t, e, `SELECT JSON_TYPE('"s"')`, "STRING")
	jsonWantText(t, e, "SELECT JSON_TYPE('1')", "INTEGER")
	jsonWantText(t, e, "SELECT JSON_TYPE('1.5')", "DOUBLE")
	jsonWantText(t, e, "SELECT JSON_TYPE('true')", "BOOLEAN")
	jsonWantText(t, e, "SELECT JSON_TYPE('null')", "NULL")
	jsonWantNull(t, e, "SELECT JSON_TYPE(NULL)")
	mustErr(t, e, "SELECT JSON_TYPE('{bad}')", "invalid JSON text")
}

// TestJSONLength JSON_LENGTH：对象/数组元素数、标量为 1、带路径、缺失路径 NULL。
func TestJSONLength(t *testing.T) {
	e := newEngine(t)
	jsonWantInt(t, e, "SELECT JSON_LENGTH('{\"a\": 1, \"b\": 2}')", 2)
	jsonWantInt(t, e, "SELECT JSON_LENGTH('[1, 2, 3]')", 3)
	jsonWantInt(t, e, "SELECT JSON_LENGTH('[]')", 0)
	jsonWantInt(t, e, "SELECT JSON_LENGTH('\"scalar\"')", 1)
	jsonWantInt(t, e, "SELECT JSON_LENGTH('{\"a\": [1, 2, 3]}', '$.a')", 3)
	jsonWantNull(t, e, "SELECT JSON_LENGTH('{\"a\": 1}', '$.zz')")
	jsonWantNull(t, e, "SELECT JSON_LENGTH(NULL)")
	mustErr(t, e, "SELECT JSON_LENGTH('oops')", "invalid JSON text")
}

// TestJSONObject JSON_OBJECT：按键值对构造、值类型映射、参数个数与 NULL 键报错。
func TestJSONObject(t *testing.T) {
	e := newEngine(t)
	jsonWantText(t, e, "SELECT JSON_OBJECT('a', 1, 'b', 'x')", `{"a": 1, "b": "x"}`)
	jsonWantText(t, e, "SELECT JSON_OBJECT('n', NULL)", `{"n": null}`)
	jsonWantText(t, e, "SELECT JSON_OBJECT('d', 1.25)", `{"d": 1.25}`)
	jsonWantText(t, e, "SELECT JSON_OBJECT()", `{}`)
	mustErr(t, e, "SELECT JSON_OBJECT('a')", "incorrect parameter count")
	mustErr(t, e, "SELECT JSON_OBJECT(NULL, 1)", "key must not be NULL")
}

// TestJSONArray JSON_ARRAY：混类型构造、空数组、嵌套。
func TestJSONArray(t *testing.T) {
	e := newEngine(t)
	jsonWantText(t, e, "SELECT JSON_ARRAY(1, 'a', NULL)", `[1, "a", null]`)
	jsonWantText(t, e, "SELECT JSON_ARRAY()", `[]`)
	jsonWantText(t, e, "SELECT JSON_ARRAY(JSON_ARRAY(1), JSON_OBJECT('k', 2))", `[[1], {"k": 2}]`)
}

// TestJSONSet JSON_SET：替换/新增键、嵌套创建、数组追加与越界报错、根替换。
func TestJSONSet(t *testing.T) {
	e := newEngine(t)
	jsonWantText(t, e, "SELECT JSON_SET('{\"a\": 1}', '$.a', 2)", `{"a": 2}`)
	jsonWantText(t, e, "SELECT JSON_SET('{\"a\": 1}', '$.b', 3)", `{"a": 1, "b": 3}`)
	// 中间层级为标量时写入报错（仅缺失键与数组末尾追加可创建）。
	mustErr(t, e, "SELECT JSON_SET('{\"a\": 1}', '$.a.b', 3)", "not an object")
	jsonWantText(t, e, "SELECT JSON_SET('[1, 2]', '$[0]', 9)", `[9, 2]`)
	jsonWantText(t, e, "SELECT JSON_SET('[1, 2]', '$[2]', 3)", `[1, 2, 3]`)
	jsonWantText(t, e, "SELECT JSON_SET('{\"a\": 1}', '$.a', 'x')", `{"a": "x"}`)
	jsonWantText(t, e, "SELECT JSON_SET('{\"a\": 1}', '$.a', NULL)", `{"a": null}`)
	jsonWantText(t, e, "SELECT JSON_SET('{\"a\": 1}', '$', 5)", `5`)
	// 多路径依次写入。
	jsonWantText(t, e, "SELECT JSON_SET('{\"a\": 1}', '$.b', 2, '$.c', 3)", `{"a": 1, "b": 2, "c": 3}`)
	jsonWantNull(t, e, "SELECT JSON_SET(NULL, '$.a', 1)")
	mustErr(t, e, "SELECT JSON_SET('1', '$.a', 2)", "not an object")
	mustErr(t, e, "SELECT JSON_SET('{\"a\": []}', '$.a[3]', 1)", "out of range")
	mustErr(t, e, "SELECT JSON_SET('{}', '$.a')", "incorrect parameter count")
}

// TestJSONNestedCalls 嵌套调用：函数作为其它函数实参。
func TestJSONNestedCalls(t *testing.T) {
	e := newEngine(t)
	jsonWantText(t, e, "SELECT JSON_UNQUOTE(JSON_EXTRACT('{\"a\": \"x\"}', '$.a'))", "x")
	jsonWantText(t, e, "SELECT JSON_EXTRACT(JSON_OBJECT('k', JSON_ARRAY(1, 2)), '$.k[0]')", "1")
	jsonWantInt(t, e, "SELECT JSON_LENGTH(JSON_ARRAY(1, 2, 3))", 3)
	jsonWantText(t, e, "SELECT JSON_EXTRACT(JSON_SET('{\"a\": 1}', '$.b', 2), '$.b')", "2")
	jsonWantInt(t, e, "SELECT JSON_VALID(JSON_OBJECT('a', 1))", 1)
	// 嵌套三层。
	jsonWantText(t, e, "SELECT JSON_UNQUOTE(JSON_EXTRACT(JSON_SET('{}', '$.k', 'v'), '$.k'))", "v")
}

// TestJSONColumnRef 列引用：投影、多行、别名、列参与 WHERE 过滤。
func TestJSONColumnRef(t *testing.T) {
	e := newEngine(t)
	setupDocs(t, e)

	rows := mustRows(t, e, "SELECT id, JSON_EXTRACT(doc, '$.name') FROM docs WHERE JSON_VALID(doc) = 1 ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("JSON_VALID filter: want 2 rows, got %d", len(rows))
	}
	if rows[0][1].Kind != "TEXT" || rows[0][1].S != `"alice"` {
		t.Fatalf("row0 name: want \"alice\", got kind=%s val=%q", rows[0][1].Kind, rows[0][1].S)
	}
	if rows[1][1].S != `"bob"` {
		t.Fatalf("row1 name: want \"bob\", got %q", rows[1][1].S)
	}

	// 别名覆盖输出列名。
	res := mustExec(t, e, "SELECT JSON_LENGTH(doc, '$.name') AS n FROM docs WHERE id = 1")
	if len(res.Columns) != 1 || res.Columns[0] != "n" {
		t.Fatalf("alias: want column n, got %v", res.Columns)
	}

	// WHERE 中 JSON_UNQUOTE(JSON_EXTRACT(...)) 过滤。
	rows = mustRows(t, e, "SELECT id FROM docs WHERE id < 3 AND JSON_UNQUOTE(JSON_EXTRACT(doc, '$.name')) = 'bob'")
	if len(rows) != 1 || rows[0][0].I != 2 {
		t.Fatalf("unquote filter: want id 2, got %v", rows)
	}

	// JSON_LENGTH 入参为列时按标量语义返回 1（字符串值）。
	jsonWantInt(t, e, "SELECT JSON_LENGTH(doc, '$.age') FROM docs WHERE id = 1", 1)
}

// TestJSONWithAggregateAndCase 与聚合 / CASE / 算术表达式组合。
func TestJSONWithAggregateAndCase(t *testing.T) {
	e := newEngine(t)
	setupDocs(t, e)

	jsonWantInt(t, e, "SELECT COUNT(*) FROM docs WHERE JSON_VALID(doc) = 1", 2)
	jsonWantInt(t, e, "SELECT JSON_LENGTH('{\"a\": 1}') + 1", 2)
	jsonWantText(t, e, "SELECT CASE WHEN JSON_VALID('{}') = 1 THEN 'ok' ELSE 'bad' END", "ok")
	jsonWantText(t, e, "SELECT CASE WHEN JSON_VALID('{bad}') = 1 THEN 'ok' ELSE 'bad' END", "bad")

	rows := mustRows(t, e, "SELECT JSON_EXTRACT(doc, '$.age') FROM docs WHERE JSON_VALID(doc) = 1 ORDER BY id")
	if len(rows) != 2 || rows[0][0].S != "30" || rows[1][0].S != "25" {
		t.Fatalf("extract age: want 30/25, got %v", rows)
	}
}

// TestJSONUpdateSet UPDATE SET 中的 JSON 函数（含列引用入参）。
func TestJSONUpdateSet(t *testing.T) {
	e := newEngine(t)
	setupDocs(t, e)

	mustExec(t, e, "UPDATE docs SET doc = JSON_SET(doc, '$.age', 31) WHERE id = 1")
	jsonWantText(t, e, "SELECT JSON_EXTRACT(doc, '$.age') FROM docs WHERE id = 1", "31")

	mustExec(t, e, "UPDATE docs SET doc = JSON_OBJECT('id', id) WHERE id = 2")
	jsonWantText(t, e, "SELECT doc FROM docs WHERE id = 2", `{"id": 2}`)
	jsonWantInt(t, e, "SELECT JSON_LENGTH(doc) FROM docs WHERE id = 2", 1)
}

// TestJSONInvalidInput 非法 JSON 文本与非法路径一律报错，不静默返回 NULL。
func TestJSONInvalidInput(t *testing.T) {
	e := newEngine(t)
	mustErr(t, e, "SELECT JSON_EXTRACT('{bad}', '$.a')", "invalid JSON text")
	mustErr(t, e, "SELECT JSON_EXTRACT('{\"a\": 1}', 'a.b')", "invalid JSON path")
	mustErr(t, e, "SELECT JSON_EXTRACT('{\"a\": 1}', '$..a')", "empty member name")
	mustErr(t, e, "SELECT JSON_EXTRACT('{\"a\": 1}', '$.a[')", "missing ']'")
	mustErr(t, e, "SELECT JSON_EXTRACT('{\"a\": 1}', '$.\"a')", "unterminated quoted member")
	mustErr(t, e, "SELECT JSON_LENGTH('[1]', '$[-1]')", "invalid array index")
	mustErr(t, e, "SELECT JSON_SET('{bad}', '$.a', 1)", "invalid JSON text")
	// 路径实参必须是字符串。
	mustErr(t, e, "SELECT JSON_EXTRACT('{\"a\": 1}', 5)", "path expression must be a string")
}

// TestJSONSelectNoFrom 无 FROM 常量投影路径（不经 FROM 也能求值）。
func TestJSONSelectNoFrom(t *testing.T) {
	e := newEngine(t)
	jsonWantText(t, e, "SELECT JSON_EXTRACT('{\"a\": [1, 2]}', '$.a[0]')", "1")
	jsonWantInt(t, e, "SELECT JSON_VALID('{}')", 1)
	rows := mustRows(t, e, "SELECT JSON_ARRAY(1, 2), JSON_OBJECT('a', 1)")
	if len(rows) != 1 || len(rows[0]) != 2 {
		t.Fatalf("multi column: got %v", rows)
	}
	if rows[0][0].S != `[1, 2]` || rows[0][1].S != `{"a": 1}` {
		t.Fatalf("multi column values: got %q / %q", rows[0][0].S, rows[0][1].S)
	}
}

// TestJSONFuncNotSupportedName 未注册函数名保持原有报错口径。
func TestJSONFuncNotSupportedName(t *testing.T) {
	e := newEngine(t)
	mustErr(t, e, "SELECT JSON_MERGE_PATCH('{}', '{}')", "not supported")
}
