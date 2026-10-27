package sql_test

import (
	"testing"
)

// v1.15.0 W4：ORDER BY 索引顺序扫描 + DISTINCT 免排序（DEV_PLAN_2026Q7）。
// 覆盖：ORDER BY 主键/二级索引快路径、编码序偏离比较序时回退原排序路径、
// DISTINCT 相邻去重 / map 去重回退、WHERE/LIMIT/OFFSET 混合场景，以及
// 快路径与慢路径结果一致性（均以具体结果断言验证语义等价）。

// TestOrderIndexPKFastPath ORDER BY 主键升序：主键扫描序即排序序，免全量排序。
func TestOrderIndexPKFastPath(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	// 无修饰 ORDER BY id
	res := mustExec(t, e, "SELECT id, name FROM users ORDER BY id")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 1 || res.Rows[1][0].I != 2 || res.Rows[2][0].I != 3 {
		t.Fatalf("rows = %+v, want [[1 alice] [2 bob] [3 carol]]", res.Rows)
	}

	// WHERE + ORDER BY id（过滤保序）
	res = mustExec(t, e, "SELECT id FROM users WHERE age >= 30 ORDER BY id")
	if len(res.Rows) != 2 || res.Rows[0][0].I != 1 || res.Rows[1][0].I != 3 {
		t.Fatalf("where rows = %+v, want [[1] [3]]", res.Rows)
	}

	// LIMIT/OFFSET + ORDER BY id
	res = mustExec(t, e, "SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1")
	if len(res.Rows) != 2 || res.Rows[0][0].I != 2 || res.Rows[1][0].I != 3 {
		t.Fatalf("limit rows = %+v, want [[2] [3]]", res.Rows)
	}

	// SELECT * + ORDER BY id（Star 投影不影响排序快路径）
	res = mustExec(t, e, "SELECT * FROM users ORDER BY id")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 1 || res.Rows[0][1].S != "alice" {
		t.Fatalf("star rows = %+v", res.Rows)
	}
}

// TestOrderIndexPKFallbackNegative 单列 INT 主键负值：pkBytes 不翻转符号位，
// 编码序（大端无符号）偏离比较序，运行时已序验证必须失败并回退原排序。
func TestOrderIndexPKFallbackNegative(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE tneg (id INT, v TEXT, PRIMARY KEY (id))")
	mustExec(t, e, "INSERT INTO tneg (id, v) VALUES (-5, 'a'), (3, 'b'), (-1, 'c')")

	res := mustExec(t, e, "SELECT id FROM tneg ORDER BY id")
	if len(res.Rows) != 3 || res.Rows[0][0].I != -5 || res.Rows[1][0].I != -1 || res.Rows[2][0].I != 3 {
		t.Fatalf("rows = %+v, want [[-5] [-1] [3]]（负值编码序偏离比较序，必须回退排序）", res.Rows)
	}
}

// TestOrderIndexPKFallbackDesc DESC 与引用非主键列：不命中候选，走原排序。
func TestOrderIndexPKFallbackDesc(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)

	res := mustExec(t, e, "SELECT id FROM users ORDER BY id DESC")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 3 || res.Rows[2][0].I != 1 {
		t.Fatalf("desc rows = %+v, want [[3] [2] [1]]", res.Rows)
	}

	res = mustExec(t, e, "SELECT id FROM users ORDER BY name")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 1 || res.Rows[1][0].I != 2 || res.Rows[2][0].I != 3 {
		t.Fatalf("name rows = %+v, want [[1] [2] [3]]", res.Rows)
	}
}

// TestOrderIndexSecondaryFastPath 二级索引顺序扫描：ORDER BY 索引列升序命中
// 单列索引，按索引序全范围扫描回表构建 Rel，免排序输出。
func TestOrderIndexSecondaryFastPath(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")

	// ORDER BY age（插入序与 age 无关：alice=30, bob=25, carol=35）
	res := mustExec(t, e, "SELECT id, name, age FROM users ORDER BY age")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 2 || res.Rows[1][0].I != 1 || res.Rows[2][0].I != 3 {
		t.Fatalf("rows = %+v, want [[2 bob 25] [1 alice 30] [3 carol 35]]", res.Rows)
	}

	// WHERE 引用非索引列 + ORDER BY 索引列（主键范围物化后过滤保序）
	res = mustExec(t, e, "SELECT name FROM users WHERE id > 1 ORDER BY age")
	if len(res.Rows) != 2 || res.Rows[0][0].S != "bob" || res.Rows[1][0].S != "carol" {
		t.Fatalf("where rows = %+v, want [[bob] [carol]]", res.Rows)
	}

	// LIMIT + ORDER BY 索引列
	res = mustExec(t, e, "SELECT id FROM users ORDER BY age LIMIT 2")
	if len(res.Rows) != 2 || res.Rows[0][0].I != 2 || res.Rows[1][0].I != 1 {
		t.Fatalf("limit rows = %+v, want [[2] [1]]", res.Rows)
	}
}

// TestOrderIndexSecondaryFallback 二级索引未命中：无索引列/降序场景回退原路径。
func TestOrderIndexSecondaryFallback(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")

	// ORDER BY 无索引列 name
	res := mustExec(t, e, "SELECT id FROM users ORDER BY name")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 1 || res.Rows[1][0].I != 2 || res.Rows[2][0].I != 3 {
		t.Fatalf("name rows = %+v, want [[1] [2] [3]]", res.Rows)
	}

	// ORDER BY age DESC：不命中升序候选，回退排序
	res = mustExec(t, e, "SELECT id FROM users ORDER BY age DESC")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 3 || res.Rows[2][0].I != 2 {
		t.Fatalf("desc rows = %+v, want [[3] [1] [2]]", res.Rows)
	}
}

// TestOrderIndexDistinctFastPath DISTINCT 免排序：单列投影=ORDER BY 列且扫描有序时
// 相邻去重替代 map 去重；结果与保序去重语义一致。
func TestOrderIndexDistinctFastPath(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")

	// DISTINCT age ORDER BY age：索引扫描有序 + 单列投影=排序列 → 相邻去重
	res := mustExec(t, e, "SELECT DISTINCT age FROM users ORDER BY age")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 25 || res.Rows[1][0].I != 30 || res.Rows[2][0].I != 35 {
		t.Fatalf("rows = %+v, want [[25] [30] [35]]", res.Rows)
	}

	// 重复值场景：相邻去重输出唯一值
	mustExec(t, e, "INSERT INTO users (id, name, age) VALUES (4, 'dave', 30), (5, 'erin', 25)")
	res = mustExec(t, e, "SELECT DISTINCT age FROM users ORDER BY age")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 25 || res.Rows[1][0].I != 30 || res.Rows[2][0].I != 35 {
		t.Fatalf("dedup rows = %+v, want [[25] [30] [35]]", res.Rows)
	}
}

// TestOrderIndexDistinctFallback DISTINCT 回退：多列投影/无 ORDER BY 走 map 去重保序。
func TestOrderIndexDistinctFallback(t *testing.T) {
	e := newEngine(t)
	setupUsers(t, e)
	mustExec(t, e, "CREATE INDEX idx_age ON users (age)")

	// 多列投影（去重键≠有序维度）→ map 去重
	res := mustExec(t, e, "SELECT DISTINCT age, name FROM users ORDER BY age")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 25 || res.Rows[0][1].S != "bob" {
		t.Fatalf("multi rows = %+v, want [[25 bob] [30 alice] [35 carol]]", res.Rows)
	}

	// 无 ORDER BY → ordered=false，map 去重保持首次出现顺序
	res = mustExec(t, e, "SELECT DISTINCT age FROM users")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 30 || res.Rows[1][0].I != 25 || res.Rows[2][0].I != 35 {
		t.Fatalf("noorder rows = %+v, want 首现 [[30] [25] [35]]", res.Rows)
	}
}

// TestOrderIndexMixedScenario 混合场景：GROUP BY 聚合路径不受干扰；复合主键首列命中。
func TestOrderIndexMixedScenario(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE sales (pid INT, city TEXT, amt INT, PRIMARY KEY (pid))")
	mustExec(t, e, "INSERT INTO sales VALUES (1, 'bj', 100), (2, 'bj', 200), (3, 'sh', 150), (4, 'sh', 300)")

	// GROUP BY + ORDER BY city（聚合路径独立处理，不进入 finishRel 快路径）
	res := mustExec(t, e, "SELECT city, SUM(amt) FROM sales GROUP BY city ORDER BY city")
	if len(res.Rows) != 2 || res.Rows[0][0].S != "bj" || res.Rows[0][1].I != 300 || res.Rows[1][0].S != "sh" {
		t.Fatalf("group rows = %+v, want [[bj 300] [sh 450]]", res.Rows)
	}

	// 复合主键首列 ORDER BY（INT 正数编码前缀一致，验证通过免排序，结果正确）
	mustExec(t, e, "CREATE TABLE tc (a INT, b TEXT, v INT, PRIMARY KEY (a, b))")
	mustExec(t, e, "INSERT INTO tc VALUES (2, 'x', 1), (1, 'y', 2), (3, 'z', 3)")
	res = mustExec(t, e, "SELECT v FROM tc ORDER BY a")
	if len(res.Rows) != 3 || res.Rows[0][0].I != 2 || res.Rows[1][0].I != 1 || res.Rows[2][0].I != 3 {
		t.Fatalf("composite rows = %+v, want [[2] [1] [3]]", res.Rows)
	}
}
