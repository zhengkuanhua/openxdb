package sql

import (
	"encoding/json"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// MatViewMeta 物化视图元数据：名称 + 定义（SELECT AST）+ 影子表 ID。
// 定义以 SelectStmt JSON 持久化；物化视图的 SELECT 直接读同名影子表，
// 不走虚拟视图展开（m:views），因此与虚拟视图互不干扰。
// v1.9.0 增量刷新扩展字段（旧目录反序列化为零值，向后兼容）：
//   - Deps：直接依赖的基表名（虚拟视图引用已递归展开为基表/物化视图）；
//   - DepMVs：直接依赖的物化视图名（依赖链刷新顺序与失效传播用）；
//   - Version：上次刷新消费的变更版本号（无新变更时 REFRESH 幂等跳过）；
//   - Simple：定义是否为"直接投影"（单基表纯列引用，可走行级增量 apply）。
type MatViewMeta struct {
	Name     string      `json:"name"`
	Select   *SelectStmt `json:"select"`
	ShadowID uint64      `json:"shadow_id"`
	Deps     []string    `json:"deps,omitempty"`
	DepMVs   []string    `json:"dep_mvs,omitempty"`
	Version  uint64      `json:"version,omitempty"`
	Simple   bool        `json:"simple,omitempty"`
}

// matViewsKey 物化视图目录键（独立于 m:views）。
var matViewsKey = []byte("m:matviews")

// mvHiddenPK 影子表内部主键列名。用户不可见：星号展开时按 __mv_ 前缀过滤。
const mvHiddenPK = "__mv_rid"

// loadMatViews 读取物化视图目录。
func loadMatViews(get func(key []byte) ([]byte, error)) ([]*MatViewMeta, error) {
	raw, err := get(matViewsKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	if raw == nil || len(raw) == 0 {
		return nil, nil
	}
	var mvs []*MatViewMeta
	if err := json.Unmarshal(raw, &mvs); err != nil {
		return nil, &SQLError{Msg: "corrupted materialized view catalog"}
	}
	return mvs, nil
}

// saveMatViews 序列化物化视图目录。
func saveMatViews(mvs []*MatViewMeta) ([]byte, error) {
	return json.Marshal(mvs)
}

// findMatView 按名查找物化视图。
func findMatView(mvs []*MatViewMeta, name string) *MatViewMeta {
	for _, m := range mvs {
		if m.Name == name {
			return m
		}
	}
	return nil
}

// visibleColumns 返回表对外可见列（过滤物化视图影子表内部隐藏列 __mv_*）。
func visibleColumns(meta *TableMeta) []ColumnDef {
	if !hasMvHiddenColumns(meta) {
		return meta.Columns
	}
	out := make([]ColumnDef, 0, len(meta.Columns))
	for _, c := range meta.Columns {
		if strings.HasPrefix(c.Name, "__mv_") {
			continue
		}
		out = append(out, c)
	}
	return out
}

// visibleRow 返回行中可见列值（与 visibleColumns 对齐；无隐藏列时直接返回原切片）。
func visibleRow(meta *TableMeta, row []Value) []Value {
	if !hasMvHiddenColumns(meta) {
		return row
	}
	out := make([]Value, 0, len(row))
	for i, c := range meta.Columns {
		if strings.HasPrefix(c.Name, "__mv_") {
			continue
		}
		out = append(out, row[i])
	}
	return out
}

// hasMvHiddenColumns 判断表是否含物化视图内部隐藏列。
func hasMvHiddenColumns(meta *TableMeta) bool {
	for _, c := range meta.Columns {
		if strings.HasPrefix(c.Name, "__mv_") {
			return true
		}
	}
	return false
}

// execCreateMatView CREATE MATERIALIZED VIEW <name> AS <SELECT>（M15）。
// 流程：名称/循环/引用检查 → 事务内执行 SELECT 得到快照结果 → 推断影子表 schema →
// 注册影子表（m:tables）→ 落盘快照行 → 持久化物化视图元数据（m:matviews）。
// 整个创建在单个事务内完成：影子表注册、快照落盘、元数据持久化要么全部生效要么全部回滚。
func (e *Executor) execCreateMatView(sess *Session, s *CreateMatViewStmt) (*Result, error) {
	if s.Name == "" {
		return nil, &SQLError{Msg: "materialized view name required"}
	}
	if s.Select == nil {
		return nil, &SQLError{Msg: "materialized view requires AS SELECT"}
	}
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	views, err := loadViews(tx.Get)
	if err != nil {
		return nil, err
	}
	mvs, err := loadMatViews(tx.Get)
	if err != nil {
		return nil, err
	}
	if findMatView(mvs, s.Name) != nil {
		// T41：CREATE MATERIALIZED VIEW IF NOT EXISTS——已存在时静默跳过
		if s.IfNotExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "materialized view already exists: " + s.Name}
	}
	if findTable(tabs, s.Name) != nil {
		return nil, &SQLError{Msg: "materialized view name conflicts with table: " + s.Name}
	}
	if findView(views, s.Name) != nil {
		return nil, &SQLError{Msg: "materialized view name conflicts with view: " + s.Name}
	}
	// 循环引用防护：定义不能直接引用自身（间接环因创建顺序 DAG 约束无法形成）。
	for _, n := range viewRefNames(s.Select) {
		if n == s.Name {
			return nil, &SQLError{Msg: "cyclic materialized view reference: " + s.Name + " references itself"}
		}
	}
	// 定义引用的表 / 虚拟视图 / 物化视图必须已存在。
	for _, n := range viewRefNames(s.Select) {
		if findTable(tabs, n) == nil && findView(views, n) == nil && findMatView(mvs, n) == nil {
			return nil, &SQLError{Msg: "table not exists: " + n}
		}
	}
	// 单语句自动提交场景包装为事务，保证创建原子性；显式事务中由用户 COMMIT。
	needTx := !sess.inTx
	if needTx {
		if err := e.beginTx(sess); err != nil {
			return nil, err
		}
		defer func() {
			if sess.inTx {
				_ = e.rollbackTx(sess)
			}
		}()
		tx = sess.curTx
	}
	res, err := e.execSelect(sess, s.Select)
	if err != nil {
		return nil, err
	}
	cols, err := matViewColumnNames(res.Columns)
	if err != nil {
		return nil, err
	}
	defs, normRows, err := inferMatViewSchema(cols, res.Rows)
	if err != nil {
		return nil, err
	}
	// v1.9.0：解析定义依赖（基表 deps + 物化视图 depMVs，虚拟视图引用递归展开）
	// 与 Simple 行级增量判定；Version 初始化为当前全局变更版本（创建即最新，
	// 后续无新变更时 REFRESH 幂等跳过）。
	deps, depMVs := collectMatViewDeps(s.Select, tabs, views, mvs)
	info, err := simpleMatViewInfoOf(s.Select, tabs, mvs, views)
	if err != nil {
		return nil, err
	}
	ver, err := currentMVVersion(tx.Get)
	if err != nil {
		return nil, err
	}
	pkType := "INT"
	if info != nil {
		pkType = info.pkType
	}
	shadow := &TableMeta{
		ID:      nextTableID(tabs),
		Name:    s.Name,
		Columns: append(defs, ColumnDef{Name: mvHiddenPK, Type: pkType}),
		PK:      mvHiddenPK,
	}
	tabs = append(tabs, shadow)
	raw, err := saveTables(tabs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	var n int
	if info != nil {
		n, err = e.writeMatViewRowsPK(tx, shadow, normRows, info.pkIdx)
	} else {
		n, err = e.writeMatViewRows(tx, shadow, normRows)
	}
	if err != nil {
		return nil, err
	}
	mvs = append(mvs, &MatViewMeta{Name: s.Name, Select: s.Select, ShadowID: shadow.ID, Deps: deps, DepMVs: depMVs, Version: ver, Simple: info != nil})
	e.buildMVDependencySet(mvs)
	raw, err = saveMatViews(mvs)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(matViewsKey, raw); err != nil {
		return nil, err
	}
	if needTx {
		if err := e.commitTx(sess); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: int(n)}, nil
}

// execRefreshMatView REFRESH MATERIALIZED VIEW <name> [CONCURRENTLY]（M15）。
// v1.9.0 语义化：依赖链刷新 + 变更版本幂等跳过 + Simple 行级增量 + 快照交换。
// 整体在单个事务内完成：依赖 MV 深度优先先刷新 → 失效检测（依赖侧版本无新
// 变更时跳过）→ Simple 行级增量 apply 或全量重建（差异写回，提交前读请求
// 看到旧快照、提交后原子看到新快照，即快照交换，不阻塞读写）。
// 重算失败（如源表被 DDL 删除/改列）时整个事务回滚，旧快照保持不变。
func (e *Executor) execRefreshMatView(sess *Session, s *RefreshMatViewStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	if auto {
		defer tx.Rollback()
	}
	mvs, err := loadMatViews(tx.Get)
	if err != nil {
		return nil, err
	}
	if findMatView(mvs, s.Name) == nil {
		return nil, &SQLError{Msg: "materialized view not exists: " + s.Name}
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	views, err := loadViews(tx.Get)
	if err != nil {
		return nil, err
	}
	n, err := e.refreshMatViewRec(sess, tx, mvs, tabs, views, s.Name, s.Concurrently, map[string]bool{})
	if err != nil {
		return nil, err
	}
	e.buildMVDependencySet(mvs)
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: int(n)}, nil
}

// execDropMatView DROP MATERIALIZED VIEW [IF EXISTS] <name>（M15）。
// 删除影子表数据与表目录条目 + 物化视图元数据；被其它虚拟视图/物化视图
// 引用时按 RESTRICT 语义拒绝删除。
func (e *Executor) execDropMatView(sess *Session, s *DropMatViewStmt) (*Result, error) {
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	auto := e.autocommit(sess)
	mvs, err := loadMatViews(tx.Get)
	if err != nil {
		return nil, err
	}
	mv := findMatView(mvs, s.Name)
	if mv == nil {
		if s.IfExists {
			return &Result{}, nil
		}
		return nil, &SQLError{Msg: "materialized view not exists: " + s.Name}
	}
	views, err := loadViews(tx.Get)
	if err != nil {
		return nil, err
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	// RESTRICT：虚拟视图或其它物化视图定义引用该物化视图时拒绝删除。
	for _, v := range views {
		for _, n := range viewRefNames(v.Select) {
			if n == s.Name {
				return nil, &SQLError{Msg: "cannot drop materialized view " + s.Name + ": view " + v.Name + " depends on it"}
			}
		}
	}
	for _, m := range mvs {
		if m.Name == s.Name {
			continue
		}
		for _, n := range viewRefNames(m.Select) {
			if n == s.Name {
				return nil, &SQLError{Msg: "cannot drop materialized view " + s.Name + ": materialized view " + m.Name + " depends on it"}
			}
		}
	}
	shadow := findTable(tabs, s.Name)
	if shadow != nil {
		if err := e.clearMatViewRows(sess, tx, shadow); err != nil {
			return nil, err
		}
	}
	// 从表目录移除影子表。
	out := tabs[:0]
	for _, t := range tabs {
		if t.Name != s.Name {
			out = append(out, t)
		}
	}
	raw, err := saveTables(out)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(metaKey, raw); err != nil {
		return nil, err
	}
	// 从物化视图目录移除。
	outM := mvs[:0]
	for _, m := range mvs {
		if m.Name != s.Name {
			outM = append(outM, m)
		}
	}
	raw, err = saveMatViews(outM)
	if err != nil {
		return nil, err
	}
	if err := tx.Put(matViewsKey, raw); err != nil {
		return nil, err
	}
	if auto {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	// 事务提交后重建依赖表缓存（显式事务由用户 COMMIT 后同样生效；
	// 回滚场景缓存超前仅造成无害的冗余跟踪，正确性不受影响）。
	e.buildMVDependencySet(outM)
	return &Result{AffectedRows: 0}, nil
}

// matViewColumnNames 归一化物化视图结果列名。
// JOIN 等场景下引擎结果列名可能带表/别名限定前缀（如 u.name、orders.amount），
// 物化列名应去掉限定前缀（与普通表列一致）。若去掉前缀后重名，保留完整限定名
// 以避免歧义。空列名报错；__mv_ 前缀为内部保留列名（影子表隐藏主键），报错拒绝。
func matViewColumnNames(cols []string) ([]string, error) {
	out := make([]string, len(cols))
	seen := map[string]bool{}
	for i, c := range cols {
		if c == "" {
			return nil, &SQLError{Msg: "materialized view column name missing"}
		}
		if strings.HasPrefix(c, "__mv_") {
			return nil, &SQLError{Msg: "reserved column name: " + c}
		}
		base := c
		if k := strings.LastIndexByte(c, '.'); k >= 0 {
			base = c[k+1:]
		}
		if seen[base] {
			base = c // 重名：保留完整限定名
		} else {
			seen[base] = true
		}
		out[i] = base
	}
	return out, nil
}

// inferMatViewSchema 根据结果行推断物化视图列定义并归一化值。
// 每列按实际值 Kind 推断统一类型：全 INT→INT；INT/DECIMAL→DECIMAL；全 DATE→DATE；
// 全 BLOB→BLOB；其余（INT/BLOB/TEXT 任意组合）→TEXT（字符串化）。
// 混合类型（如 DATE+INT、DECIMAL+TEXT）不支持并报错。空结果集全部 TEXT
// （REFRESH 后有数据时 schema 自动更新为实际类型）。
func inferMatViewSchema(cols []string, rows [][]Value) ([]ColumnDef, [][]Value, error) {
	defs := make([]ColumnDef, len(cols))
	for i, c := range cols {
		defs[i] = ColumnDef{Name: c, Type: "TEXT"}
	}
	if len(rows) == 0 {
		return defs, nil, nil
	}
	kinds := make([]map[string]bool, len(cols))
	for i := range kinds {
		kinds[i] = map[string]bool{}
	}
	for _, row := range rows {
		if len(row) != len(cols) {
			return nil, nil, &SQLError{Msg: "corrupted materialized view result"}
		}
		for i := range cols {
			kinds[i][row[i].Kind] = true
		}
	}
	types := make([]string, len(cols))
	for j := range cols {
		types[j] = inferMatViewColType(kinds[j])
		if types[j] == "" {
			return nil, nil, &SQLError{Msg: "cannot materialize mixed value types in column: " + cols[j]}
		}
		defs[j].Type = types[j]
	}
	norm := make([][]Value, len(rows))
	for i, row := range rows {
		nr := make([]Value, len(cols))
		for j := range cols {
			v, err := normalizeMatViewValue(types[j], cols[j], row[j])
			if err != nil {
				return nil, nil, err
			}
			nr[j] = v
		}
		norm[i] = nr
	}
	return defs, norm, nil
}

// inferMatViewColType 单列类型推断；返回 "" 表示混合类型不支持。
func inferMatViewColType(kinds map[string]bool) string {
	switch {
	case kinds["INT"] && len(kinds) == 1:
		return "INT"
	case kinds["DECIMAL"] && !kinds["DATE"] && !kinds["BLOB"] && !kinds["TEXT"]:
		return "DECIMAL" // 纯 DECIMAL 或 INT+DECIMAL（INT 升格）
	case kinds["DATE"] && len(kinds) == 1:
		return "DATE"
	case kinds["BLOB"] && len(kinds) == 1:
		return "BLOB"
	case !kinds["DATE"] && !kinds["DECIMAL"]:
		return "TEXT" // INT/BLOB/TEXT 任意组合 → 字符串化
	}
	return ""
}

// normalizeMatViewValue 归一化值为目标列类型。
// TEXT 列接受任意标量（统一字符串化）；其余类型复用 coerceValue 的校验/转换。
func normalizeMatViewValue(typ, name string, v Value) (Value, error) {
	if typ == "TEXT" {
		return StrVal(v.String()), nil
	}
	return coerceValue(typ, name, v)
}

// writeMatViewRows 把归一化结果行写入影子表：行键 rid 递增，附加隐藏 PK 列。
// 写入键与普通 INSERT 一致（locateRow 路由 + rowKeyAt 带 region 前缀），
// 保证 rowRanges 扫描可命中。仅用于 create/drop 等无旧行冲突的场景。
func (e *Executor) writeMatViewRows(tx txn.Txn, shadow *TableMeta, rows [][]Value) (int, error) {
	for i, row := range rows {
		vals := make([]Value, 0, len(row)+1)
		vals = append(vals, row...)
		vals = append(vals, IntVal(int64(i)))
		raw, err := encodeRow(shadow, vals)
		if err != nil {
			return 0, err
		}
		pk, err := pkBytes(shadow, IntVal(int64(i)))
		if err != nil {
			return 0, err
		}
		rid, err := e.locateRow(shadow, pk)
		if err != nil {
			return 0, err
		}
		key := e.rowKeyAt(shadow, rid, pk)
		if err := tx.Put(key, raw); err != nil {
			return 0, err
		}
	}
	return len(rows), nil
}

// refreshMatViewRows 全量重写影子表快照（REFRESH 用）。
// 注意：存储层 WriteBatch 按「先 puts 后 deletes」应用（bridge.cc xdb_write_batch），
// 同一 key 在同一批次内先 Delete 后 Put 时最终 Delete 生效；因此不能采用
// 「先清空再全量写回」的模式。这里改为「差异删除 + 全量写回」：
//  1) 先在未写入前扫描旧行，构造新快照 key 集合；
//  2) 仅删除旧行中不存在于新快照的 key（残留行）；
//  3) 全量 Put 新快照（与 Delete 无同 key 冲突，Put 全部生效）。
func (e *Executor) refreshMatViewRows(sess *Session, tx txn.Txn, shadow *TableMeta, rows [][]Value) (int, error) {
	keys := make([][]byte, len(rows))
	rawRows := make([][]byte, len(rows))
	newKeys := make(map[string]struct{}, len(rows))
	for i, row := range rows {
		vals := make([]Value, 0, len(row)+1)
		vals = append(vals, row...)
		vals = append(vals, IntVal(int64(i)))
		raw, err := encodeRow(shadow, vals)
		if err != nil {
			return 0, err
		}
		pk, err := pkBytes(shadow, IntVal(int64(i)))
		if err != nil {
			return 0, err
		}
		rid, err := e.locateRow(shadow, pk)
		if err != nil {
			return 0, err
		}
		key := e.rowKeyAt(shadow, rid, pk)
		keys[i] = key
		rawRows[i] = raw
		newKeys[string(key)] = struct{}{}
	}
	// 删除旧快照中不在新快照的残留行（避免与后续 Put 同 key 冲突）。
	oldPairs, err := e.scanMergedCluster(sess, tx, e.rowRanges(shadow))
	if err != nil {
		return 0, err
	}
	for _, kv := range oldPairs {
		if _, ok := newKeys[string(kv.Key)]; !ok {
			if err := tx.Delete(kv.Key); err != nil {
				return 0, err
			}
		}
	}
	// 全量写回新快照。
	for i, key := range keys {
		if err := tx.Put(key, rawRows[i]); err != nil {
			return 0, err
		}
	}
	return len(rows), nil
}

// clearMatViewRows 删除影子表全部旧行。
func (e *Executor) clearMatViewRows(sess *Session, tx txn.Txn, shadow *TableMeta) error {
	pairs, err := e.scanMergedCluster(sess, tx, e.rowRanges(shadow))
	if err != nil {
		return err
	}
	for _, kv := range pairs {
		if err := tx.Delete(kv.Key); err != nil {
			return err
		}
	}
	return nil
}

// matViewSchemaChanged 比较结果列定义与影子表现有列（忽略隐藏 PK 列）。
func matViewSchemaChanged(shadow *TableMeta, defs []ColumnDef) bool {
	n := len(shadow.Columns) - 1
	if n < 0 || len(defs) != n {
		return true
	}
	for i := 0; i < n; i++ {
		if shadow.Columns[i].Name != defs[i].Name || shadow.Columns[i].Type != defs[i].Type {
			return true
		}
	}
	return false
}

// writeMatViewRowsPK Simple 物化视图写快照（v1.9.0）：隐藏 PK 列与基表主键
// 同值，行键按主键值定位——与增量 apply 的 key 完全一致，保证增量/全量路径
// 可相互切换且互不冲突。仅用于 create 等无旧行冲突的场景。
func (e *Executor) writeMatViewRowsPK(tx txn.Txn, shadow *TableMeta, rows [][]Value, pkIdx int) (int, error) {
	for _, row := range rows {
		if len(row) <= pkIdx {
			return 0, &SQLError{Msg: "corrupted materialized view row"}
		}
		pkV := row[pkIdx]
		vals := make([]Value, 0, len(row)+1)
		vals = append(vals, row...)
		vals = append(vals, pkV)
		raw, err := encodeRow(shadow, vals)
		if err != nil {
			return 0, err
		}
		pk, err := pkBytes(shadow, pkV)
		if err != nil {
			return 0, err
		}
		rid, err := e.locateRow(shadow, pk)
		if err != nil {
			return 0, err
		}
		key := e.rowKeyAt(shadow, rid, pk)
		if err := tx.Put(key, raw); err != nil {
			return 0, err
		}
	}
	return len(rows), nil
}

// refreshMatViewRowsPK Simple 物化视图全量重写（v1.9.0）：同 refreshMatViewRows
// 的「差异删除 + 全量写回」防同 key 冲突结构，但行键按基表主键定位
// （隐藏 PK 列 = 主键值），与增量路径共享同一 key 空间。
func (e *Executor) refreshMatViewRowsPK(sess *Session, tx txn.Txn, shadow *TableMeta, rows [][]Value, pkIdx int) (int, error) {
	keys := make([][]byte, len(rows))
	rawRows := make([][]byte, len(rows))
	newKeys := make(map[string]struct{}, len(rows))
	for i, row := range rows {
		if len(row) <= pkIdx {
			return 0, &SQLError{Msg: "corrupted materialized view row"}
		}
		pkV := row[pkIdx]
		vals := make([]Value, 0, len(row)+1)
		vals = append(vals, row...)
		vals = append(vals, pkV)
		raw, err := encodeRow(shadow, vals)
		if err != nil {
			return 0, err
		}
		pk, err := pkBytes(shadow, pkV)
		if err != nil {
			return 0, err
		}
		rid, err := e.locateRow(shadow, pk)
		if err != nil {
			return 0, err
		}
		key := e.rowKeyAt(shadow, rid, pk)
		keys[i] = key
		rawRows[i] = raw
		newKeys[string(key)] = struct{}{}
	}
	// 删除旧快照中不在新快照的残留行（避免与后续 Put 同 key 冲突）。
	oldPairs, err := e.scanMergedCluster(sess, tx, e.rowRanges(shadow))
	if err != nil {
		return 0, err
	}
	for _, kv := range oldPairs {
		if _, ok := newKeys[string(kv.Key)]; !ok {
			if err := tx.Delete(kv.Key); err != nil {
				return 0, err
			}
		}
	}
	// 全量写回新快照。
	for i, key := range keys {
		if err := tx.Put(key, rawRows[i]); err != nil {
			return 0, err
		}
	}
	return len(rows), nil
}
