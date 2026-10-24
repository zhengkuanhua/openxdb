package sql

import (
	"strconv"
	"sync"

	"github.com/zhengkuanhua/openxdb/pkg/import"
	"github.com/zhengkuanhua/openxdb/pkg/txn"
)

// execImportDump 执行 mysqldump 兼容导入（IMPORT MYSQLDUMP FROM 'path' [BATCH n] [IGNORE ERRORS]）。
//
// 工作流：
//  1. 解析 dump 文件（pkg/import/mysqldump）：抽取 DROP TABLE IF EXISTS、CREATE TABLE、INSERT；
//  2. 在导入事务中依次执行 DROP（重建语义）、CREATE TABLE、逐行 INSERT；
//  3. 无 BATCH 时整个导入为单个原子事务（任一步失败整体回滚）；
//     BATCH>0 时每批提交一次（大表迁移避免单事务过大），DDL 随首批一并提交；
//  4. 主键冲突行始终跳过计数（导入语义，与 CSV 导入一致）；
//     IgnoreErrors 时坏行/建表错误/执行错误跳过计数，否则首个错误即回滚。
//
// P2-12：CONCURRENCY k>1（且 BATCH>0）时启用表级并行导入（execImportDumpParallel）：
// DDL（DROP+CREATE）串行单事务原子提交后，各表数据由独立会话/独立事务并发灌入，
// 表内仍按 BATCH 分批提交；并行导入在显式事务内调用会报错（无法保证外层原子性）。
//
// 返回 Result：Columns=[tables, inserted, skipped, errors, batches]。
func (e *Executor) execImportDump(sess *Session, s *ImportStmt) (*Result, error) {
	if s.Format == "MYSQLDUMP" && s.Concurrency > 1 {
		if s.Batch <= 0 {
			return nil, &SQLError{Msg: "CONCURRENCY requires BATCH (atomic import cannot be parallelized)"}
		}
		return e.execImportDumpParallel(sess, s)
	}
	// P2-13：mysqldump 全选项贯通到解析
	opt := mysqldump.Options{
		IgnoreErrors:    s.IgnoreErrors,
		NoData:          s.NoData,
		NoCreateInfo:    s.NoCreateInfo,
		CompleteInsert:  s.CompleteInsert,
		ExtendedInsert:  s.ExtendedInsert,
		InsertIgnore:    s.InsertIgnore,
		Replace:         s.Replace,
		HexBlob:         s.HexBlob,
		SetNamesCharset: s.SetNamesCharset,
		DropTables:      s.DropTables,
	}
	dump, err := mysqldump.ParseFile(s.Path, opt)
	if err != nil {
		return nil, &SQLError{Msg: "import failed: " + err.Error()}
	}
	// 导入事务：已在显式会话事务中则复用（不自行提交）；否则自开并控制提交（BATCH 按批）。
	outer := false
	sess.mu.Lock()
	if !sess.inTx {
		outer = true
	}
	sess.mu.Unlock()
	if outer {
		if err := e.beginTx(sess); err != nil {
			return nil, err
		}
		defer func() { _ = e.rollbackTx(sess) }()
	}
	tables, inserted, skipped, bad, batches := 0, 0, 0, 0, 0
	// flush：仅 BATCH 模式真正提交并开启新事务；原子模式为空操作（结尾统一提交）。
	flush := func() error {
		if !outer || s.Batch <= 0 {
			return nil
		}
		if err := e.commitTx(sess); err != nil {
			return err
		}
		if err := e.beginTx(sess); err != nil {
			return err
		}
		batches++
		return nil
	}
	// 1) DROP TABLE IF EXISTS（重建语义：目标库同名表被替换）
	//    P2-13：NO DROP TABLES 时跳过 DROP，改为"保留目标库存量 + 覆盖式灌入"。
	for _, dn := range dump.Drops {
		if !s.DropTables {
			break
		}
		if _, err := e.execDropTable(sess, &DropTableStmt{Name: dn, IfExists: true}); err != nil {
			if !s.IgnoreErrors {
				return nil, err
			}
			bad++
		}
	}
	// 2) CREATE TABLE + INSERT 灌入
	for _, t := range dump.Tables {
		if len(t.Columns) == 0 {
			continue
		}
		cols := make([]ColumnDef, len(t.Columns))
		pk := ""
		for i, c := range t.Columns {
			cols[i] = ColumnDef{Name: c.Name, Type: c.Type}
			if c.Primary && pk == "" {
				pk = c.Name
			}
		}
		// P2-13：NO CREATE INFO 不建表（目标库表须已存在），仍从目标库取 meta 灌入。
		if !s.NoCreateInfo {
			if _, err := e.execCreateTable(sess, &CreateTableStmt{Name: t.Name, Columns: cols, PK: pk}); err != nil {
				if !s.IgnoreErrors {
					return nil, err
				}
				bad++
				continue
			}
		}
		tables++
		tx, err := e.getTx(sess)
		if err != nil {
			return nil, err
		}
		tabs, err := loadTables(tx.Get)
		if err != nil {
			return nil, err
		}
		meta := findTable(tabs, t.Name)
		if meta == nil {
			if s.NoCreateInfo && s.IgnoreErrors {
				bad++
				continue
			}
			return nil, &SQLError{Msg: "table not exists after create: " + t.Name}
		}
		var ops []kvOp
		// seen：文件内重复主键（同事务未提交写不可见，需本表独立去重）
		seen := make(map[string]bool)
		// pending：REPLACE 模式下同事务唯一索引冲突检测缓冲（flush 后失效需重置）
		var pending [][]Value
		for _, r := range t.Rows {
			vals, verr := dumpRowValues(t, r, s.HexBlob)
			if verr != nil {
				if !s.IgnoreErrors {
					return nil, verr
				}
				bad++
				continue
			}
			pk, perr := e.pkOf(meta, vals)
			if perr != nil {
				if !s.IgnoreErrors {
					return nil, perr
				}
				bad++
				continue
			}
			if seen[string(pk)] {
				skipped++
				continue
			}
			seen[string(pk)] = true
			if s.Replace {
				// --replace：REPLACE INTO 语义，主键/唯一冲突覆盖旧行
				rops, _, oerr := e.insertRowUpsertOps(sess, tx, tabs, meta, nil, vals, nil, true, &pending)
				if oerr != nil {
					if !s.IgnoreErrors {
						return nil, oerr
					}
					bad++
					continue
				}
				ops = append(ops, rops...)
				inserted++
			} else {
				rops, ok, oerr := e.insertRowOps(sess, tx, tabs, meta, nil, vals, true)
				if oerr != nil {
					if !s.IgnoreErrors {
						return nil, oerr
					}
					bad++
					continue
				}
				if !ok {
					skipped++
					continue
				}
				ops = append(ops, rops...)
				inserted++
			}
			if s.Batch > 0 && len(ops) >= s.Batch {
				if err := e.applyImportOps(tx, ops); err != nil {
					return nil, err
				}
				ops = nil
				if err := flush(); err != nil {
					return nil, err
				}
				pending = nil // 新事务：pending 唯一冲突缓冲失效
				// flush 已开启新事务，重新取 tx/tabs/meta
				tx, err = e.getTx(sess)
				if err != nil {
					return nil, err
				}
				tabs, err = loadTables(tx.Get)
				if err != nil {
					return nil, err
				}
				meta = findTable(tabs, t.Name)
				if meta == nil {
					return nil, &SQLError{Msg: "table not exists after batch: " + t.Name}
				}
			}
		}
		if len(ops) > 0 {
			if err := e.applyImportOps(tx, ops); err != nil {
				return nil, err
			}
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if outer && s.Batch <= 0 {
		batches = 1
		if err := e.commitTx(sess); err != nil {
			return nil, err
		}
	} else if outer {
		// BATCH 模式：末批已提交，回滚残余空事务（数据已在各批落盘）
		_ = e.rollbackTx(sess)
		if batches == 0 {
			batches = 1 // 空导入也计为一次事务（无数据）
		}
	}
	return &Result{
		Columns:      []string{"tables", "inserted", "skipped", "errors", "batches"},
		AffectedRows: inserted,
		Rows:         [][]Value{{IntVal(int64(tables)), IntVal(int64(inserted)), IntVal(int64(skipped)), IntVal(int64(bad)), IntVal(int64(batches))}},
	}, nil
}

// execImportDumpParallel P2-12：mysqldump 表级并行导入（CONCURRENCY k>1 且 BATCH>0）。
//
// 并行策略：
//   - DDL（DROP + CREATE）串行执行并单事务原子提交：表目录（metaKey）是全局共享键，
//     并行写会互相覆盖，必须串行；DDL 提交后各表结构对全部 worker 可见。
//   - INSERT 阶段按表分发到 worker 池：每个 worker 使用独立 Session（NewSession）与
//     独立存储事务并发灌入，快照隔离保证并行安全；表内仍按 BATCH 分批提交，
//     与串行 BATCH 模式"部分完成可恢复"语义一致。
//   - 错误处理：IgnoreErrors 时坏行/建表错误计数跳过；否则任一表内错误停止该表
//     （已提交批次保留），整体返回首个错误并明确报告部分完成统计。
//   - 显式会话事务内调用报错：worker 独立提交会破坏外层事务原子性，禁止混用。
//
// 返回 Result：Columns=[tables, inserted, skipped, errors, batches]。
func (e *Executor) execImportDumpParallel(sess *Session, s *ImportStmt) (*Result, error) {
	// 显式事务内禁止并行导入（外层原子性无法保证）。
	sess.mu.Lock()
	inTx := sess.inTx
	sess.mu.Unlock()
	if inTx {
		return nil, &SQLError{Msg: "CONCURRENCY import cannot run inside an explicit transaction"}
	}
	opt := mysqldump.Options{
		IgnoreErrors:    s.IgnoreErrors,
		NoData:          s.NoData,
		NoCreateInfo:    s.NoCreateInfo,
		CompleteInsert:  s.CompleteInsert,
		ExtendedInsert:  s.ExtendedInsert,
		InsertIgnore:    s.InsertIgnore,
		Replace:         s.Replace,
		HexBlob:         s.HexBlob,
		SetNamesCharset: s.SetNamesCharset,
		DropTables:      s.DropTables,
	}
	dump, err := mysqldump.ParseFile(s.Path, opt)
	if err != nil {
		return nil, &SQLError{Msg: "import failed: " + err.Error()}
	}

	// ---- 阶段 1（串行）：DDL 单事务原子提交 ----
	if err := e.beginTx(sess); err != nil {
		return nil, err
	}
	tables, badDDL := 0, 0
	for _, dn := range dump.Drops {
		if !s.DropTables {
			break
		}
		if _, err := e.execDropTable(sess, &DropTableStmt{Name: dn, IfExists: true}); err != nil {
			if !s.IgnoreErrors {
				_ = e.rollbackTx(sess)
				return nil, err
			}
			badDDL++
		}
	}
	// 记录成功建表的表（按 dump 顺序，供并行灌入）。
	type tblTask struct {
		idx int
		t   *mysqldump.Table
	}
	var tasks []tblTask
	for i, t := range dump.Tables {
		if len(t.Columns) == 0 {
			continue
		}
		cols := make([]ColumnDef, len(t.Columns))
		pk := ""
		for j, c := range t.Columns {
			cols[j] = ColumnDef{Name: c.Name, Type: c.Type}
			if c.Primary && pk == "" {
				pk = c.Name
			}
		}
		// P2-13：NO CREATE INFO 不建表（目标库表须已存在）
		if !s.NoCreateInfo {
			if _, err := e.execCreateTable(sess, &CreateTableStmt{Name: t.Name, Columns: cols, PK: pk}); err != nil {
				if !s.IgnoreErrors {
					_ = e.rollbackTx(sess)
					return nil, err
				}
				badDDL++
				continue
			}
		}
		tables++
		tasks = append(tasks, tblTask{idx: i, t: t})
	}
	if err := e.commitTx(sess); err != nil {
		return nil, err
	}

	// ---- 阶段 2（并行）：各表独立会话/独立事务并发灌入 ----
	if len(tasks) == 0 {
		batches := 1 // 空导入也计为一次事务（无数据，与串行一致）
		return &Result{
			Columns:      []string{"tables", "inserted", "skipped", "errors", "batches"},
			AffectedRows: 0,
			Rows:         [][]Value{{IntVal(int64(tables)), IntVal(0), IntVal(0), IntVal(int64(badDDL)), IntVal(int64(batches))}},
		}, nil
	}
	conc := s.Concurrency
	if conc > len(tasks) {
		conc = len(tasks)
	}
	type tblStat struct {
		inserted, skipped, bad, batches int64
		err                              error
	}
	stats := make([]tblStat, len(tasks))
	ch := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range ch {
				tk := tasks[idx]
				ws := e.NewSession()
				if err := e.beginTx(ws); err != nil {
					stats[idx].err = err
					continue
				}
				ins, skp, bad, bat, terr := e.importDumpTableRows(ws, tk.t, s.Batch, opt)
				stats[idx].inserted, stats[idx].skipped, stats[idx].bad, stats[idx].batches = ins, skp, bad, bat
				stats[idx].err = terr
				// 表数据已按批提交；清残余事务（无未提交数据，仅回滚空事务）
				_ = e.rollbackTx(ws)
			}
		}()
	}
	for i := range tasks {
		ch <- i
	}
	close(ch)
	wg.Wait()

	// ---- 汇总：按表顺序合并统计，返回首个错误（部分完成状态明确报告） ----
	var firstErr error
	var inserted, skipped, bad, batches int64
	for i := range tasks {
		st := stats[i]
		inserted += st.inserted
		skipped += st.skipped
		bad += st.bad
		batches += st.batches
		if st.err != nil && firstErr == nil {
			firstErr = st.err
		}
	}
	bad += int64(badDDL)
	if batches == 0 {
		batches = 1 // 无数据行时仍计一次事务
	}
	if firstErr != nil {
		return &Result{
			Columns:      []string{"tables", "inserted", "skipped", "errors", "batches"},
			AffectedRows: int(inserted),
			Rows:         [][]Value{{IntVal(int64(tables)), IntVal(inserted), IntVal(skipped), IntVal(bad), IntVal(batches)}},
		}, firstErr
	}
	return &Result{
		Columns:      []string{"tables", "inserted", "skipped", "errors", "batches"},
		AffectedRows: int(inserted),
		Rows:         [][]Value{{IntVal(int64(tables)), IntVal(inserted), IntVal(skipped), IntVal(bad), IntVal(batches)}},
	}, nil
}

// importDumpTableRows P2-12：单表数据灌入（并行导入 worker 专用）。
// 调用方须已通过 beginTx 为 ws 开启事务；表内逐行 INSERT，达到 batch 行时
// applyImportOps 落盘并 flush（commitTx + beginTx）开启新事务继续；返回统计与首个错误。
func (e *Executor) importDumpTableRows(ws *Session, t *mysqldump.Table, batch int, opt mysqldump.Options) (inserted, skipped, bad, batches int64, retErr error) {
	tx, err := e.getTx(ws)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	meta := findTable(tabs, t.Name)
	if meta == nil {
		return 0, 0, 0, 0, &SQLError{Msg: "table not exists after create: " + t.Name}
	}
	var ops []kvOp
	seen := make(map[string]bool)
	var pending [][]Value
	// flush 仅 BATCH 模式启用（并行入口已保证 batch>0）
	flush := func() error {
		if err := e.commitTx(ws); err != nil {
			return err
		}
		if err := e.beginTx(ws); err != nil {
			return err
		}
		batches++
		return nil
	}
	for _, r := range t.Rows {
		vals, verr := dumpRowValues(t, r, opt.HexBlob)
		if verr != nil {
			if !opt.IgnoreErrors {
				return inserted, skipped, bad, batches, verr
			}
			bad++
			continue
		}
		pk, perr := e.pkOf(meta, vals)
		if perr != nil {
			if !opt.IgnoreErrors {
				return inserted, skipped, bad, batches, perr
			}
			bad++
			continue
		}
		if seen[string(pk)] {
			skipped++
			continue
		}
		seen[string(pk)] = true
		if opt.Replace {
			rops, _, oerr := e.insertRowUpsertOps(ws, tx, tabs, meta, nil, vals, nil, true, &pending)
			if oerr != nil {
				if !opt.IgnoreErrors {
					return inserted, skipped, bad, batches, oerr
				}
				bad++
				continue
			}
			ops = append(ops, rops...)
			inserted++
		} else {
			rops, ok, oerr := e.insertRowOps(ws, tx, tabs, meta, nil, vals, true)
			if oerr != nil {
				if !opt.IgnoreErrors {
					return inserted, skipped, bad, batches, oerr
				}
				bad++
				continue
			}
			if !ok {
				skipped++
				continue
			}
			ops = append(ops, rops...)
			inserted++
		}
		if len(ops) >= batch {
			if err := e.applyImportOps(tx, ops); err != nil {
				return inserted, skipped, bad, batches, err
			}
			ops = nil
			pending = nil
			if err := flush(); err != nil {
				return inserted, skipped, bad, batches, err
			}
			// flush 已开启新事务，重新取 tx/tabs/meta
			tx, err = e.getTx(ws)
			if err != nil {
				return inserted, skipped, bad, batches, err
			}
			tabs, err = loadTables(tx.Get)
			if err != nil {
				return inserted, skipped, bad, batches, err
			}
			meta = findTable(tabs, t.Name)
			if meta == nil {
				return inserted, skipped, bad, batches, &SQLError{Msg: "table not exists after batch: " + t.Name}
			}
		}
	}
	if len(ops) > 0 {
		if err := e.applyImportOps(tx, ops); err != nil {
			return inserted, skipped, bad, batches, err
		}
		if err := flush(); err != nil {
			return inserted, skipped, bad, batches, err
		}
	}
	return inserted, skipped, bad, batches, nil
}

// applyImportOps 应用一批导入写入（模拟显式事务内仅支持本地 region 写，与显式事务一致）。
func (e *Executor) applyImportOps(tx txn.Txn, ops []kvOp) error {
	if e.hasRemoteOps(ops) {
		return &SQLError{Msg: "distributed write inside import transaction not supported in M14 (autocommit 2PC only)"}
	}
	return applyOps(tx, ops)
}

// isHexLiteral 判断字符串是否为十六进制字面量内容（仅含 0-9 A-F，长度须为偶数）。
// 解析层已把 X''/0x 前缀剥离并统一大写，此处校验剩余内容即可。
func isHexLiteral(s string) bool {
	if len(s) == 0 || len(s)%2 != 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9') && !(c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// dumpRowValues 将 mysqldump 解析单元格转换为 OpenXDB 存储值：
//   - INT：十进制字符串 → IntVal；解析失败报错（NULL → 0）
//   - TEXT：字符串原样（NULL → 空串）
//   - DATE：校验 YYYY-MM-DD（NULL → 1970-01-01）
//   - DECIMAL：parseDecimal 规范化（NULL → 0.0000）
//   - BLOB：解析层规范化文本（hex 字面量统一大写，普通字符串原样）（NULL → 空串）；
//     P2-13 HexBlob=true 时强制校验 BLOB 内容为十六进制字面量（--hex-blob 语义），
//     非 hex 内容报错。
//
// OpenXDB 无 NULL 语义，NULL 统一映射为列类型零值（文档 T27 说明）。
func dumpRowValues(t *mysqldump.Table, r []mysqldump.Value, hexBlob bool) ([]Value, error) {
	if len(r) != len(t.Columns) {
		return nil, &SQLError{Msg: "import failed: row column count mismatch"}
	}
	vals := make([]Value, len(r))
	for i, c := range t.Columns {
		cell := r[i]
		if cell.Null {
			switch c.Type {
			case "INT":
				vals[i] = IntVal(0)
			case "TEXT", "BLOB":
				vals[i] = Value{Kind: c.Type, S: ""}
			case "DATE":
				vals[i] = Value{Kind: "DATE", S: "1970-01-01"}
			case "DECIMAL":
				scaled, canon, err := parseDecimal("0")
				if err != nil {
					return nil, err
				}
				vals[i] = Value{Kind: "DECIMAL", I: scaled, S: canon}
			default:
				return nil, &SQLError{Msg: "import failed: unsupported column type: " + c.Type}
			}
			continue
		}
		switch c.Type {
		case "INT":
			n, err := strconv.ParseInt(cell.Raw, 10, 64)
			if err != nil {
				return nil, &SQLError{Msg: "import failed: invalid INT: " + cell.Raw}
			}
			vals[i] = IntVal(n)
		case "TEXT":
			vals[i] = StrVal(cell.Raw)
		case "DATE":
			if err := validateDate(cell.Raw); err != nil {
				return nil, err
			}
			vals[i] = Value{Kind: "DATE", S: cell.Raw}
		case "DECIMAL":
			scaled, canon, err := parseDecimal(cell.Raw)
			if err != nil {
				return nil, err
			}
			vals[i] = Value{Kind: "DECIMAL", I: scaled, S: canon}
		case "BLOB":
			// 解析层已把 hex 字面量（X''/0x）统一为大写文本；普通字符串按原样存储。
			raw := cell.Raw
			if hexBlob && raw != "" {
				// --hex-blob：BLOB 列必须为十六进制字面量（解析层统一为不含 X''/0x 前缀的大写 hex）
				if !isHexLiteral(raw) {
					return nil, &SQLError{Msg: "import failed: --hex-blob expects hex literal for BLOB column " + c.Name + ": " + raw}
				}
			}
			vals[i] = Value{Kind: "BLOB", S: raw}
		default:
			return nil, &SQLError{Msg: "import failed: unsupported column type: " + c.Type}
		}
	}
	return vals, nil
}
