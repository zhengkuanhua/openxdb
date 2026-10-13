package sql

import (
	"fmt"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/cdc"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// execShowBinlogEvents SHOW BINLOG EVENTS [FROM <lsn>] [LIMIT <n>]（M14 CDC）：
// 只读展示 binlog 增量变更流，供外部订阅 / 增量同步巡检与调试。
//
// 输出列：lsn / commit_lsn / schema_ver / kind / table / pk / row。
//   - kind：PUT（写入/更新）或 DELETE；
//   - table：行级变更还原为表名（按表 ID 查表目录；未知表 ID 显示 #table-<id>）；
//     IsIndex 事件显示 "(index)"（二级索引键，跟随行变更，消费方通常忽略）；
//   - pk：主键字节文本；
//   - row：PUT 行值按表列解码为 "col=val, ..." 摘要（元数据缺失时显示原始 JSON 字节）。
func (e *Executor) execShowBinlogEvents(sess *Session, s *ShowBinlogEventsStmt) (*Result, error) {
	if e.binlogPath == "" {
		return nil, &SQLError{Msg: "binlog not enabled"}
	}
	// 表目录：tableID -> 表元数据（行值列级解码用）。
	tx, err := e.getTx(sess)
	if err != nil {
		return nil, err
	}
	if e.autocommit(sess) {
		defer tx.Rollback()
	}
	tabs, err := loadTables(tx.Get)
	if err != nil {
		return nil, err
	}
	byID := make(map[uint64]*TableMeta, len(tabs))
	for _, t := range tabs {
		byID[t.ID] = t
	}

	sub, err := cdc.NewSubscriber(e.binlogPath)
	if err != nil {
		return nil, &SQLError{Msg: "open binlog: " + err.Error()}
	}
	defer sub.Close()

	var from storage.LSN
	if s.From != nil {
		from = storage.LSN(*s.From)
	}
	evs, _, err := sub.Fetch(from, s.Limit)
	if err != nil {
		return nil, &SQLError{Msg: "read binlog: " + err.Error()}
	}

	res := &Result{Columns: []string{"lsn", "commit_lsn", "schema_ver", "kind", "table", "pk", "row"}}
	for _, ev := range evs {
		table, row := "", ""
		switch {
		case ev.IsIndex:
			table = "(index)"
		default:
			if t, ok := byID[ev.TableID]; ok {
				table = t.Name
				if ev.Kind == cdc.KindPut && len(ev.RawValue) > 0 {
					if vals, derr := decodeRow(t, ev.RawValue); derr == nil {
						parts := make([]string, len(t.Columns))
						for i, c := range t.Columns {
							parts[i] = c.Name + "=" + vals[i].String()
						}
						row = "{" + strings.Join(parts, ", ") + "}"
					} else {
						row = "(decode: " + derr.Error() + ")"
					}
				}
			} else {
				table = fmt.Sprintf("#table-%d", ev.TableID)
			}
		}
		res.Rows = append(res.Rows, []Value{
			IntVal(int64(ev.LSN)),
			IntVal(int64(ev.CommitLSN)),
			IntVal(int64(ev.SchemaVer)),
			StrVal(ev.Kind.String()),
			StrVal(table),
			StrVal(string(ev.PK)),
			StrVal(row),
		})
	}
	return res, nil
}
