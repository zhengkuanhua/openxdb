package mysqlproto

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/cdc"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// 复制协议（v1.6.0 P1-2）：COM_REGISTER_SLAVE / COM_BINLOG_DUMP。
//
// COM_BINLOG_DUMP 响应为流式 binlog 事件包：外壳沿用 MySQL binlog event header
// （timestamp u32 LE | type u8 | server_id u32 LE | event_size u32 LE |
// log_pos u32 LE | flags u16 LE），事件体为 OpenXDB 专有格式（不伪装 MySQL
// row event 列类型二进制语义）：
//
//	type=99  OPENXDB_FORMAT_EVENT：magic "OPENXDB" | filename NUL | start_lsn u64 LE | last_lsn u64 LE
//	type=100 OPENXDB_ROWS_EVENT：lsn u64 | commit_lsn u64 | schema_ver u64 | kind u8(1=PUT/2=DELETE) |
//	         table_id u64 | is_index u8 | table_name lenenc | pk lenenc | raw_value lenenc
//	type=101 OPENXDB_XID_EVENT：commit_lsn u64（每条 binlog 记录 = 一次提交，末尾发送）
//
// position 参数映射为内部 LSN 起点（0 = 从头）；log_pos 取事件 LSN 推进位点，
// 订阅方按 SHOW MASTER STATUS / COM_BINLOG_DUMP 语义续传。读完文件尾后轮询
// 等待新事件，连接断开（写失败）退出。

const (
	evOpenXDBFormat = 99
	evOpenXDBRows   = 100
	evOpenXDBXID    = 101
)

const binlogDumpPollInterval = 200 * time.Millisecond

// handleRegisterSlave COM_REGISTER_SLAVE：解析复制客户端登记负载并返回 OK。
// OpenXDB 无真实从库注册表，仅做协议握手兼容（校验负载边界）。
func (c *mysqlConn) handleRegisterSlave(p []byte) error {
	if _, err := parseRegisterSlave(p); err != nil {
		return c.pc.writePacket(errPacket(err))
	}
	return c.pc.writePacket(okPacket(0, 0, c.status()))
}

// parseRegisterSlave 解析 COM_REGISTER_SLAVE 负载：
// server-id u32 | hostname_len u8 + hostname | user_len u8 + user |
// password_len u8 + password | port u16 | rank u32 | master-id u32。
func parseRegisterSlave(p []byte) (uint32, error) {
	if len(p) < 4 {
		return 0, fmt.Errorf("OpenXDB: malformed COM_REGISTER_SLAVE")
	}
	serverID := binary.LittleEndian.Uint32(p[0:4])
	off := 4
	// hostname / user / password：1 字节长度 + 内容。
	for i := 0; i < 3; i++ {
		if off >= len(p) {
			return 0, fmt.Errorf("OpenXDB: malformed COM_REGISTER_SLAVE")
		}
		n := int(p[off])
		off++
		if off+n > len(p) {
			return 0, fmt.Errorf("OpenXDB: malformed COM_REGISTER_SLAVE")
		}
		off += n
	}
	// 尾部 port/rank/master-id 不足时容忍（部分客户端裁短负载）。
	return serverID, nil
}

// handleBinlogDump COM_BINLOG_DUMP：从 position（LSN）起流式推送 binlog 事件，
// 读完文件尾后轮询等待新事件，直到连接关闭。
func (c *mysqlConn) handleBinlogDump(p []byte) error {
	pos, flags, serverID, filename, err := parseBinlogDump(p)
	if err != nil {
		return c.pc.writePacket(errPacket(err))
	}
	_ = flags
	_ = serverID
	_ = filename

	binlogPath, tables, err := c.engine.BinlogWireInfo()
	if err != nil {
		return c.pc.writePacket(errPacket(fmt.Errorf("OpenXDB: binlog: %v", err)))
	}
	if binlogPath == "" {
		return c.pc.writePacket(errPacket(fmt.Errorf("OpenXDB: binlog not enabled")))
	}
	sub, err := cdc.NewSubscriber(binlogPath)
	if err != nil {
		return c.pc.writePacket(errPacket(fmt.Errorf("OpenXDB: open binlog: %v", err)))
	}
	defer sub.Close()

	byID := make(map[uint64]sql.TableMeta, len(tables))
	for _, t := range tables {
		byID[t.ID] = t
	}

	var from storage.LSN
	if pos > 0 {
		from = storage.LSN(pos)
	}
	// 当前文件尾位点（格式事件 last_lsn 用）：Fetch(0,0) 一次读完，next = 尾 + 1。
	_, tailNext, err := sub.Fetch(0, 0)
	if err != nil {
		return err
	}
	var lastLSN storage.LSN
	if tailNext > 0 {
		lastLSN = tailNext - 1
	}

	if err := c.pc.writePacket(binlogEvent(evOpenXDBFormat, from, encodeFormatEvent("openxdb-binlog", from, lastLSN))); err != nil {
		return err
	}

	// 主循环：增量拉取 + 事件编码推送；文件尾后轮询等待新事件。
	var curLSN storage.LSN = ^storage.LSN(0)
	var curCommit storage.LSN
	for {
		evs, next, err := sub.Fetch(from, 0)
		if err != nil {
			return err
		}
		for _, ev := range evs {
			if curLSN != ^storage.LSN(0) && ev.LSN != curLSN {
				// 进入新 binlog 记录：先为上一记录发送 XID 提交边界。
				if err := c.pc.writePacket(binlogEvent(evOpenXDBXID, curLSN, encodeXIDEvent(curCommit))); err != nil {
					return err
				}
			}
			curLSN, curCommit = ev.LSN, ev.CommitLSN
			body := encodeRowsEvent(ev, tableName(byID, ev))
			if err := c.pc.writePacket(binlogEvent(evOpenXDBRows, ev.LSN, body)); err != nil {
				return err
			}
		}
		if curLSN != ^storage.LSN(0) {
			if err := c.pc.writePacket(binlogEvent(evOpenXDBXID, curLSN, encodeXIDEvent(curCommit))); err != nil {
				return err
			}
			curLSN = ^storage.LSN(0)
		}
		from = next
		if len(evs) == 0 {
			// 文件尾：探活连接后轮询等待新事件。
			// 客户端断开时服务端收不到读错误（本循环不主动读包），
			// 需用短读超时探测：EOF/重置视为客户端关闭，退出释放订阅句柄。
			if nc, ok := c.pc.rw.(net.Conn); ok {
				_ = nc.SetReadDeadline(time.Now().Add(binlogDumpPollInterval / 2))
				var one [1]byte
				if _, err := nc.Read(one[:]); err != nil {
					if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
						return nil
					}
				}
			}
			time.Sleep(binlogDumpPollInterval)
		}
	}
}

// tableName 按表 ID 还原表名；未知表 ID 显示 #table-<id>，索引键事件显示 "(index)"。
func tableName(byID map[uint64]sql.TableMeta, ev *cdc.Event) string {
	if ev.IsIndex {
		return "(index)"
	}
	if t, ok := byID[ev.TableID]; ok {
		return t.Name
	}
	return fmt.Sprintf("#table-%d", ev.TableID)
}

// binlogEvent 组装一个 MySQL 外壳 binlog 事件包（header 19B + body）。
// log_pos 取该事件 LSN，供订阅方位点续传。
func binlogEvent(typ byte, logPos storage.LSN, body []byte) []byte {
	ev := make([]byte, 19+len(body))
	// timestamp(4) 置 0；type(1)；server_id(4)=1。
	ev[4] = typ
	ev[5] = 1
	// event_size(4)
	binary.LittleEndian.PutUint32(ev[9:13], uint32(19+len(body)))
	// log_pos(4)
	binary.LittleEndian.PutUint32(ev[13:17], uint32(logPos))
	// flags(2) 置 0
	copy(ev[19:], body)
	return ev
}

// encodeFormatEvent OPENXDB_FORMAT_EVENT 事件体。
func encodeFormatEvent(filename string, start, last storage.LSN) []byte {
	body := []byte("OPENXDB")
	body = append(body, filename...)
	body = append(body, 0)
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], uint64(start))
	binary.LittleEndian.PutUint64(b[8:16], uint64(last))
	return append(body, b[:]...)
}

// encodeRowsEvent OPENXDB_ROWS_EVENT 事件体。
func encodeRowsEvent(ev *cdc.Event, table string) []byte {
	body := make([]byte, 0, 64)
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(ev.LSN))
	body = append(body, b[:]...)
	binary.LittleEndian.PutUint64(b[:], uint64(ev.CommitLSN))
	body = append(body, b[:]...)
	binary.LittleEndian.PutUint64(b[:], ev.SchemaVer)
	body = append(body, b[:]...)
	if ev.Kind == cdc.KindDelete {
		body = append(body, 2)
	} else {
		body = append(body, 1)
	}
	binary.LittleEndian.PutUint64(b[:], ev.TableID)
	body = append(body, b[:]...)
	if ev.IsIndex {
		body = append(body, 1)
	} else {
		body = append(body, 0)
	}
	body = appendLenencBytes(body, []byte(table))
	body = appendLenencBytes(body, ev.PK)
	body = appendLenencBytes(body, ev.RawValue)
	return body
}

// encodeXIDEvent OPENXDB_XID_EVENT 事件体（提交边界）。
func encodeXIDEvent(commitLSN storage.LSN) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(commitLSN))
	return b[:]
}

// parseBinlogDump 解析 COM_BINLOG_DUMP 负载：
// position u32 | flags u16 | server-id u32 | binlog-filename NUL 字符串。
func parseBinlogDump(p []byte) (pos uint32, flags uint16, serverID uint32, filename string, err error) {
	if len(p) < 10 {
		err = fmt.Errorf("OpenXDB: malformed COM_BINLOG_DUMP")
		return
	}
	pos = binary.LittleEndian.Uint32(p[0:4])
	flags = binary.LittleEndian.Uint16(p[4:6])
	serverID = binary.LittleEndian.Uint32(p[6:10])
	if len(p) > 10 {
		filename = strings.TrimRight(string(p[10:]), "\x00")
	}
	return
}
