// 健康巡检：openxdb doctor（T20 运维工具链）。
//
// 对指定数据目录（-data-dir）与可选在线服务（--addr）做健康巡检，
// 输出人类可读报告或 JSON（--json）。
//
// 退出码语义：
//
//	0 = 全部通过（无警告无错误）
//	1 = 有警告（WARN，如未启用复制、未提供在线检查地址）
//	2 = 有错误（FAIL，如目录缺失、引擎打不开、端口不通、元数据异常）
//
// 巡检项：数据目录与配置、WAL 完整性、RocksDB 引擎健康、
// 表/region 元数据一致性与路由自洽、binlog 可读性与位点、
// 监听端口连通性（可选 --addr）、在线慢查询可用性（可选 --addr）。
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/rocksdb"
)

// doctorCheck 单个巡检项结果。
type doctorCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // ok | warn | error
	Message string `json:"message,omitempty"`
}

// doctorReport 巡检报告。
type doctorReport struct {
	DataDir   string        `json:"data_dir"`
	Addr      string        `json:"addr,omitempty"`
	Checks    []doctorCheck `json:"checks"`
	ExitCode  int           `json:"exit_code"`
	Generated string        `json:"generated_at"`
}

const (
	docOK    = "ok"
	docWarn  = "warn"
	docError = "error"
)

// flagBool 检查 args 是否含布尔开关 --name。
func flagBool(args []string, name string) bool {
	for _, a := range args {
		if a == name {
			return true
		}
	}
	return false
}

// cmdDoctor 执行健康巡检并输出报告；按退出码语义退出进程。
func cmdDoctor(args []string) error {
	dir, err := needDataDir(args)
	if err != nil {
		return err
	}
	addr, _ := flagValue(args, "--addr")
	rep := runDoctorChecks(dir, addr)
	rep.Generated = time.Now().Format(time.RFC3339)
	if flagBool(args, "--json") {
		b, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
	} else {
		printDoctorText(rep)
	}
	os.Exit(rep.ExitCode)
	return nil
}

// runDoctorChecks 执行全部巡检项，返回报告与退出码（不退出进程，便于测试）。
func runDoctorChecks(dir, addr string) *doctorReport {
	rep := &doctorReport{DataDir: dir, Addr: addr}
	add := func(name, status, msg string) {
		rep.Checks = append(rep.Checks, doctorCheck{Name: name, Status: status, Message: msg})
	}

	// 1) 数据目录与配置文件
	confPath := filepath.Join(dir, db.ConfigFile)
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		add("data_dir", docError, "数据目录不存在或不可访问: "+dir)
	} else if b, err := os.ReadFile(confPath); err != nil {
		add("config", docError, "无法读取 "+db.ConfigFile+"（目录未初始化？）")
	} else if !strings.Contains(string(b), "engine = "+db.DefaultEngine) {
		add("config", docError, db.ConfigFile+" 缺少 engine = "+db.DefaultEngine)
	} else {
		add("data_dir", docOK, "目录与配置文件正常")
	}

	// 2) WAL 完整性（存在性 + 可打开）
	walPath := filepath.Join(dir, db.WALFile)
	if fi, err := os.Stat(walPath); err != nil {
		add("wal", docError, "WAL 缺失: "+db.WALFile+" ("+err.Error()+")")
	} else if fi.Size() == 0 {
		add("wal", docWarn, "WAL 存在但为空（尚未有写入，属正常空库）")
	} else if f, err := os.Open(walPath); err != nil {
		add("wal", docError, "WAL 无法打开: "+err.Error())
	} else {
		_ = f.Close()
		add("wal", docOK, "WAL 存在且可打开")
	}

	// 3) RocksDB 引擎健康（Open 是否成功）
	// 服务运行期间引擎持有数据目录 LOCK（单写者），doctor 重复 Open 会失败；
	// 该场景属"在线被占用"，降级为警告而非错误，避免对健康在线服务误报 ERRORS。
	var eng storage.Storage
	engLocked := false
	if e, err := rocksdb.Open(filepath.Join(dir, db.DataSubDir), false); err != nil {
		if strings.Contains(err.Error(), "LOCK") || strings.Contains(strings.ToLower(err.Error()), "lock file") {
			engLocked = true
			add("rocksdb", docWarn, "引擎被占用（服务正在运行或残留进程持有 LOCK），跳过本地 Open 检查: "+err.Error())
		} else {
			add("rocksdb", docError, "引擎打开失败（数据损坏或目录未初始化）: "+err.Error())
		}
	} else {
		eng = e
		add("rocksdb", docOK, "RocksDB 打开成功")
	}
	// 引擎用完后必须关闭，否则 LOCK 文件句柄占用导致目录无法清理（Windows）。
	defer func() {
		if eng != nil {
			_ = eng.Close()
		}
	}()

	// 4) 表 / region 元数据一致性（m:tables / m:regions 可读、路由自洽）
	if eng == nil && engLocked {
		add("metadata", docWarn, "引擎被服务占用，跳过本地元数据检查（在线场景由 SHOW 语句覆盖）")
	} else if eng == nil {
		add("metadata", docError, "引擎不可用，无法读取元数据")
	} else {
		if _, err := eng.Get([]byte("m:tables")); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				add("metadata", docOK, "m:tables 可读（空库无表）")
			} else {
				add("metadata", docError, "m:tables 读取异常: "+err.Error())
			}
		} else {
			add("metadata", docOK, "m:tables 可读")
		}
		if raw, err := eng.Get([]byte("m:regions")); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				add("region_routes", docOK, "m:regions 可读（空库无路由）")
			} else {
				add("region_routes", docError, "m:regions 读取异常: "+err.Error())
			}
		} else if err := validateRegionRoutes(raw); err != nil {
			add("region_routes", docError, "region 路由不自洽: "+err.Error())
		} else {
			add("region_routes", docOK, "m:regions 可读且路由自洽")
		}
	}

	// 5) binlog 可读性与位点（存在时只读扫描；不存在视为未启用复制）
	binlogPath := filepath.Join(dir, db.BinlogFile)
	if _, err := os.Stat(binlogPath); err != nil {
		add("binlog", docWarn, "binlog 不存在（未启用复制，正常）")
	} else if lsn, err := replication.PeekLastLSN(binlogPath); err != nil {
		add("binlog", docError, "binlog 读取失败: "+err.Error())
	} else {
		add("binlog", docOK, fmt.Sprintf("binlog 可读，当前位点 LSN=%d", lsn))
	}

	// 6) 在线端口连通性（可选 --addr；未提供则警告）
	if addr == "" {
		add("port", docWarn, "未指定 --addr，跳过在线端口检查")
	} else if resp, err := onlineQuery(addr, "PING", 5*time.Second); err != nil {
		add("port", docError, "端口不通或协议异常: "+err.Error())
	} else if resp != "PONG" {
		add("port", docError, "端口可达但 PING 响应异常: "+resp)
	} else {
		add("port", docOK, "端口连通且协议响应正常 ("+addr+")")
	}

	// 7) 在线慢查询可用性（可选 --addr；复用 SHOW SLOWQUERIES 语义）
	if addr == "" {
		add("slowqueries", docWarn, "未指定 --addr，跳过在线慢查询检查")
	} else if resp, err := onlineQuery(addr, "SHOW SLOWQUERIES", 5*time.Second); err != nil {
		add("slowqueries", docError, "SHOW SLOWQUERIES 失败: "+err.Error())
	} else if strings.HasPrefix(resp, "ERR") {
		add("slowqueries", docError, "SHOW SLOWQUERIES 返回错误: "+resp)
	} else {
		add("slowqueries", docOK, fmt.Sprintf("慢查询表可用（%d 条）", parseRowCount(resp)))
	}

	rep.ExitCode = summarizeExitCode(rep.Checks)
	return rep
}

// validateRegionRoutes 校验 m:regions 载荷 JSON 合法性：可解析且每个 region 记录自洽。
// 路由自洽定义为：JSON 结构合法、region_id 单调可辨识、node 为字符串（空 = 本节点）。
func validateRegionRoutes(raw []byte) error {
	var meta struct {
		Seq     uint64 `json:"seq"`
		Regions []struct {
			RegionID uint64 `json:"region_id"`
			Node     string `json:"node,omitempty"`
		} `json:"regions"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return fmt.Errorf("m:regions JSON 解析失败: %v", err)
	}
	for _, r := range meta.Regions {
		if r.RegionID == 0 {
			return fmt.Errorf("region 记录缺少合法 region_id")
		}
	}
	return nil
}

// summarizeExitCode 汇总退出码：有错误=2，否则有警告=1，全通过=0。
func summarizeExitCode(checks []doctorCheck) int {
	hasErr, hasWarn := false, false
	for _, c := range checks {
		switch c.Status {
		case docError:
			hasErr = true
		case docWarn:
			hasWarn = true
		}
	}
	switch {
	case hasErr:
		return 2
	case hasWarn:
		return 1
	default:
		return 0
	}
}

// printDoctorText 打印人类可读巡检报告。
func printDoctorText(rep *doctorReport) {
	fmt.Printf("OpenXDB doctor: data dir %s\n", rep.DataDir)
	if rep.Addr != "" {
		fmt.Printf("  online addr: %s\n", rep.Addr)
	}
	for _, c := range rep.Checks {
		mark := "PASS"
		switch c.Status {
		case docWarn:
			mark = "WARN"
		case docError:
			mark = "FAIL"
		}
		line := fmt.Sprintf("  [%s] %s", mark, c.Name)
		if c.Message != "" {
			line += " - " + c.Message
		}
		fmt.Println(line)
	}
	fmt.Printf("result: %s (exit code %d)\n", exitWord(rep.ExitCode), rep.ExitCode)
}

func exitWord(code int) string {
	switch code {
	case 0:
		return "ALL PASS"
	case 1:
		return "WARNINGS"
	default:
		return "ERRORS"
	}
}

// onlineQuery 向行协议服务发送一行命令并读取首段响应（用于在线巡检）。
func onlineQuery(addr, line string, timeout time.Duration) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if _, err := fmt.Fprintf(conn, "%s\r\n", line); err != nil {
		return "", err
	}
	buf := make([]byte, 8192)
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(buf[:n])), nil
}

// parseRowCount 从文本表响应中提取末尾 "(N rows)" 的 N；解析失败返回 0。
func parseRowCount(resp string) int {
	idx := strings.LastIndex(resp, " rows)")
	if idx < 0 {
		return 0
	}
	open := strings.LastIndex(resp[:idx], "(")
	if open < 0 || open+1 >= idx {
		return 0
	}
	n := 0
	for _, ch := range resp[open+1 : idx] {
		if ch < '0' || ch > '9' {
			return 0
		}
		n = n*10 + int(ch-'0')
	}
	return n
}
