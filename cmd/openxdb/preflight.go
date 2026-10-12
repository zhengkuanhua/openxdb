// M16-3 灰度升级辅助：openxdb preflight 升级前健康检查。
//
// 升级前置检查（PREFLIGHT CHECK）：对指定数据目录做只读健康检查，
// 全部通过才允许进入滚动升级流程。检查项：
//   1) data_dir          数据目录与 openxdb.conf 存在且 engine 匹配；
//   2) persisted_config  持久化配置（persisted.json，M16-1）JSON 可解析；
//   3) open_storage      数据目录可完整打开（复用 db.Open：RocksDB + WAL +
//                        txn.Recover + LoadPersistedConfig，覆盖存储一致性，
//                        不重复造轮子）；
//   4) version_compat    数据文件格式版本兼容（数据目录格式 <= 当前二进制
//                        格式；--target-version 输出目标版本提示）。
//
// 退出码：0=PASS（全部通过）；2=FAIL（任一检查失败）；1=WARN（预留）。
// 输出人类可读清单；--json 输出 JSON 报告。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
)

const (
	pfPass = "PASS"
	pfWarn = "WARN"
	pfFail = "FAIL"
)

// preflightCheck 单个前置检查项结果。
type preflightCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// preflightReport 升级前健康检查报告。
type preflightReport struct {
	DataDir           string           `json:"data_dir"`
	BinaryVersion     string           `json:"binary_version"`
	DataFormatVersion int              `json:"data_format_version"` // 数据目录格式版本（读取失败为 0）
	TargetVersion     string           `json:"target_version,omitempty"`
	Checks            []preflightCheck `json:"checks"`
	ExitCode          int              `json:"exit_code"`
	Generated         string           `json:"generated_at"`
}

// cmdPreflight 执行升级前健康检查并输出报告；按退出码语义退出进程。
func cmdPreflight(args []string) error {
	dir, err := needDataDir(args)
	if err != nil {
		return err
	}
	target, _ := flagValue(args, "--target-version")
	rep := runPreflightChecks(dir, target)
	rep.Generated = time.Now().Format(time.RFC3339)
	if flagBool(args, "--json") {
		b, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
	} else {
		printPreflightText(rep)
	}
	os.Exit(rep.ExitCode)
	return nil
}

// runPreflightChecks 执行全部前置检查项，返回报告与退出码（不退出进程，便于测试）。
func runPreflightChecks(dir, target string) *preflightReport {
	rep := &preflightReport{DataDir: dir, BinaryVersion: version, TargetVersion: target}
	add := func(name, status, msg string) {
		rep.Checks = append(rep.Checks, preflightCheck{Name: name, Status: status, Message: msg})
	}

	// 1) 数据目录与配置文件（存在性 / 可读 / engine 匹配）。
	confPath := filepath.Join(dir, db.ConfigFile)
	confOK := false
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		add("data_dir", pfFail, "数据目录不存在或不可访问: "+dir)
	} else if b, err := os.ReadFile(confPath); err != nil {
		add("data_dir", pfFail, "无法读取 "+db.ConfigFile+"（目录未初始化？）: "+err.Error())
	} else if !strings.Contains(string(b), "engine = "+db.DefaultEngine) {
		add("data_dir", pfFail, db.ConfigFile+" 缺少 engine = "+db.DefaultEngine)
	} else {
		confOK = true
		add("data_dir", pfPass, "数据目录与配置文件存在，engine = "+db.DefaultEngine)
	}

	// 2) 持久化配置（persisted.json）JSON 可解析；键/值合法性由 3) 中
	//    db.Open 的 LoadPersistedConfig（M16-1）统一校验，不重复造轮子。
	persistPath := filepath.Join(dir, db.PersistedConfigFile)
	if _, err := os.Stat(persistPath); err != nil {
		if os.IsNotExist(err) {
			add("persisted_config", pfPass, "无持久化配置记录（persisted.json 不存在），按默认配置即可启动")
		} else {
			add("persisted_config", pfFail, "persisted.json 不可访问: "+err.Error())
		}
	} else if b, err := os.ReadFile(persistPath); err != nil {
		add("persisted_config", pfFail, "persisted.json 读取失败: "+err.Error())
	} else {
		var m map[string]string
		if err := json.Unmarshal(b, &m); err != nil {
			add("persisted_config", pfFail, "persisted.json JSON 解析失败（M16-1 持久化配置损坏）: "+err.Error())
		} else {
			add("persisted_config", pfPass, fmt.Sprintf("persisted.json 可解析（%d 项），键/值合法性由 db.Open 校验", len(m)))
		}
	}

	// 3) 数据目录可完整打开：db.Open 试开（RocksDB 打开 + WAL 打开 +
	//    txn.Recover 回放 + LoadPersistedConfig 合并）。Recover 即 WAL/存储
	//    一致性校验，Open 成功即 reopen 验证通过。
	if confOK {
		d, err := db.Open(dir)
		if err != nil {
			add("open_storage", pfFail, "数据目录无法打开: "+err.Error())
		} else {
			_ = d.Close()
			add("open_storage", pfPass, "数据目录可打开：存储/WAL/事务恢复/持久化配置全部通过（reopen 验证）")
		}
	} else {
		add("open_storage", pfFail, "跳过：数据目录配置无效，无法试开")
	}

	// 4) 数据格式版本兼容 + 目标版本提示（升级判定依据）。
	dirFormat, err := db.ReadDataFormatVersion(dir)
	if err != nil {
		add("version_compat", pfFail, err.Error())
	} else {
		rep.DataFormatVersion = dirFormat
		if err := db.CheckDataFormatCompat(dir, db.DataFormatVersion); err != nil {
			add("version_compat", pfFail, err.Error())
		} else {
			msg := fmt.Sprintf("数据格式 v%d <= 当前二进制 v%d，兼容（升级路径：旧数据可被当前版本打开）",
				dirFormat, db.DataFormatVersion)
			if target != "" {
				msg += fmt.Sprintf("；目标版本 %s：升级前请确认目标二进制数据格式版本 >= %d（兼容矩阵见 docs/T29_m16_upgrade.md）",
					target, dirFormat)
			}
			add("version_compat", pfPass, msg)
		}
	}

	// 汇总：任一 FAIL → 退出码 2；否则 0。
	failed := false
	for _, c := range rep.Checks {
		if c.Status == pfFail {
			failed = true
		}
	}
	if failed {
		rep.ExitCode = 2
	} else {
		rep.ExitCode = 0
	}
	return rep
}

// printPreflightText 输出人类可读前置检查报告。
func printPreflightText(rep *preflightReport) {
	fmt.Println("OpenXDB upgrade preflight (M16-3)")
	fmt.Printf("  data dir      : %s\n", rep.DataDir)
	fmt.Printf("  binary version: %s (data format v%d)\n", rep.BinaryVersion, db.DataFormatVersion)
	if rep.TargetVersion != "" {
		fmt.Printf("  target version: %s\n", rep.TargetVersion)
	}
	fmt.Println("  checks:")
	for _, c := range rep.Checks {
		fmt.Printf("    [%s] %-16s %s\n", c.Status, c.Name, c.Message)
	}
	result := "PASS"
	if rep.ExitCode == 2 {
		result = "FAIL"
	} else if rep.ExitCode == 1 {
		result = "WARN"
	}
	fmt.Printf("  PREFLIGHT RESULT: %s\n", result)
}
