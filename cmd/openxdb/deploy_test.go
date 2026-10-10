// 部署脚本语法级验证测试（T20 运维工具链）。
//
// 端到端冒烟（init + start + doctor + 停服）在验收环节用真实二进制执行，
// 见 docs/T20_ops_toolchain.md「测试清单」；本文件只做脚本语法级验证：
// Windows PowerShell 解析通过、bash -n 语法通过（系统存在 bash 时）。
package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repoScriptsDir(t *testing.T) string {
	t.Helper()
	// 测试运行时工作目录为 cmd/openxdb，仓库根在 ../..
	dir, err := filepath.Abs(filepath.Join("..", "..", "scripts"))
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "deploy.ps1")); err != nil {
		t.Fatalf("deploy.ps1 not found: %v", err)
	}
	return dir
}

// TestDeployPowerShellParse：deploy.ps1 可被 PowerShell 解析（语法级）。
func TestDeployPowerShellParse(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("PowerShell 校验仅在 Windows 上执行")
	}
	dir := repoScriptsDir(t)
	script := filepath.Join(dir, "deploy.ps1")
	// [scriptblock]::Create 仅做编译解析，不执行脚本体
	cmd := exec.Command("powershell", "-NoProfile", "-NonInteractive", "-Command",
		"$null = [scriptblock]::Create((Get-Content -Raw '"+script+"')); if ($?) { 'PARSE_OK' } else { exit 1 }")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("deploy.ps1 语法解析失败: %v\n%s", err, string(out))
	}
	if string(out) == "" {
		t.Fatalf("deploy.ps1 解析无输出，视为失败")
	}
}

// TestDeployBashSyntax：deploy.sh 通过 bash -n 语法检查（找到真实 bash 时）。
func TestDeployBashSyntax(t *testing.T) {
	dir := repoScriptsDir(t)
	bash := findRealBash()
	if bash == "" {
		t.Skip("未找到可用的真实 bash（Git bash / MSYS bash），跳过 bash -n 校验")
	}
	cmd := exec.Command(bash, "-n", filepath.Join(dir, "deploy.sh"))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("deploy.sh 语法检查失败: %v\n%s", err, string(out))
	}
}

// findRealBash 查找真实 bash：优先 Git/MSYS 安装，排除 WSL stub（System32\bash.exe）。
// 找到候选后运行 bash --version 验证输出不含 WSL/适用于 Linux 特征且命令成功。
func findRealBash() string {
	preferred := []string{
		`D:\Program Files\Git\bin\bash.exe`,
		`C:\Program Files\Git\bin\bash.exe`,
		`C:\Program Files (x86)\Git\bin\bash.exe`,
		`C:\Program Files\Git\usr\bin\bash.exe`,
		`C:\msys64\usr\bin\bash.exe`,
	}
	for _, p := range preferred {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	p, err := exec.LookPath("bash")
	if err != nil {
		return ""
	}
	lower := strings.ToLower(p)
	if strings.Contains(lower, `system32\bash.exe`) || strings.Contains(lower, `windows\system32`) {
		return ""
	}
	out, err := exec.Command(p, "--version").Output()
	if err != nil {
		return ""
	}
	s := strings.ToLower(string(out))
	if strings.Contains(s, "wsl") || strings.Contains(s, "microsoft") || strings.Contains(s, "适用于 linux") {
		return ""
	}
	return p
}

// TestDeployScriptsExist：两份部署脚本均存在且非空。
func TestDeployScriptsExist(t *testing.T) {
	dir := repoScriptsDir(t)
	for _, name := range []string{"deploy.ps1", "deploy.sh"} {
		fi, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("%s missing: %v", name, err)
		}
		if fi.Size() == 0 {
			t.Fatalf("%s is empty", name)
		}
	}
}
