<#
# rolling_upgrade.ps1 - OpenXDB 滚动升级辅助脚本（M16-3 灰度升级辅助）
#
# 编排单节点滚动升级流程（多节点集群请对各节点依次执行本脚本）：
#   1) 升级前健康检查（openxdb preflight，全 PASS 才继续）
#   2) 优雅停机（通过 TCP 发送 SHUTDOWN GRACEFUL，复用 M16-2 能力；
#      可选 -SkipShutdown 跳过，便于停机已由外部完成）
#   3) 启动新版二进制（openxdb start --data-dir --port）
#   4) 升级后复检（等待就绪 + preflight + doctor --addr 在线检查）
#
# 回滚说明：升级前建议备份数据目录（-BackupDir 可选）；回滚 = 停止新版 +
# 恢复备份 + 旧版二进制 reopen（数据格式版本兼容矩阵见 docs/T29_m16_upgrade.md）。
#
# 用法：
#   .\rolling_upgrade.ps1 -DataDir <dir> -NewBin <path>
#       [-OldBin <path>] [-Port <n>] [-Addr <host>] [-TargetVersion <ver>]
#       [-BackupDir <dir>] [-SkipShutdown] [-NoStart] [-Help]
#
# 示例：
#   .\rolling_upgrade.ps1 -DataDir D:\openxdb-data -NewBin D:\openxdb-v0.20.0\openxdb.exe -TargetVersion v0.20.0-alpha
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$DataDir,

    [Parameter(Mandatory = $true)]
    [string]$NewBin,

    [string]$OldBin = "",

    [string]$Addr = "127.0.0.1",

    [int]$Port = 7788,

    [string]$TargetVersion = "",

    [string]$BackupDir = "",

    [switch]$SkipShutdown,
    [switch]$NoStart,
    [switch]$Help
)

$ErrorActionPreference = 'Stop'

function Show-Help {
    Write-Host @"
OpenXDB 滚动升级辅助脚本 rolling_upgrade.ps1（M16-3）

用法:
  .\rolling_upgrade.ps1 -DataDir <dir> -NewBin <path>
              [-OldBin <path>] [-Addr <host>] [-Port <n>]
              [-TargetVersion <ver>] [-BackupDir <dir>]
              [-SkipShutdown] [-NoStart]

流程:
  1) 升级前健康检查: openxdb preflight --data-dir（全 PASS 才继续）
  2) 优雅停机:       通过 TCP 发送 SHUTDOWN GRACEFUL（M16-2）
  3) 启动新版:       $NewBin start --data-dir --port
  4) 升级后复检:     preflight + doctor --addr 在线检查

参数:
  -DataDir       数据目录（必填）
  -NewBin        新版 openxdb 可执行文件路径（必填）
  -OldBin        旧版可执行文件路径（preflight 优先用旧版检查旧数据，默认 NewBin）
  -Addr/-Port    服务监听地址与端口（默认 127.0.0.1:7788）
  -TargetVersion 目标版本号（透传给 preflight 做版本兼容提示）
  -BackupDir     升级前备份目录（可选；回滚=备份+旧版 reopen）
  -SkipShutdown  跳过优雅停机步骤（停机已由外部完成时使用）
  -NoStart       检查通过后不启动新版（仅检查模式）

退出码: 0=成功 1=检查失败 2=停机失败 3=启动失败
"@
}

if ($Help) {
    Show-Help
    exit 0
}

# Send-OpenXDBCommand 通过 TCP 向服务发送一行命令并读取响应（文本协议）。
function Send-OpenXDBCommand {
    param(
        [string]$Host,
        [int]$Port,
        [string]$Line
    )
    $client = New-Object System.Net.Sockets.TcpClient
    $client.Connect($Host, $Port)
    try {
        $stream = $client.GetStream()
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Line + "`n")
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush()
        $buffer = New-Object byte[] 8192
        $ms = New-Object System.IO.MemoryStream
        do {
            Start-Sleep -Milliseconds 50
            $n = $stream.Read($buffer, 0, $buffer.Length)
            if ($n -eq 0) { break }
            $ms.Write($buffer, 0, $n)
        } while ($stream.DataAvailable)
        return [System.Text.Encoding]::UTF8.GetString($ms.ToArray())
    } finally {
        $client.Close()
    }
}

if (-not $DataDir -or -not $NewBin) {
    Show-Help
    Write-Error "缺少必填参数 -DataDir <dir> 与 -NewBin <path>"
    exit 1
}

$checkExe = if ($OldBin) { $OldBin } else { $NewBin }

# ---------- 步骤 0：可选备份 ----------
if ($BackupDir) {
    Write-Host "[upgrade] 备份数据目录 $DataDir -> $BackupDir"
    New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null
    Copy-Item -Path (Join-Path $DataDir '*') -Destination $BackupDir -Recurse -Force
    Write-Host "[upgrade] 备份完成（回滚 = 停止新版 + 恢复备份 + 旧版 reopen）"
}

# ---------- 步骤 1：升级前健康检查 ----------
Write-Host "[upgrade] 步骤 1/4: 升级前健康检查 (preflight)"
$pfArgs = @("preflight", "--data-dir", $DataDir)
if ($TargetVersion) { $pfArgs += @("--target-version", $TargetVersion) }
& $checkExe @pfArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "[upgrade] preflight 未全部通过（exit $LASTEXITCODE），中止升级"
    exit 1
}
Write-Host "[upgrade] preflight PASS，可继续升级"

# ---------- 步骤 2：优雅停机 ----------
if (-not $SkipShutdown) {
    Write-Host "[upgrade] 步骤 2/4: 优雅停机 (SHUTDOWN GRACEFUL @ $Addr`:$Port)"
    try {
        $resp = Send-OpenXDBCommand -Host $Addr -Port $Port -Line "SHUTDOWN GRACEFUL"
        Write-Host "[upgrade] 停机响应: $($resp.Trim())"
        if ($resp -notmatch 'OK') {
            Write-Error "[upgrade] 停机未确认（$($resp.Trim())）；请人工执行 SHUTDOWN GRACEFUL 或 Ctrl-C 后重试"
            exit 2
        }
        # 等待端口关闭（停机流程完成、存储已 flush/关闭）
        $closed = $false
        for ($i = 0; $i -lt 40; $i++) {
            Start-Sleep -Milliseconds 500
            try {
                $c = New-Object System.Net.Sockets.TcpClient
                $c.Connect($Addr, $Port)
                $c.Close()
            } catch {
                $closed = $true
                break
            }
        }
        if (-not $closed) {
            Write-Error "[upgrade] 停机后端口仍开放，存储可能未关闭，中止"
            exit 2
        }
        Write-Host "[upgrade] 优雅停机完成，存储已关闭"
    } catch {
        Write-Error "[upgrade] 无法连接服务 $Addr`:$Port 发送 SHUTDOWN：$($_.Exception.Message)"
        Write-Host "[upgrade] 若服务已停止，可加 -SkipShutdown 跳过本步骤"
        exit 2
    }
} else {
    Write-Host "[upgrade] 步骤 2/4: 跳过优雅停机（-SkipShutdown）"
}

if ($NoStart) {
    Write-Host "[upgrade] -NoStart 指定，检查完成不启动新版；可手动执行:"
    Write-Host "        $NewBin start --data-dir $DataDir --port $Port"
    exit 0
}

# ---------- 步骤 3：启动新版 ----------
Write-Host "[upgrade] 步骤 3/4: 启动新版 $NewBin"
$stdout = Join-Path $DataDir "rolling_upgrade.stdout.log"
$stderr = Join-Path $DataDir "rolling_upgrade.stderr.log"
$argsList = @("start", "--data-dir", $DataDir, "--port", "$Port")
$proc = Start-Process -FilePath $NewBin -ArgumentList $argsList `
    -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
if (-not $proc) {
    Write-Error "[upgrade] 启动新版失败"
    exit 3
}
Write-Host "[upgrade] 新版进程 PID: $($proc.Id)"

# ---------- 步骤 4：升级后复检（等待就绪 + preflight + doctor） ----------
Write-Host "[upgrade] 步骤 4/4: 升级后复检"
$ready = $false
for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep -Milliseconds 500
    try {
        $resp = Send-OpenXDBCommand -Host $Addr -Port $Port -Line "PING"
        if ($resp -match 'PONG') { $ready = $true; break }
    } catch { }
}
if (-not $ready) {
    Write-Error "[upgrade] 新版未就绪（查看 $stderr）"
    exit 3
}
Write-Host "[upgrade] 新版已就绪（PING 通过）"

& $NewBin preflight --data-dir $DataDir
if ($LASTEXITCODE -ne 0) {
    Write-Error "[upgrade] 升级后 preflight 未通过（exit $LASTEXITCODE）"
    exit 1
}
& $NewBin doctor --data-dir $DataDir --addr "$Addr`:$Port"
if ($LASTEXITCODE -eq 2) {
    Write-Error "[upgrade] 升级后 doctor 存在 FAIL（exit 2），请人工排查"
    exit 1
}
Write-Host "[upgrade] 滚动升级完成: 旧版已停机 -> 新版已启动并复检通过（PID $($proc.Id)）"
exit 0
