<#
# deploy.ps1 - OpenXDB 一键部署脚本（T20 运维工具链）
#
# 用法：
#   .\deploy.ps1 -DataDir <dir> [-Port <n>] [-Bin <path>] [-ReplicaPort <n>] [-ClusterAddr <addr>] [-NoStart] [-Help]
#
# 功能：
#   1) 初始化数据目录（openxdb init --data-dir）
#   2) 启动服务（openxdb start --data-dir --port，端口默认 7788）
#   3) 健康自检（openxdb doctor --data-dir --addr，等待服务就绪）
#   4) 输出部署摘要（PID / 端口 / 数据目录 / binlog 位点）
#
# 示例：
#   .\deploy.ps1 -DataDir D:\openxdb-data -Port 7788
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$DataDir,

    [int]$Port = 7788,

    [string]$Bin = "",

    [int]$ReplicaPort = 0,

    [string]$ClusterAddr = "",

    [switch]$NoStart,

    [switch]$Help
)

$ErrorActionPreference = 'Stop'

function Show-Help {
    Write-Host @"
OpenXDB 一键部署脚本 deploy.ps1（T20）

用法:
  .\deploy.ps1 -DataDir <dir> [-Port <n>] [-Bin <path>]
              [-ReplicaPort <n>] [-ClusterAddr <addr>] [-NoStart]

参数:
  -DataDir      数据目录（必填）
  -Port         服务监听端口，默认 7788
  -Bin          openxdb 可执行文件路径（默认 PATH 中的 openxdb）
  -ReplicaPort  主节点复制端口（可选，启用 M2 复制）
  -ClusterAddr  集群节点链路地址（可选，启用 M4 节点链路）
  -NoStart      仅初始化与检查，不启动服务

输出: 部署摘要（PID / 端口 / 数据目录 / binlog 位点）
"@
}

if ($Help) {
    Show-Help
    exit 0
}

if (-not $DataDir) {
    Write-Error "缺少必填参数 -DataDir <dir>"
    exit 1
}

$exe = if ($Bin) { $Bin } else { "openxdb" }

# ---------- 1) 初始化数据目录 ----------
Write-Host "[deploy] 初始化数据目录: $DataDir"
& $exe init --data-dir $DataDir
if ($LASTEXITCODE -ne 0) {
    Write-Error "[deploy] 初始化失败 (exit $LASTEXITCODE)"
    exit 1
}

if ($NoStart) {
    Write-Host "[deploy] -NoStart 指定，跳过启动；数据目录已就绪: $DataDir"
    exit 0
}

# ---------- 2) 启动服务 ----------
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
$stdout = Join-Path $DataDir "deploy.stdout.log"
$stderr = Join-Path $DataDir "deploy.stderr.log"
$argsList = @("start", "--data-dir", $DataDir, "--port", "$Port")
if ($ReplicaPort -gt 0) { $argsList += @("--replica-port", "$ReplicaPort") }
if ($ClusterAddr) { $argsList += @("--cluster-addr", $ClusterAddr) }

Write-Host "[deploy] 启动服务: $exe $($argsList -join ' ')"
$proc = Start-Process -FilePath $exe -ArgumentList $argsList `
    -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
if (-not $proc) {
    Write-Error "[deploy] 启动失败"
    exit 1
}
$serverPid = $proc.Id
Write-Host "[deploy] 服务进程 PID: $serverPid"

# ---------- 3) 健康自检（等待就绪 + doctor） ----------
$addr = "127.0.0.1:$Port"
$ready = $false
for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep -Milliseconds 500
    try {
        $resp = (New-Object System.Net.Sockets.TcpClient($addr.Split(':')[0], [int]$addr.Split(':')[1]))
        $ready = $true
        $resp.Close()
        break
    } catch {
        if ($proc.HasExited) { break }
    }
}
if (-not $ready) {
    Write-Error "[deploy] 服务未在 $Port 端口就绪（进程退出=$($proc.HasExited)）"
    exit 1
}
Write-Host "[deploy] 端口就绪: $addr"

& $exe doctor --data-dir $DataDir --addr $addr
if ($LASTEXITCODE -ge 2) {
    Write-Error "[deploy] 健康自检失败 (doctor exit $LASTEXITCODE)"
    exit 1
}
Write-Host "[deploy] 健康自检通过 (doctor exit $LASTEXITCODE)"

# ---------- 4) 部署摘要 ----------
Write-Host ""
Write-Host "=== OpenXDB 部署摘要 ==="
Write-Host ("PID:           {0}" -f $serverPid)
Write-Host ("端口:          {0}" -f $Port)
Write-Host ("数据目录:      {0}" -f $DataDir)
Write-Host ("stdout 日志:   {0}" -f $stdout)
Write-Host ("stderr 日志:   {0}" -f $stderr)
Write-Host ("状态:          运行中 (doctor 通过)")
exit 0
