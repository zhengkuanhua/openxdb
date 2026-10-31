# D7 MySQL 兼容性专项验证脚本（run_d7_mysql_compat_validate.ps1）
# 覆盖 5 项验收（DEV_PLAN_v1.17.0 Phase G）：
#   G1 握手版本串 "8.0.36-OpenXDB v1.17.0"
#   G3 select version() 返回 v1.17.0
#   G4 show create table t 输出可重建 schema
#   G5 show indexes from t 与 SHOW INDEX FROM t 等价（含大小写组合）
#   G2 交互提示符 oxsql> 生效
# 依赖：仓库根目录 bin/openxdb.exe、bin/oxsql.exe（先 go build）
# 说明：数据目录使用带时间戳的唯一路径，脚本不删除任何既有目录。
# 说明：oxsql 调用统一走 System.Diagnostics.Process 直接重定向 stdout/stdin，
#       规避 PowerShell 5.1 对原生程序 2>&1 的 CLIXML 包装与 cmd /c 引号歧义。
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$bin  = Join-Path $root "bin"
$stamp = Get-Date -Format "yyyyMMddHHmmss"
$work = Join-Path $env:TEMP "openxdb_d7_run_validate_$stamp"
$data = Join-Path $work "data"
New-Item -ItemType Directory -Force -Path $data | Out-Null

$openxdb = Join-Path $bin "openxdb.exe"
$oxsql   = Join-Path $bin "oxsql.exe"
$mPort = 17791; $mRep = 17792; $mSql = 13311

$pass = 0; $fail = 0
function Check($name, $cond, $detail) {
    if ($cond) { $script:pass++; Write-Output ("[PASS] " + $name) }
    else { $script:fail++; Write-Output ("[FAIL] " + $name + " :: " + $detail) }
}
function Invoke-Oxsql([string]$sql, [int]$skip) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $oxsql
    $psi.Arguments = "-h 127.0.0.1 -P $mSql -u root -c `"$sql`""
    $psi.RedirectStandardOutput = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $proc = [System.Diagnostics.Process]::Start($psi)
    $outText = $proc.StandardOutput.ReadToEnd()
    $proc.WaitForExit()
    return @(($outText -split "`r?`n") | Where-Object { $_ -ne "" } | Select-Object -Skip $skip)
}
function WaitPort($port, $timeoutSec) {
    $dl = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $dl) {
        $c = New-Object System.Net.Sockets.TcpClient
        try { $c.Connect("127.0.0.1", $port); $c.Close(); return $true }
        catch { $c.Dispose() }
        Start-Sleep -Milliseconds 300
    }
    return $false
}
function StopProc($p) {
    if ($null -ne $p -and -not $p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
}

Get-Process openxdb -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
& $openxdb init --data-dir $data 2>&1 | Out-Null

$p = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$data,"--port",$mPort,"--replica-port",$mRep,"--mysql-port",$mSql,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 4
Check "server listeners up" (WaitPort $mSql 10) "mysql wire not listening"

# G1 握手版本串（oxsql 连接首行打印 server version）
$conn = Invoke-Oxsql "select 1" 0
$connTxt = $conn -join "`n"
Check "G1 handshake version string" ($connTxt -match "server version: 8.0.36-OpenXDB v1.17.0") $connTxt

# G3 select version()
$v = Invoke-Oxsql "select version();" 1
$vTxt = $v -join "`n"
Check "G3 select version() returns v1.17.0" ($vTxt -match "8.0.36-OpenXDB v1.17.0") $vTxt

# 建表（含主键 + 二级索引，供 G4/G5）
Invoke-Oxsql "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, age INT, KEY idx_age (age))" 1 | Out-Null

# G4 SHOW CREATE TABLE（可重建 schema）
$sc = Invoke-Oxsql "show create table t;" 1
$scTxt = $sc -join "`n"
Check "G4 show create table rebuildable" (($scTxt -match "CREATE TABLE ``t``") -and ($scTxt -match "PRIMARY KEY") -and ($scTxt -match "KEY")) $scTxt

# G5 SHOW INDEXES 与 SHOW INDEX 等价（四种大小写组合）
$g5a = Invoke-Oxsql "show indexes from t;" 1
$g5b = Invoke-Oxsql "SHOW INDEXES FROM t;" 1
$g5c = Invoke-Oxsql "show index from t;" 1
$g5d = Invoke-Oxsql "SHOW INDEX FROM t;" 1
$eq1 = ($g5a -join "`n") -ceq ($g5d -join "`n")
$eq2 = ($g5a -join "`n") -ceq ($g5b -join "`n")
$eq3 = ($g5a -join "`n") -ceq ($g5c -join "`n")
Check "G5 SHOW INDEXES == SHOW INDEX (4 case combos)" ($eq1 -and $eq2 -and $eq3) "lower INDEXES==upper INDEX:$eq1, ==upper INDEXES:$eq2, ==lower INDEX:$eq3"

# G2 提示符 oxsql>（交互 REPL：重定向 stdin 注入命令后读 stdout）
$psi2 = New-Object System.Diagnostics.ProcessStartInfo
$psi2.FileName = $oxsql
$psi2.Arguments = "-h 127.0.0.1 -P $mSql -u root"
$psi2.RedirectStandardOutput = $true
$psi2.RedirectStandardInput = $true
$psi2.UseShellExecute = $false
$psi2.CreateNoWindow = $true
$proc2 = [System.Diagnostics.Process]::Start($psi2)
$proc2.StandardInput.Write("select 1;`nselect version();`nquit`n")
$proc2.StandardInput.Close()
$replTxt = $proc2.StandardOutput.ReadToEnd()
$proc2.WaitForExit()
$promptCount = ([regex]::Matches($replTxt, "oxsql> ")).Count
Check "G2 oxsql> prompt shown (x$promptCount)" (($promptCount -ge 2) -and ($replTxt -match "server version: 8.0.36-OpenXDB v1.17.0")) $replTxt

StopProc $p
""
"=== D7 validation: PASS=$pass FAIL=$fail ==="
if ($fail -gt 0) { exit 1 }
