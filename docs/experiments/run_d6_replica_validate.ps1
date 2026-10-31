# D6 备库常驻读服务专项验证脚本（run_d6_replica_validate.ps1）
# 覆盖 6 项验收：常驻查询 / 写拒 / 滞后窗口 / 崩溃恢复 / (build+test 见文档) / 主端无回归
# 依赖：仓库根目录 bin/openxdb.exe、bin/oxsql.exe（先 go build）
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
$bin  = Join-Path $root "bin"
$work = Join-Path $root "d6_run_validate"
$masterDir = Join-Path $work "master"
$slaveDir  = Join-Path $work "slave"
New-Item -ItemType Directory -Force -Path $masterDir, $slaveDir | Out-Null

$openxdb = Join-Path $bin "openxdb.exe"
$oxsql   = Join-Path $bin "oxsql.exe"
$mPort = 17788; $mRep = 17789; $mSql = 13306; $sSql = 13307

$pass = 0; $fail = 0
function Check($name, $cond, $detail) {
    if ($cond) { $script:pass++; Write-Output ("[PASS] " + $name) }
    else { $script:fail++; Write-Output ("[FAIL] " + $name + " :: " + $detail) }
}
function Qry($port, $sql) {
    $out = & $oxsql -h 127.0.0.1 -P $port -u root -c $sql 2>&1
    return ($out -join "`n")
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
& $openxdb init --data-dir $masterDir 2>&1 | Out-Null
& $openxdb init --data-dir $slaveDir 2>&1 | Out-Null

$mP = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$masterDir,"--port",$mPort,"--replica-port",$mRep,"--mysql-port",$mSql,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 3
$sP = Start-Process -FilePath $openxdb -ArgumentList @("replica","--data-dir",$slaveDir,"--master","127.0.0.1:$mRep","--mysql-port",$sSql,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 5
Check "main listeners up" ((WaitPort $mSql 10) -and (WaitPort $sSql 10)) "mysql wire not listening"

# ① 常驻查询 + ② 写拒
$ddl = Qry $mSql "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(64))"
1..10 | ForEach-Object { Qry $mSql "INSERT INTO t VALUES ($_,'m$_')" | Out-Null }
Start-Sleep -Seconds 2
$rc = Qry $sSql "SELECT COUNT(*) FROM t" | Select-String '^\d+$' | ForEach-Object {[int]$_.Line} | Select-Object -First 1
Check "replica resident read count=10" ($rc -eq 10) "got rc=$rc"
$wr = Qry $sSql "INSERT INTO t VALUES (2000,'x')"
Check "write rejected on replica" ($wr -match "read-only replica") $wr
$rc2 = Qry $sSql "SELECT COUNT(*) FROM t" | Select-String '^\d+$' | ForEach-Object {[int]$_.Line} | Select-Object -First 1
Check "replica data unpoisoned" ($rc2 -eq 10) "got rc2=$rc2"

# ③ 滞后窗口：主端写 11..40，每轮断言备库不超前主端
$lagOk = $true; $worst = 0
11..40 | ForEach-Object {
    Qry $mSql "INSERT INTO t VALUES ($_,'m$_')" | Out-Null
    $mc = Qry $mSql "SELECT COUNT(*) FROM t" | Select-String '^\d+$' | ForEach-Object {[int]$_.Line} | Select-Object -First 1
    $sc = Qry $sSql "SELECT COUNT(*) FROM t" | Select-String '^\d+$' | ForEach-Object {[int]$_.Line} | Select-Object -First 1
    if ($null -eq $sc) { $sc = 0 }
    if ($sc -gt $mc) { $lagOk = $false; Write-Output ("LAG VIOLATION slave=$sc master=$mc at id=$_") }
    if (($mc - $sc) -gt $worst) { $worst = $mc - $sc }
}
Check "lag window no-ahead (worstGap=$worst)" $lagOk "slave ahead of master"

# ④ 崩溃恢复：强杀备库，主端写 41..60，重启后追平且顺序一致（两轮）
foreach ($round in 1..2) {
    $rP = Get-Process openxdb | Where-Object { $_.Id -ne $mP.Id } | Select-Object -First 1
    StopProc $rP
    Start-Sleep -Seconds 2
    41..60 | ForEach-Object { Qry $mSql "INSERT INTO t VALUES ($_,'m$_')" | Out-Null }
    $sP = Start-Process -FilePath $openxdb -ArgumentList @("replica","--data-dir",$slaveDir,"--master","127.0.0.1:$mRep","--mysql-port",$sSql,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden
    $dl = (Get-Date).AddSeconds(40); $sc = 0
    while ((Get-Date) -lt $dl) {
        try { $sc = Qry $sSql "SELECT COUNT(*) FROM t" | Select-String '^\d+$' | ForEach-Object {[int]$_.Line} | Select-Object -First 1 } catch { $sc = 0 }
        if ($sc -ge 60) { break }
        Start-Sleep -Milliseconds 800
    }
    $v60 = Qry $sSql "SELECT v FROM t WHERE id=60" | Select-String '^m60' | Select-Object -First 1
    Check "crash-recovery round$round count=60 order-ok" ($sc -ge 60 -and $v60) "sc=$sc v60=$v60"
}

# ⑤ 主端无回归
$ins = Qry $mSql "INSERT INTO t VALUES (1000,'m1000')"
$sel = Qry $mSql "SELECT v FROM t WHERE id=1000" | Select-String '^m1000' | Select-Object -First 1
$upd = Qry $mSql "UPDATE t SET v='m1000b' WHERE id=1000"
$sel2 = Qry $mSql "SELECT v FROM t WHERE id=1000" | Select-String '^m1000b' | Select-Object -First 1
Check "master no-regression insert/select/update" ($ins -match "Query OK" -and $sel -and $upd -match "Query OK" -and $sel2) "ins=$ins sel=$sel upd=$upd sel2=$sel2"

StopProc $mP
Get-Process openxdb -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Write-Output ("SUMMARY pass=$pass fail=$fail")
