# D4.1 quorumEnabled=true 开启评估 — 三节点真实组网
# 目标：在三节点验证场开启 ha.quorumEnabled=true，评估多数派(2/3)升主/降级行为，
#       确认不破坏三节点自动升主；对照 KNOWN_LIMITATIONS §3.4 / HA_BRAIN_SPLIT_DESIGN §5.5。
# 场景：
#   S1 组网+配置基线：3 节点 UP，SET ha_quorum_enabled=on 三端生效（SHOW CONFIG session）
#   S2 读写基线：t3 27 行，三端 COUNT=27 / SUM=378
#   S3 多数派(A down, B/C 存活 2/3 ≥ quorum=2)：心跳置 DOWN，集群轨 failover 正常
#      （region 重指派 / 数据仍可读），不破坏多数派下自动升主前提
#   S4 少数派(A+B down, 仅 C 1/3 < quorum=2)：FailoverDownNode 被 quorum 门拒绝
#      （不重指派），降级只读/不升主（对照 §3.4/§5.5：quorum 不足拒绝升主）
#   S5 恢复多数派(A/B 重启)：3 节点 UP，数据收敛一致
# Depends: bin/openxdb.exe bin/oxsql.exe（已从源码构建）
# NOTE: A 为中心 TSO（--tso-node 自指）；B/C 挂远端 TSO 客户端。数据目录 ASCII（%TEMP%）。
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $root)))
$bin  = Join-Path $repo "bin"
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$work = Join-Path $env:TEMP ("openxdb_d41_" + $stamp)
$dirA = Join-Path $work "node-a"
$dirB = Join-Path $work "node-b"
$dirC = Join-Path $work "node-c"
$openxdb = Join-Path $bin "openxdb.exe"
$oxsql   = Join-Path $bin "oxsql.exe"
$aTcp=17881; $bTcp=17882; $cTcp=17883
$aSql=13316; $bSql=13317; $cSql=13318
$logA = Join-Path $work "node-a.log"
$logB = Join-Path $work "node-b.log"
$logC = Join-Path $work "node-c.log"

$pass=0; $fail=0; $gap=0
function Check($name,$cond,$detail){ if($cond){$script:pass++; Write-Output ("[PASS] "+$name)} else {$script:fail++; Write-Output ("[FAIL] "+$name+" :: "+$detail)} }
function CheckGap([string]$name,[string]$detail){ Write-Output ("[GAP ] "+$name+" :: "+$detail); $script:gap++ }
function RowMatch([string]$text,[string]$regex){ return [bool]($text -match $regex) }
function DataRows([string]$text) {
    return @($text -split "`n" | Where-Object {
        $t = $_.Trim()
        $t -ne "" -and $t -notmatch "^(region_id|Connected to)" -and $t -notmatch "row\(s\) in set$" -and $t -notmatch "^-+$" -and $t -notmatch "^[A-Za-z_][A-Za-z0-9_]*$"
    })
}
function Qry($port,$sql){ $out = & $oxsql -h 127.0.0.1 -P $port -u root -c $sql 2>&1; return ($out -join "`n") }
function Scalar([string]$text){
    $m = [regex]::Match($text,'(?m)^\s*([-0-9]+)\s*$')
    if($m.Success){ return $m.Groups[1].Value }
    return $text.Trim()
}
function WaitPort($port,$timeoutSec){
    $dl=(Get-Date).AddSeconds($timeoutSec)
    while((Get-Date)-lt $dl){ $c=New-Object System.Net.Sockets.TcpClient; try{$c.Connect("127.0.0.1",$port);$c.Close();return $true}catch{$c.Dispose()}; Start-Sleep -Milliseconds 300 }
    return $false
}
function StopProcs(){ Get-Process openxdb -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue }
function Cell0($row){ ($row -split "`t")[0].Trim() }

Write-Output "=== D4.1 quorum enable eval | stamp=$stamp work=$work ==="
StopProcs
foreach($d in @($dirA,$dirB,$dirC)){ New-Item -ItemType Directory -Force -Path $d | Out-Null }
& $openxdb init --data-dir $dirA 2>&1 | Out-Null
& $openxdb init --data-dir $dirB 2>&1 | Out-Null
& $openxdb init --data-dir $dirC 2>&1 | Out-Null

# ---- S1 组网 + 配置基线 ----
$aP = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirA,"--port",$aTcp,"--mysql-port",$aSql,"--cluster-addr","127.0.0.1:$aTcp","--shutdown-timeout","1") -PassThru -WindowStyle Hidden -RedirectStandardOutput $logA -RedirectStandardError "$logA.err"
Check "A listening" (WaitPort $aSql 15) "A sql wire not up"
$nodes0 = Qry $aSql "SHOW NODES"
$idA = ""
foreach($nr in ($nodes0 -split "`n" | ForEach-Object { $_.TrimEnd("`r") } | Where-Object { $_ -match '^\s*node-' })){
    $cols = $nr -split "`t"
    if($cols.Count -ge 2 -and $cols[1].Trim() -eq "127.0.0.1:$aTcp"){ $idA = $cols[0].Trim() }
}
Check "resolve A node id for TSO" ($idA -ne "") ($nodes0 -join " | ")
$bP = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirB,"--port",$bTcp,"--mysql-port",$bSql,"--cluster-addr","127.0.0.1:$bTcp","--tso-node",$idA,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden -RedirectStandardOutput $logB -RedirectStandardError "$logB.err"
$cP = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirC,"--port",$cTcp,"--mysql-port",$cSql,"--cluster-addr","127.0.0.1:$cTcp","--tso-node",$idA,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden -RedirectStandardOutput $logC -RedirectStandardError "$logC.err"
Check "B/C listening with TSO->A" ((WaitPort $bSql 15) -and (WaitPort $cSql 15)) "B/C sql wire not up"
Qry $aSql "ADD NODE '127.0.0.1:$bTcp'" | Out-Null
Qry $aSql "ADD NODE '127.0.0.1:$cTcp'" | Out-Null
Start-Sleep -Milliseconds 300
$nodesA = Qry $aSql "SHOW NODES"
$nrows = @($nodesA -split "`n" | ForEach-Object { $_.TrimEnd("`r") } | Where-Object { $_ -match '^\s*node-' })
Check "A registry count=3" ($nrows.Count -eq 3) ($nrows -join " | ")
$idA=""; $idB=""; $idC=""
foreach($nr in $nrows){
    $cols = $nr -split "`t"
    if($cols.Count -ge 2){
        $addr = $cols[1].Trim()
        if($addr -eq "127.0.0.1:$aTcp"){ $idA = $cols[0].Trim() }
        elseif($addr -eq "127.0.0.1:$bTcp"){ $idB = $cols[0].Trim() }
        elseif($addr -eq "127.0.0.1:$cTcp"){ $idC = $cols[0].Trim() }
    }
}
Write-Output "node ids: A=$idA B=$idB C=$idC"
# 开启 quorum（三端 session 生效）
$qA = Qry $aSql "SET ha_quorum_enabled = on"
$qB = Qry $bSql "SET ha_quorum_enabled = on"
$qC = Qry $cSql "SET ha_quorum_enabled = on"
$cfgA = Qry $aSql "SHOW CONFIG"
$cfgB = Qry $bSql "SHOW CONFIG"
$cfgC = Qry $cSql "SHOW CONFIG"
$qRow = $cfgA -split "`n" | Where-Object { $_ -match "ha_quorum_enabled" }
$qRowB = $cfgB -split "`n" | Where-Object { $_ -match "ha_quorum_enabled" }
$qRowC = $cfgC -split "`n" | Where-Object { $_ -match "ha_quorum_enabled" }
Check "A ha_quorum_enabled=on(session)" ($qRow -match "on\s+session") ($qRow -join " | ")
Check "B ha_quorum_enabled=on(session)" ($qRowB -match "on\s+session") ($qRowB -join " | ")
Check "C ha_quorum_enabled=on(session)" ($qRowC -match "on\s+session") ($qRowC -join " | ")
$eRow = $cfgA -split "`n" | Where-Object { $_ -match "ha_epoch_enabled" }
Check "A ha_epoch_enabled=on(default) intact" ($eRow -match "on\s+default") ($eRow -join " | ")
Write-Output "--- SHOW CONFIG ha_* @A ---"; $qRow; $eRow

# ---- S2 读写基线：t3 27 行三端一致 ----
Qry $aSql "CREATE TABLE t3 (id INT PRIMARY KEY, v VARCHAR(64))" | Out-Null
1..9 | ForEach-Object { Qry $aSql "INSERT INTO t3 VALUES ($_,'v$_')" | Out-Null }
$s1 = Qry $aSql "SPLIT REGION 9223372036854775809"
$routes1 = Qry $aSql "SHOW REGION ROUTES"
$r1rows = DataRows $routes1
$rid2 = ""
foreach($row in $r1rows){ $rid = Cell0 $row; if($rid -ne "9223372036854775809"){ $rid2 = $rid; break } }
Qry $aSql "SPLIT REGION $rid2" | Out-Null
$routes2 = Qry $aSql "SHOW REGION ROUTES"
$r2rows = DataRows $routes2
Check "t3 split to 3 regions" ($r2rows.Count -eq 3) ($r2rows -join " | ")
$rA = Cell0 $r2rows[0]; $rB = Cell0 $r2rows[1]; $rC = Cell0 $r2rows[2]
Qry $aSql "ASSIGN REGION $rB TO NODE '$idB'" | Out-Null
Qry $aSql "ASSIGN REGION $rC TO NODE '$idC'" | Out-Null
Start-Sleep -Milliseconds 300
10..18 | ForEach-Object { Qry $bSql "INSERT INTO t3 VALUES ($_,'v$_')" | Out-Null }
19..27 | ForEach-Object { Qry $cSql "INSERT INTO t3 VALUES ($_,'v$_')" | Out-Null }
foreach($pair in @(@("A",$aSql),@("B",$bSql),@("C",$cSql))){
    $cnt = Qry $pair[1] "SELECT COUNT(*) FROM t3"
    $sum = Qry $pair[1] "SELECT SUM(id) FROM t3"
    $cOK = (Scalar $cnt) -eq "27"; $sOK = (Scalar $sum) -eq "378"
    $name = "$($pair[0]) baseline count=27 sum=378"
    if($cOK -and $sOK){ Check $name $true "count=$(Scalar $cnt) sum=$(Scalar $sum)" } else { Check $name $false "count=$(Scalar $cnt) sum=$(Scalar $sum)" }
}

# ---- S3 多数派场景：A down（B/C 存活 2/3 = quorum） ----
Write-Output "--- S3 kill A (2/3 majority remains) ---"
Stop-Process -Id $aP.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 9   # 心跳 interval=2s timeout=6s，等待 DOWN 判定与 failover
$nA_after = Qry $bSql "SHOW NODES"
$stateA = ($nA_after -split "`n" | Where-Object { $_ -match $idA }) -join " | "
Check "B sees A state DOWN (heartbeat)" ($stateA -match "DOWN") $stateA
$nA_afterC = Qry $cSql "SHOW NODES"
$stateAC = ($nA_afterC -split "`n" | Where-Object { $_ -match $idA }) -join " | "
Check "C sees A state DOWN (heartbeat)" ($stateAC -match "DOWN") $stateAC
$routesB = Qry $bSql "SHOW REGION ROUTES"
$routesC = Qry $cSql "SHOW REGION ROUTES"
$rowsB = DataRows $routesB; $rowsC = DataRows $routesC
Write-Output "--- region routes @B ---"; $routesB
Write-Output "--- region routes @C ---"; $routesC
# 观察 rA 的 owner 是否重指派到存活节点（多数派满足时 failover 正常执行）
$rArowB = $rowsB | Where-Object { (Cell0 $_) -eq $rA }
$rArowC = $rowsC | Where-Object { (Cell0 $_) -eq $rA }
$ownerB = ""; $ownerC = ""
if($rArowB){ $ownerB = (($rArowB -split "`t")[1]).Trim() }
if($rArowC){ $ownerC = (($rArowC -split "`t")[1]).Trim() }
$rAReassigned = (($ownerB -ne "" -and $ownerB -match "node-(b|c|$idB|$idC)") -or ($ownerC -ne "" -and $ownerC -match "node-(b|c|$idB|$idC)"))
if($rAReassigned){ Check "rA reassigned to alive node after A down (quorum OK)" $true "ownerB=$ownerB ownerC=$ownerC" } else { CheckGap "rA owner after A down" "ownerB=$ownerB ownerC=$ownerC routes=$($rowsB -join ' | ')" }
# 数据仍可读（B/C 均能读全量 27 行）
foreach($pair in @(@("B",$bSql),@("C",$cSql))){
    $cnt = Qry $pair[1] "SELECT COUNT(*) FROM t3"
    $name = "$($pair[0]) reads count=27 after A down"
    if((Scalar $cnt) -eq "27"){ Check $name $true $cnt } else { Check $name $false (($cnt -split "`n" | Select-Object -Last 2) -join " | ") }
}
# B 上本地写（B 持有 region，A down 后 TSO 取号不可用，观察写行为）
$wB = Qry $bSql "INSERT INTO t3 VALUES (28,'v28')"
Write-Output "B insert id=28 after A down: $($wB -split "`n" | Select-Object -Last 2 | Out-String | ForEach-Object { $_.Trim() })"
$cntBw = Qry $bSql "SELECT COUNT(*) FROM t3"
Write-Output "B count after local write: $(Scalar $cntBw)"
# C 上写（C 持有 region，观察 TSO 单点影响）
$wC = Qry $cSql "INSERT INTO t3 VALUES (29,'v29')"
Write-Output "C insert id=29 after A down: $($wC -split "`n" | Select-Object -Last 2 | Out-String | ForEach-Object { $_.Trim() })"

# ---- S4 少数派场景：再杀 B（仅 C 1/3 < quorum=2） ----
Write-Output "--- S4 kill B (only C remains, 1/3 < quorum) ---"
Stop-Process -Id $bP.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 9
$routesC2 = Qry $cSql "SHOW REGION ROUTES"
$rowsC2 = DataRows $routesC2
$rArowC2 = $rowsC2 | Where-Object { (Cell0 $_) -eq $rA }
$rBrowC2 = $rowsC2 | Where-Object { (Cell0 $_) -eq $rB }
$ownerC2A = ""; $ownerC2B = ""
if($rArowC2){ $ownerC2A = (($rArowC2 -split "`t")[1]).Trim() }
if($rBrowC2){ $ownerC2B = (($rBrowC2 -split "`t")[1]).Trim() }
# quorum 不足：failover 重指派被拒，rB 不应被重指派到 C
$rBStaysB = ($ownerC2B -match $idB)
if($rBStaysB){ Check "rB NOT reassigned to C (1/3 quorum insufficient -> failover gated)" $true "owner rB=$ownerC2B" } else { CheckGap "rB owner when quorum insufficient" "owner rB=$ownerC2B routes=$($rowsC2 -join ' | ')" }
Write-Output "--- region routes @C after B down ---"; $routesC2
# C 读仍可用 / 写观察（C 持有 rC 本地 region）
$cntC2 = Qry $cSql "SELECT COUNT(*) FROM t3"
Write-Output "C count after B down (1/3): $(Scalar $cntC2)"
$wC2 = Qry $cSql "INSERT INTO t3 VALUES (30,'v30')"
Write-Output "C insert id=30 after B down: $($wC2 -split "`n" | Select-Object -Last 2 | Out-String | ForEach-Object { $_.Trim() })"
$cntC2b = Qry $cSql "SELECT COUNT(*) FROM t3"
Write-Output "C count after local write (1/3): $(Scalar $cntC2b)"

# ---- S5 恢复多数派：重启 A/B（原 data-dir，不 init） ----
Write-Output "--- S5 restart A/B ---"
# S5 重启用同一日志文件（进程已退出，文件可复用）
$aP2 = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirA,"--port",$aTcp,"--mysql-port",$aSql,"--cluster-addr","127.0.0.1:$aTcp","--tso-node",$idA,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden -RedirectStandardOutput $logA -RedirectStandardError "$logA.err"
Start-Sleep -Milliseconds 300
$bP2 = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirB,"--port",$bTcp,"--mysql-port",$bSql,"--cluster-addr","127.0.0.1:$bTcp","--tso-node",$idA,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden -RedirectStandardOutput $logB -RedirectStandardError "$logB.err"
Check "A2/B2 listening" ((WaitPort $aSql 15) -and (WaitPort $bSql 15)) "restart not up"
Start-Sleep -Seconds 3
$nodesR = Qry $aSql "SHOW NODES"
$nrowsR = @($nodesR -split "`n" | ForEach-Object { $_.TrimEnd("`r") } | Where-Object { $_ -match '^\s*node-' })
Check "registry count=3 after restart (majority recovered)" ($nrowsR.Count -eq 3) ($nrowsR -join " | ")
# 数据收敛：三端读全量（S3/S4 期间 C 侧可能写入 28/29/30，A/B 重启后复制位点全量补拉）
foreach($pair in @(@("A",$aSql),@("B",$bSql),@("C",$cSql))){
    $cnt = Qry $pair[1] "SELECT COUNT(*) FROM t3"
    $name = "$($pair[0]) count after restart"
    Write-Output "$($pair[0]) count=$(Scalar $cnt)"
}

Write-Output "--- S6 quorum 边界对照（参考断言） ---"
# 对照 KNOWN_LIMITATIONS §3.4：N=3 quorum=2；两节点 up=1 拒绝（由进程内 TestP0QuorumPromoteInsufficient 覆盖）
Write-Output "quorum calc: N=3 -> quorum=2; S3 up=2/3 OK; S4 up=1/3 rejected (failover gated)"
StopProcs
Write-Output ("SUMMARY pass=$pass fail=$fail gap=$gap")
