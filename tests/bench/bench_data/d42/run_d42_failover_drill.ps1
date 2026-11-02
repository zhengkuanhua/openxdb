# D4.2 宕机 failover 演练 — 三节点真实组网（quorum 按 D4.1 结论开启）
# 目标：杀归属节点进程 -> 读 failover（resolveRegionOwner 自动重指派）-> 数据仍可读；
#       结合 D4.1 B1 缺口（单副本分片 down 后 failoverRegion 放弃重指派，
#       pkg/sql/ha.go failoverRegionP1 / tryFailoverTargetP1 持有校验）评估：
#         S2 单副本边界：region 唯一副本在被杀节点 -> 无存活副本 -> 重指派放弃（预期丢行 FAIL）
#         S3 存活副本场景：region 在存活节点有真实副本（创建者保留副本语义）-> 自动重指派 -> 数据仍可读（PASS）
#       P1 failover 路径：failoverRegionP1 候选目标逐个做 regionHoldsLocalTarget 持有校验
#       （物理行数 > 0），全部不持有才回本节点；本机也不持有返回 "no live data copy"。
# 场景：
#   S1 组网+配置基线：3 节点 UP，SET ha_quorum_enabled=on 三端生效；t3 27 行 3 regions
#       （A 创建者保留全部 region 副本；B 持 rB 归属+副本；C 持 rC 归属+副本）
#   S2 单副本边界（B1 复现）：杀 A（rA 唯一副本在 A）-> B/C 心跳 DOWN -> 聚合读丢 rA 行
#       （count=25，预期 FAIL）-> 日志 "failover skip candidate ... region 3" -> 写失败观察（TSO 单点 B2）
#   S2R 恢复：重启 A -> 三端 count=27
#   S3 存活副本可读路径：杀 B（rB 副本在创建者 A 保留）-> A/C DOWN 判定 -> rB 自动重指派到 A
#       （resolveRegionOwner -> FailoverDownNode -> tryFailoverTargetP1 持有校验通过）-> 聚合读 count=27
#       -> C 写 id=28 成功（TSO=A 存活）-> count=28
#   S3R 恢复：重启 B -> 三端 count=28
# Depends: bin/openxdb.exe bin/oxsql.exe（已从源码构建）
# NOTE: A 为中心 TSO（--tso-node 自指）；B/C 挂远端 TSO 客户端。数据目录 ASCII（%TEMP%）。
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $root)))
$bin  = Join-Path $repo "bin"
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$work = Join-Path $env:TEMP ("openxdb_d42_" + $stamp)
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
function NodeCol($row){ (($row -split "`t")[5]).Trim() }
function Errs([string]$path){ if(Test-Path $path){ return (Get-Content $path -Raw -ErrorAction SilentlyContinue) } return "" }
function HasText([string]$text,[string]$regex){ return [bool]($text -match $regex) }

Write-Output "=== D4.2 failover drill | stamp=$stamp work=$work ==="
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

# ---- S1b 读写基线：t3 27 行三端一致，A 创建者保留全部副本 ----
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
Write-Output "region ids: rA=$rA rB=$rB rC=$rC"
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
# 路由基线：rA->A rB->B rC->C
$routes0 = Qry $aSql "SHOW REGION ROUTES"
$r0rows = DataRows $routes0
$nA0 = ""; $nB0 = ""; $nC0 = ""
foreach($row in $r0rows){ $rid=Cell0 $row; $nd=NodeCol $row; if($rid -eq $rA){$nA0=$nd} elseif($rid -eq $rB){$nB0=$nd} elseif($rid -eq $rC){$nC0=$nd} }
Check "baseline routes rA=A rB=B rC=C" (($nA0 -eq $idA) -and ($nB0 -eq $idB) -and ($nC0 -eq $idC)) "rA=$nA0 rB=$nB0 rC=$nC0"

# ---- S2 单副本边界（B1 复现）：杀 A，rA 唯一副本在 A ----
Write-Output "--- S2 kill A (single-copy boundary, B1 repro) ---"
Stop-Process -Id $aP.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 9
$nA_after = Qry $bSql "SHOW NODES"
$stateA = ($nA_after -split "`n" | Where-Object { $_ -match $idA }) -join " | "
Check "B sees A state DOWN (heartbeat)" ($stateA -match "DOWN") $stateA
$nA_afterC = Qry $cSql "SHOW NODES"
$stateAC = ($nA_afterC -split "`n" | Where-Object { $_ -match $idA }) -join " | "
Check "C sees A state DOWN (heartbeat)" ($stateAC -match "DOWN") $stateAC
$routesB = Qry $bSql "SHOW REGION ROUTES"
$routesC = Qry $cSql "SHOW REGION ROUTES"
$rowsB = DataRows $routesB; $rowsC = DataRows $routesC
$nB_rA = ""; $nC_rA = ""
foreach($row in $rowsB){ if((Cell0 $row) -eq $rA){ $nB_rA = NodeCol $row } }
foreach($row in $rowsC){ if((Cell0 $row) -eq $rA){ $nC_rA = NodeCol $row } }
CheckGap "S2 rA owner after A down" "ownerB=$nB_rA ownerC=$nC_rA (expected: still A, no live copy -> failover aborted)"
# 聚合读：B/C 应丢 rA 行 -> count=25（预期 FAIL，单副本边界如实记录）
foreach($pair in @(@("B",$bSql),@("C",$cSql))){
    $cnt = Qry $pair[1] "SELECT COUNT(*) FROM t3"
    $name = "$($pair[0]) reads count=27 after A down (single-copy boundary, expected FAIL)"
    if((Scalar $cnt) -eq "27"){ Check $name $true $cnt } else { Check $name $false "count=$(Scalar $cnt) (expected 25: rA 2 rows lost)" }
}
# 日志证据：failover skip candidate（B1 复现）
$errB = Errs "$logB.err"; $errC = Errs "$logC.err"
$skipLog = HasText ($errB + "`n" + $errC) "failover skip candidate .* for region ${rA}: .*not held by candidate"
Check "S2 log: failover skip candidate for rA (B1 repro evidence)" $skipLog "B/C err: $($errB -split "`n" | Select-Object -First 2 | Out-String | ForEach-Object {$_.Trim()})"
# 写观察（TSO 单点 B2）：A down 后 B/C 写应失败 tso unavailable
$wB = Qry $bSql "INSERT INTO t3 VALUES (101,'v101')"
Write-Output "S2 B insert after A down: $($wB -split "`n" | Select-Object -Last 2 | Out-String | ForEach-Object { $_.Trim() })"

# ---- S2R 恢复：重启 A（原 data-dir） ----
Write-Output "--- S2R restart A ---"
$aP2 = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirA,"--port",$aTcp,"--mysql-port",$aSql,"--cluster-addr","127.0.0.1:$aTcp","--tso-node",$idA,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden -RedirectStandardOutput $logA -RedirectStandardError "$logA.err"
Check "A2 listening" (WaitPort $aSql 15) "restart not up"
Start-Sleep -Seconds 3
$nodesR = Qry $aSql "SHOW NODES"
$nrowsR = @($nodesR -split "`n" | ForEach-Object { $_.TrimEnd("`r") } | Where-Object { $_ -match '^\s*node-' })
Check "registry count=3 after A restart" ($nrowsR.Count -eq 3) ($nrowsR -join " | ")
foreach($pair in @(@("A",$aSql),@("B",$bSql),@("C",$cSql))){
    $cnt = Qry $pair[1] "SELECT COUNT(*) FROM t3"
    $name = "$($pair[0]) count=27 after A restart"
    if((Scalar $cnt) -eq "27"){ Check $name $true $cnt } else { Check $name $false "count=$(Scalar $cnt)" }
}

# ---- S3 存活副本可读路径：杀 B（rB 副本在创建者 A 保留） ----
Write-Output "--- S3 kill B (live copy exists: creator A retains rB) ---"
Stop-Process -Id $bP.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 9
$nB_after = Qry $aSql "SHOW NODES"
$stateB = ($nB_after -split "`n" | Where-Object { $_ -match $idB }) -join " | "
Check "A sees B state DOWN (heartbeat)" ($stateB -match "DOWN") $stateB
$nB_afterC = Qry $cSql "SHOW NODES"
$stateBC = ($nB_afterC -split "`n" | Where-Object { $_ -match $idB }) -join " | "
Check "C sees B state DOWN (heartbeat)" ($stateBC -match "DOWN") $stateBC
# 读触发 failover（resolveRegionOwner -> FailoverDownNode -> tryFailoverTargetP1 持有校验）
$cntA3 = Qry $aSql "SELECT COUNT(*) FROM t3"
$cntC3 = Qry $cSql "SELECT COUNT(*) FROM t3"
$routesA3 = Qry $aSql "SHOW REGION ROUTES"
$routesC3 = Qry $cSql "SHOW REGION ROUTES"
$rowsA3 = DataRows $routesA3; $rowsC3 = DataRows $routesC3
$nA3_rB = ""; $nC3_rB = ""
foreach($row in $rowsA3){ if((Cell0 $row) -eq $rB){ $nA3_rB = NodeCol $row } }
foreach($row in $rowsC3){ if((Cell0 $row) -eq $rB){ $nC3_rB = NodeCol $row } }
Check "S3 rB reassigned to A after B down (resolveRegionOwner auto reassign)" (($nA3_rB -eq $idA) -and ($nC3_rB -eq $idA)) "rB@A=$nA3_rB rB@C=$nC3_rB"
Write-Output "--- region routes @A after B down ---"; $routesA3
Write-Output "--- region routes @C after B down ---"; $routesC3
foreach($pair in @(@("A",$aSql,$cntA3),@("C",$cSql,$cntC3))){
    $name = "$($pair[0]) reads count=27 after B down (live copy failover, PASS expected)"
    if((Scalar $pair[2]) -eq "27"){ Check $name $true $pair[2] } else { Check $name $false "count=$(Scalar $pair[2])" }
}
# 写观察：TSO=A 存活，C 上写应成功
$wC = Qry $cSql "INSERT INTO t3 VALUES (28,'v28')"
$wCOK = HasText $wC "OK|Query OK|row affected|1 row affected|0 row affected"
Check "S3 C insert id=28 succeeds (TSO A alive)" ($wCOK -and -not (HasText $wC "error")) (($wC -split "`n" | Select-Object -Last 2) -join " | ")
$cntC4 = Qry $cSql "SELECT COUNT(*) FROM t3"
Check "S3 C count=28 after local write" ((Scalar $cntC4) -eq "28") "count=$(Scalar $cntC4)"

# ---- S3R 恢复：重启 B ----
Write-Output "--- S3R restart B ---"
$bP2 = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirB,"--port",$bTcp,"--mysql-port",$bSql,"--cluster-addr","127.0.0.1:$bTcp","--tso-node",$idA,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden -RedirectStandardOutput $logB -RedirectStandardError "$logB.err"
Check "B2 listening" (WaitPort $bSql 15) "restart not up"
Start-Sleep -Seconds 3
$nodesR2 = Qry $aSql "SHOW NODES"
$nrowsR2 = @($nodesR2 -split "`n" | ForEach-Object { $_.TrimEnd("`r") } | Where-Object { $_ -match '^\s*node-' })
Check "registry count=3 after B restart" ($nrowsR2.Count -eq 3) ($nrowsR2 -join " | ")
foreach($pair in @(@("A",$aSql),@("B",$bSql),@("C",$cSql))){
    $cnt = Qry $pair[1] "SELECT COUNT(*) FROM t3"
    $name = "$($pair[0]) count=28 after B restart"
    if((Scalar $cnt) -eq "28"){ Check $name $true $cnt } else { Check $name $false "count=$(Scalar $cnt)" }
}

Write-Output "--- S6 边界对照（参考断言） ---"
Write-Output "single-copy boundary: S2 rA unique copy on A -> failover aborted, 2 rows lost (B1 repro, expected FAIL)"
Write-Output "live-copy path: S3 rB live copy on A (creator retain) -> reassign to A, reads intact (PASS)"
Write-Output "quorum: 2/3 majority remains in both S2/S3 -> failover gated by quorum OK, then by live copy check"
StopProcs
Write-Output ("SUMMARY pass=$pass fail=$fail gap=$gap")
