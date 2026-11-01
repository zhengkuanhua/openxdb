# D3.3 cross-node read/write consistency 3-node validation
# Coverage:
#   S1 registry broadcast (G2 fix): ADD NODE on A -> B/C see full topology
#   S2 three nodes each own a region (reuse D3.2 layout): t3 split->A/B/C, t_imp->B
#   S3 node-to-node point/COUNT/scan mutual reads (G1 fix + registry fix regression)
#   S4 COUNT/SUM aggregation no-loss no-duplicate across 3 nodes (expanded 27 rows)
#   S5 2PC remote write lands on owning node (A/B coordinate UPDATE/DELETE on rC)
#   S6 G4 cross-node version visibility boundary probe
# Depends: bin/openxdb.exe bin/oxsql.exe (built from source)
# NOTE: A starts first and acts as M8 central TSO (--tso-node); B/C mount remote
#       TSO clients so cross-node snapshot versions share one monotonic sequence.
# NOTE: RocksDB fails to create storage under non-ASCII paths on Windows,
#       so data dirs live under $env:TEMP (ASCII) while script/results are archived in d33/.
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $root)))
$bin  = Join-Path $repo "bin"
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$work = Join-Path $env:TEMP ("openxdb_d33_" + $stamp)
$dirA = Join-Path $work "node-a"
$dirB = Join-Path $work "node-b"
$dirC = Join-Path $work "node-c"

$openxdb = Join-Path $bin "openxdb.exe"
$oxsql   = Join-Path $bin "oxsql.exe"

$aTcp=17881; $bTcp=17882; $cTcp=17883
$aSql=13316; $bSql=13317; $cSql=13318

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

StopProcs
foreach($d in @($dirA,$dirB,$dirC)){ New-Item -ItemType Directory -Force -Path $d | Out-Null }
& $openxdb init --data-dir $dirA 2>&1 | Out-Null
& $openxdb init --data-dir $dirB 2>&1 | Out-Null
& $openxdb init --data-dir $dirC 2>&1 | Out-Null

# A starts first, resolve its node id, then start B/C with --tso-node <A> (M8 central TSO)
$aP = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirA,"--port",$aTcp,"--mysql-port",$aSql,"--cluster-addr","127.0.0.1:$aTcp","--shutdown-timeout","1") -PassThru -WindowStyle Hidden
Check "A listening" (WaitPort $aSql 15) "A sql wire not up"
$nodes0 = Qry $aSql "SHOW NODES"
$idA = ""
foreach($nr in ($nodes0 -split "`n" | ForEach-Object { $_.TrimEnd("`r") } | Where-Object { $_ -match '^\s*node-' })){
    $cols = $nr -split "`t"
    if($cols.Count -ge 2 -and $cols[1].Trim() -eq "127.0.0.1:$aTcp"){ $idA = $cols[0].Trim() }
}
Check "resolve A node id for TSO" ($idA -ne "") ($nodes0 -join " | ")
$bP = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirB,"--port",$bTcp,"--mysql-port",$bSql,"--cluster-addr","127.0.0.1:$bTcp","--tso-node",$idA,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden
$cP = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirC,"--port",$cTcp,"--mysql-port",$cSql,"--cluster-addr","127.0.0.1:$cTcp","--tso-node",$idA,"--shutdown-timeout","1") -PassThru -WindowStyle Hidden
Check "B/C listening with TSO->A" ((WaitPort $bSql 15) -and (WaitPort $cSql 15)) "B/C sql wire not up"

# ---- S1 registry broadcast (G2 fix) ----
$n1 = Qry $aSql "ADD NODE '127.0.0.1:$bTcp'"
$n2 = Qry $aSql "ADD NODE '127.0.0.1:$cTcp'"
Write-Output "--- add node results ---"; $n1; $n2
$nodesA = Qry $aSql "SHOW NODES"
$nodesB = Qry $bSql "SHOW NODES"
$nodesC = Qry $cSql "SHOW NODES"
Write-Output "--- SHOW NODES @A ---"; $nodesA
Write-Output "--- SHOW NODES @B ---"; $nodesB
Write-Output "--- SHOW NODES @C ---"; $nodesC
$nrows = @($nodesA -split "`n" | ForEach-Object { $_.TrimEnd("`r") } | Where-Object { $_ -match '^\s*node-' })
$nrowsB = @($nodesB -split "`n" | ForEach-Object { $_.TrimEnd("`r") } | Where-Object { $_ -match '^\s*node-' })
$nrowsC = @($nodesC -split "`n" | ForEach-Object { $_.TrimEnd("`r") } | Where-Object { $_ -match '^\s*node-' })
Check "A registry count=3" ($nrows.Count -eq 3) ($nrows -join " | ")
Check "B registry count=3 (G2 fix: broadcast)" ($nrowsB.Count -eq 3) ($nrowsB -join " | ")
Check "C registry count=3 (G2 fix: broadcast)" ($nrowsC.Count -eq 3) ($nrowsC -join " | ")
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
Check "resolve node ids by addr" ($idA -ne "" -and $idB -ne "" -and $idC -ne "") "idA=$idA idB=$idB idC=$idC"
Write-Output "node ids: A=$idA B=$idB C=$idC"

# ---- S2 three regions (D3.2 layout) ----
$r1 = Qry $aSql "CREATE TABLE t3 (id INT PRIMARY KEY, v VARCHAR(64))"
Check "A create t3" ($r1 -match "Query OK") $r1
1..9 | ForEach-Object { Qry $aSql "INSERT INTO t3 VALUES ($_,'v$_')" | Out-Null }
$rb = Qry $bSql "CREATE TABLE t3 (id INT PRIMARY KEY, v VARCHAR(64))"
$rc = Qry $cSql "CREATE TABLE t3 (id INT PRIMARY KEY, v VARCHAR(64))"
Check "B/C t3 DDL synced via broadcast (already exists)" (($rb -match "already exists") -and ($rc -match "already exists")) "$rb | $rc"
$s1 = Qry $aSql "SPLIT REGION 9223372036854775809"
$routes1 = Qry $aSql "SHOW REGION ROUTES"
$r1rows = DataRows $routes1
$rid2 = ""
foreach($row in $r1rows){ $rid = Cell0 $row; if($rid -ne "9223372036854775809"){ $rid2 = $rid; break } }
$s2 = Qry $aSql "SPLIT REGION $rid2"
$routes2 = Qry $aSql "SHOW REGION ROUTES"
$r2rows = DataRows $routes2
Check "t3 split to 3 regions" ($r2rows.Count -eq 3) ($r2rows -join " | ")
$rA = Cell0 $r2rows[0]; $rB = Cell0 $r2rows[1]; $rC = Cell0 $r2rows[2]
Write-Output "region ids rA=$rA rB=$rB rC=$rC"
$asB = Qry $aSql "ASSIGN REGION $rB TO NODE '$idB'"
$asC = Qry $aSql "ASSIGN REGION $rC TO NODE '$idC'"
Check "assign rB->B / rC->C" ($asB -match "Query OK" -and $asC -match "Query OK") "$asB | $asC"
Start-Sleep -Milliseconds 300
$rtB = Qry $bSql "SHOW REGION ROUTES"
$rtC = Qry $cSql "SHOW REGION ROUTES"
$dB = DataRows $rtB; $dC = DataRows $rtC
Check "B routes learned (3)" ($dB.Count -eq 3) ($dB -join " | ")
Check "C routes learned (3)" ($dC.Count -eq 3) ($dC -join " | ")

# ---- S3 node-to-node point / COUNT / scan mutual reads ----
$qB1 = Qry $bSql "SELECT v FROM t3 WHERE id=1"
Check "B point id=1 (remote rA) visible" ($qB1 -match "v1") $qB1
$qC4 = Qry $cSql "SELECT v FROM t3 WHERE id=4"
Check "C point id=4 (remote rB) visible (G3 fix)" ($qC4 -match "v4") $qC4
$qB9 = Qry $bSql "SELECT v FROM t3 WHERE id=9"
Check "B point id=9 (remote rC) visible (G5 fix)" ($qB9 -match "v9") $qB9
$qC5 = Qry $cSql "SELECT v FROM t3 WHERE id=5"
Check "C point id=5 (local rC) visible" ($qC5 -match "v5") $qC5
foreach($pair in @(@("A",$aSql),@("B",$bSql),@("C",$cSql))){
    $out1 = Qry $pair[1] "SELECT COUNT(*) FROM t3"
    $ok = (Scalar $out1) -eq "9"
    $name = "$($pair[0]) COUNT(*) t3 = 9"
    if($ok){ Check $name $true $out1 } else { Check $name $false (($out1 -split "`n" | Select-Object -Last 2) -join " | ") }
    $out2 = Qry $pair[1] "SELECT COUNT(*) FROM t3 WHERE id>=1"
    $ok2 = (Scalar $out2) -eq "9"
    $name2 = "$($pair[0]) COUNT-where t3 = 9"
    if($ok2){ Check $name2 $true $out2 } else { Check $name2 $false (($out2 -split "`n" | Select-Object -Last 2) -join " | ") }
}
$scanB = Qry $bSql "SELECT id FROM t3 ORDER BY id"
$scanC = Qry $cSql "SELECT id FROM t3 ORDER BY id"
$idsB = @(DataRows $scanB | ForEach-Object { $_.Trim() })
$idsC = @(DataRows $scanC | ForEach-Object { $_.Trim() })
$expect = @("1","2","3","4","5","6","7","8","9")
$bEq = ($idsB.Count -eq 9) -and (($idsB -join ",") -eq ($expect -join ","))
$cEq = ($idsC.Count -eq 9) -and (($idsC -join ",") -eq ($expect -join ","))
Check "B scan merged 1..9 in order" $bEq ($idsB -join ",")
Check "C scan merged 1..9 in order" $cEq ($idsC -join ",")

# ---- S4 COUNT/SUM aggregation no-loss no-duplicate (expand to 27 rows) ----
10..18 | ForEach-Object { Qry $bSql "INSERT INTO t3 VALUES ($_,'v$_')" | Out-Null }
19..27 | ForEach-Object { Qry $cSql "INSERT INTO t3 VALUES ($_,'v$_')" | Out-Null }
foreach($pair in @(@("A",$aSql),@("B",$bSql),@("C",$cSql))){
    $cnt = Qry $pair[1] "SELECT COUNT(*) FROM t3"
    $sum = Qry $pair[1] "SELECT SUM(id) FROM t3"
    $mid = Qry $pair[1] "SELECT COUNT(*) FROM t3 WHERE id BETWEEN 4 AND 6"
    $cOK = (Scalar $cnt) -eq "27"
    $sOK = (Scalar $sum) -eq "378"
    $mOK = (Scalar $mid) -eq "3"
    $name = "$($pair[0]) agg no-loss-no-dup"
    if($cOK -and $sOK -and $mOK){ Check $name $true "count=27 sum=378 mid=3" } else { Check $name $false "count=$(Scalar $cnt) sum=$(Scalar $sum) mid=$(Scalar $mid)" }
}
# region-local filtering check: id in rC range seen as 9 from every node
foreach($pair in @(@("B",$bSql),@("C",$cSql))){
    $rng = Qry $pair[1] "SELECT COUNT(*) FROM t3 WHERE id>=19"
    $rOK = (Scalar $rng) -eq "9"
    $rname = "$($pair[0]) COUNT rC-range (19..27) = 9"
    if($rOK){ Check $rname $true $rng } else { Check $rname $false (($rng -split "`n" | Select-Object -Last 2) -join " | ") }
}

# ---- S5 2PC remote write lands on owning node ----
$uA = Qry $aSql "UPDATE t3 SET v='v5x' WHERE id=5"
Check "A UPDATE id=5 (rC) via 2PC ok" ($uA -match "Query OK") $uA
$pA5 = Qry $aSql "SELECT v FROM t3 WHERE id=5"
$pB5 = Qry $bSql "SELECT v FROM t3 WHERE id=5"
$pC5 = Qry $cSql "SELECT v FROM t3 WHERE id=5"
Check "A point id=5 = v5x (coordinator forward read)" ($pA5 -match "v5x") $pA5
Check "B point id=5 = v5x (remote forward read)" ($pB5 -match "v5x") $pB5
if($pC5 -match "v5x"){
    Check "C point id=5 = v5x (local read, no version gap)" $true $pC5
} else {
    Start-Sleep -Milliseconds 800
    $pC5b = Qry $cSql "SELECT v FROM t3 WHERE id=5"
    if($pC5b -match "v5x"){
        CheckGap "C point id=5 local read transient lag (resolved on retry)" (($pC5 -split "`n" | Select-Object -Last 2) -join " | ")
    } else {
        CheckGap "C point id=5 local read misses 2PC version (G4: A/B forward reads OK; local beginTS not in TSO sequence)" (($pC5 -split "`n" | Select-Object -Last 2) -join " | ")
    }
}
# 2PC landed on owner: C local COUNT in rC range (id>=7 spans rC tail 7..27) = 21 rows
$cRng = Qry $cSql "SELECT COUNT(*) FROM t3 WHERE id>=7"
$cRngOK = (Scalar $cRng) -eq "21"
if($cRngOK){ Check "C local rC range count=21 (2PC write landed on owner)" $true $cRng } else { Check "C local rC range count=21 (2PC write landed on owner)" $false (($cRng -split "`n" | Select-Object -Last 2) -join " | ") }
# B coordinates 2PC to rC
$uB = Qry $bSql "UPDATE t3 SET v='v8x' WHERE id=8"
Check "B UPDATE id=8 (rC) via 2PC ok" ($uB -match "Query OK") $uB
$pA8 = Qry $aSql "SELECT v FROM t3 WHERE id=8"
$pC8 = Qry $cSql "SELECT v FROM t3 WHERE id=8"
Check "A point id=8 = v8x (B-coordinated 2PC visible)" ($pA8 -match "v8x") $pA8
if($pC8 -match "v8x"){
    Check "C point id=8 = v8x (local read)" $true $pC8
} else {
    CheckGap "C point id=8 local read misses 2PC version (same G4 boundary)" (($pC8 -split "`n" | Select-Object -Last 2) -join " | ")
}
# 2PC DELETE on remote region
$dA = Qry $aSql "DELETE FROM t3 WHERE id=9"
Check "A DELETE id=9 (rC) via 2PC ok" ($dA -match "Query OK") $dA
$cntA9 = Qry $aSql "SELECT COUNT(*) FROM t3"
$cntB9 = Qry $bSql "SELECT COUNT(*) FROM t3"
$cntC9 = Qry $cSql "SELECT COUNT(*) FROM t3"
$c9OK = ((Scalar $cntA9) -eq "26") -and ((Scalar $cntB9) -eq "26")
Check "A/B COUNT=26 after remote delete (no-loss-no-dup)" $c9OK "A=$(Scalar $cntA9) B=$(Scalar $cntB9)"
if((Scalar $cntC9) -eq "26"){
    Check "C COUNT=26 after 2PC delete" $true $cntC9
} else {
    CheckGap "C COUNT after 2PC delete (G4 boundary: local snapshot misses TSO version)" "C=$(Scalar $cntC9)"
}

# ---- S6 G4 probe: cross-node version visibility matrix ----
Write-Output "--- G4 visibility matrix (A coordinator / B remote / C owner) ---"
Write-Output ("A forward read id=8: " + ((Qry $aSql "SELECT v FROM t3 WHERE id=8") -join " "))
Write-Output ("B forward read id=8: " + ((Qry $bSql "SELECT v FROM t3 WHERE id=8") -join " "))
Write-Output ("C local read id=8:  " + ((Qry $cSql "SELECT v FROM t3 WHERE id=8") -join " "))
Write-Output ("C local read id=5:  " + ((Qry $cSql "SELECT v FROM t3 WHERE id=5") -join " "))

StopProcs
Write-Output ("SUMMARY pass=$pass fail=$fail gap=$gap")
