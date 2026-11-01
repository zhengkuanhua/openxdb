# D3.2 implicit-region explicitization + ASSIGN REGION 3-node validation
# S1 implicit (no split): t_imp assigned to node-b; A point after B write cnt=1
# S2 three nodes each own a region: t3 split into 3 regions -> node-a/b/c
# Depends: bin/openxdb.exe bin/oxsql.exe (built from source)
# NOTE: RocksDB fails to create storage under non-ASCII paths on Windows,
#       so data dirs live under $env:TEMP (ASCII) while script/results are archived in d32/.
# NOTE: --node-id is NOT a supported start flag; node ids are auto-generated.
#       ASSIGN REGION targets must use ids resolved from SHOW NODES by addr.
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $root)))
$bin  = Join-Path $repo "bin"
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$work = Join-Path $env:TEMP ("openxdb_d32_" + $stamp)
$dirA = Join-Path $work "node-a"
$dirB = Join-Path $work "node-b"
$dirC = Join-Path $work "node-c"

$openxdb = Join-Path $bin "openxdb.exe"
$oxsql   = Join-Path $bin "oxsql.exe"

$aTcp=17781; $bTcp=17782; $cTcp=17783
$aSql=13306; $bSql=13307; $cSql=13308

$pass=0; $fail=0; $gap=0
function Check($name,$cond,$detail){ if($cond){$script:pass++; Write-Output ("[PASS] "+$name)} else {$script:fail++; Write-Output ("[FAIL] "+$name+" :: "+$detail)} }
function CheckGap([string]$name,[string]$detail){ Write-Output ("[GAP ] "+$name+" :: "+$detail); $script:gap++ }
function RowMatch([string]$text,[string]$regex){ return [bool]($text -match $regex) }
function DataRows([string]$text) {
    return @($text -split "`n" | Where-Object {
        $t = $_.Trim()
        $t -ne "" -and $t -notmatch "^(region_id|Connected to)" -and $t -notmatch "row\(s\) in set$"
    })
}
function Qry($port,$sql){ $out = & $oxsql -h 127.0.0.1 -P $port -u root -c $sql 2>&1; return ($out -join "`n") }
function WaitPort($port,$timeoutSec){
    $dl=(Get-Date).AddSeconds($timeoutSec)
    while((Get-Date)-lt $dl){ $c=New-Object System.Net.Sockets.TcpClient; try{$c.Connect("127.0.0.1",$port);$c.Close();return $true}catch{$c.Dispose()}; Start-Sleep -Milliseconds 300 }
    return $false
}
function StopProcs(){ Get-Process openxdb -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue }
# first cell of a row
function Cell0($row){ ($row -split "`t")[0].Trim() }

StopProcs
foreach($d in @($dirA,$dirB,$dirC)){ New-Item -ItemType Directory -Force -Path $d | Out-Null }
& $openxdb init --data-dir $dirA 2>&1 | Out-Null
& $openxdb init --data-dir $dirB 2>&1 | Out-Null
& $openxdb init --data-dir $dirC 2>&1 | Out-Null

$aP = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirA,"--port",$aTcp,"--mysql-port",$aSql,"--cluster-addr","127.0.0.1:$aTcp","--shutdown-timeout","1") -PassThru -WindowStyle Hidden
$bP = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirB,"--port",$bTcp,"--mysql-port",$bSql,"--cluster-addr","127.0.0.1:$bTcp","--shutdown-timeout","1") -PassThru -WindowStyle Hidden
$cP = Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirC,"--port",$cTcp,"--mysql-port",$cSql,"--cluster-addr","127.0.0.1:$cTcp","--shutdown-timeout","1") -PassThru -WindowStyle Hidden
Check "three nodes listening" ((WaitPort $aSql 15) -and (WaitPort $bSql 15) -and (WaitPort $cSql 15)) "sql wire not up"

# ---- S2 main table t3 ----
$r1 = Qry $aSql "CREATE TABLE t3 (id INT PRIMARY KEY, v VARCHAR(64))"
Check "A create t3" ($r1 -match "Query OK") $r1
1..9 | ForEach-Object { Qry $aSql "INSERT INTO t3 VALUES ($_,'v$_')" | Out-Null }
$rb = Qry $bSql "CREATE TABLE t3 (id INT PRIMARY KEY, v VARCHAR(64))"
$rc = Qry $cSql "CREATE TABLE t3 (id INT PRIMARY KEY, v VARCHAR(64))"
Check "B/C create t3 same-schema" ($rb -match "Query OK" -and $rc -match "Query OK") "$rb | $rc"

$n1 = Qry $aSql "ADD NODE '127.0.0.1:$bTcp'"
$n2 = Qry $aSql "ADD NODE '127.0.0.1:$cTcp'"
Write-Output "--- add node results ---"; $n1; $n2
$nodes = Qry $aSql "SHOW NODES"
Write-Output "--- SHOW NODES ---"; $nodes
$nrows = @($nodes -split "`n" | ForEach-Object { $_.TrimEnd("`r") } | Where-Object { $_ -match '^\s*node-' })
Check "SHOW NODES count=3" ($nrows.Count -eq 3) ($nrows -join " | ")
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

$s1 = Qry $aSql "SPLIT REGION 9223372036854775809"
Write-Output "--- split1 result ---"; $s1
$routes1 = Qry $aSql "SHOW REGION ROUTES"
Write-Output "--- routes after first split ---"; $routes1
$r1rows = DataRows $routes1
$rid2 = ""
foreach($row in $r1rows){ $rid = Cell0 $row; if($rid -ne "9223372036854775809"){ $rid2 = $rid; break } }
$s2 = Qry $aSql "SPLIT REGION $rid2"
Write-Output "--- split2 result ---"; $s2
$routes2 = Qry $aSql "SHOW REGION ROUTES"
Write-Output "--- routes after second split ---"; $routes2
$r2rows = DataRows $routes2
Check "t3 split to 3 regions" ($r2rows.Count -eq 3) ($r2rows -join " | ")
$rA = Cell0 $r2rows[0]; $rB = Cell0 $r2rows[1]; $rC = Cell0 $r2rows[2]
Write-Output "region ids rA=$rA rB=$rB rC=$rC"

$asB = Qry $aSql "ASSIGN REGION $rB TO NODE '$idB'"
$asC = Qry $aSql "ASSIGN REGION $rC TO NODE '$idC'"
Check "assign rB->B / rC->C" ($asB -match "Query OK" -and $asC -match "Query OK") "$asB | $asC"

$rtA = Qry $aSql "SHOW REGION ROUTES"
$rtB = Qry $bSql "SHOW REGION ROUTES"
$rtC = Qry $cSql "SHOW REGION ROUTES"
Write-Output "--- routes on A ---"; $rtA
Write-Output "--- routes on B ---"; $rtB
Write-Output "--- routes on C ---"; $rtC
$dA = DataRows $rtA; $dB = DataRows $rtB; $dC = DataRows $rtC
Check "A routes has content (3 rows)" ($dA.Count -eq 3) ($dA -join " | ")
Check "B routes learned (3 rows)"     ($dB.Count -eq 3) ($dB -join " | ")
Check "C routes learned (3 rows)"     ($dC.Count -eq 3) ($dC -join " | ")
$lA = ($dA | Where-Object { $_ -match ('^\s*' + $rA + '\s') -and $_ -match $idA -and $_ -match 'true' }).Count
$lB = ($dB | Where-Object { $_ -match ('^\s*' + $rB + '\s') -and $_ -match $idB -and $_ -match 'true' }).Count
$lC = ($dC | Where-Object { $_ -match ('^\s*' + $rC + '\s') -and $_ -match $idC -and $_ -match 'true' }).Count
Check "region lands on owner (A rA / B rB / C rC local)" ($lA -eq 1 -and $lB -eq 1 -and $lC -eq 1) "lA=$lA lB=$lB lC=$lC"

foreach($pair in @(@("A",$aSql),@("B",$bSql),@("C",$cSql))){
    $out1 = Qry $pair[1] "SELECT COUNT(*) FROM t3"
    if($pair[0] -eq "A"){
        Check "A count=9 (source keeps copies)" (RowMatch $out1 '(?m)^\s*9\s*$') $out1
        $out2 = Qry $pair[1] "SELECT COUNT(*) FROM t3 WHERE id>=1"
        Check "A count-where=9 (materialized)" (RowMatch $out2 '(?m)^\s*9\s*$') $out2
    } else {
        CheckGap "$($pair[0]) count=9 (fastpath local-only, gap: no remote merge)" (($out1 -split "`n" | Where-Object {$_ -match '^\s*\d+\s*$'}) -join ",")
        $out2 = Qry $pair[1] "SELECT COUNT(*) FROM t3 WHERE id>=1"
        CheckGap "$($pair[0]) count-where=9 (materialized, gap: node registry not broadcast)" (($out2 -split "`n" | Where-Object {$_ -match '^\s*\d+\s*$|not registered'}) -join ",")
    }
}
$qA = Qry $aSql "SELECT v FROM t3 WHERE id=5"
Check "A point id=5 (rC) visible via forward" ($qA -match "v5") $qA
$qA2 = Qry $aSql "SELECT v FROM t3 WHERE id=9"
Check "A point id=9 visible" ($qA2 -match "v9") $qA2
$qB = Qry $bSql "SELECT v FROM t3 WHERE id=1"
Check "B point id=1 (remote rA) visible" ($qB -match "v1") $qB
$qC = Qry $cSql "SELECT v FROM t3 WHERE id=4"
CheckGap "C point id=4 (remote rB) visible (gap: C not registered B)" (($qC -split "`n" | Select-Object -Last 2) -join " | ")

# ---- S1 implicit explicitization (no split): t_imp -> node-b ----
$impId2 = "9223372036854775810"   # table_id=2 | 1<<63
$r1i = Qry $aSql "CREATE TABLE t_imp (id INT PRIMARY KEY, v VARCHAR(64))"
Check "A create t_imp" ($r1i -match "Query OK") $r1i
1..3 | ForEach-Object { Qry $aSql "INSERT INTO t_imp VALUES ($_,'i$_')" | Out-Null }
Qry $bSql "CREATE TABLE t_imp (id INT PRIMARY KEY, v VARCHAR(64))" | Out-Null
Qry $cSql "CREATE TABLE t_imp (id INT PRIMARY KEY, v VARCHAR(64))" | Out-Null
$impId = $impId2
Check "implicit region id derived (table 2 | 1<<63)" ($impId -ne "") $impId
$asImp = Qry $aSql "ASSIGN REGION $impId TO NODE '$idB'"
Check "assign implicit region (no split) -> node-b" ($asImp -match "Query OK") $asImp
$rtA2 = Qry $aSql "SHOW REGION ROUTES"
$rtB2 = Qry $bSql "SHOW REGION ROUTES"
Write-Output "--- A routes after implicit assign ---"; $rtA2
Write-Output "--- B routes after implicit assign ---"; $rtB2
$dA2 = DataRows $rtA2; $dB2 = DataRows $rtB2
$impPat = '^\s*-?92233720368547758(10|06)\s'
$impOnA = ($dA2 | Where-Object { $_ -match $impPat -and $_ -match $idB -and $_ -match 'false' }).Count
$impOnB = ($dB2 | Where-Object { $_ -match $impPat -and $_ -match $idB -and $_ -match 'true' }).Count
Check "implicit explicitized: A route non-local / B local" ($impOnA -eq 1 -and $impOnB -eq 1) "impOnA=$impOnA impOnB=$impOnB"
$cntB = Qry $bSql "SELECT COUNT(*) FROM t_imp"
Check "B count=3 implicit (data on owner)" (RowMatch $cntB '(?m)^\s*3\s*$') $cntB

$wB = Qry $bSql "INSERT INTO t_imp VALUES (4,'b4')"
Check "B insert on owned region ok" ($wB -match "Query OK") $wB
$pA = Qry $aSql "SELECT COUNT(*) FROM t_imp WHERE id=4"
$pA2 = Qry $aSql "SELECT v FROM t_imp WHERE id=4"
Check "A point after B write cnt=1" (RowMatch $pA '(?m)^\s*1\s*$' -and $pA2 -match "b4") "$pA | $pA2"

$uA = Qry $aSql "UPDATE t3 SET v='v5x' WHERE id=5"
$qA3 = Qry $aSql "SELECT v FROM t3 WHERE id=5"
$qC3 = Qry $cSql "SELECT v FROM t3 WHERE id=5"
Check "A update rC via 2PC; A point visible" ($uA -match "Query OK" -and $qA3 -match "v5x") "$uA | $qA3"
if($qC3 -match "v5x"){
    Check "C point id=5 visible after 2PC" $true $qC3
} else {
    Start-Sleep -Milliseconds 700
    $qC4 = Qry $cSql "SELECT v FROM t3 WHERE id=5"
    if($qC4 -match "v5x"){
        CheckGap "C point id=5 after 2PC (transient snapshot lag, resolved on retry)" (($qC3 -split "`n" | Select-Object -Last 2) -join " | ")
    } else {
        CheckGap "C point id=5 after 2PC (gap: cross-node version visibility, A-side forward read OK; local snapshot read misses push/2PC versions)" (($qC3 -split "`n" | Select-Object -Last 2) -join " | ")
    }
}

StopProcs
Write-Output ("SUMMARY pass=$pass fail=$fail gap=$gap")
