# D3.3 diagnostic: reproduce S1/S2/S4 and dump visible id sets per node
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $root)))
$bin  = Join-Path $repo "bin"
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$work = Join-Path $env:TEMP ("openxdb_d33diag_" + $stamp)
$dirA = Join-Path $work "node-a"; $dirB = Join-Path $work "node-b"; $dirC = Join-Path $work "node-c"
$openxdb = Join-Path $bin "openxdb.exe"; $oxsql = Join-Path $bin "oxsql.exe"
$aTcp=17981; $bTcp=17982; $cTcp=17983
$aSql=13416; $bSql=13417; $cSql=13418
function Qry($port,$sql){ $out = & $oxsql -h 127.0.0.1 -P $port -u root -c $sql 2>&1; return ($out -join "`n") }
function WaitPort($port,$timeoutSec){ $dl=(Get-Date).AddSeconds($timeoutSec); while((Get-Date)-lt $dl){ $c=New-Object System.Net.Sockets.TcpClient; try{$c.Connect("127.0.0.1",$port);$c.Close();return $true}catch{$c.Dispose()}; Start-Sleep -Milliseconds 300 }; return $false }
function StopProcs(){ Get-Process openxdb -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue }
function DataRows([string]$text) { return @($text -split "`n" | Where-Object { $t=$_.Trim(); $t -ne "" -and $t -notmatch "^(region_id|Connected to)" -and $t -notmatch "row\(s\) in set$" }) }
function Cell0($row){ ($row -split "`t")[0].Trim() }
StopProcs
foreach($d in @($dirA,$dirB,$dirC)){ New-Item -ItemType Directory -Force -Path $d | Out-Null }
& $openxdb init --data-dir $dirA 2>&1 | Out-Null
& $openxdb init --data-dir $dirB 2>&1 | Out-Null
& $openxdb init --data-dir $dirC 2>&1 | Out-Null
Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirA,"--port",$aTcp,"--mysql-port",$aSql,"--cluster-addr","127.0.0.1:$aTcp","--shutdown-timeout","1") -WindowStyle Hidden | Out-Null
Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirB,"--port",$bTcp,"--mysql-port",$bSql,"--cluster-addr","127.0.0.1:$bTcp","--shutdown-timeout","1") -WindowStyle Hidden | Out-Null
Start-Process -FilePath $openxdb -ArgumentList @("start","--data-dir",$dirC,"--port",$cTcp,"--mysql-port",$cSql,"--cluster-addr","127.0.0.1:$cTcp","--shutdown-timeout","1") -WindowStyle Hidden | Out-Null
if(-not ((WaitPort $aSql 15) -and (WaitPort $bSql 15) -and (WaitPort $cSql 15))){ Write-Output "nodes not up"; exit 1 }
$n1 = Qry $aSql "ADD NODE '127.0.0.1:$bTcp'"; $n2 = Qry $aSql "ADD NODE '127.0.0.1:$cTcp'"
$nodesA = Qry $aSql "SHOW NODES"
$idA="";$idB="";$idC=""
foreach($nr in @($nodesA -split "`n")){ $cols = $nr.TrimEnd("`r") -split "`t"; if($cols.Count -ge 2){ $addr=$cols[1].Trim(); if($addr -eq "127.0.0.1:$aTcp"){$idA=$cols[0].Trim()}elseif($addr -eq "127.0.0.1:$bTcp"){$idB=$cols[0].Trim()}elseif($addr -eq "127.0.0.1:$cTcp"){$idC=$cols[0].Trim()} } }
Write-Output "ids A=$idA B=$idB C=$idC"
Qry $aSql "CREATE TABLE t3 (id INT PRIMARY KEY, v VARCHAR(64))" | Out-Null
1..9 | ForEach-Object { Qry $aSql "INSERT INTO t3 VALUES ($_,'v$_')" | Out-Null }
Qry $bSql "CREATE TABLE t3 (id INT PRIMARY KEY, v VARCHAR(64))" | Out-Null
Qry $cSql "CREATE TABLE t3 (id INT PRIMARY KEY, v VARCHAR(64))" | Out-Null
$s1 = Qry $aSql "SPLIT REGION 9223372036854775809"
$routes1 = Qry $aSql "SHOW REGION ROUTES"
$rid2 = ""
foreach($row in (DataRows $routes1)){ $rid = Cell0 $row; if($rid -ne "9223372036854775809"){ $rid2=$rid; break } }
Qry $aSql "SPLIT REGION $rid2" | Out-Null
$routes2 = Qry $aSql "SHOW REGION ROUTES"
$r2rows = DataRows $routes2
$rA = Cell0 $r2rows[0]; $rB = Cell0 $r2rows[1]; $rC = Cell0 $r2rows[2]
Write-Output "regions rA=$rA rB=$rB rC=$rC"
Qry $aSql "ASSIGN REGION $rB TO NODE '$idB'" | Out-Null
Qry $aSql "ASSIGN REGION $rC TO NODE '$idC'" | Out-Null
Start-Sleep -Milliseconds 300
Write-Output "=== routes @A ==="; Qry $aSql "SHOW REGION ROUTES"
Write-Output "=== routes @B ==="; Qry $bSql "SHOW REGION ROUTES"
Write-Output "=== routes @C ==="; Qry $cSql "SHOW REGION ROUTES"
Write-Output "=== A sees after 9 ==="
(Qry $aSql "SELECT id FROM t3 ORDER BY id")
Write-Output "=== B sees after 9 ==="
(Qry $bSql "SELECT id FROM t3 ORDER BY id")
Write-Output "=== C sees after 9 ==="
(Qry $cSql "SELECT id FROM t3 ORDER BY id")
10..18 | ForEach-Object { Qry $bSql "INSERT INTO t3 VALUES ($_,'v$_')" | Out-Null }
19..27 | ForEach-Object { Qry $cSql "INSERT INTO t3 VALUES ($_,'v$_')" | Out-Null }
Write-Output "=== A sees after 27 ==="
(Qry $aSql "SELECT id FROM t3 ORDER BY id")
Write-Output "=== B sees after 27 ==="
(Qry $bSql "SELECT id FROM t3 ORDER BY id")
Write-Output "=== C sees after 27 ==="
(Qry $cSql "SELECT id FROM t3 ORDER BY id")
Write-Output "=== A COUNT/SUM ==="; (Qry $aSql "SELECT COUNT(*), SUM(id) FROM t3")
Write-Output "=== B COUNT/SUM ==="; (Qry $bSql "SELECT COUNT(*), SUM(id) FROM t3")
Write-Output "=== C COUNT/SUM ==="; (Qry $cSql "SELECT COUNT(*), SUM(id) FROM t3")
Write-Output "=== A point id=10 / 19 ==="; (Qry $aSql "SELECT v FROM t3 WHERE id=10"); (Qry $aSql "SELECT v FROM t3 WHERE id=19")
Write-Output "=== B point id=10 / 19 ==="; (Qry $bSql "SELECT v FROM t3 WHERE id=10"); (Qry $bSql "SELECT v FROM t3 WHERE id=19")
Write-Output "=== C point id=10 / 19 ==="; (Qry $cSql "SELECT v FROM t3 WHERE id=10"); (Qry $cSql "SELECT v FROM t3 WHERE id=19")
StopProcs
Write-Output "DONE"
