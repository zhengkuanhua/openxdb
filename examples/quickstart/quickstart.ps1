# OpenXDB v1.17.0 end-to-end quickstart demo (Windows PowerShell).
#
# Pipeline: init -> REPL SQL demo (create/insert/secondary index/query ->
#           EXPLAIN -> SHOW SLOWQUERIES SUMMARY) -> start server ->
#           doctor / stats -> stop.
#
# No third-party dependencies: only the OpenXDB binary + REPL stdin pipe.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File examples\quickstart\quickstart.ps1
#   powershell -ExecutionPolicy Bypass -File examples\quickstart\quickstart.ps1 -OpenXDB .\bin\openxdb.exe -DataDir .\quickstart-data -Port 7788
#
# Options:
#   -OpenXDB     <path>  openxdb binary (default: .\bin\openxdb.exe, then PATH)
#   -DataDir     <path>  scratch data dir (default: ./quickstart-data)
#   -Port        <int>   TCP port for the demo server (default: 7788)
#   -SkipClean            keep an existing data dir instead of re-creating it

param(
    [string]$OpenXDB = "",
    [string]$DataDir = "./quickstart-data",
    [int]$Port = 7788,
    [switch]$SkipClean
)

$ErrorActionPreference = "Stop"

function Write-Step([string]$msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor Cyan
}

function Resolve-OpenXDB([string]$hint) {
    if ($hint -ne "" -and (Test-Path $hint)) {
        return (Resolve-Path $hint).Path
    }
    $candidates = @(
        (Join-Path $PWD "bin\openxdb.exe"),
        (Join-Path $PWD "bin\openxdb_new.exe")
    )
    foreach ($c in $candidates) {
        if (Test-Path $c) {
            return (Resolve-Path $c).Path
        }
    }
    $cmd = Get-Command openxdb -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Source
    }
    throw "Cannot locate openxdb binary. Pass -OpenXDB <path> or run go build first."
}

function Wait-Port([int]$port, [int]$timeoutSec = 20) {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $timeoutSec) {
        try {
            $c = New-Object System.Net.Sockets.TcpClient
            $c.Connect("127.0.0.1", $port)
            $c.Close()
            return $true
        } catch {
            Start-Sleep -Milliseconds 300
        }
    }
    return $false
}

$openxdb = Resolve-OpenXDB $OpenXDB
Write-Host "Using OpenXDB binary: $openxdb"

# ---- reset scratch data dir (idempotent re-runs) ----
if (-not $SkipClean) {
    if (Test-Path $DataDir) {
        Remove-Item -Recurse -Force $DataDir
    }
    # guard: make sure no stale demo server holds the old dir lock
    Get-Process -Name "openxdb" -ErrorAction SilentlyContinue |
        Where-Object { $_.Path -like "*openxdb*" } |
        ForEach-Object { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue }
}

# ---- 1. init ----
Write-Step "1/6 init --data-dir $DataDir"
& $openxdb init --data-dir $DataDir
if ($LASTEXITCODE -ne 0) { throw "init failed (exit $LASTEXITCODE)" }

# ---- 2. SQL demo via REPL stdin pipe (create/insert/index/query/explain/slowlog) ----
Write-Step "2/6 REPL SQL demo: DDL/DML/secondary index/EXPLAIN/SHOW SLOWQUERIES SUMMARY"

$bigValues = ((1..300) | ForEach-Object { "($_ , 'value-$_')" }) -join ", "

$sql = @"
CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id));
INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35);
CREATE INDEX idx_age ON users (age);
SELECT name FROM users WHERE age >= 25 AND age < 35 ORDER BY age;
EXPLAIN SELECT name FROM users WHERE id = 1;
EXPLAIN SELECT name FROM users WHERE age >= 25 ORDER BY name;
CREATE TABLE big (id INT, v TEXT, PRIMARY KEY (id));
INSERT INTO big (id, v) VALUES $bigValues;
SET slow_query_threshold = 1;
SELECT id FROM big WHERE id > 0;
SELECT id FROM big WHERE id > 100;
SELECT id FROM big WHERE id > 250;
SHOW SLOWQUERIES SUMMARY TOP 5;
SHOW SLOWQUERIES;
"@

$sql | & $openxdb repl --data-dir $DataDir
if ($LASTEXITCODE -ne 0) { throw "repl demo failed (exit $LASTEXITCODE)" }

# ---- 3. start server ----
Write-Step "3/6 start --data-dir $DataDir --port $Port (background)"
$server = Start-Process -FilePath $openxdb `
    -ArgumentList @("start", "--data-dir", $DataDir, "--port", "$Port") `
    -WorkingDirectory $PWD -PassThru -WindowStyle Hidden
if (-not (Wait-Port $Port)) {
    Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
    throw "server did not listen on port $Port in time"
}
Write-Host "server listening on 127.0.0.1:$Port (pid $($server.Id))"

# ---- 4. doctor (online health check) ----
Write-Step "4/6 doctor --data-dir $DataDir --addr 127.0.0.1:$Port"
& $openxdb doctor --data-dir $DataDir --addr "127.0.0.1:$Port"
if ($LASTEXITCODE -gt 1) { throw "doctor reported errors (exit $LASTEXITCODE)" }

# ---- 5. stats (live metrics) ----
Write-Step "5/6 stats --addr 127.0.0.1:$Port"
& $openxdb stats --addr "127.0.0.1:$Port"
if ($LASTEXITCODE -ne 0) { throw "stats failed (exit $LASTEXITCODE)" }

# ---- 6. stop ----
Write-Step "6/6 stop demo server"
Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
Wait-Process -Id $server.Id -ErrorAction SilentlyContinue
Write-Host "demo server stopped. Scratch data dir: $DataDir (delete it anytime)"

Write-Host ""
Write-Host "Quickstart demo finished OK. Next: openxdb repl --data-dir $DataDir" -ForegroundColor Green
