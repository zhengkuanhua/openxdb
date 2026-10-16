# E5 benchmark orchestration for OpenXDB (Windows / PowerShell 5.1)
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File tests/bench/e5/run_e5.ps1 -Phase init
#   powershell -ExecutionPolicy Bypass -File tests/bench/e5/run_e5.ps1 -Phase start
#   powershell -ExecutionPolicy Bypass -File tests/bench/e5/run_e5.ps1 -Phase prepare
#   powershell -ExecutionPolicy Bypass -File tests/bench/e5/run_e5.ps1 -Phase run
#   powershell -ExecutionPolicy Bypass -File tests/bench/e5/run_e5.ps1 -Phase stop
#   powershell -ExecutionPolicy Bypass -File tests/bench/e5/run_e5.ps1 -Phase all
#
# Note: OpenXDB data dir is passed as a RELATIVE path and the server process
# is launched with WorkingDirectory=project root (absolute path with Chinese
# characters breaks RocksDB open). Server stdout/stderr are NOT redirected so
# the PowerShell session does not wait on inherited handles.
param(
    [ValidateSet('init','start','prepare','run','stop','all')]
    [string]$Phase = 'all',
    [int]$Port = 7797,
    [int]$OltpThreads = 8,
    [int]$OltpTime = 60,
    [int]$TpccThreads = 8,
    [int]$TpccTime = 120,
    [int]$TpccWarmup = 10,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
$Root = 'D:\AI项目\OpenXDB开源数据库开发\openxdb'
$DataDir = 'tests\bench\e5_data'
$Exe = Join-Path $Root 'bin\openxdb.exe'
$E5 = Join-Path $Root 'tests\bench\e5'
$PyDir = Join-Path $Root 'drivers\python'

function Invoke-Init {
    $absData = Join-Path $Root $DataDir
    if (Test-Path $absData) {
        $items = Get-ChildItem $absData -Force
        if ($items.Count -gt 0) {
            Write-Host '[init] data dir exists, skip init (idempotent)'
            return
        }
    }
    Write-Host '[init] initializing e5_data ...'
    Push-Location $Root
    try {
        & $Exe init --data-dir $DataDir 2>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'init failed' }
    } finally { Pop-Location }
    Write-Host '[init] done'
}

function Invoke-Build {
    if ($SkipBuild) { Write-Host '[build] skipped'; return }
    Write-Host '[build] go build ./... (CGO with RocksDB 11.8.1) ...'
    $env:CGO_CFLAGS = '-IC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-11.8.1\include'
    $env:CGO_CXXFLAGS = '-IC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-11.8.1\include'
    $env:CGO_LDFLAGS = '-LC:\Users\27756\AppData\Local\OpenXDBTools\rocksdb-build -lrocksdb -lm'
    Remove-Item Env:CGO_ENABLED -ErrorAction SilentlyContinue
    Push-Location $Root
    try {
        & go build ./... 2>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'go build ./... failed' }
        & go build -o bin\openxdb.exe .\cmd\openxdb 2>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'openxdb.exe build failed' }
    } finally { Pop-Location }
    Write-Host '[build] done'
}

function Invoke-Start {
    $proc = Get-Process openxdb -ErrorAction SilentlyContinue
    if ($proc) {
        Write-Host ('[start] existing openxdb pid(s): ' + ($proc.Id -join ','))
        $proc | Stop-Process -Force
        Start-Sleep -Seconds 2
    }
    Write-Host '[start] launching openxdb v0.27.0-alpha on :7797 (relative data dir) ...'
    Start-Process -FilePath $Exe -ArgumentList "start --data-dir $DataDir --port $Port --log-level info" -WorkingDirectory $Root
    Start-Sleep -Seconds 5
    $alive = Get-Process openxdb -ErrorAction SilentlyContinue
    if (-not $alive) { throw 'server did not start' }
    Write-Host ('[start] server pid=' + $alive.Id)
}

function Invoke-Prepare {
    Write-Host '[prepare] oltp ...'
    & python (Join-Path $E5 'bench_oltp.py') --port $Port --prepare
    if ($LASTEXITCODE -ne 0) { throw 'oltp prepare failed' }
    Write-Host '[prepare] tpcc ...'
    & python (Join-Path $E5 'bench_tpcc.py') --port $Port --prepare
    if ($LASTEXITCODE -ne 0) { throw 'tpcc prepare failed' }
    Write-Host '[prepare] done'
}

function Invoke-Run {
    foreach ($mode in @('read', 'write', 'read_write')) {
        Write-Host "[run] oltp mode=$mode ..."
        & python (Join-Path $E5 'bench_oltp.py') --port $Port --mode $mode --threads $OltpThreads --time $OltpTime
        if ($LASTEXITCODE -ne 0) { throw "oltp $mode failed" }
    }
    Write-Host "[run] tpcc (${TpccThreads} threads, ${TpccTime}s, warmup ${TpccWarmup}s) ..."
    & python (Join-Path $E5 'bench_tpcc.py') --port $Port --threads $TpccThreads --time $TpccTime --warmup $TpccWarmup
    if ($LASTEXITCODE -ne 0) { throw 'tpcc run failed' }
    Write-Host '[run] done'
}

function Invoke-Stop {
    $proc = Get-Process openxdb -ErrorAction SilentlyContinue
    if ($proc) {
        $proc | Stop-Process -Force
        Write-Host '[stop] server stopped'
    } else {
        Write-Host '[stop] no server process'
    }
}

switch ($Phase) {
    'init'   { Invoke-Build; Invoke-Init }
    'start'  { Invoke-Start }
    'prepare'{ Invoke-Prepare }
    'run'    { Invoke-Run }
    'stop'   { Invoke-Stop }
    'all'    { Invoke-Build; Invoke-Init; Invoke-Start; Invoke-Prepare; Invoke-Run; Invoke-Stop }
}
