# =====================================================================
# T38 real-user validation orchestration (Windows PowerShell)
# Stages: init/start -> SQL scripts (01-04) -> Python driver E2E
#         -> CLI toolchain -> replication slave -> mysqldump import
#         -> CDC subscriber -> TLS -> summary
# Usage: powershell -ExecutionPolicy Bypass -File run_validation.ps1
# =====================================================================
#Requires -Version 5.1
param(
  [string]$Root = 'D:\AI项目\OpenXDB开源数据库开发\openxdb',
  [string]$WorkRoot = 'C:\Users\27756\AppData\Roaming\Tencent\Marvis\User\oAN1i2RORm9ewWWUXrcT2XBfRk0c\workspace\conv_5d15f4c8d8ec46778266acaa06c35c33\temp\openxdb-t38',
  [int]$Port = 7788,
  [int]$ReplicaPort = 7789,
  [int]$MetricsPort = 7790,
  [int]$TlsPort = 7799,
  [int]$AuthPort = 7787,
  [int]$ImportPort = 7802,
  [bool]$CleanWork = $true
)
$Bin = Join-Path $Root 'bin\openxdb.exe'
$PyDir = Join-Path $Root 'drivers\python'
$Results = Join-Path $Root 'validation\t38\results'
$V38 = Join-Path $Root 'validation\t38'
# 默认清理临时工作目录，确保每次在干净数据目录上执行完整验证（重跑幂等不靠遗留数据）
if ($CleanWork -and (Test-Path $WorkRoot)) {
  Remove-Item -Path $WorkRoot -Recurse -Force -ErrorAction SilentlyContinue
}
New-Item -ItemType Directory -Force $WorkRoot, $Results | Out-Null
# _ops.py 从项目资产复制到工作目录（工作目录每次清理重建）
Copy-Item -Path (Join-Path $V38 '_ops.py') -Destination $WorkRoot -Force
$masterDir = Join-Path $WorkRoot 'master'
$slaveDir = Join-Path $WorkRoot 'slave'
$tlsDir = Join-Path $WorkRoot 'tls'
$impDir = Join-Path $WorkRoot 'sample_import'
$bakPath = (Join-Path $WorkRoot 't38-backup.bak').Replace('\','/')
$certPem = Join-Path $WorkRoot 'cert.pem'
$keyPem  = Join-Path $WorkRoot 'key.pem'
$ops     = Join-Path $WorkRoot '_ops.py'

$env:CGO_CFLAGS   = '-ID:\AI项目\OpenXDB开源数据库开发\OpenXDBTools\rocksdb-11.8.1\include'
$env:CGO_CXXFLAGS = '-ID:\AI项目\OpenXDB开源数据库开发\OpenXDBTools\rocksdb-11.8.1\include'
$env:CGO_LDFLAGS  = '-LD:\AI项目\OpenXDB开源数据库开发\OpenXDBTools\rocksdb-build -lrocksdb -lm'

function Log($m) { Write-Host ("[{0}] {1}" -f (Get-Date -Format HH:mm:ss), $m) }
$script:FAILED = 0
function Mark($name, $ok, $detail = '') {
  $st = 'PASS'
  if (-not $ok) { $st = 'FAIL'; $script:FAILED++ }
  Log ("  [{0}] {1} {2}" -f $st, $name, $detail)
  Add-Content -Path (Join-Path $Results 'run.log') -Value ("[{0}] {1} {2}" -f $st, $name, $detail) -Encoding UTF8
}
function Invoke-PyOp([string[]]$ArgsList, [string]$OutFile) {
  $o = (& python $ops @ArgsList 2>&1 | Out-String)
  if ($OutFile) { $o | Out-File $OutFile -Encoding UTF8 }
  return $o
}

$procs = New-Object System.Collections.ArrayList
function Init-DataDir([string]$dir, [string]$tag) {
  $nonEmpty = (Test-Path $dir) -and ((Get-ChildItem $dir -Force -ErrorAction SilentlyContinue | Measure-Object).Count -gt 0)
  if ($nonEmpty) { Mark ("init {0}" -f $tag) $true '(exists, skipped)'; return $true }
  & $Bin init --data-dir $dir *> (Join-Path $Results ("step01_init_{0}.txt" -f $tag))
  Mark ("init {0}" -f $tag) ($LASTEXITCODE -eq 0)
  return $false
}
function Start-Daemon([string]$Name, [string]$ArgsLine, [string]$OutF, [string]$ErrF) {
  $p = Start-Process -FilePath $Bin -ArgumentList $ArgsLine -PassThru `
      -RedirectStandardOutput $OutF -RedirectStandardError $ErrF
  [void]$procs.Add($p)
  return $p
}
function Stop-Daemons() {
  foreach ($p in $procs) { if (-not $p.HasExited) { try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch {} } }
}

try {
  # ---------- 0. basics ----------
  Mark 'binary exists' (Test-Path $Bin)
  $py = (Get-Command python -ErrorAction SilentlyContinue).Source
  if (-not $py) { $py = (Get-Command py -ErrorAction SilentlyContinue).Source }
  Mark 'python available' ([bool]$py) $py

  # ---------- 1. master init + start ----------
  $null = Init-DataDir $masterDir 'master'
  $null = Start-Daemon 'master' "start --data-dir `"$masterDir`" --port $Port --replica-port $ReplicaPort --metrics-port $MetricsPort --log-level info" `
    (Join-Path $Results 'step02_master_stdout.txt') (Join-Path $Results 'step02_master_stderr.txt')
  $ready = Invoke-PyOp @('wait', "$Port") $null
  Mark 'master ready' ($ready -match 'ready') $ready.Trim()

  # ---------- 2. SQL scripts 01-04 ----------
  $i = 0
  foreach ($sql in @('01_schema_business.sql','02_txn_acid.sql','03_query_analysis.sql','04_ops_ecosystem.sql')) {
    $i++
    $out = Invoke-PyOp @('execsql', "$Port", (Join-Path $V38 $sql)) (Join-Path $Results ("step03_sql_{0}_{1}.txt" -f $i, $sql.Replace('.sql','')))
    $m = [regex]::Match($out, 'ok=(\d+) errs=(\d+)')
    $okN = if ($m.Success) { [int]$m.Groups[1].Value } else { -1 }
    $erN = if ($m.Success) { [int]$m.Groups[2].Value } else { -1 }
    Mark ("SQL script {0}" -f $sql) ($okN -gt 0 -and $erN -eq 0) "ok=$okN err=$erN"
  }

  # ---------- 3. TLS instance ----------
  & $Bin gen-cert --cert $certPem --key $keyPem --cn localhost --days 365 *> (Join-Path $Results 'step04_gencert.txt')
  Mark 'gen-cert created' (Test-Path $certPem)
  $null = Init-DataDir $tlsDir 'tls'
  $null = Start-Daemon 'tls' "start --data-dir `"$tlsDir`" --port $TlsPort --tls-cert `"$certPem`" --tls-key `"$keyPem`" --log-level info" `
    (Join-Path $Results 'step06_tls_stdout.txt') (Join-Path $Results 'step06_tls_stderr.txt')
  $tready = Invoke-PyOp @('wait', "$TlsPort", '1', $certPem) $null
  Mark 'tls instance ready' ($tready -match 'ready') $tready.Trim()

  # ---------- 3b. standalone auth instance (M11 AUTH / RBAC) ----------
  $authDir = Join-Path $WorkRoot 'auth'
  $null = Init-DataDir $authDir 'auth'
  $null = Start-Daemon 'auth' "start --data-dir `"$authDir`" --port $AuthPort --auth --root-password rootpw_t38 --log-level info" `
    (Join-Path $Results 'step05b_auth_stdout.txt') (Join-Path $Results 'step05b_auth_stderr.txt')
  $aready = Invoke-PyOp @('wait', "$AuthPort") $null
  Mark 'auth instance ready' ($aready -match 'ready') $aready.Trim()
  if ($aready -match 'ready') {
    $env:OPENXDB_USER = 'root'; $env:OPENXDB_PASSWORD = 'rootpw_t38'
    $aout = Invoke-PyOp @('execsql', "$AuthPort", (Join-Path $V38 '01_schema_business.sql')) (Join-Path $Results 'step05b_sql_01_auth.txt')
    Remove-Item Env:OPENXDB_USER -ErrorAction SilentlyContinue; Remove-Item Env:OPENXDB_PASSWORD -ErrorAction SilentlyContinue
    $am = [regex]::Match($aout, 'ok=(\d+) errs=(\d+)')
    $aok = if ($am.Success) { [int]$am.Groups[1].Value } else { -1 }
    $aer = if ($am.Success) { [int]$am.Groups[2].Value } else { -1 }
    Mark 'auth schema loaded' ($aok -gt 0 -and $aer -eq 0) "ok=$aok err=$aer"
  } else { Mark 'auth schema loaded' $false $aready.Trim() }

  # ---------- 4. Python driver E2E ----------
  $vOut = (& python (Join-Path $V38 'validate_real_user.py') --port $Port --tls-port $TlsPort --tls-ca $certPem --auth-port $AuthPort --backup $bakPath --result (Join-Path $Results 'validate_results.json') 2>&1 | Out-String)
  $vOut | Out-File (Join-Path $Results 'step07_validate_real_user.txt') -Encoding UTF8
  Mark 'python driver E2E' ($LASTEXITCODE -eq 0) "exit=$LASTEXITCODE"
  ($vOut -split "`n" | Where-Object { $_ -match '^\[(PASS|FAIL|INFO)\]' }) | ForEach-Object { Log "    $_" }

  # ---------- 5. CLI toolchain ----------
  foreach ($cli in @(
    @{ n = 'doctor';   a = @('doctor', '--data-dir', $masterDir, '--addr', "127.0.0.1:$Port") },
    @{ n = 'status';   a = @('status', '--addr', "127.0.0.1:$Port") },
    @{ n = 'stats';    a = @('stats', '--addr', "127.0.0.1:$Port") },
    @{ n = 'metrics';  a = @('metrics', '--addr', "127.0.0.1:$Port") }
  )) {
    $o = (& $Bin @($cli.a) 2>&1 | Out-String)
    $o | Out-File (Join-Path $Results ("step08_cli_{0}.txt" -f $cli.n)) -Encoding UTF8
    $pass = if ($cli.n -eq 'doctor') { $LASTEXITCODE -le 1 } else { $LASTEXITCODE -eq 0 }
    Mark ("CLI {0}" -f $cli.n) $pass "exit=$LASTEXITCODE"
  }

  # ---------- 6. replication slave ----------
  $null = Init-DataDir $slaveDir 'slave'
  $slaveData = Join-Path $slaveDir 'data'
  $baseBytes = (Get-ChildItem $slaveData -Recurse -File -ErrorAction SilentlyContinue | Measure-Object Length -Sum).Sum
  $null = Start-Daemon 'replica' "replica --data-dir `"$slaveDir`" --master 127.0.0.1:$ReplicaPort" `
    (Join-Path $Results 'step10_replica_stdout.txt') (Join-Path $Results 'step10_replica_stderr.txt')
  Start-Sleep -Seconds 10
  $afterBytes = (Get-ChildItem $slaveData -Recurse -File -ErrorAction SilentlyContinue | Measure-Object Length -Sum).Sum
  $repOk = $afterBytes -gt $baseBytes
  Mark 'replica synced' $repOk ("slave data bytes $baseBytes -> $afterBytes")
  $dOut = (& $Bin doctor --data-dir $slaveDir 2>&1 | Out-String)
  $dOut | Out-File (Join-Path $Results 'step11_doctor_slave.txt') -Encoding UTF8
  Mark 'doctor on slave' ($LASTEXITCODE -le 1)
  Stop-Daemons
  $procs.Clear()

  # ---------- 7. mysqldump import ----------
  $impSkipped = Init-DataDir $impDir 'import'
  if ($impSkipped) {
    Mark 'mysqldump import' $true '(exists, skipped)'
  } else {
    $imp = (& $Bin import --data-dir $impDir --dump (Join-Path $V38 't38_mysqldump_sample.sql') 2>&1 | Out-String)
    $imp | Out-File (Join-Path $Results 'step13_import_mysqldump.txt') -Encoding UTF8
    Mark 'mysqldump import' ($LASTEXITCODE -eq 0) $imp.Trim()
  }
  $null = Start-Daemon 'import' "start --data-dir `"$impDir`" --port $ImportPort --log-level error" `
    (Join-Path $Results 'step14_import_stdout.txt') (Join-Path $Results 'step14_import_stderr.txt')
  $impReady = Invoke-PyOp @('wait', "$ImportPort") $null
  if ($impReady -match 'ready') {
    $cnt = Invoke-PyOp @('query', "$ImportPort", 'SELECT COUNT(*) FROM legacy_customers') $null
    Mark 'imported data queryable' ($cnt -match '3') $cnt.Trim()
    Stop-Daemons; $procs.Clear()
  } else { Mark 'import instance ready' $false $impReady.Trim(); Stop-Daemons; $procs.Clear() }

  # ---------- 8. CDC subscriber ----------
  $sub = Join-Path $WorkRoot 'binlog_subscriber.exe'
  Push-Location $Root
    $b = (& go build -o $sub examples\binlog_subscriber.go 2>&1 | Out-String)
    Pop-Location
  if ($LASTEXITCODE -eq 0) {
    $c = (& $sub --binlog (Join-Path $masterDir 'binlog.log') --offset (Join-Path $WorkRoot 'sub.offset') --limit 30 2>&1 | Out-String)
    $c | Out-File (Join-Path $Results 'step15_cdc_subscriber.txt') -Encoding UTF8
    $hasEvents = $c -match 'INSERT|UPDATE|DELETE|BEGIN|COMMIT|LSN|event'
    Mark 'CDC subscriber events' ($c.Length -gt 0 -and $hasEvents) ("chars=" + $c.Length)
  } else {
    Mark 'CDC build' $false ($b.Trim())
  }

  # ---------- 9. offline preflight (all daemons stopped) ----------
  $pf = (& $Bin preflight --data-dir $masterDir 2>&1 | Out-String)
  $pf | Out-File (Join-Path $Results 'step16_preflight_offline.txt') -Encoding UTF8
  Mark 'preflight offline' ($LASTEXITCODE -eq 0) "exit=$LASTEXITCODE"

  # ---------- 10. summary ----------
  $summary = "T38 real user validation - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')`nFAILED markers: $script:FAILED`n"
  $summary | Out-File (Join-Path $Results 'SUMMARY.txt') -Encoding UTF8
  Log "T38 orchestration done, FAILED=$script:FAILED, results in: $Results"
  exit $script:FAILED
}
finally {
  Stop-Daemons
}
