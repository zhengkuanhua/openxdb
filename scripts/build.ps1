# OpenXDB Windows 可复现构建脚本（P0-1）
# 用法:
#   .\scripts\build.ps1                          # 自动探测 RocksDB，构建 openxdb.exe
#   .\scripts\build.ps1 -RocksSrcDir C:\rocksdb-src -RocksBuildDir C:\rocksdb-build
#   .\scripts\build.ps1 -RunTests                # 构建后跑 go test ./...
#   .\scripts\build.ps1 -SkipAllBuild            # 仅设置环境（供手动 go build 使用）
# 探测顺序（未显式指定时）:
#   1. 环境变量 ROCKSDB_SRC / ROCKSDB_BUILD
#   2. C:\rocksdb-src / C:\rocksdb-build（CI 约定路径）
#   3. D:\AI项目\OpenXDB开源数据库开发\OpenXDBTools\rocksdb-* / rocksdb-build
param(
    [string]$RocksSrcDir = "",
    [string]$RocksBuildDir = "",
    [string]$OutDir = "",
    [switch]$RunTests,
    [switch]$SkipAllBuild
)
$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot

function Resolve-RocksDirs {
    param([string]$Src, [string]$Build)
    if (-not $Src) {
        if ($env:ROCKSDB_SRC) { $Src = $env:ROCKSDB_SRC }
        elseif (Test-Path "C:\rocksdb-src") { $Src = "C:\rocksdb-src" }
        else {
            $cand = Get-ChildItem "D:\AI项目\OpenXDB开源数据库开发\OpenXDBTools" -Directory -ErrorAction SilentlyContinue |
                Where-Object { $_.Name -like "rocksdb-*" } | Select-Object -First 1
            if ($cand) { $Src = $cand.FullName }
        }
    }
    if (-not $Build) {
        if ($env:ROCKSDB_BUILD) { $Build = $env:ROCKSDB_BUILD }
        elseif (Test-Path "C:\rocksdb-build") { $Build = "C:\rocksdb-build" }
        else {
            $cand = "D:\AI项目\OpenXDB开源数据库开发\OpenXDBTools\rocksdb-build"
            if (Test-Path $cand) { $Build = $cand }
        }
    }
    if ($Src) {
        $hdrDir = Join-Path $Src "include\rocksdb"
        $hdrOk = (Test-Path $hdrDir) -and ((Get-ChildItem $hdrDir -Filter "*.h" -ErrorAction SilentlyContinue | Measure-Object).Count -gt 0)
    } else {
        $hdrOk = $false
    }
    if (-not $hdrOk) { throw "RocksDB source include not found (missing include\rocksdb\*.h). Pass -RocksSrcDir or set ROCKSDB_SRC." }
    if (-not $Build) {
        $lib = Get-ChildItem $Src -Recurse -Filter "librocksdb*.a" -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($lib) { $Build = $lib.DirectoryName }
    }
    if (-not $Build -or -not (Get-ChildItem $Build -Filter "*.a" -ErrorAction SilentlyContinue)) {
        throw "RocksDB static library not found. Pass -RocksBuildDir or set ROCKSDB_BUILD."
    }
    return @{ Src = $Src; Build = $Build }
}

$dirs = Resolve-RocksDirs $RocksSrcDir $RocksBuildDir
Write-Host "[build] RocksDB src   = $($dirs.Src)"
Write-Host "[build] RocksDB build = $($dirs.Build)"

$env:CGO_ENABLED = "1"
$env:CGO_CFLAGS = "-I$($dirs.Src)\include"
$env:CGO_CXXFLAGS = "-I$($dirs.Src)\include"
$env:CGO_LDFLAGS = "-L$($dirs.Build) -lrocksdb"

if ($SkipAllBuild) {
    Write-Host "[build] CGO_CFLAGS=$env:CGO_CFLAGS"
    Write-Host "[build] CGO_LDFLAGS=$env:CGO_LDFLAGS"
    Write-Host "[build] environment ready (skip build)"
    exit 0
}

if (-not $OutDir) { $OutDir = Join-Path $repo "bin" }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$exe = Join-Path $OutDir "openxdb.exe"

Write-Host "[build] go build ./cmd/openxdb -> $exe"
Push-Location $repo
try {
    & go build -o $exe ./cmd/openxdb
    if ($LASTEXITCODE -ne 0) { throw "go build ./cmd/openxdb failed (exit $LASTEXITCODE)" }
    Write-Host "[build] OK: $exe"

    Write-Host "[build] go build ./... (verify whole repo)"
    & go build ./...
    if ($LASTEXITCODE -ne 0) { throw "go build ./... failed (exit $LASTEXITCODE)" }
    Write-Host "[build] OK: go build ./..."

    if ($RunTests) {
        Write-Host "[build] go test ./..."
        & go test ./...
        if ($LASTEXITCODE -ne 0) { throw "go test ./... failed (exit $LASTEXITCODE)" }
        Write-Host "[build] OK: go test ./..."
    }
} finally {
    Pop-Location
}
