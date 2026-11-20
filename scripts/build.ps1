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

function Resolve-Objdump {
    $cmd = Get-Command objdump -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $cc = $null
    try { $cc = (& go env CC 2>$null) } catch { $cc = $null }
    if ($cc -and (Test-Path $cc)) {
        $cand = Join-Path (Split-Path -Parent $cc) "objdump.exe"
        if (Test-Path $cand) { return $cand }
    }
    $gcc = Get-Command gcc -ErrorAction SilentlyContinue
    if ($gcc) {
        $cand = Join-Path (Split-Path -Parent $gcc.Source) "objdump.exe"
        if (Test-Path $cand) { return $cand }
    }
    return $null
}

# Assert-StaticMinGWRuntime 用 objdump 校验 exe 导入表，确保不含 MinGW 私有运行时 DLL。
# 背景：动态链接时导入表会带 libwinpthread-1.dll，分发到未安装 MinGW 的机器上会在
# 进程启动阶段因缺该 DLL 直接静默退出（无任何报错），本函数即为该问题的构建期闸门。
function Get-PEImportDllNames {
    param([string]$ExePath)
    $bytes = [System.IO.File]::ReadAllBytes($ExePath)
    if ($bytes.Length -lt 0x40) { throw "not a PE file (too small): $ExePath" }
    $peOff = [BitConverter]::ToInt32($bytes, 0x3C)
    if ([BitConverter]::ToUInt32($bytes, $peOff) -ne 0x00004550) { throw "not a PE file (bad signature): $ExePath" }
    $numSec = [BitConverter]::ToUInt16($bytes, $peOff + 6)
    $optSize = [BitConverter]::ToUInt16($bytes, $peOff + 20)
    $optOff = $peOff + 24
    $magic = [BitConverter]::ToUInt16($bytes, $optOff)
    if ($magic -eq 0x20B) { $ddOff = $optOff + 112 } else { $ddOff = $optOff + 96 }
    $importRva = [BitConverter]::ToUInt32($bytes, $ddOff + 8)
    if ($importRva -eq 0) { return @() }
    $secOff = $optOff + $optSize
    $secs = @()
    for ($i = 0; $i -lt $numSec; $i++) {
        $s = $secOff + $i * 40
        $vsize = [BitConverter]::ToUInt32($bytes, $s + 8)
        $va = [BitConverter]::ToUInt32($bytes, $s + 12)
        $rawSize = [BitConverter]::ToUInt32($bytes, $s + 16)
        $rawPtr = [BitConverter]::ToUInt32($bytes, $s + 20)
        $size = $rawSize
        if ($vsize -gt $rawSize) { $size = $vsize }
        $secs += [pscustomobject]@{ VA = $va; Size = $size; Ptr = $rawPtr }
    }
    $rva2off = {
        param($rva)
        foreach ($sec in $secs) {
            if ($rva -ge $sec.VA -and $rva -lt ($sec.VA + $sec.Size)) { return [int]($sec.Ptr + ($rva - $sec.VA)) }
        }
        return -1
    }
    $names = @()
    $desc = & $rva2off $importRva
    while ($desc -ge 0 -and ($desc + 20) -le $bytes.Length) {
        $oft = [BitConverter]::ToUInt32($bytes, $desc)
        $nameRva = [BitConverter]::ToUInt32($bytes, $desc + 12)
        $firstThunk = [BitConverter]::ToUInt32($bytes, $desc + 16)
        if ($nameRva -eq 0 -and $oft -eq 0 -and $firstThunk -eq 0) { break }
        $nameOff = & $rva2off $nameRva
        if ($nameOff -lt 0) { break }
        $sb = New-Object System.Text.StringBuilder
        while ($nameOff -lt $bytes.Length -and $bytes[$nameOff] -ne 0) {
            [void]$sb.Append([char]$bytes[$nameOff])
            $nameOff++
        }
        $names += $sb.ToString()
        $desc += 20
    }
    return $names
}

# Assert-StaticMinGWRuntime 校验 exe 导入表，确保不含 MinGW 私有运行时 DLL。
# 背景：动态链接时导入表会带 libwinpthread-1.dll，分发到未安装 MinGW 的机器上会在
# 进程启动阶段因缺该 DLL 直接静默退出（无任何报错），本函数即为该问题的构建期闸门。
# 优先用 objdump 读取；objdump 缺失或执行失败（例如仓库位于非 ASCII 路径下、MinGW
# 工具无法解析该路径）时回退到内置 PE 导入表解析，保证校验一定会执行而不是被跳过。
function Assert-StaticMinGWRuntime {
    param([string]$ExePath)
    $imports = @()
    $source = "objdump"
    $objdump = Resolve-Objdump
    if ($objdump) {
        $out = & $objdump -p $ExePath 2>&1
        if ($LASTEXITCODE -eq 0) {
            foreach ($line in $out) {
                $text = "$line"
                $idx = $text.IndexOf("DLL Name:")
                if ($idx -ge 0) { $imports += $text.Substring($idx + 9).Trim() }
            }
        } else {
            Write-Host "[build] WARN: objdump could not read $ExePath (exit $LASTEXITCODE); falling back to built-in PE parser"
        }
    } else {
        Write-Host "[build] WARN: objdump not found; falling back to built-in PE parser"
    }
    if ($imports.Count -eq 0) {
        $imports = @(Get-PEImportDllNames -ExePath $ExePath)
        $source = "built-in PE parser"
    }
    if ($imports.Count -eq 0) {
        throw "[build] could not read the import table of $ExePath; refusing to ship an unverified binary"
    }
    $runtimeDlls = @('libwinpthread-1.dll', 'libgcc_s_seh-1.dll', 'libgcc_s_dw2-1.dll', 'libstdc++-6.dll', 'libssp-0.dll')
    $hits = @($imports | Where-Object { $runtimeDlls -contains $_.ToLower() })
    if ($hits.Count -gt 0) {
        Write-Host "[build] ERROR: import table still depends on MinGW runtime DLL(s) [$source]:"
        $hits | ForEach-Object { Write-Host "         - $_" }
        throw ("[build] Windows release package must link the MinGW runtime statically: this exe imports " + ($hits -join ', ') +
            ", and on a machine without MinGW installed the process exits silently at startup (missing DLL, no error message shown to the user). " +
            "Check that CGO_LDFLAGS contains '$MinGWStaticFlags' and that your MinGW provides static libs (libwinpthread.a / libgcc.a / libstdc++.a).")
    }
    Write-Host "[build] OK: no MinGW runtime DLL in import table via $source (statically linked; $($imports.Count) system DLL imports)"
}

$dirs = Resolve-RocksDirs $RocksSrcDir $RocksBuildDir
Write-Host "[build] RocksDB src   = $($dirs.Src)"
Write-Host "[build] RocksDB build = $($dirs.Build)"

$env:CGO_ENABLED = "1"
$env:CGO_CFLAGS = "-I$($dirs.Src)\include"
$env:CGO_CXXFLAGS = "-I$($dirs.Src)\include"

# Windows 发行包必须静态链接 MinGW 运行时（GCC / libstdc++ / winpthread）。
# 动态链接时 exe 的导入表会带 libwinpthread-1.dll 等 MinGW 私有 DLL，分发到
# 未安装 MinGW 的机器上会在进程启动阶段因找不到该 DLL 而直接静默退出（无任何
# 报错输出，表现为"进程飞掉"），且该 DLL 不在系统目录中，用户无从自查。
# 因此这里固定追加 -static -static-libgcc -static-libstdc++，构建后再用 objdump
# 校验导入表（见 Assert-StaticMinGWRuntime），绝不允许动态依赖流出。
$MinGWStaticFlags = "-static -static-libgcc -static-libstdc++"
$env:CGO_LDFLAGS = "-L$($dirs.Build) -lrocksdb $MinGWStaticFlags"

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

    # 构建后强制校验导入表：不含 MinGW 运行时 DLL 才算合格产物
    Assert-StaticMinGWRuntime -ExePath $exe

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
