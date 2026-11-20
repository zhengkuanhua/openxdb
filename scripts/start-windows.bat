@echo off
chcp 65001 >nul
rem AIGC: 内容由AI生成，仅供参考
rem start-windows.bat - OpenXDB Windows 双击启动器（自动 init + 端口检查 + 出错停住窗口）
setlocal

set "DIR=%~dp0"
set "EXE=%DIR%openxdb.exe"
set "DATA=%DIR%data"
if not "%~1"=="" set "DATA=%~1"

echo ============================================================
echo   OpenXDB Windows 启动器
echo ============================================================

if not exist "%EXE%" (
    echo [错误] 未找到 openxdb.exe：%EXE%
    echo        请把本脚本与 openxdb.exe 放在同一目录下再双击运行。
    pause
    exit /b 1
)

if not exist "%DATA%\openxdb.conf" (
    echo [初始化] 数据目录尚未初始化，正在执行 init --data-dir "%DATA%"
    "%EXE%" init --data-dir "%DATA%"
    if errorlevel 1 (
        echo [错误] 数据目录初始化失败，请查看上方输出信息。
        pause
        exit /b 1
    )
)

netstat -ano | findstr ":7788" | findstr "LISTENING" >nul
if not errorlevel 1 (
    echo [错误] 端口 7788 已被占用，OpenXDB 无法启动。
    echo        如需释放端口，请另开窗口执行：taskkill /IM openxdb.exe /F
    pause
    exit /b 1
)

echo   数据目录 : %DATA%
echo   监听地址 : 0.0.0.0:7788（默认端口，可用 --port 修改）
echo   停止方式 : 在本窗口按 Ctrl+C；或另开窗口执行 taskkill /IM openxdb.exe /F
echo ============================================================
echo   正在启动 OpenXDB，日志输出如下：
echo.

"%EXE%" start --data-dir "%DATA%"
set "CODE=%ERRORLEVEL%"

echo.
echo [退出] OpenXDB 进程已结束，退出码：%CODE%
pause
endlocal
exit /b %CODE%
