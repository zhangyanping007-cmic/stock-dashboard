@echo off
chcp 65001 >nul
cd /d "%~dp0"

set PY=
where python >nul 2>nul && set PY=python
if "%PY%"=="" where py >nul 2>nul && set PY=py
if "%PY%"=="" (
  echo [ERROR] Python not found. Please install Python 3.8 or newer.
  pause
  exit /b 1
)

echo ============================================================
echo   Stock Market Dashboard - local server
echo ============================================================
echo   Browser will open automatically in 3 seconds.
echo   Press Ctrl+C to stop the server.
echo ============================================================
echo.

start "" cmd /c "timeout /t 3 >nul & start "" http://localhost:8765/dashboard.html"

%PY% server.py
pause
