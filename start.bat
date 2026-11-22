@echo off
rem Math Gambling local console -- double-click to start; the UI opens in
rem your browser. To see the console window and live errors: start.bat debug
rem
rem Note: this must run under python, not pythonw. The stop button relies on
rem Windows console events; without a console no stop signal can be sent.
setlocal
cd /d "%~dp0"

if not exist "logs" mkdir "logs"

where python >nul 2>nul
if errorlevel 1 (
  echo [error] Python not found. Install Python 3.11 or newer first.
  echo         Download: https://www.python.org/downloads/
  pause
  exit /b 1
)

if /i "%~1"=="debug" goto debug

rem Start the console minimised; logs also go to logs\console.log
start "" /min python "app\server.py"
exit /b 0

:debug
python "app\server.py"
if errorlevel 1 pause
exit /b 0
