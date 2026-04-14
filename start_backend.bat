@echo off
title TradeMachine Backend
color 0A

echo ============================================================
echo   INNER CIRCLE - TRADEMACHINE BACKEND
echo ============================================================
echo.

REM Uccidi eventuali processi sulla porta 8001
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8001 " ^| findstr "LISTENING"') do (
    echo Processo %%a su porta 8001 - KILL
    taskkill /PID %%a /F >nul 2>&1
    timeout /t 1 /nobreak >nul
)

cd /d "%~dp0backend"
echo Cartella: %CD%
echo.
echo Avvio server (auto-restart se crasha)...
echo ============================================================
echo.
python run.py
echo.
echo Server terminato.
pause
