@echo off
echo === Inner Circle TradeMachine ===

:: Avvia frontend
start "TradeMachine Frontend" cmd /k "cd /d "%~dp0frontend" && npm run dev"

:: Avvia backend con auto-restart
start "TradeMachine Backend" cmd /k "%~dp0start_backend.bat"

echo.
echo Frontend: http://localhost:3000
echo Backend:  http://localhost:8001
echo.
pause >/dev/null
