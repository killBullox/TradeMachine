@echo off
title Riavvio TradeMachine
color 0A
echo ============================================
echo   RIAVVIO TRADEMACHINE
echo ============================================
echo.
echo [1/3] Kill processo sulla porta 8002...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8002 " ^| findstr "LISTEN"') do taskkill /F /PID %%a
timeout /t 3 /nobreak >nul
echo.
echo [2/3] Avvio TradeMachine...
schtasks /Run /TN TradeMachine
timeout /t 10 /nobreak >nul
echo.
echo [3/3] Verifica...
curl -s http://127.0.0.1:8002/api/mt5/status
echo.
echo.
echo ============================================
echo   FATTO - chiudi questa finestra
echo ============================================
pause
