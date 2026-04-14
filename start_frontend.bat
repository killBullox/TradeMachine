@echo off
title TradeMachine Frontend
color 0B

echo ============================================================
echo   INNER CIRCLE - TRADEMACHINE FRONTEND
echo ============================================================
echo.

cd /d "%~dp0frontend"
echo Cartella: %CD%
echo.

echo Avvio Vite...
echo Apri il browser all'indirizzo mostrato sotto (es. http://localhost:5173)
echo ------------------------------------------------------------
node_modules\.bin\vite
echo ------------------------------------------------------------
echo Frontend terminato.
pause
