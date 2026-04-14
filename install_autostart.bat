@echo off
title Installa Avvio Automatico TradeMachine
echo Installo TradeMachine nell'avvio automatico di Windows...
echo.

set "DIR=%~dp0"
set "DIR=%DIR:~0,-1%"

:: Task per il backend (avvio con login utente, finestra minimizzata)
schtasks /create /tn "TradeMachine Backend" /tr "cmd /c start /min \"TradeMachine Backend\" \"%DIR%\start_backend.bat\"" /sc onlogon /ru "%USERNAME%" /f
if errorlevel 1 (
    echo ERRORE: impossibile creare task backend
    pause
    exit /b 1
)
echo [OK] Task backend creato

:: Task per il frontend (avvio con login utente, finestra minimizzata)
schtasks /create /tn "TradeMachine Frontend" /tr "cmd /c start /min \"TradeMachine Frontend\" \"%DIR%\start_frontend.bat\"" /sc onlogon /ru "%USERNAME%" /f
if errorlevel 1 (
    echo ERRORE: impossibile creare task frontend
    pause
    exit /b 1
)
echo [OK] Task frontend creato

echo.
echo ============================================================
echo   Installazione completata!
echo   Backend e Frontend si avvieranno ad ogni login Windows.
echo   Per rimuovere: esegui remove_autostart.bat
echo ============================================================
pause
