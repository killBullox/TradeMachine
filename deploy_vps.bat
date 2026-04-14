@echo off
title TradeMachine - Deploy VPS Setup
color 0A

echo ============================================================
echo   INNER CIRCLE TRADEMACHINE - SETUP VPS
echo ============================================================
echo.

REM 1. Installa dipendenze Python
echo [1/4] Installazione dipendenze Python...
cd /d "%~dp0backend"
pip install -r requirements.txt
echo.

REM 2. Build frontend
echo [2/4] Build frontend produzione...
cd /d "%~dp0frontend"
call npm install
call npm run build
echo.

REM 3. Verifica .env
cd /d "%~dp0backend"
if not exist .env (
    echo [ERRORE] File .env non trovato!
    echo Copia .env.example in .env e compila le credenziali:
    echo   copy .env.example .env
    echo   notepad .env
    pause
    exit /b 1
)
echo [3/4] File .env trovato OK
echo.

REM 4. Verifica MT5
echo [4/4] Verifica connessione MT5...
python -c "import MetaTrader5 as mt5; ok=mt5.initialize(); print('MT5: OK' if ok else 'MT5: ERRORE - assicurati che MT5 sia aperto'); mt5.shutdown() if ok else None"
echo.

echo ============================================================
echo   SETUP COMPLETATO
echo.
echo   Per avviare: start_backend.bat
echo   Frontend servito su http://localhost:8001
echo   (non serve piu' npm run dev / porta 3000)
echo ============================================================
pause
