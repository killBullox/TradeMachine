@echo off
title Rimuovi Avvio Automatico TradeMachine
schtasks /delete /tn "TradeMachine Backend" /f
schtasks /delete /tn "TradeMachine Frontend" /f
echo Avvio automatico rimosso.
pause
