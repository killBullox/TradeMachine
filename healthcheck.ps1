$ErrorActionPreference = "SilentlyContinue"
$logFile = "C:\TradeMachine\healthcheck.log"
function Log($msg) {
    $ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss")
    Add-Content -Path $logFile -Value "[$ts UTC] $msg"
}

Log "=== healthcheck start ==="

# 1) MT5 terminal Avatrade deve essere running
$mt5 = Get-Process -Name "terminal64" -ErrorAction SilentlyContinue
if (-not $mt5) {
    Log "MT5 terminal NON running -> avvio MT5-Ava"
    schtasks /run /tn "MT5-Ava" | Out-Null
    Start-Sleep -Seconds 30
} else {
    Log "MT5 terminal OK ($($mt5.Count) processi)"
}

# 2) Backend TradeMachine: ping HTTP
$backendOk = $false
try {
    $resp = Invoke-WebRequest -Uri "http://localhost:8002/api/signals?limit=1" -TimeoutSec 8 -UseBasicParsing
    if ($resp.StatusCode -eq 200) { $backendOk = $true }
} catch {}

# Seconda possibilita' prima di uccidere: un singolo rallentamento non e' un
# blocco. Uccidere un backend vivo e' peggio che aspettare 20 secondi: durante
# il riavvio (1-2 minuti) i messaggi del trader non vengono trattati.
if (-not $backendOk) {
    Start-Sleep -Seconds 5
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:8002/api/signals?limit=1" -TimeoutSec 15 -UseBasicParsing
        if ($resp.StatusCode -eq 200) { $backendOk = $true; Log "Backend lento ma vivo (secondo tentativo OK)" }
    } catch {}
}

# Periodo di grazia all'avvio: mentre carica lo storico Telegram il backend
# non risponde per 1-2 minuti. Il 17/09 alle 09:26 UTC il healthcheck lo ha
# ucciso proprio in quella fase, innescando un nuovo riavvio.
if (-not $backendOk) {
    $vivo = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
            Where-Object { $_.CommandLine -match "run\.py" } | Select-Object -First 1
    if ($vivo) {
        $eta = (Get-Date) - $vivo.CreationDate
        if ($eta.TotalMinutes -lt 4) {
            Log ("Backend in avvio da {0:N0}s -> niente kill, ricontrollo al prossimo giro" -f $eta.TotalSeconds)
            $backendOk = $true
        }
    }
}

if (-not $backendOk) {
    Log "Backend HTTP NON risponde -> kill python + restart TradeMachine"
    # Kill solo processi python di TradeMachine (run.py / backend\main.py).
    # NON usare taskkill /IM python.exe perche' uccideremmo TradeWizard.
    $procs = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
             Where-Object { $_.CommandLine -match "TradeMachine|run\.py|backend\\main\.py" }
    foreach ($p in $procs) {
        Log "  kill PID=$($p.ProcessId) cmd=$($p.CommandLine)"
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 3
    schtasks /run /tn "TradeMachine" | Out-Null
    Start-Sleep -Seconds 25
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:8002/api/signals?limit=1" -TimeoutSec 8 -UseBasicParsing
        if ($resp.StatusCode -eq 200) { Log "Backend RIPARTITO OK" } else { Log "Backend NON risponde dopo restart (HTTP $($resp.StatusCode))" }
    } catch { Log "Backend NON risponde dopo restart: $($_.Exception.Message)" }
} else {
    Log "Backend HTTP OK"
}

# 3) MT5 trade_allowed (AutoTrading attivo): solo check, non auto-fix (richiede UI)
try {
    $resp = Invoke-WebRequest -Uri "http://localhost:8002/api/mt5/status" -TimeoutSec 8 -UseBasicParsing
    $j = $resp.Content | ConvertFrom-Json
    if ($j.terminal -and $j.terminal.trade_allowed -eq $false) {
        Log "WARNING: AutoTrading DISABILITATO sul terminale MT5 (richiede intervento manuale UI)"
    }
} catch {}

Log "=== healthcheck end ==="
