# Prop Mode — Guida operativa

## Cosa è

Sistema di guardie attivabili per-account per gestire i vincoli dei prop firm (Funded Elite, ecc.):
- **Daily DD kill-switch**: blocca nuovi trade quando la perdita giornaliera supera la soglia
- **Trailing equity DD**: traccia il peak equity e calcola la distanza dal "pavimento" inseguito
- **Coerenza monitor**: misura il rapporto max-day / total-profit (regola 30% prop)
- **Max concurrent trades**: blocca nuovi trade se posizioni aperte ≥ limite

## Architettura: zero impatto su account non-prop

Tutto gated da `account.prop_mode`. Per ogni account:
- `prop_mode=False` (default) → **nessuna guardia attiva**, comportamento bot 100% identico a prima
- `prop_mode=True` + soglie settate → guardie attive

Esempio:
```
Avatrade demo: prop_mode=False        → bot esegue come prima
Funded Elite 25K: prop_mode=True      → kill-switch + trailing DD + coerenza attivi
                  daily_dd_limit=500
                  max_total_dd=2000
```

## Setup di un nuovo account prop

### 1. Aggiungi MT5 terminal dedicato sul VPS
```
C:\Program Files\MT5-FundedElite\
```
Crea sessione, fai login con credenziali Funded Elite.

### 2. Aggiungi account via API
```
POST /api/mt5/add-account?login=12345&server=FundedElite-MT5&label=Funded%20Elite%2025K&is_demo=false&pin=XXX&mt5_path=C:\Program Files\MT5-FundedElite\terminal64.exe&broker=fundedelite
```

### 3. Configura parametri prop
```
PATCH /api/mt5/prop-settings/<account_id>?pin=XXX
  &prop_mode=true
  &daily_dd_limit_usd=500
  &daily_dd_warning_usd=300
  &max_total_dd_usd=2000
  &consistency_threshold_pct=30
  &max_concurrent_trades=3
```

Valori suggeriti per Funded Elite 25K (Flash Activation):
- Daily DD limit: 500 ($750 hard limit - 250 buffer)
- Daily DD warning: 300
- Max total DD: 2000 (= 8% di 25k)
- Consistency: 30%
- Max concurrent: 3

### 4. Switch active account
```
POST /api/mt5/switch-account?account_id=<funded_elite_id>&pin=XXX
```

### 5. Verifica
- Apri Dashboard: deve apparire il **pannello Prop Mode** in viola sopra MT5
- Endpoint `/api/prop/status` ritorna `enabled: true` e info sulle 4 guardie

## Comportamento delle guardie

### Daily DD kill-switch
Quando il P&L del giorno (somma di tutti i trade chiusi oggi in fuso Roma) raggiunge o supera la soglia negativa, **nuovi signal vengono ignorati** con log esplicito.

**Posizioni aperte continuano normalmente**: trail, TP, SL gestiti come sempre. Si protegge solo dall'apertura di NUOVI trade dopo aver toccato la soglia.

Reset: a mezzanotte Roma (il calcolo è basato sull'inizio giornata locale).

### Trailing DD (informativo)
Mostra in UI il buffer residuo. NON blocca trade per ora (è solo monitor). Da implementare se serve hard-block.

### Coerenza (informativo)
Mostra il rapporto max-day vs total. NON blocca trade (gestione di timing payout è dell'utente). Suggerisce a quale total-profit richiedere payout per restare sotto 30%.

### Max concurrent
Blocca apertura nuovo signal se le posizioni aperte (status open/tp1/tp2/pending con mt5_tickets attivi) raggiungono il limite.

## Garanzia "zero impatto su Avatrade"

Test automatici verificano che:
- `get_prop_settings()` ritorna `None` quando `prop_mode=False`
- `should_block_new_trades()` ritorna `None` per account non-prop, qualunque sia il P&L
- `check_max_concurrent_trades()` ritorna `None` per account non-prop
- Schema DB additivo: account pre-esistenti non vengono modificati dalla migrazione

Per verificare: `cd backend && python -m pytest tests/`

Soglia minima per essere sicuri: tutti i test verdi (101+ test attualmente).

## Test prima di deploy

```bash
cd backend
python -m pytest tests/ -v
```

Se tutti verdi → deploy sicuro.

## Note operative

- **Reset peak_equity_usd al passaggio di account**: quando si passa da Avatrade (no peak) a Funded Elite (con peak), il peak viene inizializzato sul primo update di equity dopo lo switch.
- **AutoTrading MT5 off**: blocca comunque i nuovi ordini al livello broker, indipendentemente da prop_mode.
- **Multi-account: solo uno attivo per volta**: il bot ha un solo `is_active=True` in `Mt5Account`. Per testare Funded Elite mentre tieni Avatrade demo come backup: switch a Funded Elite, testa, riswitcha ad Avatrade per la quotidianità.
