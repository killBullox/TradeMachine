"""
Price Service — recupera prezzi da MT5 (primario) o yfinance (fallback).
Valuta entry/TP/SL per ogni segnale e aggiorna lo stato nel DB.
"""
import asyncio
import warnings
from datetime import datetime, timedelta, timezone
from typing import Optional

warnings.filterwarnings("ignore")

import yfinance as yf

from database import SessionLocal, Signal

# ─── Mappatura simboli ────────────────────────────────────────────────────────

YF_MAP = {
    "XAUUSD": "GC=F",
    "XAGUSD": "SI=F",
    "USOIL":  "CL=F",
    "OIL":    "CL=F",
    "WTI":    "CL=F",
    "USDJPY": "USDJPY=X",
    "GBPJPY": "GBPJPY=X",
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "USDCHF": "USDCHF=X",
    "GBPCHF": "GBPCHF=X",
    "EURJPY": "EURJPY=X",
    "AUDUSD": "AUDUSD=X",
    "NZDUSD": "NZDUSD=X",
    "USDCAD": "USDCAD=X",
    "BTCUSD": "BTC-USD",
    "ETHUSD": "ETH-USD",
    "NASDAQ": "NQ=F",
    "US30":   "YM=F",
    "US500":  "ES=F",
}

# Mapping simboli ICT → XM MT5 (aggiungere altri se necessario)
MT5_MAP = {
    "XAUUSD": "GOLD#",
    "XAGUSD": "SILVER#",
    "USOIL":  "OILCash#",
    "OIL":    "OILCash#",
    "WTI":    "OILCash#",
    "USDJPY": "USDJPY#",
    "GBPJPY": "GBPJPY#",
    "EURUSD": "EURUSD#",
    "GBPUSD": "GBPUSD#",
    "USDCHF": "USDCHF#",
    "GBPCHF": "GBPCHF#",
    "EURJPY": "EURJPY#",
    "AUDUSD": "AUDUSD#",
    "NZDUSD": "NZDUSD#",
    "USDCAD": "USDCAD#",
    # BTCUSD non disponibile su XM → fallback yfinance
}

TV_MAP = {
    "XAUUSD": ("XAUUSD", "OANDA"),
    "XAGUSD": ("XAGUSD", "OANDA"),
    "USOIL":  ("USOIL", "OANDA"),
    "OIL":    ("USOIL", "OANDA"),
    "WTI":    ("USOIL", "OANDA"),
    "USDJPY": ("USDJPY", "OANDA"),
    "GBPJPY": ("GBPJPY", "OANDA"),
    "EURUSD": ("EURUSD", "OANDA"),
    "GBPUSD": ("GBPUSD", "OANDA"),
    "USDCHF": ("USDCHF", "OANDA"),
    "GBPCHF": ("GBPCHF", "OANDA"),
    "EURJPY": ("EURJPY", "OANDA"),
    "AUDUSD": ("AUDUSD", "OANDA"),
    "BTCUSD": ("BTCUSD", "COINBASE"),
    "ETHUSD": ("ETHUSD", "COINBASE"),
    "NASDAQ": ("NQ1!", "CME_MINI"),
    "US30":   ("YM1!", "CME_MINI"),
}


import logging
_logger = logging.getLogger("trademachine")

def log(msg):
    safe = ''.join(c if ord(c) < 128 else '?' for c in str(msg))
    print(safe, flush=True)
    _logger.info(safe)



# ─── MT5 helpers ──────────────────────────────────────────────────────────────

def _utc_to_local(dt_utc: datetime) -> datetime:
    """Converte datetime UTC naive → locale (DST-aware, usa il timezone del sistema)."""
    import calendar
    epoch = calendar.timegm(dt_utc.timetuple())
    return datetime.fromtimestamp(epoch)


def _mt5_init() -> bool:
    """Inizializza MT5 se non ancora connesso. Ritorna True se OK.
    Disabilitato se MT5_DISABLED=1 nel .env (es. su laptop senza MT5).
    """
    if os.getenv("MT5_DISABLED", "").strip() in ("1", "true", "yes"):
        return False
    try:
        import MetaTrader5 as mt5
        if mt5.terminal_info() is None:
            return mt5.initialize()
        return True
    except Exception:
        return False


_mt5_server_offset_seconds: Optional[int] = None


def _get_mt5_server_offset() -> int:
    """
    Calcola l'offset in secondi tra il clock del server MT5 e UTC reale.
    Il server XM usa EET (UTC+2): i tick.time sono ~7200 secondi avanti rispetto a UTC.
    Viene calcolato una volta sola confrontando l'epoch dell'ultimo tick con time.time().
    """
    global _mt5_server_offset_seconds
    if _mt5_server_offset_seconds is not None:
        return _mt5_server_offset_seconds
    try:
        import MetaTrader5 as mt5
        import time as time_module
        if not _mt5_init():
            _mt5_server_offset_seconds = 0
            return 0
        # Prova con GOLD# o EURUSD come riferimento
        for sym in ("GOLD#", "EURUSD", "GBPJPY#"):
            tick = mt5.symbol_info_tick(sym)
            if tick and tick.time > 0:
                utc_now = int(time_module.time())
                offset = tick.time - utc_now
                # Arrotonda all'ora più vicina (±30 min)
                offset_h = round(offset / 3600) * 3600
                _mt5_server_offset_seconds = offset_h
                log(f"[MT5] Server offset rilevato: {offset_h//3600:+d}h (raw={offset}s)")
                return offset_h
    except Exception:
        pass
    _mt5_server_offset_seconds = 0
    return 0


def get_ticks_mt5(symbol: str, since_utc: datetime, until_utc: Optional[datetime] = None):
    """
    Tick data reale dal broker MT5.
    since_utc / until_utc → datetime naive UTC.
    Ritorna DataFrame con colonne: time_utc (datetime), bid, ask, mid.

    Il server XM usa EET (UTC+2): compensiamo l'offset automaticamente.
    """
    import pandas as pd
    mt5_sym = MT5_MAP.get(symbol.upper())
    if not mt5_sym:
        return None
    if not _mt5_init():
        log(f"[MT5] Impossibile connettersi al terminale")
        return None
    try:
        import MetaTrader5 as mt5
        mt5.symbol_select(mt5_sym, True)

        # Offset server: aggiungiamo al datetime UTC per ottenere il "server time"
        offset_s = _get_mt5_server_offset()
        server_since = since_utc + timedelta(seconds=offset_s)

        if until_utc:
            server_until = until_utc + timedelta(seconds=offset_s)
            raw = mt5.copy_ticks_range(mt5_sym, server_since, server_until, mt5.COPY_TICKS_ALL)
        else:
            raw = mt5.copy_ticks_from(mt5_sym, server_since, 500_000, mt5.COPY_TICKS_ALL)

        if raw is None or len(raw) == 0:
            return None
        df = pd.DataFrame(raw)
        # Correggi timestamp: sottrai l'offset server per ottenere UTC reale
        df["time_utc"] = (
            pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_localize(None)
            - pd.Timedelta(seconds=offset_s)
        )
        df["mid"] = (df["bid"] + df["ask"]) / 2
        return df[["time_utc", "bid", "ask", "mid"]].reset_index(drop=True)
    except Exception as e:
        log(f"[MT5] Tick error {symbol}: {str(e)[:80]}")
        return None


def get_current_price_mt5(symbol: str) -> Optional[float]:
    """Prezzo corrente (last tick mid) da MT5."""
    mt5_sym = MT5_MAP.get(symbol.upper())
    if not mt5_sym:
        return None
    if not _mt5_init():
        return None
    try:
        import MetaTrader5 as mt5
        tick = mt5.symbol_info_tick(mt5_sym)
        if tick:
            return (tick.bid + tick.ask) / 2
    except Exception:
        pass
    return None


# ─── Valutazione segnale su tick data (MT5) ──────────────────────────────────

def evaluate_signal_on_ticks(signal: Signal, ticks, sl_schedule: list = None) -> dict:
    """
    Valutazione precisa tick-per-tick con trailing SL opzionale.
    sl_schedule: lista [{ts: datetime, new_sl: float}] ordinata per ts.
    """
    if ticks is None or len(ticks) == 0:
        return {}

    is_buy  = (signal.direction or "buy").lower() == "buy"
    entry   = signal.entry_price or signal.entry_price_high
    sl      = signal.stoploss
    tps     = [(i + 1, tp) for i, tp in enumerate([signal.tp1, signal.tp2, signal.tp3]) if tp]

    entry_hit = entry is None
    actual_entry_price = None
    entered_at = None
    best_tp: dict = {}
    tp_hits: dict = {}
    sl_event: dict = {}
    sl_moves: list = []          # eventi sl_move per il trade_log

    # Prepara lo schedule del trailing SL (solo mosse dopo l'entry)
    sl_sched = list(sl_schedule or [])
    sl_sched_idx = 0

    for row in ticks.itertuples(index=False):
        mid = row.mid
        ts  = row.time_utc

        # Step 1: entry
        if not entry_hit:
            if (is_buy and mid >= entry) or (not is_buy and mid <= entry):
                entry_hit = True
                actual_entry_price = row.ask if is_buy else row.bid
                entered_at = ts
            else:
                continue

        # Step 1b: applica trailing SL se il suo timestamp è arrivato
        while sl_sched_idx < len(sl_sched) and ts >= sl_sched[sl_sched_idx]["ts"]:
            move = sl_sched[sl_sched_idx]
            new_sl_val = move["new_sl"]
            if new_sl_val is None and actual_entry_price:
                new_sl_val = actual_entry_price   # break-even
            if new_sl_val is not None:
                sl_moves.append({"price": new_sl_val, "ts": ts})
                sl = new_sl_val
            sl_sched_idx += 1

        # Step 2: SL
        if sl:
            check_price = row.ask if is_buy else row.bid
            if (is_buy and check_price <= sl) or (not is_buy and check_price >= sl):
                if not sl_event:
                    sl_event = {"price": sl, "ts": ts}
                if best_tp:
                    break
                return {
                    "status": "sl_hit", "exit_price": sl, "closed_at": ts,
                    "actual_entry_price": actual_entry_price, "entered_at": entered_at,
                    "trade_log": _build_trade_log(entered_at, actual_entry_price,
                                                  tp_hits, sl_event, "sl_hit", sl_moves),
                }

        # Step 3: TP
        check_price = row.bid if is_buy else row.ask
        for tp_num, tp_price in tps:
            if (is_buy and check_price >= tp_price) or (not is_buy and check_price <= tp_price):
                if tp_num not in tp_hits:
                    tp_hits[tp_num] = {"price": tp_price, "ts": ts}
                    # Quando TP1 è hit → sposta automaticamente SL a breakeven
                    if tp_num == 1 and actual_entry_price:
                        sl = actual_entry_price
                        sl_moves.append({"price": sl, "ts": ts, "auto_breakeven": True})
                if not best_tp or tp_num > best_tp.get("num", 0):
                    best_tp = {"num": tp_num, "price": tp_price, "ts": ts}

    if best_tp:
        status_map = {1: "tp1", 2: "tp2", 3: "tp3"}
        final_status = status_map[best_tp["num"]]
        max_tp_num = max(tp_num for tp_num, _ in tps) if tps else best_tp["num"]
        effective_sl = sl_event if best_tp["num"] < max_tp_num else {}
        return {
            "status": final_status,
            "exit_price": best_tp["price"],
            "closed_at": best_tp["ts"],
            "actual_entry_price": actual_entry_price, "entered_at": entered_at,
            "trade_log": _build_trade_log(entered_at, actual_entry_price,
                                          tp_hits, effective_sl, final_status, sl_moves),
        }

    if entry_hit:
        return {"status": "open", "actual_entry_price": actual_entry_price, "entered_at": entered_at,
                "trade_log": _build_trade_log(entered_at, actual_entry_price, tp_hits, {}, "open", sl_moves)}

    return {}


def _build_trade_log(entered_at, entry_price, tp_hits: dict, sl_event: dict, final_status: str, sl_moves: list = None) -> str:
    """Costruisce il log JSON del trade con tutti gli eventi in ordine cronologico."""
    import json
    events = []

    def _fmt_ts(ts):
        if ts is None:
            return None
        if hasattr(ts, 'isoformat'):
            return ts.isoformat()
        return str(ts)

    # Raccoglie tutti gli eventi con timestamp per ordinarli
    raw_events = []

    if entered_at is not None:
        raw_events.append((entered_at, {"event": "entry",
            "price": round(float(entry_price), 5) if entry_price else None,
            "ts": _fmt_ts(entered_at)}))

    for m in (sl_moves or []):
        label = "breakeven" if m.get("auto_breakeven") else "sl_move"
        raw_events.append((m["ts"], {"event": label,
            "price": round(float(m["price"]), 5),
            "ts": _fmt_ts(m["ts"])}))

    for tp_num in sorted(tp_hits.keys()):
        h = tp_hits[tp_num]
        raw_events.append((h["ts"], {"event": f"tp{tp_num}",
            "price": round(float(h["price"]), 5),
            "ts": _fmt_ts(h["ts"])}))

    if sl_event:
        raw_events.append((sl_event["ts"], {"event": "sl_hit",
            "price": round(float(sl_event["price"]), 5),
            "ts": _fmt_ts(sl_event["ts"])}))

    raw_events.sort(key=lambda x: x[0])
    return json.dumps([e for _, e in raw_events])


# ─── Prezzo corrente ──────────────────────────────────────────────────────────

def get_current_price(symbol: str) -> Optional[float]:
    """Prezzo corrente: MT5 (primario) → yfinance (fallback). tvdatafeed disabilitato."""
    sym = symbol.upper()

    # 1. MT5 — last tick, istantaneo
    price = get_current_price_mt5(sym)
    if price:
        return price

    # 2. yfinance
    try:
        ticker = YF_MAP.get(sym, sym + "=X")
        hist = yf.Ticker(ticker).history(period="1d", interval="1m")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        log(f"[Price] YF error {sym}: {str(e)[:60]}")

    return None


# ─── Dati storici 1m/1h via yfinance ─────────────────────────────────────────

def get_ohlc_since(symbol: str, since: datetime):
    """OHLC dal momento del segnale ad oggi, intervallo adattivo."""
    import pandas as pd

    ticker = YF_MAP.get(symbol.upper(), symbol + "=X")
    days_back = (datetime.utcnow() - since).days + 2

    # yfinance limite: 1m max 7 giorni, 2m max 60 giorni, 1h max 730 giorni
    if days_back <= 7:
        period, interval = "7d", "1m"
    elif days_back <= 60:
        period, interval = "60d", "2m"
    else:
        period, interval = "2y", "1h"

    try:
        df = yf.Ticker(ticker).history(period=period, interval=interval)
        if df.empty:
            return None
        # Normalizza timezone: converti sempre in UTC prima di rimuovere tz
        if df.index.tzinfo is not None:
            df.index = df.index.tz_convert("UTC").tz_localize(None)
        since_naive = since.replace(tzinfo=None) if since.tzinfo else since
        df = df[df.index >= since_naive]
        return df if not df.empty else None
    except Exception as e:
        log(f"[Price] OHLC error {symbol}: {str(e)[:80]}")
        return None


# ─── Valutazione segnale su dati storici ─────────────────────────────────────

def evaluate_signal_on_history(signal: Signal, df) -> dict:
    """
    Analizza le OHLC bar dal segnale e ritorna:
    {'status': ..., 'exit_price': ..., 'closed_at': ...}
    La logica controlla prima SL (priorità), poi il TP più alto raggiunto.
    """
    if df is None or df.empty:
        return {}

    is_buy = (signal.direction or "buy").lower() == "buy"
    entry = signal.entry_price or signal.entry_price_high
    sl = signal.stoploss
    tps = [(i + 1, tp) for i, tp in enumerate([signal.tp1, signal.tp2, signal.tp3]) if tp]

    entry_hit = entry is None  # senza entry → già a mercato

    best_tp = {}  # miglior TP raggiunto nella sessione

    for ts, row in df.iterrows():
        high, low = row["High"], row["Low"]
        open_p = row.get("Open", row.get("open", 0))
        close_p = row.get("Close", row.get("close", 0))

        # Step 1: check entry
        if not entry_hit:
            if (is_buy and low <= entry) or (not is_buy and high >= entry):
                entry_hit = True
            else:
                continue

        # Step 2: determina se SL e/o TP sono toccati in questa candle
        sl_triggered = bool(sl and ((is_buy and low <= sl) or (not is_buy and high >= sl)))

        bar_best_tp = None
        for tp_num, tp_price in tps:
            if (is_buy and high >= tp_price) or (not is_buy and low <= tp_price):
                if bar_best_tp is None or tp_num > bar_best_tp["num"]:
                    bar_best_tp = {"num": tp_num, "price": tp_price}

        # Se entrambi toccati nello stesso candle, usa la direzione del candle come euristica:
        # candle favorevole = il prezzo si è mosso verso il TP → TP ha priorità
        if sl_triggered and bar_best_tp:
            # SL e TP nello stesso candle: usa direzione del candle come euristica
            favorable = (is_buy and close_p >= open_p) or (not is_buy and close_p <= open_p)
            if favorable:
                sl_triggered = False   # candle favorevole → TP first
            else:
                bar_best_tp = None     # candle sfavorevole → SL first

        if sl_triggered:
            # SL toccato — ma se un TP era già stato raggiunto in una barra precedente,
            # quel trade era già chiuso in profitto: il TP ha priorità
            if best_tp:
                break  # esce dal loop, best_tp verrà restituito sotto
            return {"status": "sl_hit", "exit_price": sl, "closed_at": ts}

        if bar_best_tp:
            if not best_tp or bar_best_tp["num"] > best_tp.get("num", 0):
                best_tp = {**bar_best_tp, "ts": ts}

    if best_tp:
        status_map = {1: "tp1", 2: "tp2", 3: "tp3"}
        return {
            "status": status_map[best_tp["num"]],
            "exit_price": best_tp["price"],
            "closed_at": best_tp["ts"],
        }

    # Entry raggiunto ma nessun esito → segnale ancora aperto
    if entry_hit:
        return {"status": "open"}

    return {}


# ─── Backfill storico ─────────────────────────────────────────────────────────

def evaluate_signal_on_telegram_messages(sig: Signal, messages: list) -> dict:
    """
    Valuta il segnale usando i messaggi live del canale Telegram.
    Usa i price update del bot (es. "4322.00 to 4308.00") e le conferme
    esplicite ("First Target Done", "2nd Target Done", "SL Hit" ecc.).
    Questa è la fonte più accurata — usa gli stessi prezzi del signal provider.
    """
    import re

    is_buy  = (sig.direction or "buy").lower() == "buy"
    entry   = sig.entry_price or sig.entry_price_high
    sl      = sig.stoploss
    tps     = [(i + 1, tp) for i, tp in enumerate([sig.tp1, sig.tp2, sig.tp3]) if tp]

    # Pattern: "4322.00 to 4308.00"
    price_update_re = re.compile(r'[\d,]+\.?\d*\s+to\s+([\d,]+\.?\d*)', re.IGNORECASE)
    # Pattern espliciti
    target_done_re  = re.compile(r'(1st|first|2nd|second|3rd|third|last)\s+target\s+done', re.IGNORECASE)
    tp_num_map = {'1st': 1, 'first': 1, '2nd': 2, 'second': 2, '3rd': 3, 'third': 3, 'last': 3}
    sl_text_re  = re.compile(r'stop\s*loss\s*(hit|triggered|done)', re.IGNORECASE)

    entry_hit = entry is None
    actual_entry_price = None
    entered_at = None
    best_tp: dict = {}
    sl_result: dict = {}

    for msg in messages:
        text = (msg.get("text") or "").strip()
        ts   = msg.get("created_at")

        # Cerca conferme esplicite TP
        td = target_done_re.search(text)
        if td:
            key = td.group(1).lower()
            n   = tp_num_map.get(key, 1)
            if not best_tp or n > best_tp.get("num", 0):
                # Recupera il prezzo dal testo se presente
                pm = price_update_re.search(text)
                price = float(pm.group(1).replace(',', '')) if pm else (tps[n-1][1] if n <= len(tps) else None)
                best_tp = {"num": n, "price": price, "ts": ts}
            continue

        # Cerca SL esplicito nel testo
        if sl_text_re.search(text):
            if not best_tp:  # SL solo se nessun TP già confermato
                sl_result = {"status": "sl_hit", "exit_price": sl, "closed_at": ts,
                             "actual_entry_price": actual_entry_price, "entered_at": entered_at}
            continue

        # Cerca price update: "entry to current"
        pm = price_update_re.search(text)
        if not pm:
            continue
        try:
            current = float(pm.group(1).replace(',', ''))
        except ValueError:
            continue

        # Entry hit quando il prezzo corrente raggiunge la entry
        if not entry_hit and entry:
            if (is_buy and current >= entry) or (not is_buy and current <= entry):
                entry_hit = True
                actual_entry_price = entry  # prezzo di ingresso = entry del segnale
                entered_at = ts

        if not entry_hit:
            continue

        # Check SL da price update (solo se nessun TP ancora confermato)
        if sl and not best_tp:
            if (is_buy and current <= sl) or (not is_buy and current >= sl):
                sl_result = {"status": "sl_hit", "exit_price": sl, "closed_at": ts,
                             "actual_entry_price": actual_entry_price, "entered_at": entered_at}
                # Non fermarci — un messaggio successivo potrebbe confermare TP
                continue

        # Check TP da price update
        for tp_num, tp_price in tps:
            if (is_buy and current >= tp_price) or (not is_buy and current <= tp_price):
                if not best_tp or tp_num > best_tp.get("num", 0):
                    best_tp = {"num": tp_num, "price": tp_price, "ts": ts}

    if best_tp:
        status_map = {1: "tp1", 2: "tp2", 3: "tp3"}
        return {
            "status": status_map[best_tp["num"]],
            "exit_price": best_tp["price"],
            "closed_at": best_tp["ts"],
            "actual_entry_price": actual_entry_price, "entered_at": entered_at,
        }

    if sl_result:
        return sl_result

    if entry_hit:
        return {"status": "open", "actual_entry_price": actual_entry_price, "entered_at": entered_at}

    return {}


def _load_sl_schedule(sig: Signal) -> list:
    """Carica gli SL moves dal DB per questo segnale, ordinati per created_at."""
    from database import SLMove
    db = SessionLocal()
    try:
        moves = db.query(SLMove).filter(
            SLMove.signal_id == sig.id,
            SLMove.new_sl.isnot(None)
        ).order_by(SLMove.created_at).all()
        return [{"ts": m.created_at, "new_sl": m.new_sl} for m in moves]
    finally:
        db.close()


def _backfill_signal(sig: Signal) -> dict:
    """
    Valuta un singolo segnale. Usa tick MT5 se disponibile, altrimenti OHLC yfinance.
    Chiamato in executor (thread separato).
    """
    since = sig.created_at or datetime.utcnow() - timedelta(days=1)
    sl_schedule = _load_sl_schedule(sig)

    # Tick MT5 (alta precisione)
    if sig.symbol.upper() in MT5_MAP:
        ticks = get_ticks_mt5(sig.symbol, since)
        if ticks is not None and len(ticks) > 0:
            ticks = ticks[ticks["time_utc"] >= since].reset_index(drop=True)
            if len(ticks) == 0:
                log(f"[Backfill] #{sig.id} {sig.symbol} — nessun tick dopo {since}")
            else:
                log(f"[Backfill] #{sig.id} {sig.symbol} — {len(ticks)} tick MT5, sl_moves={len(sl_schedule)}")
                return evaluate_signal_on_ticks(sig, ticks, sl_schedule)

    # Fallback OHLC yfinance
    df = get_ohlc_since(sig.symbol, since)
    return evaluate_signal_on_history(sig, df)


async def backfill_all(force: bool = False):
    """
    Valuta segnali con tick MT5 (o OHLC yfinance) + ricalcola P&L.
    force=True: ri-valuta TUTTI i segnali (anche già chiusi), utile per correggere backfill errati.
    force=False (default): valuta solo pending/open.
    """
    import risk as risk_module
    db = SessionLocal()
    try:
        if force:
            signals = db.query(Signal).all()
        else:
            signals = db.query(Signal).filter(Signal.status.in_(["pending", "open"])).all()
        log(f"[Backfill] Valutazione {len(signals)} segnali...")
        updated = 0

        for sig in signals:
            # Segnali MT5: stato e P&L autorevoli da sync_positions, non toccare
            if sig.mt5_ticket or sig.mt5_tickets:
                continue
            try:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, _backfill_signal, sig
                )

                if result:
                    new_status = result.get("status", sig.status)
                    changed = new_status != sig.status
                    if changed:
                        sig.status = new_status
                        sig.exit_price = result.get("exit_price")
                        closed = result.get("closed_at")
                        if closed and new_status not in ("open",):
                            sig.closed_at = closed if isinstance(closed, datetime) else closed.to_pydatetime()
                    # Aggiorna sempre entry reale (può arrivare anche senza cambio status)
                    if result.get("actual_entry_price"):
                        sig.actual_entry_price = result["actual_entry_price"]
                    if result.get("entered_at") is not None:
                        raw_ea = result["entered_at"]
                        sig.entered_at = raw_ea if isinstance(raw_ea, datetime) else raw_ea.to_pydatetime()
                    if result.get("trade_log"):
                        sig.trade_log = result["trade_log"]
                    if changed or result.get("actual_entry_price"):
                        sig.updated_at = datetime.utcnow()
                        db.add(sig)
                        db.commit()
                        if changed:
                            updated += 1
                            log(f"[Backfill] #{sig.id} {sig.symbol} {sig.direction} → {sig.status} @ {sig.exit_price}")

            except Exception as e:
                log(f"[Backfill] Errore #{sig.id}: {str(e)[:100]}")
                db.rollback()

            await asyncio.sleep(0.1)

        log(f"[Backfill] Completato: {updated}/{len(signals)} aggiornati")
    finally:
        db.close()

    risk_module.recalculate_all()


# ─── Monitor real-time ────────────────────────────────────────────────────────

_monitor_running = False


async def start_price_monitor():
    """Loop di monitoraggio ogni 15s per segnali open/pending."""
    global _monitor_running
    if _monitor_running:
        return
    _monitor_running = True
    log("[Monitor] Price monitor avviato (intervallo 15s)")

    while True:
        try:
            await _check_open_signals()
        except Exception as e:
            log(f"[Monitor] Errore loop: {str(e)[:100]}")
        await asyncio.sleep(15)


async def _check_open_signals():
    db = SessionLocal()
    try:
        signals = db.query(Signal).filter(
            Signal.status.in_(["pending", "open", "tp1", "tp2"])
        ).all()

        if not signals:
            return

        # Una sola chiamata per simbolo
        symbols = list({s.symbol.upper() for s in signals})
        prices = {}
        for sym in symbols:
            price = await asyncio.get_event_loop().run_in_executor(
                None, get_current_price, sym
            )
            if price is not None:
                prices[sym] = price

        now = datetime.utcnow()
        for sig in signals:
            price = prices.get(sig.symbol.upper())
            if price is None:
                continue
            _update_realtime(db, sig, price, now)

    finally:
        db.close()


def _update_realtime(db, sig: Signal, price: float, now: datetime):
    """State machine real-time per un singolo segnale."""
    # Segnali MT5: stato autorevole da sync_positions, non toccare
    if sig.mt5_ticket or sig.mt5_tickets:
        return
    # Segnali senza ticket con MT5 abilitato: sono stati rigettati/mancati, non trackare
    try:
        import mt5_trader
        if mt5_trader.is_enabled():
            return
    except Exception:
        pass
    is_buy = (sig.direction or "buy").lower() == "buy"
    changed = False

    # pending → open se entry toccato
    if sig.status == "pending":
        entry = sig.entry_price or sig.entry_price_high
        if entry is None or (is_buy and price >= entry) or (not is_buy and price <= entry):
            sig.status = "open"
            sig.actual_entry_price = price
            sig.entered_at = now
            changed = True

    if sig.status not in ("open", "tp1", "tp2"):
        if changed:
            sig.updated_at = now
            db.add(sig)
            db.commit()
        return

    # SL check — solo su segnali "open" (non su TP parziali già in profitto)
    # Una volta raggiunto un TP, l'SL non può retrocedere il risultato
    if sig.status == "open" and sig.stoploss:
        if (is_buy and price <= sig.stoploss) or (not is_buy and price >= sig.stoploss):
            sig.status = "sl_hit"
            sig.exit_price = sig.stoploss  # fill at SL level, not at detected price
            sig.closed_at = now
            sig.updated_at = now
            db.add(sig)
            db.commit()
            log(f"[Monitor] SL HIT {sig.symbol} #{sig.id} @ {price:.5f}")
            return

    # TP checks — prende il più alto raggiunto
    tps = [(3, sig.tp3), (2, sig.tp2), (1, sig.tp1)]
    for tp_num, tp_price in tps:
        if tp_price is None:
            continue
        hit = (is_buy and price >= tp_price) or (not is_buy and price <= tp_price)
        if hit:
            current = {"tp1": 1, "tp2": 2, "tp3": 3, "open": 0}.get(sig.status, 0)
            if tp_num > current:
                status_map = {1: "tp1", 2: "tp2", 3: "tp3"}
                sig.status = status_map[tp_num]
                if tp_num == 3:
                    sig.exit_price = tp_price
                    sig.closed_at = now
                sig.updated_at = now
                changed = True
                log(f"[Monitor] TP{tp_num} {sig.symbol} #{sig.id} @ {price:.5f}")
            break

    if changed:
        db.add(sig)
        db.commit()
