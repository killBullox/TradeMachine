"""
FastAPI backend per Inner Circle TradeMachine.
"""
import asyncio
import json
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List, Optional

# Forza UTF-8 su stdout/stderr per Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')


import logging
# Log su file persistente (non tocca il root logger per non interferire con uvicorn)
_logger = logging.getLogger("trademachine")
_logger.setLevel(logging.INFO)
_file_handler = logging.FileHandler("app.log", encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_logger.addHandler(_file_handler)

def safe_print(msg: str):
    safe = ''.join(c if ord(c) < 128 else '?' for c in str(msg))
    print(safe, flush=True)

from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, field_serializer
from sqlalchemy.orm import Session

import telegram_client as tg_module
import price_service as ps
import risk as risk_module
import mt5_trader
from database import (
    get_db, init_db, SessionLocal, Signal, TradeUpdate, MarketLevel,
    JournalEntry, RawMessage, RiskSettings, RestorePoint, Mt5Account
)


# ─── Lifespan ─────────────────────────────────────────────────────────────────

def _handle_asyncio_exception(loop, context):
    """Cattura eccezioni non gestite nei task asyncio — logga senza crashare."""
    msg = context.get("exception", context.get("message", "unknown"))
    safe_print(f"[AsyncIO] Eccezione non gestita: {str(msg)[:200]}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Handler globale per eccezioni asyncio non gestite
    asyncio.get_event_loop().set_exception_handler(_handle_asyncio_exception)
    init_db()
    # Carica account MT5 attivo dal DB (sovrascrive .env se presente)
    mt5_trader._load_active_account()
    # Ripristina stato auto-trade dal DB
    db = SessionLocal()
    try:
        rs = db.query(RiskSettings).first()
        if rs and rs.auto_trade:
            mt5_trader.enable()
    finally:
        db.close()
    asyncio.create_task(startup_telegram())
    asyncio.create_task(startup_prices())
    asyncio.create_task(startup_mt5_sync())
    yield
    # Shutdown pulito — disconnette Telegram per evitare sessioni zombie
    await tg_module.disconnect_client()


async def startup_mt5_sync():
    """Sync posizioni MT5 ogni 30s + cancella segnali expired."""
    await asyncio.sleep(15)
    while True:
        try:
            if mt5_trader.is_enabled():
                updated = await asyncio.get_event_loop().run_in_executor(None, mt5_trader.sync_positions)
                if updated:
                    safe_print(f"[MT5Sync] Aggiornati segnali: {updated}")
                cancelled = await asyncio.get_event_loop().run_in_executor(None, mt5_trader.cancel_expired_signals)
                if cancelled:
                    safe_print(f"[MT5Sync] Annullati segnali expired: {cancelled}")
        except Exception as e:
            safe_print(f"[MT5Sync] Errore: {str(e)[:100]}")
        await asyncio.sleep(30)


async def startup_prices():
    safe_print("[Startup] Avvio price service...")
    while True:
        try:
            await asyncio.sleep(10)
            await ps.backfill_all()
            await ps.start_price_monitor()
        except Exception as e:
            safe_print(f"[Price] Errore, riavvio in 15s: {str(e)[:200]}")
            await asyncio.sleep(15)


async def startup_telegram():
    import traceback
    safe_print("[Startup] Avvio connessione Telegram...")
    while True:
        try:
            await tg_module.load_history(limit=2000)
            await tg_module.start_listener()
        except Exception as e:
            err_str = str(e)
            safe_print(f"[Telegram] Errore, riavvio in 15s: {err_str[:200]}")
            tb = ''.join(c if ord(c) < 128 else '?' for c in traceback.format_exc())
            safe_print(tb)
            # Se la sessione è invalidata, aspetta ri-autenticazione via web
            if "AuthKeyDuplicated" in err_str or "not valid" in err_str.lower():
                safe_print("[Telegram] Sessione invalidata — in attesa di ri-autenticazione via /api/telegram/auth/request")
                # Aspetta finché non viene ri-autenticato
                while tg_module.get_tg_status() != "connected":
                    await asyncio.sleep(10)
                safe_print("[Telegram] Ri-autenticato, riprendo...")
                continue
            await asyncio.sleep(15)


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="Inner Circle TradeMachine", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── WebSocket ────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    tg_module.connected_ws.add(websocket)
    try:
        while True:
            await websocket.receive_text()  # keep-alive
    except WebSocketDisconnect:
        tg_module.connected_ws.discard(websocket)


# ─── Schemi Pydantic ──────────────────────────────────────────────────────────

def _utc(dt) -> Optional[str]:
    """Serializza un datetime naive come stringa UTC con 'Z'."""
    if dt is None:
        return None
    return dt.isoformat() + 'Z'


class _UTCModel(BaseModel):
    """Base class che serializza tutti i datetime come UTC (aggiunge 'Z')."""
    model_config = ConfigDict(from_attributes=True)

    @field_serializer('*', when_used='always')
    @classmethod
    def _serialize_dt(cls, v):
        if isinstance(v, datetime):
            return v.isoformat() + 'Z'
        return v


class SignalOut(_UTCModel):
    id: int
    symbol: str
    direction: str
    entry_price: Optional[float]
    entry_price_high: Optional[float]
    tp1: Optional[float]
    tp2: Optional[float]
    tp3: Optional[float]
    stoploss: Optional[float]
    status: str
    notes: Optional[str]
    actual_entry_price: Optional[float] = None
    entered_at: Optional[datetime] = None
    exit_price: Optional[float] = None
    closed_at: Optional[datetime] = None
    trade_log: Optional[str] = None
    mt5_ticket: Optional[int] = None
    mt5_tickets: Optional[str] = None
    is_risky: bool = False
    risk_usd: Optional[float] = None
    position_size: Optional[float] = None
    pnl_usd: Optional[float] = None
    running_balance: Optional[float] = None
    trail_stop_enabled: Optional[bool] = None  # override per-trade del default globale
    broker: Optional[str] = None  # broker su cui e' stato eseguito il trade
    mt5_account: Optional[int] = None  # numero account MT5 al momento dell'apertura
    created_at: datetime
    updated_at: datetime


class SignalUpdate(BaseModel):
    status: Optional[str] = None
    notes: Optional[str] = None
    stoploss: Optional[float] = None
    tp1: Optional[float] = None
    tp2: Optional[float] = None
    tp3: Optional[float] = None


class JournalIn(BaseModel):
    signal_id: Optional[int] = None
    title: Optional[str] = None
    content: str
    trade_result: Optional[float] = None
    emotion: Optional[str] = None


class JournalOut(_UTCModel):
    id: int
    signal_id: Optional[int]
    title: Optional[str]
    content: str
    trade_result: Optional[float]
    emotion: Optional[str]
    created_at: datetime


# ─── Endpoints: Signals ───────────────────────────────────────────────────────

def _attach_running_balance(signals: list, account_size: float) -> list:
    """
    Calcola il saldo cumulativo su tutti i segnali chiusi (ordinati per closed_at),
    poi assegna running_balance a ogni segnale nella lista originale.
    """
    # Costruisce mappa id → running_balance dai segnali chiusi in ordine cronologico
    closed = sorted(
        [s for s in signals if s.pnl_usd is not None and s.closed_at is not None],
        key=lambda s: s.closed_at
    )
    balance = account_size
    balance_map = {}
    for s in closed:
        balance = round(balance + s.pnl_usd, 2)
        balance_map[s.id] = balance

    for s in signals:
        s.running_balance = balance_map.get(s.id)
    return signals


@app.get("/api/signals", response_model=List[SignalOut])
def get_signals(
    status: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    limit: int = Query(50, le=2000),
    offset: int = Query(0),
    db: Session = Depends(get_db)
):
    q = db.query(Signal).filter(Signal.is_archived == False)
    if status:
        q = q.filter(Signal.status == status)
    if symbol:
        q = q.filter(Signal.symbol.ilike(f"%{symbol}%"))
    signals = q.order_by(Signal.created_at.desc()).offset(offset).limit(limit).all()

    # Per calcolare il running_balance correttamente servono TUTTI i segnali chiusi
    all_closed = db.query(Signal).filter(
        Signal.is_archived == False,
        Signal.pnl_usd.isnot(None),
        Signal.closed_at.isnot(None)
    ).all()
    rs = db.query(RiskSettings).first()
    account_size = rs.account_size if rs else 10000.0

    # Calcola mappa id → balance su tutti i chiusi
    closed_sorted = sorted(all_closed, key=lambda s: s.closed_at)
    balance = account_size
    balance_map = {}
    for s in closed_sorted:
        balance = round(balance + s.pnl_usd, 2)
        balance_map[s.id] = balance

    for s in signals:
        s.running_balance = balance_map.get(s.id)

    return signals


@app.get("/api/signals/{signal_id}", response_model=SignalOut)
def get_signal(signal_id: int, db: Session = Depends(get_db)):
    sig = db.query(Signal).filter(Signal.id == signal_id).first()
    if not sig:
        raise HTTPException(404, "Signal not found")
    return sig


@app.patch("/api/signals/{signal_id}", response_model=SignalOut)
def update_signal(signal_id: int, body: SignalUpdate, db: Session = Depends(get_db)):
    sig = db.query(Signal).filter(Signal.id == signal_id).first()
    if not sig:
        raise HTTPException(404, "Signal not found")
    for field, val in body.dict(exclude_none=True).items():
        setattr(sig, field, val)
    sig.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(sig)
    return sig


@app.get("/api/signals/{signal_id}/activity")
def get_signal_activity(signal_id: int, db: Session = Depends(get_db)):
    """Ritorna il log di attività dettagliato per un segnale."""
    sig = db.query(Signal).filter(Signal.id == signal_id).first()
    if not sig:
        raise HTTPException(404, "Signal not found")
    # Parse trade_log
    trade_log = []
    if sig.trade_log:
        try:
            trade_log = json.loads(sig.trade_log)
        except Exception:
            pass
    # Aggiungi eventi strutturati dal segnale stesso
    timeline = []
    if sig.created_at:
        timeline.append({"ts": sig.created_at.isoformat(), "event": "signal_saved", "detail": f"Segnale #{sig.id} salvato nel DB: {sig.symbol} {sig.direction}"})
    for entry in trade_log:
        timeline.append(entry)
    if sig.entered_at and sig.entered_at != sig.created_at:
        timeline.append({"ts": sig.entered_at.isoformat(), "event": "entry", "detail": f"Entry confermata @ {sig.actual_entry_price}"})
    if sig.closed_at:
        timeline.append({"ts": sig.closed_at.isoformat(), "event": "closed", "detail": f"Trade chiuso: status={sig.status} pnl={sig.pnl_usd}"})
    # Ordina per timestamp
    def _ts(e):
        try: return e.get("ts","") or ""
        except: return ""
    timeline.sort(key=_ts)
    return {
        "signal_id": signal_id,
        "symbol": sig.symbol,
        "direction": sig.direction,
        "status": sig.status,
        "mt5_ticket": sig.mt5_ticket,
        "mt5_tickets": sig.mt5_tickets,
        "pnl_usd": sig.pnl_usd,
        "timeline": timeline,
    }


@app.delete("/api/signals/{signal_id}")
def archive_signal(signal_id: int, db: Session = Depends(get_db)):
    sig = db.query(Signal).filter(Signal.id == signal_id).first()
    if not sig:
        raise HTTPException(404, "Signal not found")
    sig.is_archived = True
    db.commit()
    return {"ok": True}


# ─── Endpoints: Updates ───────────────────────────────────────────────────────

@app.get("/api/updates")
def get_updates(
    symbol: Optional[str] = Query(None),
    limit: int = Query(100, le=2000),
    db: Session = Depends(get_db)
):
    q = db.query(TradeUpdate)
    if symbol:
        q = q.filter(TradeUpdate.symbol.ilike(f"%{symbol}%"))
    items = q.order_by(TradeUpdate.created_at.desc()).limit(limit).all()
    return [
        {
            "id": u.id, "symbol": u.symbol,
            "price_from": u.price_from, "price_to": u.price_to,
            "update_text": u.update_text,
            "created_at": _utc(u.created_at),
        }
        for u in items
    ]


# ─── Endpoints: Market Levels ─────────────────────────────────────────────────

@app.get("/api/levels")
def get_levels(symbol: Optional[str] = Query(None), db: Session = Depends(get_db)):
    q = db.query(MarketLevel)
    if symbol:
        q = q.filter(MarketLevel.symbol.ilike(f"%{symbol}%"))
    items = q.order_by(MarketLevel.date.desc()).limit(30).all()
    result = []
    for lv in items:
        result.append({
            "id": lv.id, "symbol": lv.symbol,
            "support_levels": json.loads(lv.support_levels or "[]"),
            "resistance_levels": json.loads(lv.resistance_levels or "[]"),
            "date": lv.date,
        })
    return result


# ─── Endpoints: Journal ───────────────────────────────────────────────────────

@app.get("/api/journal", response_model=List[JournalOut])
def get_journal(db: Session = Depends(get_db)):
    return db.query(JournalEntry).order_by(JournalEntry.created_at.desc()).all()


@app.post("/api/journal", response_model=JournalOut)
def create_journal(body: JournalIn, db: Session = Depends(get_db)):
    entry = JournalEntry(**body.dict())
    db.add(entry)
    db.commit()
    db.refresh(entry)
    return entry


@app.delete("/api/journal/{entry_id}")
def delete_journal(entry_id: int, db: Session = Depends(get_db)):
    e = db.query(JournalEntry).filter(JournalEntry.id == entry_id).first()
    if not e:
        raise HTTPException(404, "Entry not found")
    db.delete(e)
    db.commit()
    return {"ok": True}


# ─── Endpoints: Performance ───────────────────────────────────────────────────

@app.get("/api/performance")
def get_performance(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    q = db.query(Signal).filter(Signal.is_archived == False)
    if date_from:
        try:
            q = q.filter(Signal.created_at >= datetime.fromisoformat(date_from))
        except Exception:
            pass
    if date_to:
        try:
            dt_to = datetime.fromisoformat(date_to)
            # include tutta la giornata finale
            if 'T' not in date_to:
                dt_to = dt_to.replace(hour=23, minute=59, second=59)
            q = q.filter(Signal.created_at <= dt_to)
        except Exception:
            pass
    signals = q.all()
    total = len(signals)

    status_counts = {}
    for s in signals:
        status_counts[s.status] = status_counts.get(s.status, 0) + 1

    closed_statuses = {"tp1", "tp2", "tp3", "closed"}
    tp_hits = sum(status_counts.get(k, 0) for k in closed_statuses)
    sl_hits = status_counts.get("sl_hit", 0)
    closed_total = tp_hits + sl_hits
    win_rate = round(tp_hits / closed_total * 100, 1) if closed_total > 0 else None

    # P&L dai segnali (calcolato con risk module)
    closed = [s for s in signals if s.pnl_usd is not None]
    total_pnl  = round(sum(s.pnl_usd for s in closed), 2)
    total_wins = round(sum(s.pnl_usd for s in closed if s.pnl_usd > 0), 2)
    total_loss = round(sum(s.pnl_usd for s in closed if s.pnl_usd < 0), 2)
    avg_win    = round(total_wins / tp_hits, 2) if tp_hits > 0 else 0
    avg_loss   = round(total_loss / sl_hits, 2) if sl_hits > 0 else 0
    profit_factor = round(abs(total_wins / total_loss), 2) if total_loss != 0 else None

    # Max drawdown (equity curve semplice)
    pnl_series = [s.pnl_usd for s in sorted(closed, key=lambda x: x.closed_at or x.created_at) if s.pnl_usd]
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnl_series:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
    max_dd = round(max_dd, 2)

    # Streak
    sorted_closed = sorted(closed, key=lambda x: x.closed_at or x.created_at)
    cur_streak = best_streak = worst_streak = 0
    for s in sorted_closed:
        if s.pnl_usd and s.pnl_usd > 0:
            cur_streak = max(cur_streak + 1, 1) if cur_streak >= 0 else 1
        elif s.pnl_usd and s.pnl_usd < 0:
            cur_streak = min(cur_streak - 1, -1) if cur_streak <= 0 else -1
        best_streak = max(best_streak, cur_streak)
        worst_streak = min(worst_streak, cur_streak)

    # Per simbolo
    all_closed = closed_statuses | {"sl_hit"}
    by_symbol = {}
    for s in signals:
        sym = s.symbol
        if sym not in by_symbol:
            by_symbol[sym] = {"count": 0, "wins": 0, "losses": 0, "pnl": 0.0}
        by_symbol[sym]["count"] += 1
        if s.status in all_closed:
            if s.pnl_usd is not None:
                by_symbol[sym]["pnl"] += s.pnl_usd
            if s.status in closed_statuses:
                by_symbol[sym]["wins"] += 1
            elif s.status == "sl_hit":
                by_symbol[sym]["losses"] += 1
    from mt5_trader import MT5_SYMBOL_MAP
    tradeable = set(MT5_SYMBOL_MAP.keys())
    top_symbols = sorted(by_symbol.items(), key=lambda x: -x[1]["count"])[:8]
    untradeable = {sym for sym in by_symbol if sym.upper() not in tradeable}

    # Ultimi 7 giorni
    week_ago = datetime.utcnow() - timedelta(days=7)
    recent = [s for s in signals if s.created_at and s.created_at >= week_ago]
    recent_pnl = round(sum(s.pnl_usd for s in recent if s.pnl_usd), 2)

    # Segnali "gestiti" = quelli che hanno effettivamente prodotto un trade su MT5.
    # Serve sia un ticket sia un actual_entry_price (= almeno una posizione si è
    # davvero aperta). I segnali con solo pending mai riempiti, o cancellati,
    # NON sono gestiti.
    def _is_managed(s):
        return (bool(s.mt5_ticket or s.mt5_tickets)
                and s.status != "cancelled"
                and s.actual_entry_price is not None)
    managed_total  = sum(1 for s in signals if _is_managed(s))
    managed_recent = sum(1 for s in recent   if _is_managed(s))

    # P&L di oggi
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_closed = [s for s in closed if s.closed_at and s.closed_at >= today_start]
    today_pnl = round(sum(s.pnl_usd for s in today_closed), 2)
    today_wins = sum(1 for s in today_closed if s.pnl_usd and s.pnl_usd > 0)
    today_losses = sum(1 for s in today_closed if s.pnl_usd and s.pnl_usd < 0)
    today_trades = len(today_closed)

    # Risk settings
    rs = db.query(RiskSettings).first()
    risk_info = {
        "account_size": rs.account_size if rs else 10000,
        "risk_per_trade_pct": rs.risk_per_trade_pct if rs else 1.0,
        "risk_per_trade_usd": rs.risk_per_trade_usd if rs else None,
        "use_fixed_usd": rs.use_fixed_usd if rs else False,
        "entry_tolerance_pips": (getattr(rs, "entry_tolerance_pips", None) or 3.0) if rs else 3.0,
    }

    return {
        "total_signals": total,
        "closed_trades": closed_total,
        "status_breakdown": status_counts,
        "tp_hits": tp_hits,
        "sl_hits": sl_hits,
        "win_rate_pct": win_rate,
        # P&L
        "total_pnl_usd": total_pnl,
        "total_wins_usd": total_wins,
        "total_loss_usd": total_loss,
        "avg_win_usd": avg_win,
        "avg_loss_usd": avg_loss,
        "profit_factor": profit_factor,
        "max_drawdown_usd": max_dd,
        "best_streak": best_streak,
        "worst_streak": worst_streak,
        # Oggi
        "today_pnl": today_pnl,
        "today_trades": today_trades,
        "today_wins": today_wins,
        "today_losses": today_losses,
        # Recente
        "signals_last_7d": len(recent),
        "pnl_last_7d": recent_pnl,
        # Segnali gestiti (con ordine MT5 piazzato)
        "managed_signals": managed_total,
        "managed_signals_last_7d": managed_recent,
        # Per simbolo
        "by_symbol": [
            {"symbol": sym, **data}
            for sym, data in top_symbols
        ],
        "risk_settings": risk_info,
        "untradeable_symbols": list(untradeable),
    }


# ─── Risk Settings ────────────────────────────────────────────────────────────

class RiskSettingsIn(BaseModel):
    account_size: float
    risk_per_trade_pct: float = 1.0
    risk_per_trade_usd: Optional[float] = None
    use_fixed_usd: bool = False
    entry_tolerance_pips: float = 3.0
    trail_stop_enabled: bool = False


@app.get("/api/performance/calendar")
def get_calendar(year: int, month: int, db: Session = Depends(get_db)):
    """Ritorna P&L giornaliero e win-rate per un mese."""
    import calendar as cal_mod
    from datetime import date
    first = datetime(year, month, 1)
    last  = datetime(year, month, cal_mod.monthrange(year, month)[1], 23, 59, 59)
    sigs  = db.query(Signal).filter(
        Signal.closed_at >= first,
        Signal.closed_at <= last,
        Signal.pnl_usd.isnot(None),
        Signal.is_archived == False,
    ).all()

    days: dict = {}
    for s in sigs:
        d = s.closed_at.strftime("%Y-%m-%d")
        if d not in days:
            days[d] = {"pnl": 0.0, "wins": 0, "total": 0}
        days[d]["pnl"]   += s.pnl_usd
        days[d]["total"] += 1
        if s.pnl_usd > 0:
            days[d]["wins"] += 1

    return {
        "year": year, "month": month,
        "days": {
            d: {
                "pnl":      round(v["pnl"], 2),
                "win_rate": round(v["wins"] / v["total"] * 100) if v["total"] else 0,
                "trades":   v["total"],
            }
            for d, v in days.items()
        }
    }


@app.get("/api/risk-settings")
def get_risk_settings_api(db: Session = Depends(get_db)):
    rs = db.query(RiskSettings).first()
    if not rs:
        return {"account_size": 10000, "risk_per_trade_pct": 1.0,
                "risk_per_trade_usd": None, "use_fixed_usd": False,
                "entry_tolerance_pips": 3.0, "trail_stop_enabled": False}
    return {
        "account_size": rs.account_size,
        "risk_per_trade_pct": rs.risk_per_trade_pct,
        "risk_per_trade_usd": rs.risk_per_trade_usd,
        "use_fixed_usd": rs.use_fixed_usd,
        "entry_tolerance_pips": getattr(rs, "entry_tolerance_pips", None) or 3.0,
        "trail_stop_enabled": bool(getattr(rs, "trail_stop_enabled", False)),
    }


@app.post("/api/risk-settings")
async def update_risk_settings(body: RiskSettingsIn, db: Session = Depends(get_db)):
    rs = db.query(RiskSettings).first()
    if not rs:
        rs = RiskSettings()
        db.add(rs)
    rs.account_size = body.account_size
    rs.risk_per_trade_pct = body.risk_per_trade_pct
    rs.risk_per_trade_usd = body.risk_per_trade_usd
    rs.use_fixed_usd = body.use_fixed_usd
    rs.entry_tolerance_pips = body.entry_tolerance_pips
    rs.trail_stop_enabled = body.trail_stop_enabled
    rs.updated_at = datetime.utcnow()
    db.commit()
    async def _run(): await asyncio.get_event_loop().run_in_executor(None, risk_module.recalculate_all)
    asyncio.create_task(_run())
    return {"ok": True, "message": "Settings salvati, ricalcolo P&L in corso"}


@app.post("/api/recalculate")
async def recalculate_pnl():
    """Ricalcola position size e P&L per tutti i segnali (in background)."""
    async def _run(): await asyncio.get_event_loop().run_in_executor(None, risk_module.recalculate_all)
    asyncio.create_task(_run())
    return {"ok": True}


class ResetRequest(BaseModel):
    since: Optional[str] = None  # ISO date "YYYY-MM-DD", se None resetta tutto

@app.post("/api/reset")
async def reset_and_reload(body: ResetRequest):
    """
    Cancella tutti i segnali (opzionalmente solo quelli da 'since'),
    poi ricarica la storia dal canale Telegram.
    """
    since_dt = None
    if body.since:
        try:
            since_dt = datetime.fromisoformat(body.since)
        except ValueError:
            return {"ok": False, "error": "Formato data non valido (usa YYYY-MM-DD)"}

    db = SessionLocal()
    try:
        q = db.query(Signal)
        if since_dt:
            q = q.filter(Signal.created_at >= since_dt)
        deleted = q.count()
        q.delete(synchronize_session=False)

        # Cancella anche i raw messages corrispondenti
        qr = db.query(RawMessage)
        if since_dt:
            qr = qr.filter(RawMessage.created_at >= since_dt)
        qr.delete(synchronize_session=False)

        db.commit()
    finally:
        db.close()

    # Ricarica storia Telegram in background
    import telegram_client as tg_module
    async def _reload():
        await tg_module.load_history(limit=5000, since=since_dt)
        await asyncio.get_event_loop().run_in_executor(None, risk_module.recalculate_all)

    asyncio.create_task(_reload())
    return {"ok": True, "deleted": deleted, "since": body.since}


# ─── Endpoints: Raw messages ──────────────────────────────────────────────────

@app.get("/api/messages")
def get_raw_messages(limit: int = Query(100, le=2000), db: Session = Depends(get_db)):
    items = db.query(RawMessage).order_by(RawMessage.created_at.desc()).limit(limit).all()
    return [
        {"id": m.id, "sender": m.sender, "text": m.text, "type": m.msg_type, "created_at": _utc(m.created_at)}
        for m in items
    ]


# ─── Reload storico manuale ───────────────────────────────────────────────────

@app.post("/api/reload-history")
async def reload_history(limit: int = Query(2000)):
    asyncio.create_task(tg_module.load_history(limit=limit))
    return {"ok": True, "message": f"Caricamento {limit} messaggi avviato in background"}


# ─── Endpoints: Telegram Auth ────────────────────────────────────────────────

@app.get("/api/telegram/status")
async def telegram_status():
    return {"status": tg_module.get_tg_status()}


@app.post("/api/telegram/auth/request")
async def telegram_auth_request():
    """Invia il codice di verifica al telefono registrato."""
    try:
        await tg_module.request_auth_code()
        return {"ok": True, "message": "Codice inviato al telefono"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


class TelegramAuthCode(BaseModel):
    code: str


@app.post("/api/telegram/auth/verify")
async def telegram_auth_verify(body: TelegramAuthCode):
    """Verifica il codice e completa l'autenticazione."""
    try:
        await tg_module.complete_auth(body.code)
        # Riavvia listener dopo ri-autenticazione
        asyncio.create_task(startup_telegram())
        return {"ok": True, "message": "Autenticazione completata"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


@app.post("/api/backfill")
async def trigger_backfill(force: bool = False):
    """
    Rivaluta segnali con dati storici MT5/yfinance.
    force=true: ri-valuta TUTTI i segnali (anche già chiusi).
    """
    asyncio.create_task(ps.backfill_all(force=force))
    mode = "FORCE (tutti)" if force else "normale (pending/open)"
    return {"ok": True, "message": f"Backfill {mode} avviato in background"}


@app.get("/api/price/{symbol}")
async def get_price(symbol: str):
    """Prezzo corrente di un simbolo."""
    price = await asyncio.get_event_loop().run_in_executor(
        None, ps.get_current_price, symbol.upper()
    )
    if price is None:
        raise HTTPException(status_code=404, detail=f"Prezzo non disponibile per {symbol}")
    return {"symbol": symbol.upper(), "price": price, "timestamp": datetime.utcnow().isoformat()}


# ─── MT5 Auto-Trading ─────────────────────────────────────────────────────────

@app.get("/api/mt5/status")
async def mt5_status():
    info = await asyncio.get_event_loop().run_in_executor(None, mt5_trader.get_account_info)
    positions = await asyncio.get_event_loop().run_in_executor(None, mt5_trader.get_open_positions)
    return {
        "auto_trade": mt5_trader.is_enabled(),
        "account": info,
        "open_positions": positions,
    }

@app.post("/api/mt5/enable")
async def mt5_enable(db: Session = Depends(get_db)):
    mt5_trader.enable()
    rs = db.query(RiskSettings).first()
    if rs:
        rs.auto_trade = True
        db.add(rs)
        db.commit()
    return {"ok": True, "auto_trade": True}

@app.post("/api/mt5/disable")
async def mt5_disable(db: Session = Depends(get_db)):
    mt5_trader.disable()
    rs = db.query(RiskSettings).first()
    if rs:
        rs.auto_trade = False
        db.add(rs)
        db.commit()
    return {"ok": True, "auto_trade": False}

@app.get("/api/mt5/accounts")
async def mt5_accounts():
    """Elenca gli account disponibili nel terminale MT5."""
    import MetaTrader5 as mt5
    mt5.initialize()
    # Account corrente
    info = mt5.account_info()
    current = {
        "login": info.login if info else None,
        "name": info.name if info else None,
        "server": info.server if info else None,
        "balance": info.balance if info else 0,
        "demo": info.trade_mode == 0 if info else None,
    } if info else None
    # Carica conti dal DB
    db = SessionLocal()
    try:
        accounts = db.query(Mt5Account).order_by(Mt5Account.is_default.desc(), Mt5Account.login).all()
        # Se il DB è vuoto, inserisci i conti di default
        if not accounts:
            defaults = [
                Mt5Account(login=27640489, server="XM.COM-MT5", label="Demo Account", is_demo=True, is_default=True),
                Mt5Account(login=15055521, server="XM.COM-MT5", label="Gianluca Davino", is_demo=False, is_default=False),
            ]
            for a in defaults:
                db.add(a)
            db.commit()
            accounts = defaults
        available = [
            {
                "id": a.id, "login": a.login, "server": a.server, "label": a.label,
                "demo": a.is_demo, "is_default": a.is_default, "is_active": a.is_active,
                "mt5_path": a.mt5_path, "broker": a.broker,
            }
            for a in accounts
        ]
    finally:
        db.close()
    return {
        "current": current,
        "configured": {
            "login": mt5_trader.MT5_ACCOUNT,
            "server": mt5_trader.MT5_SERVER,
        },
        "available": available,
    }


@app.post("/api/mt5/switch-account")
async def mt5_switch_account(login: int = Query(...), server: str = Query(...), pin: str = Query(...)):
    """Cambia l'account MT5. Richiede PIN di conferma."""
    if pin != ARCHIVE_PIN:
        raise HTTPException(status_code=403, detail="PIN non valido")
    result = await asyncio.get_event_loop().run_in_executor(
        None, mt5_trader.switch_account, login, server
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "Errore sconosciuto"))
    return result


@app.post("/api/mt5/add-account")
async def mt5_add_account(
    login: int = Query(...),
    server: str = Query(...),
    label: str = Query(...),
    is_demo: bool = Query(True),
    pin: str = Query(...),
    mt5_path: str = Query(""),
    broker: str = Query(""),
    db: Session = Depends(get_db),
):
    """Aggiunge un account MT5. Richiede PIN."""
    _verify_pin(pin)
    existing = db.query(Mt5Account).filter(Mt5Account.login == login).first()
    if existing:
        raise HTTPException(status_code=400, detail=f"Account {login} già presente")
    acc = Mt5Account(
        login=login, server=server, label=label, is_demo=is_demo,
        mt5_path=mt5_path or None, broker=broker or None,
    )
    db.add(acc)
    db.commit()
    return {"ok": True, "login": login}


@app.patch("/api/mt5/update-account/{account_id}")
async def mt5_update_account(
    account_id: int,
    pin: str = Query(...),
    label: str = Query(None),
    server: str = Query(None),
    mt5_path: str = Query(None),
    broker: str = Query(None),
    is_demo: bool = Query(None),
    db: Session = Depends(get_db),
):
    """Aggiorna campi di un account esistente. Richiede PIN."""
    _verify_pin(pin)
    acc = db.query(Mt5Account).filter(Mt5Account.id == account_id).first()
    if not acc:
        raise HTTPException(status_code=404, detail="Account non trovato")
    if label is not None: acc.label = label
    if server is not None: acc.server = server
    if mt5_path is not None: acc.mt5_path = mt5_path or None
    if broker is not None: acc.broker = broker or None
    if is_demo is not None: acc.is_demo = is_demo
    db.commit()
    return {"ok": True}


@app.delete("/api/mt5/remove-account/{account_id}")
async def mt5_remove_account(account_id: int, pin: str = Query(...), db: Session = Depends(get_db)):
    """Rimuove un account MT5. Richiede PIN."""
    _verify_pin(pin)
    acc = db.query(Mt5Account).filter(Mt5Account.id == account_id).first()
    if not acc:
        raise HTTPException(status_code=404, detail="Account non trovato")
    if acc.is_default:
        raise HTTPException(status_code=400, detail="Non puoi rimuovere l'account di default")
    db.delete(acc)
    db.commit()
    return {"ok": True}


class TestPlaceOrderIn(BaseModel):
    symbol: str
    direction: str = "buy"  # "buy" o "sell"
    # Modo manuale: entry/SL/TP espliciti
    entry_low: Optional[float] = None
    entry_high: Optional[float] = None
    stoploss: Optional[float] = None
    tp1: Optional[float] = None
    tp2: Optional[float] = None
    tp3: Optional[float] = None
    is_risky: bool = False
    # Modo auto: il backend legge ask/bid corrente dal terminale e costruisce
    # entry/SL/TP a sl_points/tp_points dal prezzo corrente. Se il broker ha
    # uno stops_level piu' grande, il distance viene clampato a stops_level*5.
    auto: bool = False
    sl_points_factor: float = 5.0  # SL dist = stops_level * factor (min 50)
    tp_points_factor: float = 5.0  # TP1 dist = stops_level * factor (TP2=2x, TP3=3x)


@app.post("/api/test/place-order")
async def test_place_order(body: TestPlaceOrderIn, pin: str = Query(...), db: Session = Depends(get_db)):
    """Test endpoint: crea un Signal sintetico e lo manda al pipeline place_orders.
    Richiede PIN e auto-trade attivo. Usato per verificare symbol map e specs broker."""
    _verify_pin(pin)
    if not mt5_trader._auto_trade_enabled:
        raise HTTPException(status_code=400, detail="Abilita prima auto-trade dalla Dashboard")
    direction = body.direction.lower()
    if direction not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="direction deve essere 'buy' o 'sell'")
    entry_low = body.entry_low
    entry_high = body.entry_high
    stoploss = body.stoploss
    tp1, tp2, tp3 = body.tp1, body.tp2, body.tp3
    if body.auto:
        # Risolvi prezzo corrente e specs dal broker attivo
        bsym = mt5_trader.get_mt5_symbol(body.symbol)
        mt5 = mt5_trader._get_mt5()
        if mt5 is None:
            raise HTTPException(status_code=503, detail="MT5 non disponibile")
        info = mt5.symbol_info(bsym)
        if info is None:
            raise HTTPException(status_code=400, detail=f"Simbolo {body.symbol} ({bsym}) non trovato sul broker")
        if not info.visible:
            mt5.symbol_select(bsym, True)
            import time as _t
            for _ in range(10):  # attendi fino a 1s che il tick si propaghi
                _t.sleep(0.1)
                info = mt5.symbol_info(bsym)
                if info and info.visible:
                    tk = mt5.symbol_info_tick(bsym)
                    if tk and tk.ask:
                        break
        tick = mt5.symbol_info_tick(bsym)
        if tick is None or not tick.ask:
            raise HTTPException(status_code=400, detail=f"Nessun tick per {bsym} dopo symbol_select")
        ref = tick.ask if direction == "buy" else tick.bid
        sl_pts = max(int(info.trade_stops_level * body.sl_points_factor), 50)
        tp_pts = max(int(info.trade_stops_level * body.tp_points_factor), 50)
        d = info.digits
        if direction == "buy":
            entry_low = round(ref, d)
            entry_high = entry_low
            stoploss = round(ref - sl_pts * info.point, d)
            tp1 = round(ref + tp_pts * info.point, d)
            tp2 = round(ref + 2 * tp_pts * info.point, d)
            tp3 = round(ref + 3 * tp_pts * info.point, d)
        else:
            entry_low = round(ref, d)
            entry_high = entry_low
            stoploss = round(ref + sl_pts * info.point, d)
            tp1 = round(ref - tp_pts * info.point, d)
            tp2 = round(ref - 2 * tp_pts * info.point, d)
            tp3 = round(ref - 3 * tp_pts * info.point, d)
    if entry_low is None or stoploss is None:
        raise HTTPException(status_code=400, detail="entry_low e stoploss richiesti (o usa auto=true)")
    if not (tp1 or tp2 or tp3):
        raise HTTPException(status_code=400, detail="Almeno un TP richiesto")
    sig = Signal(
        symbol=body.symbol.upper(),
        direction=direction,
        entry_price=entry_low,
        entry_price_high=entry_high or entry_low,
        stoploss=stoploss,
        tp1=tp1, tp2=tp2, tp3=tp3,
        status="pending",
        is_risky=body.is_risky,
        raw_message=f"[TEST] {body.symbol} {direction} entry={body.entry_low}-{body.entry_high} SL={body.stoploss}",
        created_at=datetime.utcnow(),
    )
    db.add(sig); db.commit(); db.refresh(sig)
    sig_id = sig.id
    # Esegui place_orders su una session FRESCA nel worker thread (evita
    # ORM cross-thread expired attributes che causavano return [] silenzioso).
    def _run_place():
        from database import SessionLocal as _SL, Signal as _Sig
        s = _SL()
        try:
            ss = s.query(_Sig).get(sig_id)
            if not ss:
                return []
            try:
                tks = mt5_trader.place_orders(ss) or []
                # IMPORTANTE: place_orders modifica ss in-memory (trade_log, broker,
                # mt5_account, position_size). Caller deve committare.
                s.commit()
                return tks
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                print(f"[test/place-order] place_orders EXCEPTION: {tb}", flush=True)
                try: s.rollback()
                except: pass
                return {"_error": str(e), "_tb": tb[-800:]}
        finally:
            s.close()
    result = await asyncio.get_event_loop().run_in_executor(None, _run_place)
    if isinstance(result, dict) and "_error" in result:
        raise HTTPException(status_code=500, detail=f"place_orders crashed: {result['_error']}")
    tickets = result
    db.refresh(sig)
    return {
        "ok": bool(tickets),
        "signal_id": sig.id,
        "tickets": tickets,
        "status": sig.status,
        "actual_entry_price": sig.actual_entry_price,
        "position_size": sig.position_size,
        "broker": sig.broker,
        "mt5_account": sig.mt5_account,
        "notes": sig.notes,
    }


@app.post("/api/signals/{signal_id}/retry-market")
async def signal_retry_market(signal_id: int, db: Session = Depends(get_db)):
    """Retry manuale a MARKET di un signal pending: cancella eventuali pending
    sul broker e ripiazza con force_market=True. Usato quando il LIMIT/STOP
    iniziale e' stato rifiutato (es. broker stops_level)."""
    sig = db.query(Signal).filter(Signal.id == signal_id).first()
    if not sig:
        raise HTTPException(status_code=404, detail="Segnale non trovato")
    if sig.status not in ("pending", "cancelled"):
        raise HTTPException(status_code=400, detail=f"Retry consentito solo su signal pending/cancelled, non '{sig.status}'")

    def _do():
        from database import SessionLocal as _SL, Signal as _Sig
        s = _SL()
        try:
            ss = s.query(_Sig).get(signal_id)
            if not ss:
                return {"ok": False, "error": "signal vanished"}
            # Cancella eventuali pending broker residui
            mt5 = mt5_trader._get_mt5()
            cancelled_orders = 0
            if mt5 and ss.mt5_tickets:
                try:
                    existing = json.loads(ss.mt5_tickets)
                    for t in existing:
                        orders = mt5.orders_get(ticket=t)
                        if orders:
                            mt5.order_send({"action": mt5.TRADE_ACTION_REMOVE, "order": t})
                            cancelled_orders += 1
                except Exception:
                    pass
            # Reset campi che place_orders ricompila
            ss.mt5_ticket = None
            ss.mt5_tickets = None
            ss.status = "pending"
            ss.notes = (ss.notes or "") + f" [Retry MARKET manuale, {cancelled_orders} pending precedenti rimossi]"
            mt5_trader._append_trade_log_mt5(ss, "manual_retry_market",
                f"Retry manuale a mercato richiesto dall'utente. Cancellati {cancelled_orders} pending precedenti.",
                {"cancelled_orders": cancelled_orders})
            s.commit()
            try:
                tickets = mt5_trader.place_orders(ss, force_market=True) or []
                if tickets:
                    ss.mt5_ticket = tickets[0]
                    ss.mt5_tickets = json.dumps(tickets)
                    ss.status = "open"
                    mt5_trader._append_trade_log_mt5(ss, "mt5_placed",
                        f"Retry MARKET ok: tickets={tickets}", {"tickets": tickets})
                else:
                    ss.status = "cancelled"
                    ss.notes = (ss.notes or "") + " [Retry MARKET fallito]"
                s.commit()
                return {"ok": bool(tickets), "tickets": tickets, "status": ss.status,
                        "broker": ss.broker, "notes": ss.notes}
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                print(f"[retry-market] EXCEPTION: {tb}", flush=True)
                s.rollback()
                return {"ok": False, "error": str(e)}
        finally:
            s.close()
    return await asyncio.get_event_loop().run_in_executor(None, _do)


@app.post("/api/signals/{signal_id}/cancel-manual")
async def signal_cancel_manual(signal_id: int, db: Session = Depends(get_db)):
    """Cancellazione manuale: rimuove pending broker e marca signal cancelled."""
    sig = db.query(Signal).filter(Signal.id == signal_id).first()
    if not sig:
        raise HTTPException(status_code=404, detail="Segnale non trovato")
    if sig.status not in ("pending",):
        raise HTTPException(status_code=400, detail=f"Cancel manuale consentito solo su pending, non '{sig.status}'")

    def _do():
        mt5 = mt5_trader._get_mt5()
        cancelled = 0
        if mt5 and sig.mt5_tickets:
            try:
                tickets = json.loads(sig.mt5_tickets)
                for t in tickets:
                    orders = mt5.orders_get(ticket=t)
                    if orders:
                        mt5.order_send({"action": mt5.TRADE_ACTION_REMOVE, "order": t})
                        cancelled += 1
            except Exception:
                pass
        return cancelled
    cancelled = await asyncio.get_event_loop().run_in_executor(None, _do)
    sig.status = "cancelled"
    sig.notes = (sig.notes or "") + f" [Annullato manualmente da utente, {cancelled} pending broker rimossi]"
    sig.closed_at = datetime.utcnow()
    sig.updated_at = datetime.utcnow()
    # Append trade_log via session corrente
    import json as _json
    try:
        log_list = _json.loads(sig.trade_log) if sig.trade_log else []
    except Exception:
        log_list = []
    log_list.append({
        "ts": datetime.utcnow().isoformat() + "Z",
        "event": "manual_cancel",
        "detail": f"Cancellato manualmente da utente. {cancelled} ordini pending broker rimossi.",
        "cancelled_orders": cancelled,
    })
    sig.trade_log = _json.dumps(log_list)
    db.commit()
    return {"ok": True, "cancelled_orders": cancelled, "status": sig.status}


@app.post("/api/test/raw-order")
async def test_raw_order(symbol: str = Query(...), pin: str = Query(...),
                          direction: str = Query("buy"),
                          sl_factor: float = Query(8.0)):
    """Test diretto mt5.order_send con vol_min e filling dinamico — bypassa
    place_orders/risk. Apre, riporta retcode/ticket, NON chiude."""
    _verify_pin(pin)
    def _do():
        import MetaTrader5 as mt5_mod
        mt5 = mt5_trader._get_mt5()
        if mt5 is None:
            return {"ok": False, "error": "MT5 unavailable"}
        bsym = mt5_trader.get_mt5_symbol(symbol)
        mt5.symbol_select(bsym, True)
        import time as _t
        _t.sleep(0.4)
        si = mt5.symbol_info(bsym)
        if si is None:
            return {"ok": False, "error": f"symbol_info None for {bsym}"}
        tk = mt5.symbol_info_tick(bsym)
        if tk is None or not tk.ask:
            return {"ok": False, "error": f"no tick for {bsym}"}
        sl_pts = max(int(si.trade_stops_level * sl_factor), 50)
        d = si.digits
        is_buy = direction.lower() == "buy"
        price = tk.ask if is_buy else tk.bid
        sl = round(price - sl_pts * si.point, d) if is_buy else round(price + sl_pts * si.point, d)
        tp = round(price + sl_pts * si.point, d) if is_buy else round(price - sl_pts * si.point, d)
        fill = mt5_trader._pick_filling_mode(mt5, bsym)
        req = {
            "action": mt5.TRADE_ACTION_DEAL, "symbol": bsym, "volume": si.volume_min,
            "type": mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL,
            "price": price, "sl": sl, "tp": tp, "deviation": 50,
            "magic": 99999998, "comment": f"TEST {symbol}"[:31],
            "type_time": mt5.ORDER_TIME_GTC, "type_filling": fill,
        }
        res = mt5.order_send(req)
        if res is None:
            return {"ok": False, "error": "order_send None", "last_error": str(mt5.last_error())}
        return {
            "ok": res.retcode == mt5.TRADE_RETCODE_DONE,
            "retcode": int(res.retcode), "comment": res.comment,
            "ticket": int(res.order), "deal": int(res.deal) if res.deal else None,
            "symbol_broker": bsym, "vol_min": si.volume_min, "filling_modes": si.filling_mode,
            "stops_level": si.trade_stops_level, "price": price, "sl": sl, "tp": tp,
        }
    return await asyncio.get_event_loop().run_in_executor(None, _do)


@app.post("/api/test/raw-modify")
async def test_raw_modify(ticket: int = Query(...), pin: str = Query(...),
                          symbol: str = Query(...), sl: float = Query(...)):
    _verify_pin(pin)
    def _do():
        mt5 = mt5_trader._get_mt5()
        if mt5 is None:
            return {"ok": False, "error": "MT5 unavailable"}
        bsym = mt5_trader.get_mt5_symbol(symbol)
        pos = mt5.positions_get(ticket=ticket)
        if not pos:
            return {"ok": False, "error": f"position {ticket} not found"}
        p = pos[0]
        req = {"action": mt5.TRADE_ACTION_SLTP, "position": ticket, "symbol": bsym,
               "sl": sl, "tp": p.tp, "magic": p.magic}
        res = mt5.order_send(req)
        return {"ok": res is not None and res.retcode == mt5.TRADE_RETCODE_DONE,
                "retcode": int(res.retcode) if res else None,
                "comment": res.comment if res else "?"}
    return await asyncio.get_event_loop().run_in_executor(None, _do)


@app.post("/api/test/raw-close")
async def test_raw_close(ticket: int = Query(...), pin: str = Query(...),
                          symbol: str = Query(...)):
    _verify_pin(pin)
    def _do():
        mt5 = mt5_trader._get_mt5()
        if mt5 is None:
            return {"ok": False, "error": "MT5 unavailable"}
        bsym = mt5_trader.get_mt5_symbol(symbol)
        pos = mt5.positions_get(ticket=ticket)
        if not pos:
            return {"ok": False, "error": f"position {ticket} not found"}
        p = pos[0]
        tk = mt5.symbol_info_tick(bsym)
        is_buy_pos = p.type == 0
        req = {"action": mt5.TRADE_ACTION_DEAL, "symbol": bsym, "volume": p.volume,
               "type": mt5.ORDER_TYPE_SELL if is_buy_pos else mt5.ORDER_TYPE_BUY,
               "price": tk.bid if is_buy_pos else tk.ask, "position": ticket,
               "deviation": 50, "magic": p.magic, "comment": "TEST CLOSE",
               "type_time": mt5.ORDER_TIME_GTC,
               "type_filling": mt5_trader._pick_filling_mode(mt5, bsym)}
        res = mt5.order_send(req)
        return {"ok": res is not None and res.retcode == mt5.TRADE_RETCODE_DONE,
                "retcode": int(res.retcode) if res else None,
                "comment": res.comment if res else "?"}
    return await asyncio.get_event_loop().run_in_executor(None, _do)


@app.post("/api/mt5/sync")
async def mt5_sync():
    updated = await asyncio.get_event_loop().run_in_executor(None, mt5_trader.sync_positions)
    return {"ok": True, "updated": updated}


@app.post("/api/mt5/backfill-position-size")
async def mt5_backfill_position_size():
    """One-shot: ricalcola position_size dai deal MT5 per tutti i segnali con ticket attivi."""
    updated = await asyncio.get_event_loop().run_in_executor(None, mt5_trader.backfill_position_size)
    return {"ok": True, "count": len(updated), "updated": updated}


@app.post("/api/mt5/backfill-trade-log")
async def mt5_backfill_trade_log(only_today: bool = True):
    """One-shot: ricostruisce gli eventi mancanti nel trade_log dai deal MT5
    (ticket_closed, be_applied, completed). Default: solo trade di oggi."""
    def _do():
        return mt5_trader.backfill_trade_log(only_today=only_today)
    updated = await asyncio.get_event_loop().run_in_executor(None, _do)
    return {"ok": True, "count": len(updated), "updated": updated}


class ModifyTicketIn(BaseModel):
    sl: Optional[float] = None
    tp: Optional[float] = None
    symbol: str = "XAUUSD"


def _signal_tp_hit_count(sig) -> int:
    """Numero di TP raggiunti (0..3). Lo status del signal resta 'open' finche'
    tutti i ticket non sono chiusi, quindi lo deriviamo dal trade_log contando
    gli eventi ticket_closed con reason=TP. Fallback al sig.status."""
    try:
        log_list = json.loads(sig.trade_log) if sig.trade_log else []
        tp_closed = [e for e in log_list if e.get("event") == "ticket_closed" and e.get("reason") == "TP"]
        if tp_closed:
            return min(3, len(tp_closed))
    except Exception:
        pass
    if sig.status == "tp3": return 3
    if sig.status == "tp2": return 2
    if sig.status == "tp1": return 1
    return 0


class TrailToggleIn(BaseModel):
    enabled: Optional[bool] = None  # None = follow global default


@app.post("/api/signals/{signal_id}/trail")
async def set_signal_trail(signal_id: int, body: TrailToggleIn, db: Session = Depends(get_db)):
    """Override per-trade del trail-stop. enabled=True/False sovrascrive il
    default globale; enabled=null/None ripristina il follow global."""
    sig = db.query(Signal).filter(Signal.id == signal_id).first()
    if not sig:
        raise HTTPException(status_code=404, detail="Segnale non trovato")
    sig.trail_stop_enabled = body.enabled
    sig.updated_at = datetime.utcnow()
    # Log evento nel trade_log
    try:
        log_list = json.loads(sig.trade_log) if sig.trade_log else []
        if body.enabled is None:
            detail = "Trail stop: ripristinato default globale"
        else:
            detail = f"Trail stop {'attivato' if body.enabled else 'disattivato'} per questo trade"
        log_list.append({
            "ts": datetime.utcnow().isoformat() + "Z",
            "event": "trail_toggle",
            "detail": detail,
            "enabled": body.enabled,
        })
        sig.trade_log = json.dumps(log_list)
    except Exception:
        pass
    db.add(sig)
    db.commit()
    return {"ok": True, "trail_stop_enabled": sig.trail_stop_enabled}


@app.post("/api/mt5/lock-profit/{signal_id}")
async def mt5_lock_profit(signal_id: int, db: Session = Depends(get_db)):
    """Lock profit progressivo in base ai TP raggiunti:
      - 0 o 1 TP raggiunti → SL = BE +/- 1 pip
      - 2 TP raggiunti, prezzo oltre TP1 nella direzione del trade → SL = TP1
      - 2 TP raggiunti, prezzo non ancora oltre TP1 → SL = BE +/- 1 pip (fallback)
      - 3 TP raggiunti → trade gia' chiuso, errore"""
    sig = db.query(Signal).filter(Signal.id == signal_id).first()
    if not sig:
        raise HTTPException(status_code=404, detail="Segnale non trovato")
    entry = sig.actual_entry_price or sig.entry_price
    if not entry:
        return {"ok": False, "error": "Entry non disponibile"}
    is_buy = (sig.direction or "buy").lower() == "buy"
    tp_hit = _signal_tp_hit_count(sig)

    if tp_hit >= 3:
        return {"ok": False, "error": "Trade gia' chiuso a TP3"}

    def _do():
        mt5 = mt5_trader._get_mt5()
        if not mt5:
            return {"ok": False, "error": "MT5 non disponibile"}
        mt5_sym = mt5_trader.MT5_SYMBOL_MAP.get(sig.symbol.upper(), sig.symbol)
        sym_info = mt5.symbol_info(mt5_sym)
        if not sym_info:
            return {"ok": False, "error": f"Symbol info non disponibile per {mt5_sym}"}
        pip_size = sym_info.point * 10

        # Determina target SL in base ai TP raggiunti.
        # be_plus1 = entry + 1 pip (BUY) o entry - 1 pip (SELL).
        be_plus1 = round(entry + pip_size, sym_info.digits) if is_buy else round(entry - pip_size, sym_info.digits)
        new_sl = be_plus1
        rule = "BE+1pip"

        if tp_hit == 2 and sig.tp1:
            # Per spostare SL a TP1 il prezzo deve essere oltre TP1 nella
            # direzione favorevole (altrimenti l'SL=TP1 sarebbe oltre il
            # prezzo corrente -> trigger immediato / Invalid sl).
            tick = mt5.symbol_info_tick(mt5_sym)
            if tick and tick.bid > 0 and tick.ask > 0:
                # BUY: bid (chiusura su BID) deve essere > TP1
                # SELL: ask (chiusura su ASK) deve essere < TP1
                close_price = tick.bid if is_buy else tick.ask
                past_tp1 = (is_buy and close_price > float(sig.tp1)) or (not is_buy and close_price < float(sig.tp1))
                if past_tp1:
                    new_sl = round(float(sig.tp1), sym_info.digits)
                    rule = "TP1"

        tickets_list = json.loads(sig.mt5_tickets) if sig.mt5_tickets else ([sig.mt5_ticket] if sig.mt5_ticket else [])
        if not tickets_list:
            return {"ok": False, "error": "Nessun ticket associato"}

        # Filtra solo i ticket ancora attivi su MT5
        active_tickets = []
        skipped_tickets = []
        for t in tickets_list:
            if mt5.positions_get(ticket=t) or mt5.orders_get(ticket=t):
                active_tickets.append(t)
            else:
                skipped_tickets.append(t)
        if not active_tickets:
            return {"ok": False, "error": "Tutti i ticket sono gia' chiusi", "skipped": skipped_tickets}

        results = []
        all_ok = True
        for t in active_tickets:
            ok = mt5_trader.modify_sl_tp(t, new_sl, None, sig.symbol)
            results.append({"ticket": t, "ok": ok})
            if not ok:
                all_ok = False
        return {"ok": all_ok, "new_sl": new_sl, "rule": rule, "tp_hit": tp_hit,
                "pip_size": pip_size, "results": results, "skipped": skipped_tickets}

    out = await asyncio.get_event_loop().run_in_executor(None, _do)
    # Log nel trade_log
    if out.get("ok"):
        try:
            log_list = json.loads(sig.trade_log) if sig.trade_log else []
            log_list.append({
                "ts": datetime.utcnow().isoformat() + "Z",
                "event": "lock_profit",
                "detail": f"Lock profit via UI ({out['rule']}): SL spostato a {out['new_sl']} "
                          f"su {len(out.get('results', []))} ticket (TP raggiunti: {out['tp_hit']})",
                "new_sl": out["new_sl"],
                "rule": out["rule"],
                "tp_hit": out["tp_hit"],
            })
            sig.trade_log = json.dumps(log_list)
            db.add(sig)
            db.commit()
        except Exception:
            pass
    return out


@app.post("/api/mt5/modify-ticket/{ticket}")
async def mt5_modify_ticket(ticket: int, body: ModifyTicketIn):
    """Modifica SL e/o TP di un ticket MT5 (posizione aperta o ordine pendente).
    Usa il retry interno di modify_sl_tp, gestisce errori transitori."""
    def _do():
        return mt5_trader.modify_sl_tp(ticket, body.sl, body.tp, body.symbol)
    ok = await asyncio.get_event_loop().run_in_executor(None, _do)
    return {"ok": ok, "ticket": ticket, "sl": body.sl, "tp": body.tp}

@app.post("/api/mt5/close/{ticket}")
async def mt5_close(ticket: int, db: Session = Depends(get_db)):
    sig = db.query(Signal).filter(Signal.mt5_ticket == ticket).first()
    symbol = sig.symbol if sig else "XAUUSD"
    ok = await asyncio.get_event_loop().run_in_executor(None, mt5_trader.close_position, ticket, symbol)
    return {"ok": ok}

@app.post("/api/mt5/close_signal/{signal_id}")
async def mt5_close_signal(signal_id: int, db: Session = Depends(get_db)):
    """Chiude tutte le posizioni MT5 aperte per un segnale."""
    sig = db.query(Signal).filter(Signal.id == signal_id).first()
    if not sig:
        raise HTTPException(status_code=404, detail="Segnale non trovato")
    tickets = []
    try:
        if sig.mt5_tickets:
            tickets = json.loads(sig.mt5_tickets)
        elif sig.mt5_ticket:
            tickets = [sig.mt5_ticket]
    except Exception:
        pass
    if not tickets:
        return {"ok": True, "results": [], "note": "Nessun ticket MT5 associato"}

    results = []
    already_closed = 0
    for t in tickets:
        ok = await asyncio.get_event_loop().run_in_executor(None, mt5_trader.close_position, t, sig.symbol)
        if ok:
            results.append({"ticket": t, "ok": True})
        else:
            # Verifica se era già chiuso (non è un errore reale)
            mt5 = mt5_trader._get_mt5()
            if mt5:
                still_open = mt5.positions_get(ticket=t) or mt5.orders_get(ticket=t)
                if not still_open:
                    already_closed += 1
                    results.append({"ticket": t, "ok": True, "note": "già chiuso"})
                else:
                    results.append({"ticket": t, "ok": False})
            else:
                results.append({"ticket": t, "ok": False})

    all_ok = all(r["ok"] for r in results)
    # Dopo la chiusura, calcola P&L reale da MT5 deal history
    if all_ok and sig.status in ("open", "pending", "tp1", "tp2"):
        mt5 = mt5_trader._get_mt5()
        if mt5:
            total_pnl = 0.0
            is_buy = sig.direction and sig.direction.lower() == "buy"
            best_tp = 0
            for t in tickets:
                deals = mt5.history_deals_get(position=t)
                if deals:
                    for d in deals:
                        if d.entry == mt5.DEAL_ENTRY_IN and not sig.actual_entry_price:
                            sig.actual_entry_price = d.price
                        if d.entry == mt5.DEAL_ENTRY_OUT:
                            total_pnl += d.profit
                            cp = d.price
                            for tp_num, tp_val in [(3, sig.tp3), (2, sig.tp2), (1, sig.tp1)]:
                                if tp_val and ((is_buy and cp >= tp_val) or (not is_buy and cp <= tp_val)):
                                    best_tp = max(best_tp, tp_num)
                                    break
            if total_pnl != 0:
                sig.pnl_usd = round(total_pnl, 2)
            # Se non abbiamo trovato deal ma c'è già un P&L, tienilo
            sig.status = f"tp{best_tp}" if best_tp > 0 else (sig.status if sig.status.startswith("tp") else "closed")
        else:
            if not sig.status.startswith("tp"):
                sig.status = "closed"
        sig.closed_at = datetime.utcnow()
        # Log evento "manual_close" nel trade_log
        try:
            import json as _jsonlib
            log_list = _jsonlib.loads(sig.trade_log) if sig.trade_log else []
            log_list.append({
                "ts": datetime.utcnow().isoformat() + "Z",
                "event": "manual_close",
                "detail": f"Chiusura manuale via UI: {len(tickets)} ticket processati ({already_closed} gia' chiusi). Status finale: {sig.status}",
                "tickets": tickets,
                "status": sig.status,
                "pnl": sig.pnl_usd,
            })
            sig.trade_log = _jsonlib.dumps(log_list)
        except Exception:
            pass
        db.add(sig)
        db.commit()
    return {"ok": all_ok, "results": results}

@app.post("/api/mt5/close_all")
async def mt5_close_all(db: Session = Depends(get_db)):
    """Chiude tutte le posizioni MT5 aperte."""
    mt5 = mt5_trader._get_mt5()
    if mt5 is None:
        return {"ok": False, "closed": 0}
    positions = mt5.positions_get()
    if not positions:
        return {"ok": True, "closed": 0}
    closed = 0
    for pos in positions:
        ok = await asyncio.get_event_loop().run_in_executor(None, mt5_trader.close_position, pos.ticket, pos.symbol)
        if ok:
            closed += 1
    return {"ok": True, "closed": closed}

@app.post("/api/mt5/place/{signal_id}")
async def mt5_place_order(signal_id: int, db: Session = Depends(get_db)):
    """Piazza manualmente ordini MT5 per un segnale specifico (multi-ticket).

    Calcola il ritardo rispetto al created_at del segnale e usa
    catch_origin='delayed' se > 60s, così il pre-check late-catch
    (_analyze_late_catch_ticks) viene effettivamente eseguito invece di
    saltare al MARKET cieco.
    """
    sig = db.query(Signal).filter(Signal.id == signal_id).first()
    if not sig:
        raise HTTPException(status_code=404, detail="Segnale non trovato")

    signal_ts = sig.created_at or datetime.utcnow()
    delay_sec = (datetime.utcnow() - signal_ts).total_seconds()
    if delay_sec > 60:
        catch_origin = "delayed"
        catch_reason = f"Piazzamento manuale via API, ritardo {int(delay_sec)}s dal segnale"
    else:
        catch_origin = "realtime"
        catch_reason = None

    def _place():
        return mt5_trader.place_orders(sig, catch_origin=catch_origin,
                                       catch_reason=catch_reason, signal_ts=signal_ts)
    tickets = await asyncio.get_event_loop().run_in_executor(None, _place)
    if tickets:
        sig.mt5_ticket = tickets[0]
        sig.mt5_tickets = json.dumps(tickets)
        sig.status = "open"
        db.add(sig)
        db.commit()
        return {"ok": True, "tickets": tickets, "catch_origin": catch_origin, "delay_sec": int(delay_sec)}
    # Late-catch potrebbe aver annullato il segnale: rileggi lo stato dal DB
    db.refresh(sig)
    return {"ok": False, "tickets": [], "catch_origin": catch_origin,
            "delay_sec": int(delay_sec), "status": sig.status, "notes": sig.notes}


# ─── Backup & Ripristino ─────────────────────────────────────────────────────

ARCHIVE_PIN = "241287"


def _verify_pin(pin: str):
    if pin != ARCHIVE_PIN:
        raise HTTPException(status_code=403, detail="PIN non valido")


def _signal_to_dict(sig: Signal) -> dict:
    """Serializza un Signal in dict per il restore point."""
    return {
        "id": sig.id,
        "telegram_msg_id": sig.telegram_msg_id,
        "symbol": sig.symbol,
        "direction": sig.direction,
        "entry_price": sig.entry_price,
        "entry_price_high": sig.entry_price_high,
        "tp1": sig.tp1, "tp2": sig.tp2, "tp3": sig.tp3,
        "stoploss": sig.stoploss,
        "status": sig.status,
        "actual_entry_price": sig.actual_entry_price,
        "entered_at": sig.entered_at.isoformat() + "Z" if sig.entered_at else None,
        "trade_log": sig.trade_log,
        "exit_price": sig.exit_price,
        "closed_at": sig.closed_at.isoformat() + "Z" if sig.closed_at else None,
        "risk_usd": sig.risk_usd,
        "position_size": sig.position_size,
        "pnl_usd": sig.pnl_usd,
        "raw_message": sig.raw_message,
        "created_at": sig.created_at.isoformat() + "Z" if sig.created_at else None,
        "updated_at": sig.updated_at.isoformat() + "Z" if sig.updated_at else None,
        "notes": sig.notes,
        "is_archived": sig.is_archived,
        "mt5_ticket": sig.mt5_ticket,
        "mt5_tickets": sig.mt5_tickets,
        "is_risky": sig.is_risky,
    }


@app.post("/api/backup/archive")
async def archive_trades(
    pin: str = Query(...),
    db: Session = Depends(get_db)
):
    """Archivia tutti i trade chiusi/completati e crea un punto di ripristino."""
    _verify_pin(pin)

    # Trova tutti i segnali chiusi e non ancora archiviati
    closed_statuses = ["tp1", "tp2", "tp3", "sl_hit", "closed", "cancelled", "cancelled_timing"]
    signals = db.query(Signal).filter(
        Signal.status.in_(closed_statuses),
        Signal.is_archived == False,
    ).all()

    if not signals:
        return {"ok": True, "archived": 0, "message": "Nessun trade da archiviare"}

    # Crea snapshot JSON dei segnali
    snapshot = [_signal_to_dict(s) for s in signals]

    # Calcola statistiche per la descrizione
    total_pnl = sum(s.pnl_usd or 0 for s in signals)
    symbols = list({s.symbol for s in signals})

    rp = RestorePoint(
        name=f"Archivio {datetime.utcnow().strftime('%d/%m/%Y %H:%M')}",
        description=f"{len(signals)} trade archiviati | P&L: {total_pnl:+.2f}$ | Simboli: {', '.join(sorted(symbols))}",
        signals_count=len(signals),
        signals_data=json.dumps(snapshot),
    )
    db.add(rp)

    # Marca come archiviati
    for s in signals:
        s.is_archived = True
        db.add(s)

    db.commit()
    db.refresh(rp)
    return {
        "ok": True,
        "archived": len(signals),
        "restore_point_id": rp.id,
        "description": rp.description,
    }


@app.get("/api/backup/restore-points")
async def list_restore_points(db: Session = Depends(get_db)):
    """Elenca tutti i punti di ripristino."""
    points = db.query(RestorePoint).order_by(RestorePoint.created_at.desc()).all()
    return [
        {
            "id": rp.id,
            "name": rp.name,
            "description": rp.description,
            "signals_count": rp.signals_count,
            "created_at": rp.created_at.isoformat() + "Z" if rp.created_at else None,
        }
        for rp in points
    ]


@app.post("/api/backup/restore/{restore_point_id}")
async def restore_trades(
    restore_point_id: int,
    pin: str = Query(...),
    db: Session = Depends(get_db)
):
    """Ripristina i trade da un punto di ripristino (de-archivia)."""
    _verify_pin(pin)

    rp = db.query(RestorePoint).filter(RestorePoint.id == restore_point_id).first()
    if not rp:
        raise HTTPException(status_code=404, detail="Punto di ripristino non trovato")

    snapshot = json.loads(rp.signals_data)
    restored = 0
    for sig_data in snapshot:
        sig = db.query(Signal).filter(Signal.id == sig_data["id"]).first()
        if sig and sig.is_archived:
            sig.is_archived = False
            db.add(sig)
            restored += 1

    db.commit()
    return {"ok": True, "restored": restored}


@app.delete("/api/backup/restore-points/{restore_point_id}")
async def delete_restore_point(
    restore_point_id: int,
    pin: str = Query(...),
    db: Session = Depends(get_db)
):
    """Elimina un punto di ripristino."""
    _verify_pin(pin)

    rp = db.query(RestorePoint).filter(RestorePoint.id == restore_point_id).first()
    if not rp:
        raise HTTPException(status_code=404, detail="Punto di ripristino non trovato")

    db.delete(rp)
    db.commit()
    return {"ok": True}


# ─── Restart endpoint ────────────────────────────────────────────────────────

@app.post("/api/restart")
async def restart_server():
    """Riavvia il backend (il runner run.py lo rilancia automaticamente)."""
    import os, signal
    safe_print("[Restart] Riavvio richiesto via API")
    # Schedula lo shutdown dopo 1 secondo per dare tempo alla response
    async def _shutdown():
        await asyncio.sleep(1)
        os.kill(os.getpid(), signal.SIGTERM)
    asyncio.create_task(_shutdown())
    return {"ok": True, "message": "Riavvio in corso..."}


# ─── Serve frontend build (produzione) ───────────────────────────────────────
import os as _os
_frontend_dist = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "frontend", "dist")
if _os.path.isdir(_frontend_dist):
    from fastapi.responses import FileResponse

    _frontend_dist = _os.path.abspath(_frontend_dist)
    _assets_dir = _os.path.join(_frontend_dist, "assets")
    safe_print(f"[Static] Serving frontend da {_frontend_dist}")

    # Serve assets statici
    if _os.path.isdir(_assets_dir):
        app.mount("/assets", StaticFiles(directory=_assets_dir), name="static-assets")

    # Catch-all: qualsiasi route non-API serve index.html (SPA)
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        if full_path.startswith("api/") or full_path.startswith("ws"):
            raise HTTPException(404)
        file_path = _os.path.join(_frontend_dist, full_path)
        if full_path and _os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(_os.path.join(_frontend_dist, "index.html"))
