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
                "entry_tolerance_pips": 3.0}
    return {
        "account_size": rs.account_size,
        "risk_per_trade_pct": rs.risk_per_trade_pct,
        "risk_per_trade_usd": rs.risk_per_trade_usd,
        "use_fixed_usd": rs.use_fixed_usd,
        "entry_tolerance_pips": getattr(rs, "entry_tolerance_pips", None) or 3.0,
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
            {"id": a.id, "login": a.login, "server": a.server, "label": a.label, "demo": a.is_demo, "is_default": a.is_default}
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
    db: Session = Depends(get_db),
):
    """Aggiunge un account MT5. Richiede PIN."""
    _verify_pin(pin)
    existing = db.query(Mt5Account).filter(Mt5Account.login == login).first()
    if existing:
        raise HTTPException(status_code=400, detail=f"Account {login} già presente")
    acc = Mt5Account(login=login, server=server, label=label, is_demo=is_demo)
    db.add(acc)
    db.commit()
    return {"ok": True, "login": login}


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
