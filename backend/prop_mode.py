"""Helper centrali per accesso ai parametri prop_mode dell'account attivo.

Tutte le guardie prop-specific nel codice dovrebbero chiamare questi helper
invece di leggere direttamente Mt5Account, in modo che la regola
"if account.prop_mode is None or False → comportamento invariato" sia
applicata in modo uniforme e visibile.

Pattern d'uso:
    settings = get_prop_settings()  # None se prop_mode off
    if settings and settings.daily_dd_limit_usd:
        # logica prop-specific
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class PropSettings:
    """Snapshot dei parametri prop dell'account attualmente attivo.

    Tutti i campi sono opzionali: se un campo e' None, la specifica guardia
    e' DISABILITATA. Questo permette di attivare le features prop una alla
    volta (es. solo daily DD all'inizio, poi peak equity, poi coerenza).
    """
    account_id: int
    login: int
    label: str
    daily_dd_limit_usd: Optional[float] = None
    daily_dd_warning_usd: Optional[float] = None
    peak_equity_usd: Optional[float] = None
    max_total_dd_usd: Optional[float] = None
    consistency_threshold_pct: Optional[float] = None
    max_concurrent_trades: Optional[int] = None


def get_prop_settings(db=None) -> Optional[PropSettings]:
    """Ritorna PropSettings dell'account attivo se prop_mode=True, altrimenti None.

    None = niente guardie prop attive. Avatrade demo (prop_mode=False) ritorna
    sempre None, quindi nessun codice gated da `if settings:` viene eseguito.
    """
    from database import SessionLocal, Mt5Account
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    try:
        acc = db.query(Mt5Account).filter(Mt5Account.is_active == True).first()
        if acc is None or not acc.prop_mode:
            return None
        return PropSettings(
            account_id=acc.id,
            login=acc.login,
            label=acc.label,
            daily_dd_limit_usd=acc.daily_dd_limit_usd,
            daily_dd_warning_usd=acc.daily_dd_warning_usd,
            peak_equity_usd=acc.peak_equity_usd,
            max_total_dd_usd=acc.max_total_dd_usd,
            consistency_threshold_pct=acc.consistency_threshold_pct,
            max_concurrent_trades=acc.max_concurrent_trades,
        )
    finally:
        if close_db:
            db.close()


def is_prop_mode(db=None) -> bool:
    """Helper veloce: True se account attivo ha prop_mode=True."""
    return get_prop_settings(db) is not None


def get_today_pnl_usd(db=None) -> float:
    """Somma P&L USD dei trade chiusi OGGI (giorno corrente Roma) SUL CONTO
    ATTIVO. Filtra per Signal.mt5_account = login dell'account prop attivo
    per non contaminare i numeri con lo storico degli altri conti.

    Usato sia dal daily DD kill-switch sia da dashboard/monitor. Ritorna 0.0
    se nessun trade chiuso oggi. Nessun side effect.
    """
    from database import SessionLocal, Signal
    from datetime import datetime
    settings = get_prop_settings(db)
    active_login = settings.login if settings else None
    try:
        from zoneinfo import ZoneInfo
        rome = ZoneInfo("Europe/Rome")
        utc = ZoneInfo("UTC")
    except Exception:
        rome = utc = None
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    try:
        if rome is None:
            today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            now_rome = datetime.now(rome)
            today_start_rome = now_rome.replace(hour=0, minute=0, second=0, microsecond=0)
            today_start = today_start_rome.astimezone(utc).replace(tzinfo=None)
        from sqlalchemy import or_
        q = db.query(Signal).filter(
            Signal.closed_at >= today_start,
            Signal.pnl_usd.isnot(None),
        )
        if active_login is not None:
            # Include trade dell'account attivo + retrocompat: trade senza mt5_account
            # settato (test/vecchi record) passano comunque.
            q = q.filter(Signal.mt5_account == active_login)
        return float(sum(s.pnl_usd for s in q.all()))
    finally:
        if close_db:
            db.close()


def should_block_new_trades(db=None) -> Optional[str]:
    """Daily DD kill-switch. Ritorna stringa motivazione del blocco se nuovi
    trade devono essere bloccati, altrimenti None.

    Gating:
    - prop_mode=False → None (Avatrade: comportamento invariato)
    - daily_dd_limit_usd=None → None
    - today P&L > -daily_dd_limit_usd → None (sotto soglia)
    - today P&L <= -daily_dd_limit_usd → stringa con dettaglio

    Le posizioni gia' aperte NON vengono toccate (gestite normalmente da
    trail/SL/TP). Solo i NUOVI place_orders vengono bloccati.
    """
    settings = get_prop_settings(db)
    if settings is None or settings.daily_dd_limit_usd is None:
        return None
    today = get_today_pnl_usd(db)
    # FLOATING P&L: FTMO conta il daily loss sull'EQUITY (chiusi + aperti).
    # Contare solo i chiusi sottostima il rischio reale: con -3000 chiusi e
    # -2000 floating siamo gia' a -5000 per FTMO. Somma il profit delle
    # posizioni aperte (0 se MT5 non disponibile).
    floating = 0.0
    try:
        import mt5_trader
        mt5 = mt5_trader._get_mt5()
        if mt5:
            positions = mt5.positions_get()
            if positions:
                floating = sum(float(p.profit) for p in positions)
    except Exception:
        pass
    effective = today + min(floating, 0.0)  # solo floating NEGATIVO aggrava il daily
    if effective <= -settings.daily_dd_limit_usd:
        return (f"Daily DD kill-switch: P&L oggi {today:+.2f}$ (chiusi) "
                f"{floating:+.2f}$ (floating) = {effective:+.2f}$ <= soglia "
                f"-{settings.daily_dd_limit_usd:.2f}$ (account '{settings.label}'). "
                f"Nuovi trade BLOCCATI fino a mezzanotte Roma.")
    return None


def coerenza_status(db=None) -> Optional[dict]:
    """Calcola il rapporto max-day / total-profit (regola coerenza 30% prop).
    Ritorna dict con metriche se in prop_mode, None altrimenti.

    Output (se prop_mode):
      max_day_pnl: P&L del giorno migliore
      max_day_date: data del giorno migliore
      total_pnl: P&L cumulato totale
      max_day_pct: rapporto (decimale 0-1)
      threshold_pct: soglia configurata (default 30)
      breach: True se max_day_pct supera threshold
      payout_safe_at: profitto target per scendere sotto threshold
    """
    settings = get_prop_settings(db)
    if settings is None:
        return None
    threshold_pct = settings.consistency_threshold_pct or 30.0
    from database import SessionLocal, Signal
    try:
        from zoneinfo import ZoneInfo
        rome = ZoneInfo("Europe/Rome")
        utc = ZoneInfo("UTC")
    except Exception:
        rome = utc = None
    active_login = settings.login
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    try:
        from sqlalchemy import or_
        q = db.query(Signal).filter(
            Signal.pnl_usd.isnot(None),
            Signal.closed_at.isnot(None),
        )
        if active_login is not None:
            q = q.filter(Signal.mt5_account == active_login)
        sigs = q.all()
        if not sigs:
            return {
                "max_day_pnl": 0.0, "max_day_date": None,
                "total_pnl": 0.0, "max_day_pct": 0.0,
                "threshold_pct": threshold_pct,
                "breach": False, "payout_safe_at": 0.0,
            }
        from collections import defaultdict
        by_day = defaultdict(float)
        for s in sigs:
            if rome is None:
                d = s.closed_at.date()
            else:
                ts = s.closed_at.replace(tzinfo=utc) if s.closed_at.tzinfo is None else s.closed_at
                d = ts.astimezone(rome).date()
            by_day[d] += s.pnl_usd
        total_pnl = sum(by_day.values())
        if not by_day:
            max_day_pnl, max_day_date = 0.0, None
        else:
            max_day_date, max_day_pnl = max(by_day.items(), key=lambda x: x[1])
        max_day_pct = (max_day_pnl / total_pnl) if total_pnl > 0 else 0.0
        # Profitto target per portare max_day sotto soglia
        payout_safe_at = (max_day_pnl / (threshold_pct / 100)) if threshold_pct > 0 else 0.0
        return {
            "max_day_pnl": round(max_day_pnl, 2),
            "max_day_date": str(max_day_date) if max_day_date else None,
            "total_pnl": round(total_pnl, 2),
            "max_day_pct": round(max_day_pct * 100, 2),
            "threshold_pct": threshold_pct,
            "breach": (max_day_pct * 100) > threshold_pct,
            "payout_safe_at": round(payout_safe_at, 2),
        }
    finally:
        if close_db:
            db.close()


def check_max_concurrent_trades(db=None) -> Optional[str]:
    """Ritorna stringa motivazione se posizioni aperte >= max_concurrent_trades,
    None altrimenti. Gated da prop_mode + max_concurrent_trades settato.
    """
    settings = get_prop_settings(db)
    if settings is None or settings.max_concurrent_trades is None:
        return None
    from database import SessionLocal, Signal
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    try:
        active = db.query(Signal).filter(
            Signal.status.in_(("open", "tp1", "tp2", "pending")),
            Signal.mt5_tickets.isnot(None),
            Signal.closed_at.is_(None),
        ).count()
        if active >= settings.max_concurrent_trades:
            return (f"Max concurrent trades raggiunto: {active} attivi >= "
                    f"limite {settings.max_concurrent_trades} (account '{settings.label}'). "
                    f"Nuovo signal BLOCCATO.")
        return None
    finally:
        if close_db:
            db.close()


def trailing_dd_status(current_equity: float, db=None) -> Optional[dict]:
    """Trailing DD (equita' inseguita): calcola distanza dal peak e ritorna
    info per UI/log se in prop_mode. None se prop_mode OFF.
    """
    settings = get_prop_settings(db)
    if settings is None or settings.max_total_dd_usd is None:
        return None
    peak = settings.peak_equity_usd or current_equity
    if current_equity > peak:
        peak = current_equity
    distance = peak - current_equity
    return {
        "peak": peak,
        "current": current_equity,
        "distance_from_peak": distance,
        "max_total_dd": settings.max_total_dd_usd,
        "breach": distance >= settings.max_total_dd_usd,
        "warning": distance >= (settings.max_total_dd_usd * 0.5),
        "remaining_buffer": max(0, settings.max_total_dd_usd - distance),
    }


def update_peak_equity(current_equity: float, db=None) -> Optional[float]:
    """Aggiorna peak_equity_usd dell'account attivo se in prop_mode e l'equity
    attuale supera il peak. Ritorna il nuovo peak (o quello esistente), None se
    non in prop_mode."""
    from database import SessionLocal, Mt5Account
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    try:
        acc = db.query(Mt5Account).filter(Mt5Account.is_active == True).first()
        if acc is None or not acc.prop_mode:
            return None
        if acc.peak_equity_usd is None or current_equity > acc.peak_equity_usd:
            acc.peak_equity_usd = current_equity
            db.add(acc)
            db.commit()
        return acc.peak_equity_usd
    finally:
        if close_db:
            db.close()
